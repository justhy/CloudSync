// Package manager 负责把「任务定义」转化为一次可观测的异步执行。
//
// 执行流程：
//
//	触发 -> 创建 Run(pending) -> 抢占并发许可 -> 调用 RC API(_async, 带 _group)
//	-> 轮询 job/status + core/stats?group=... -> 落库并广播进度 -> 终态收敛
//
// 取消与超时统一通过 context + job/stop 实现，保证 rclone 侧真的停止传输。
//
// 并发约定：store.Run 实例（activeRun.run）的读写一律在 m.mu 保护下进行，
// 对外只返回快照（值拷贝），避免把内部指针暴露给 HTTP 层造成数据竞争。
package manager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"cloudsync/internal/logging"
	"cloudsync/internal/rclone"
	"cloudsync/internal/store"
)

// 管理器对外暴露的错误。
var (
	// ErrTaskBusy 表示同一任务已有一轮正在运行。
	ErrTaskBusy = errors.New("该任务正在运行中")
	// ErrCapacity 表示全局并发已满。
	ErrCapacity = errors.New("并发任务数已达上限，请稍后重试")
	// ErrTaskDisabled 表示任务被禁用。
	ErrTaskDisabled = errors.New("任务已禁用")
	// ErrNotRunning 表示要取消的运行记录不在运行中。
	ErrNotRunning = errors.New("该运行记录不在运行中")
)

// EventType 是事件类型。
type EventType string

// 事件类型取值。
const (
	EventRunCreated  EventType = "run.created"
	EventRunUpdated  EventType = "run.updated"
	EventRunFinished EventType = "run.finished"
)

// Event 是推送给订阅者（SSE）的事件。
type Event struct {
	Type EventType  `json:"type"`
	Run  *store.Run `json:"run"`
	At   time.Time  `json:"at"`
}

// Options 是管理器参数。
type Options struct {
	MaxConcurrent int
	// DefaultTimeout 为 0 表示不限制单次任务时长。
	DefaultTimeout time.Duration
	// SkipOverlap 为 true 时同一任务不允许并发执行。
	SkipOverlap bool
	// PollInterval 是进度轮询间隔。
	PollInterval time.Duration
	// HistoryLimit 是单任务保留的运行记录条数上限。
	HistoryLimit int
	// JournalTailLines 是每次运行保留的 rclone 日志行数。
	JournalTailLines int
}

// RC 是管理器需要的 rclone RC 能力集合。
// 抽象为接口是为了让管理器可以脱离真实 rclone 进程进行测试。
type RC interface {
	StartAsync(ctx context.Context, method string, params map[string]any, group string) (int64, error)
	Stats(ctx context.Context, group string) (*rclone.Stats, error)
	JobStatus(ctx context.Context, jobID int64) (*rclone.JobStatus, error)
	StopJob(ctx context.Context, jobID int64) error
	StopGroup(ctx context.Context, group string) error
}

// Runner 是执行一次性 rclone 子命令的能力（lsjson / deletefile 等）。
// 抽象为接口是为了让管理器不必依赖真实进程。
type Runner interface {
	// Run 执行 rclone <args...>，把合并后的输出写入 out；out 为 nil 时丢弃。
	Run(ctx context.Context, out io.Writer, args ...string) error
	// Capture 执行 rclone <args...> 并只返回标准输出（用于解析 lsjson 的 JSON）。
	// stderr 不混入 stdout，否则 JSON 会被诊断信息污染。
	Capture(ctx context.Context, args ...string) ([]byte, error)
}

// preclearMaxDeletesPerPath 是单个路径最多执行的删除次数。
//
// 允许同名的后端上，同一个路径可能对应多个对象，必须删到"这个名字不存在为止"；
// 但如果后端行为异常（比如按名字删总删不掉），没有上限就会死循环。
const preclearMaxDeletesPerPath = 10

// preclearModifyWindow 是比对 modtime 时允许的误差。
//
// 远端（如 115）常把 modtime 截断到秒，源端却带纳秒精度；不留容差会把
// "内容其实一致"的文件误判成需要同步，进而白白重传一次。
const preclearModifyWindow = time.Second

// Manager 是任务执行器。
type Manager struct {
	store   store.Store
	rc      RC
	runner  Runner
	journal *logging.RingBuffer
	logger  *slog.Logger
	opts    Options

	// sem 是全局并发信号量。
	sem chan struct{}

	mu      sync.Mutex
	active  map[int64]*activeRun // runID -> 运行上下文
	byTask  map[int64]int64      // taskID -> runID
	subs    map[int]func(Event)
	nextSub int

	baseCtx   context.Context
	cancelAll context.CancelFunc
}

type activeRun struct {
	run   *store.Run
	task  *store.Task
	jobID int64
	group string
	// curGroup 是当前步骤的 rclone group（run-<id>-<step>）。
	//
	// group 必须带步骤号：core/stats?group= 返回的是整组的累计值，多步共用一组
	// 会让"当前这一步传了多少"读成跨步总和；取消时按组停止也会误伤其他步骤。
	curGroup string
	// mark 是运行开始时 rclone 日志缓冲的游标。
	mark   int64
	cancel context.CancelFunc
	// cancelRequested 表示终止由用户主动触发，用于区分 canceled 与 failed。
	cancelRequested bool
	// fromStep 是本次执行的起始步骤下标（用于"从失败步重跑"）。
	fromStep int
	// done 是本轮已完成的步骤数，用于计算整链进度。
	done int
	// acc 是已完成步骤的累计传输量。
	acc stepAccum
	// results 是各步骤的结果摘要。
	results []store.StepResult
}

// New 创建管理器。journal 用于切分单次运行的 rclone 日志片段，可为 nil。
// runner 可为 nil —— 此时任何需要执行前置清理的任务都会直接失败，
// 而不是悄悄跳过（跳过的后果是用户以为清理过了，实际目标仍有重名）。
func New(st store.Store, rc RC, runner Runner, journal *logging.RingBuffer, opts Options, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	if journal == nil {
		journal = logging.NewRingBuffer(100)
	}
	if opts.MaxConcurrent <= 0 {
		opts.MaxConcurrent = 1
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 2 * time.Second
	}
	if opts.JournalTailLines <= 0 {
		opts.JournalTailLines = 200
	}
	return &Manager{
		store:   st,
		rc:      rc,
		runner:  runner,
		journal: journal,
		logger:  logger.With("component", "manager"),
		opts:    opts,
		sem:     make(chan struct{}, opts.MaxConcurrent),
		active:  make(map[int64]*activeRun),
		byTask:  make(map[int64]int64),
		subs:    make(map[int]func(Event)),
	}
}

// SetBaseContext 设置所有运行共享的父 context（通常绑定进程生命周期）。
func (m *Manager) SetBaseContext(ctx context.Context) {
	m.baseCtx, m.cancelAll = context.WithCancel(ctx)
}

// Subscribe 注册事件订阅者，返回取消函数。
func (m *Manager) Subscribe(fn func(Event)) func() {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := m.nextSub
	m.nextSub++
	m.subs[id] = fn
	return func() {
		m.mu.Lock()
		delete(m.subs, id)
		m.mu.Unlock()
	}
}

func (m *Manager) publish(e Event) {
	m.mu.Lock()
	subs := make([]func(Event), 0, len(m.subs))
	for _, fn := range m.subs {
		subs = append(subs, fn)
	}
	m.mu.Unlock()

	for _, fn := range subs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					m.logger.Warn("事件订阅者 panic 已恢复", "recover", fmt.Sprint(r))
				}
			}()
			fn(e)
		}()
	}
}

// ActiveCount 返回当前运行中的任务数。
func (m *Manager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.active)
}

// ActiveRunIDs 返回全部运行中的 Run ID。
func (m *Manager) ActiveRunIDs() []int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]int64, 0, len(m.active))
	for id := range m.active {
		out = append(out, id)
	}
	return out
}

// RunningByTask 返回某任务正在运行的 Run 快照（无则 nil）。
func (m *Manager) RunningByTask(taskID int64) *store.Run {
	m.mu.Lock()
	defer m.mu.Unlock()
	runID, ok := m.byTask[taskID]
	if !ok {
		return nil
	}
	a, ok := m.active[runID]
	if !ok {
		return nil
	}
	snap := *a.run
	return &snap
}

// IsRunning 判断某条运行记录是否仍在执行。
//
// 以内存中的 active 表为准，而不是库里的 status 字段：库里的记录可能是
// 上次异常退出残留的，也可能刚好还没被终态回写，只有 active 表是权威的。
func (m *Manager) IsRunning(runID int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.active[runID]
	return ok
}

// LiveLogTail 返回一条进行中的运行的 rclone 输出片段。
//
// 库里那条 Run 的 LogTail 要到终态才回写，进行中永远是空的；详情页想要
// "边跑边看"就只能直接切 journal。运行已结束时返回 nil，调用方应回落到
// 库里的 LogTail。片段规则与 finish() 保持一致（滚动提示 + 行数上限）。
func (m *Manager) LiveLogTail(runID int64) []string {
	m.mu.Lock()
	a, ok := m.active[runID]
	m.mu.Unlock()
	if !ok {
		return nil
	}

	lines, overflow := m.journal.Since(a.mark)
	if overflow && len(lines) > 0 {
		lines = append([]string{"[提示] rclone 日志缓冲已滚动，以下为最近片段"}, lines...)
	}
	if len(lines) > m.opts.JournalTailLines {
		lines = lines[len(lines)-m.opts.JournalTailLines:]
	}
	return lines
}

// Trigger 触发一次任务执行（从第一个步骤开始）。
//
// 它会同步创建运行记录并在后台开始执行，因此调用方可以立即拿到 Run 快照用于展示。
func (m *Manager) Trigger(ctx context.Context, task *store.Task, trigger store.TriggerSource) (*store.Run, error) {
	return m.TriggerFrom(ctx, task, trigger, 0)
}

// TriggerFrom 从指定步骤开始执行任务。
//
// fromStep 服务于"从失败的那一步重跑"：前面的步骤刚跑过且成功，再跑一遍要重新
// 比对整棵树，代价与首次执行相同，没有必要。
func (m *Manager) TriggerFrom(ctx context.Context, task *store.Task, trigger store.TriggerSource, fromStep int) (*store.Run, error) {
	if task == nil {
		return nil, errors.New("任务不存在")
	}
	if !task.Enabled {
		return nil, ErrTaskDisabled
	}
	steps := task.Steps
	if len(steps) == 0 {
		return nil, errors.New("任务没有配置任何步骤")
	}
	if fromStep < 0 {
		fromStep = 0
	}
	if fromStep >= len(steps) {
		return nil, fmt.Errorf("起始步骤 %d 超出范围（共 %d 步）", fromStep+1, len(steps))
	}

	if m.opts.SkipOverlap || len(steps) > 1 {
		// 多步骤任务强制串行不重叠：链的时长=各步之和+间隔，很容易超过触发周期，
		// 允许重叠会让几轮链同时跑，互相抢带宽且让"最近一次结果"失去意义。
		if running := m.RunningByTask(task.ID); running != nil && !running.Status.Finished() {
			return running, fmt.Errorf("%w（运行 #%d 尚未结束）", ErrTaskBusy, running.ID)
		}
	}

	// 非阻塞抢占并发许可：满则直接告知调用方，避免请求堆积。
	select {
	case m.sem <- struct{}{}:
	default:
		return nil, ErrCapacity
	}

	run := &store.Run{
		TaskID:    task.ID,
		TaskName:  task.Name,
		Kind:      task.Kind,
		Trigger:   trigger,
		Status:    store.StatusPending,
		StartedAt: time.Now().UTC(),
		LogMark:   m.journal.Mark(),
		StepIndex: fromStep,
		StepTotal: len(steps),
	}

	createCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	err := m.store.CreateRun(createCtx, run)
	cancel()
	if err != nil {
		<-m.sem
		return nil, err
	}

	m.logger.Info("任务已触发",
		logging.Task(task.ID, task.Name),
		logging.Run(run.ID, string(run.Status)),
		"trigger", string(trigger),
		"steps", len(steps),
		"from_step", fromStep+1,
		"kind", string(steps[fromStep].Kind),
		"src", steps[fromStep].Source,
		"dst", steps[fromStep].Dest,
	)

	runCtx := context.Background()
	if m.baseCtx != nil {
		runCtx = m.baseCtx
	}
	runCtx, cancelRun := context.WithCancel(runCtx)

	a := &activeRun{
		run:      run,
		task:     task,
		group:    fmt.Sprintf("run-%d", run.ID),
		curGroup: fmt.Sprintf("run-%d-%d", run.ID, fromStep),
		mark:     run.LogMark,
		cancel:   cancelRun,
		fromStep: fromStep,
	}
	m.mu.Lock()
	m.active[run.ID] = a
	m.byTask[task.ID] = run.ID
	snap := *run
	m.mu.Unlock()

	m.publish(Event{Type: EventRunCreated, Run: &snap, At: time.Now().UTC()})

	go m.execute(runCtx, a)
	return &snap, nil
}

// Cancel 取消一次运行。
func (m *Manager) Cancel(runID int64) error {
	m.mu.Lock()
	a, ok := m.active[runID]
	if !ok {
		m.mu.Unlock()
		return ErrNotRunning
	}
	a.cancelRequested = true
	a.cancel()
	m.mu.Unlock()

	m.logger.Info("收到取消请求", logging.Run(runID, "running"), logging.Job(a.jobID))

	// 双保险：context 取消 + 显式停止 rclone job。
	// job id 可能尚未回填（触发与 StartAsync 之间被取消），因此按组停止始终执行。
	m.stopCurrent(a)
	return nil
}

// stopCurrent 停止当前步骤的 rclone job（先按 job id，再按组兜底）。
func (m *Manager) stopCurrent(a *activeRun) {
	m.mu.Lock()
	jobID := a.jobID
	group := a.curGroup
	if group == "" {
		group = a.group
	}
	m.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if jobID > 0 {
		if err := m.rc.StopJob(ctx, jobID); err != nil {
			m.logger.Warn("停止 rclone job 失败", logging.Run(a.run.ID, "running"), logging.Job(jobID), logging.Err(err))
		}
	}
	if err := m.rc.StopGroup(ctx, group); err != nil {
		m.logger.Warn("按组停止 rclone job 失败", "group", group, logging.Err(err))
	}
}

// CancelAll 取消全部运行中的任务（用于进程退出）。
func (m *Manager) CancelAll() {
	for _, id := range m.ActiveRunIDs() {
		if err := m.Cancel(id); err != nil && !errors.Is(err, ErrNotRunning) {
			m.logger.Warn("取消任务失败", "run_id", id, logging.Err(err))
		}
	}
	if m.cancelAll != nil {
		m.cancelAll()
	}
}

// Wait 等待所有运行结束，返回是否在超时内全部结束。
func (m *Manager) Wait(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if m.ActiveCount() == 0 {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return m.ActiveCount() == 0
}

// ---------------------------------------------------------------------------
// 执行主体
// ---------------------------------------------------------------------------

// updateRun 在锁内修改运行记录并返回快照。
func (m *Manager) updateRun(run *store.Run, fn func()) *store.Run {
	m.mu.Lock()
	if fn != nil {
		fn()
	}
	snap := *run
	m.mu.Unlock()
	return &snap
}

func (m *Manager) execute(parent context.Context, a *activeRun) {
	run := a.run
	task := a.task

	defer func() {
		<-m.sem

		m.mu.Lock()
		delete(m.active, run.ID)
		if m.byTask[task.ID] == run.ID {
			delete(m.byTask, task.ID)
		}
		final := *run
		m.mu.Unlock()

		m.publish(Event{Type: EventRunFinished, Run: &final, At: time.Now().UTC()})

		// 回写任务运行态（下一次定时触发的 next_run_at 由调度器维护）。
		rtCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		runID := run.ID
		started := run.StartedAt
		rt := store.TaskRuntime{LastRunAt: &started, LastRunID: &runID, LastStatus: string(final.Status)}
		if err := m.store.UpdateTaskRuntime(rtCtx, task.ID, rt); err != nil {
			m.logger.Warn("回写任务运行态失败", logging.Task(task.ID, task.Name), logging.Err(err))
		}
		if _, err := m.store.PruneRuns(rtCtx, m.opts.HistoryLimit); err != nil {
			m.logger.Warn("清理历史运行记录失败", logging.Err(err))
		}

		m.logger.Info("运行结束",
			logging.Task(task.ID, task.Name),
			logging.Run(run.ID, string(final.Status)),
			logging.Job(a.jobID),
			"duration", time.Duration(final.DurationMS)*time.Millisecond,
			"bytes", rclone.FormatBytes(final.Bytes),
			"files", final.Files,
			"errors", final.Errors,
			logging.Err(errorFromString(final.Error)),
		)
	}()

	// 按步骤串行执行。整条链共用一个运行记录，终态由所有步骤共同决定。
	steps := task.Steps
	total := len(steps)
	if total == 0 {
		m.finish(run, a, store.StatusFailed, "任务没有配置任何步骤", nil, nil)
		return
	}
	from := a.fromStep

	m.save(m.updateRun(run, func() {
		run.Status = store.StatusRunning
		run.StepIndex = from
		run.StepTotal = total
		run.Message = "正在启动 rclone 任务"
	}))

	var failed []string
	var firstErr string
	aborted := false
	done := 0
	// 本轮实际要执行的步骤数：从中间重跑时小于任务的总步骤数，
	// 文案里的分母必须用它，否则"从第 3 步重跑且失败"会被说成"2/3 完成"。
	attempted := total - from

	for i := from; i < total; i++ {
		step := steps[i]

		// 间隔：上一步结束后等待。第一步之前不等待，最后一步之后也不等待——
		// 循环体只在"还有下一步"时才读 DelayAfter。
		if i > from {
			if prev := steps[i-1]; prev.DelayAfter > 0 {
				delay := time.Duration(prev.DelayAfter) * time.Second
				m.save(m.updateRun(run, func() {
					run.Message = fmt.Sprintf("等待 %s 后执行步骤 %d/%d", delay, i+1, total)
				}))
				fmt.Fprintf(m.journal, "[步骤间隔] 等待 %s 后执行步骤 %d/%d\n", delay, i+1, total)
				if !sleepCtx(parent, delay) {
					m.finishInterrupted(parent, a)
					return
				}
			}
		}

		out := m.runStep(parent, a, i)

		m.mu.Lock()
		a.results = append(a.results, out.result)
		m.mu.Unlock()
		m.save(m.updateRun(run, func() { run.StepResults = a.results }))

		if out.err != nil {
			failed = append(failed, stepLabel(step, i))
			if firstErr == "" {
				firstErr = out.err.Error()
			}
			// 失败策略是逐步配置的：默认 continue（一个目录挂了不该拖累其他目录），
			// 有依赖的步骤单独设 abort。
			if step.OnError == store.OnErrorAbort {
				aborted = true
				// 中止时把未执行的步骤显式标成 skipped，否则日志里看不出是"没跑"
				// 还是"跑了没记录"。
				for j := i + 1; j < total; j++ {
					fmt.Fprintf(m.journal, "[步骤 %d/%d] 已跳过（前序步骤失败且策略为中止）\n", j+1, total)
				}
				break
			}
		} else {
			done++
		}
		// 整链被取消：不再判断失败策略，取消就是取消。
		if parent.Err() != nil {
			m.finishInterrupted(parent, a)
			return
		}
	}

	if firstErr != "" {
		m.updateRun(run, func() { run.Error = firstErr })
	}

	switch {
	case len(failed) == 0:
		if attempted > 1 {
			m.finish(run, a, store.StatusSuccess, fmt.Sprintf("%d 个步骤全部完成", attempted), nil, nil)
		} else {
			m.finish(run, a, store.StatusSuccess, "", nil, nil)
		}
	case aborted:
		m.finish(run, a, store.StatusFailed,
			fmt.Sprintf("步骤 %s 失败，已中止后续步骤", strings.Join(failed, "、")), nil, nil)
	default:
		m.finish(run, a, store.StatusFailed,
			fmt.Sprintf("%d/%d 个步骤完成，失败：%s", done, attempted, strings.Join(failed, "、")),
			nil, nil)
	}
}

// stepOutcome 是单个步骤的执行结果。
type stepOutcome struct {
	result store.StepResult
	err    error
}

// runStep 执行单个步骤：前置清理 → 提交 rclone job → 轮询到结束 → 判定成败。
//
// 它不负责收敛整条链的终态（那是 execute 的事），只回答"这一步成没成"。
func (m *Manager) runStep(parent context.Context, a *activeRun, idx int) stepOutcome {
	step := a.task.Steps[idx]
	total := len(a.task.Steps)
	start := time.Now()
	label := stepLabel(step, idx)

	m.save(m.updateRun(a.run, func() {
		a.run.Status = store.StatusRunning
		a.run.StepIndex = idx
		a.run.StepTotal = total
		a.run.Message = fmt.Sprintf("步骤 %d/%d：%s", idx+1, total, label)
	}))
	fmt.Fprintf(m.journal, "[步骤 %d/%d] %s %s %s -> %s\n",
		idx+1, total, label, step.Kind, step.Source, step.Dest)

	// 超时下沉到步骤：整条链的时长是各步之和加间隔，套一个"整链超时"会让
	// 步数一变行为就变，难以预估。
	// 超时为 0 表示不限 —— WithTimeout(0) 会得到一个立刻过期的 context。
	stepCtx := parent
	if d := m.timeoutForStep(a.task, step); d > 0 {
		var cancelTimeout context.CancelFunc
		stepCtx, cancelTimeout = context.WithTimeout(parent, d)
		defer cancelTimeout()
	}

	group := fmt.Sprintf("run-%d-%d", a.run.ID, idx)
	m.mu.Lock()
	a.curGroup = group
	m.mu.Unlock()

	if err := m.dedupeBefore(stepCtx, a.run, a.task, step); err != nil {
		return m.stepDone(a, idx, start, nil, err)
	}

	method, params, err := buildRequest(step)
	if err != nil {
		return m.stepDone(a, idx, start, nil, err)
	}

	m.logger.Debug("调用 rclone RC",
		logging.Task(a.task.ID, a.task.Name),
		logging.Run(a.run.ID, "running"),
		"step", idx+1,
		"method", method,
		"params", redact(params),
	)

	jobID, err := m.rc.StartAsync(stepCtx, method, params, group)
	if err != nil {
		// 父上下文已取消说明是用户取消/进程退出，交给 execute 统一收敛。
		if parent.Err() != nil {
			return stepOutcome{err: parent.Err()}
		}
		return m.stepDone(a, idx, start, nil, fmt.Errorf("提交 rclone 任务失败: %w", err))
	}

	m.mu.Lock()
	a.jobID = jobID
	m.mu.Unlock()
	m.save(m.updateRun(a.run, func() {
		a.run.JobID = jobID
		a.run.Status = store.StatusRunning
		a.run.Message = fmt.Sprintf("步骤 %d/%d：传输中", idx+1, total)
	}))

	m.logger.Info("rclone 任务已提交",
		logging.Task(a.task.ID, a.task.Name),
		logging.Run(a.run.ID, "running"),
		logging.Job(jobID),
		"group", group,
	)

	job, stats, pollErr := m.pollStep(stepCtx, a, idx)

	// 单步超时后 rclone 侧的 job 不会自己停 —— 不显式收掉，下一步开始时上一步还在跑。
	if errors.Is(stepCtx.Err(), context.DeadlineExceeded) {
		m.stopCurrent(a)
		return m.stepDone(a, idx, start, stats, errors.New("步骤执行超时，已强制停止"))
	}
	if parent.Err() != nil {
		return stepOutcome{err: parent.Err()}
	}
	if job == nil {
		msg := pollErr
		if msg == "" {
			msg = "查询 rclone 任务状态失败"
		}
		return m.stepDone(a, idx, start, stats, errors.New(msg))
	}

	// 判定口径与旧的 finalize 一致：job 成功 + 无传输错误 + 非致命。
	// 这里必须用 updateRun 取快照读，直接读 a.run 会与轮询协程构成数据竞争。
	snap := m.updateRun(a.run, nil)
	success := job.Success && snap.Errors == 0 && !snap.FatalError
	if !success {
		msg := job.Error
		if msg == "" {
			msg = snap.Error
		}
		if msg == "" && snap.Errors > 0 {
			msg = fmt.Sprintf("传输过程中出现 %d 个错误，请查看 rclone 日志", snap.Errors)
		}
		if msg == "" && snap.FatalError {
			msg = "rclone 报告致命错误"
		}
		if msg == "" {
			msg = "步骤失败，但 rclone 未返回详细原因"
		}
		if pollErr != "" {
			msg += "（状态查询异常: " + pollErr + "）"
		}
		return m.stepDone(a, idx, start, stats, errors.New(msg))
	}

	m.logger.Info("步骤完成",
		logging.Task(a.task.ID, a.task.Name),
		logging.Run(a.run.ID, "running"),
		logging.Job(jobID),
		"step", fmt.Sprintf("%d/%d", idx+1, total),
		"duration", time.Since(start),
	)
	return m.stepDone(a, idx, start, stats, nil)
}

// stepDone 收尾单个步骤：累计传输量、推进整链进度、生成结果摘要。
func (m *Manager) stepDone(a *activeRun, idx int, start time.Time, stats *rclone.Stats, err error) stepOutcome {
	step := a.task.Steps[idx]
	status := store.StatusSuccess
	if err != nil {
		status = store.StatusFailed
	}
	// 累计值在步骤结束后才并入：进行中的实时值由 applyStepStats 临时叠加展示，
	// 提前并入会重复计数。步骤自己的量要在并入前记下来，否则结果里会变成累计值。
	var stepBytes, stepFiles int64
	if stats != nil {
		stepBytes = stats.Bytes
		stepFiles = stats.Transfers
		a.acc.add(stats)
	}
	// 失败的步骤不推进完成数：整条链没走通，进度就不该到顶。
	var pct float64
	if err != nil {
		pct = m.stepPercentInChain(a, 0)
	} else {
		a.done++
		pct = m.chainPercent(a)
	}
	m.save(m.updateRun(a.run, func() {
		applyStepStats(a.run, a.acc, nil, nil, pct)
	}))

	res := store.StepResult{
		Position:   idx,
		Name:       step.Name,
		Kind:       step.Kind,
		Status:     status,
		DurationMS: time.Since(start).Milliseconds(),
		Bytes:      stepBytes,
		Files:      stepFiles,
	}
	if err != nil {
		res.Status = store.StatusFailed
		res.Error = err.Error()
		fmt.Fprintf(m.journal, "[步骤 %d/%d] 失败：%v\n", idx+1, len(a.task.Steps), err)
	}
	return stepOutcome{result: res, err: err}
}

// stepAccum 是已完成步骤的累计传输量。
//
// rclone 的 group 统计每步独立，不累加会让"这次运行一共传了多少"只剩下最后一步。
type stepAccum struct {
	Bytes, TotalBytes, Files, TotalFiles, Checks, Transfers     int64
	Errors, Renames, Deletes, ServerSideCopies, ServerSideMoves int64
	FatalError                                                  bool
}

func (acc *stepAccum) add(s *rclone.Stats) {
	if s == nil {
		return
	}
	acc.Bytes += s.Bytes
	acc.TotalBytes += s.TotalBytes
	acc.Files += s.Transfers
	acc.TotalFiles += s.TotalTransfers
	acc.Checks += s.Checks
	acc.Transfers += s.Transfers
	acc.Errors += s.Errors
	acc.Renames += s.Renames
	acc.Deletes += s.Deletes
	acc.ServerSideCopies += s.ServerSideCopies
	acc.ServerSideMoves += s.ServerSideMoves
	acc.FatalError = acc.FatalError || s.FatalError
}

// chainPercent 把整条链的进度折算成百分比：已完成步骤数 + 当前步骤的进度。
//
// 不能用字节比：各步总量在开始前不可知，中途还会随着新步骤开始而增长，
// 百分比会往回跳。分母是本轮实际要执行的步骤数（从 fromStep 重跑时更小）。
func (m *Manager) chainPercent(a *activeRun) float64 {
	total := len(a.task.Steps) - a.fromStep
	if total <= 0 {
		return 0
	}
	done := a.done
	if done > total {
		done = total
	}
	return float64(done) / float64(total) * 100
}

// stepLabel 生成步骤的可读标签，失败时用它指出"哪一步挂了"。
func stepLabel(step *store.TaskStep, idx int) string {
	if name := strings.TrimSpace(step.Name); name != "" {
		return name
	}
	return fmt.Sprintf("步骤 %d", idx+1)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// dedupeBefore 在传输开始前清理「这次确实要传输」的那些路径上的目标同名对象。
//
// 背景：部分网盘（如 115）允许同一目录下存在多个同名对象，而 rclone 对这种情况
// 的处理是挑其中一份参与比较，其余打一条 NOTICE 后忽略 —— 既不删除也不比较。
// 后果是每次 sync/copy/move 都可能再堆一份同名对象，重复越滚越多。
//
// 所以要做的不是"整目录去重"，而是精准清理：
//  1. 列出目标端全部对象，找出同名重复；
//  2. 对每个重名路径取源端对应文件，按 sync 的默认判据（size + modtime）比较；
//  3. 只有源端与**所有**目标副本都不同（即这次一定会发生传输）时，才把目标端
//     的同名对象全部删掉，让任务从源重写一份进去 —— 删完必然回填，不留空洞；
//  4. 目标端已有一份与源一致的副本时不动它，避免把本可跳过的传输变成强制重传。
//
// 这一步必须在 StartAsync 之前完成，且过程要写进 journal —— 运行详情里的
// log_tail 取自 journal.Since(a.mark)，不写进去的话这步对用户完全不可观测。
func (m *Manager) dedupeBefore(ctx context.Context, run *store.Run, task *store.Task, step *store.TaskStep) error {
	if !step.DedupeBefore || !step.Kind.AllowsDedupeBefore() {
		return nil
	}
	if step.Dest == "" {
		return fmt.Errorf("开启了「清理目标重名」但目标路径为空")
	}
	if step.Source == "" {
		return fmt.Errorf("开启了「清理目标重名」但源路径为空")
	}
	if m.runner == nil {
		return fmt.Errorf("未注入 rclone 命令执行器，无法执行「清理目标重名」")
	}

	m.save(m.updateRun(run, func() {
		run.Status = store.StatusRunning
		run.Message = "正在检查目标重名文件"
	}))

	m.logger.Info("扫描目标重名文件",
		logging.Task(task.ID, task.Name),
		logging.Run(run.ID, "running"),
		"dest", step.Dest,
	)

	dupes, err := listDuplicateNames(ctx, m.runner, step.Dest)
	if err != nil {
		return fmt.Errorf("「清理目标重名」扫描目标失败: %w", err)
	}
	if len(dupes) == 0 {
		fmt.Fprintf(m.journal, "[清理重名] %s 未发现同名对象，跳过清理\n", step.Dest)
		return nil
	}

	names := make([]string, 0, len(dupes))
	for name := range dupes {
		names = append(names, name)
	}
	sort.Strings(names)

	src := &sourceLister{runner: m.runner, root: step.Source}
	cleared, clearedObjects, skipped, failed := 0, 0, 0, 0

	for _, name := range names {
		item, ok, err := src.stat(ctx, name)
		if err != nil {
			failed++
			m.logger.Warn("读取源对象失败，跳过清理", "path", name, logging.Err(err))
			fmt.Fprintf(m.journal, "[清理重名] 读取源对象失败，跳过 %s: %v\n", name, err)
			continue
		}
		if !ok {
			// 源端没有对应文件：目标这份要么被 sync 删除，要么被过滤规则跳过，
			// 无论哪种都不是"要传输的路径"，不参与清理。
			fmt.Fprintf(m.journal, "[清理重名] 源端不存在 %s，跳过\n", name)
			continue
		}
		if matchesAny(item, dupes[name]) {
			skipped++
			fmt.Fprintf(m.journal, "[清理重名] %s 目标已存在与源一致的副本，跳过\n", name)
			continue
		}
		n := m.deleteSameName(ctx, joinRemote(step.Dest, name), len(dupes[name]))
		if n == 0 {
			failed++
			continue
		}
		cleared++
		clearedObjects += n
	}

	fmt.Fprintf(m.journal,
		"[清理重名] 目标重名路径 %d 个：已清理 %d 个（删除对象 %d 个），内容一致跳过 %d 个，失败 %d 个\n",
		len(names), cleared, clearedObjects, skipped, failed)

	m.logger.Info("同步前清理完成",
		logging.Task(task.ID, task.Name),
		logging.Run(run.ID, "running"),
		"dupes", len(names), "cleared", cleared, "objects", clearedObjects, "skipped", skipped, "failed", failed,
	)
	return nil
}

// deleteSameName 删除某个目标路径下的同名对象，返回实际删除个数。
//
// 允许同名的后端上一次 deletefile 只删掉其中一个副本，所以要按副本数重复调用；
// 提前返回错误说明已经删干净（object not found）或后端拒绝，都属于可接受的终止条件。
func (m *Manager) deleteSameName(ctx context.Context, full string, copies int) int {
	limit := copies
	if limit > preclearMaxDeletesPerPath {
		limit = preclearMaxDeletesPerPath
	}
	if limit < 1 {
		limit = 1
	}
	deleted := 0
	for i := 0; i < limit; i++ {
		if err := m.runner.Run(ctx, m.journal, "deletefile", full); err != nil {
			if i == 0 {
				m.logger.Warn("清理目标同名对象失败", "path", full, logging.Err(err))
				fmt.Fprintf(m.journal, "[清理重名] 删除 %s 失败: %v\n", full, err)
			}
			break
		}
		deleted++
	}
	if deleted > 0 {
		fmt.Fprintf(m.journal, "[清理重名] 已删除 %s 的同名对象 %d 个\n", full, deleted)
	}
	return deleted
}

// listDuplicateNames 列出目标路径下的同名重复对象，返回「相对路径 -> 副本列表」。
func listDuplicateNames(ctx context.Context, rn Runner, dest string) (map[string][]rclone.ListItem, error) {
	out, err := rn.Capture(ctx, "lsjson", "--recursive", dest)
	if err != nil {
		return nil, err
	}
	items, err := decodeListJSON(out)
	if err != nil {
		return nil, err
	}
	seen := make(map[string][]rclone.ListItem, len(items))
	for _, it := range items {
		if it.IsDir {
			continue
		}
		seen[it.Path] = append(seen[it.Path], it)
	}
	dupes := make(map[string][]rclone.ListItem)
	for p, list := range seen {
		if len(list) > 1 {
			dupes[p] = list
		}
	}
	return dupes, nil
}

func decodeListJSON(out []byte) ([]rclone.ListItem, error) {
	out = bytes.TrimSpace(out)
	if len(out) == 0 {
		return nil, nil
	}
	var items []rclone.ListItem
	if err := json.Unmarshal(out, &items); err != nil {
		return nil, fmt.Errorf("解析 lsjson 输出失败: %w", err)
	}
	return items, nil
}

// sourceLister 按需读取源端目录，并按目录缓存结果。
//
// 只对"目标存在重名"的那些路径发起查询，避免整棵源树都列一遍。
type sourceLister struct {
	runner Runner
	root   string
	dirs   map[string]map[string]rclone.ListItem // 目录 -> 文件名 -> 条目
	errs   map[string]error
}

// stat 返回源端 rel 路径对应的条目；ok 为 false 表示源端不存在该文件。
func (s *sourceLister) stat(ctx context.Context, rel string) (item rclone.ListItem, ok bool, err error) {
	dir := path.Dir(rel)
	if dir == "." || dir == "/" {
		dir = ""
	}
	if s.dirs == nil {
		s.dirs = make(map[string]map[string]rclone.ListItem)
		s.errs = make(map[string]error)
	}
	if e, bad := s.errs[dir]; bad {
		return rclone.ListItem{}, false, e
	}
	entries, cached := s.dirs[dir]
	if !cached {
		out, err := s.runner.Capture(ctx, "lsjson", joinRemote(s.root, dir))
		if err != nil {
			s.errs[dir] = err
			return rclone.ListItem{}, false, err
		}
		items, err := decodeListJSON(out)
		if err != nil {
			s.errs[dir] = err
			return rclone.ListItem{}, false, err
		}
		entries = make(map[string]rclone.ListItem, len(items))
		for _, it := range items {
			if it.IsDir {
				continue
			}
			entries[it.Name] = it
		}
		s.dirs[dir] = entries
	}
	it, found := entries[path.Base(rel)]
	return it, found, nil
}

// matchesAny 判断源条目与目标端的任一副本是否视为同一份。
func matchesAny(src rclone.ListItem, copies []rclone.ListItem) bool {
	for _, c := range copies {
		if sameContent(src, c) {
			return true
		}
	}
	return false
}

// sameContent 按 sync 的默认判据（size + modtime）判断两份是否一致。
func sameContent(a, b rclone.ListItem) bool {
	if a.Size != b.Size {
		return false
	}
	ta, okA := parseModTime(a.ModTime)
	tb, okB := parseModTime(b.ModTime)
	if !okA || !okB {
		// 拿不到 modtime 时退化为"视为不同"：宁可多清理一次触发重传，
		// 也不要因为误判成一致而放过一个真实的修改。
		return false
	}
	d := ta.Sub(tb)
	if d < 0 {
		d = -d
	}
	return d <= preclearModifyWindow
}

// parseModTime 解析 lsjson 输出的时间；rclone 可能给出空值或零值时间。
func parseModTime(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			if t.IsZero() {
				return time.Time{}, false
			}
			return t, true
		}
	}
	return time.Time{}, false
}

// joinRemote 把根路径与相对路径拼成 rclone 可接受的完整路径。
//
// 统一用 "/" 分隔：远端形如 "115Drive:音乐"，本地形如 "D:/data"，两种写法
// rclone 都能接受斜杠分隔，反过来用反斜杠会在远端上出错。
func joinRemote(root, rel string) string {
	root = strings.TrimRight(root, "/")
	rel = strings.TrimPrefix(rel, "/")
	if rel == "" {
		return root
	}
	return root + "/" + rel
}

func (m *Manager) timeoutForStep(task *store.Task, step *store.TaskStep) time.Duration {
	// 步骤超时 > 任务级超时（各步骤的默认超时） > 全局默认。
	if step.TimeoutSeconds > 0 {
		return time.Duration(step.TimeoutSeconds) * time.Second
	}
	if task.TimeoutSeconds > 0 {
		return time.Duration(task.TimeoutSeconds) * time.Second
	}
	return m.opts.DefaultTimeout
}

// buildRequest 把步骤定义翻译为 RC 方法名与参数。
func buildRequest(step *store.TaskStep) (string, map[string]any, error) {
	method, err := rclone.MethodForKind(string(step.Kind))
	if err != nil {
		return "", nil, err
	}
	srcParam, dstParam := rclone.ParamNamesForKind(string(step.Kind))

	params := make(map[string]any, len(step.ExtraFlags)+2)
	for k, v := range step.ExtraFlags {
		params[k] = v
	}
	params[srcParam] = step.Source
	if dstParam != srcParam && step.Dest != "" {
		params[dstParam] = step.Dest
	}
	return method, params, nil
}

func redact(params map[string]any) map[string]any {
	out := make(map[string]any, len(params))
	for k, v := range params {
		// 凭据类参数不写入日志。
		switch k {
		case "pass", "password", "_config":
			out[k] = "***"
		default:
			out[k] = v
		}
	}
	return out
}

// pollStep 轮询单个步骤的 job 直到它结束。
//
// 与旧的 poll 关键差别：不再在这里收敛终态 —— 一次运行有多个步骤，"成功还是失败"
// 只能由整条链决定。这里只负责把实时进度写进 run，并在结束时返回最终 job/stats。
func (m *Manager) pollStep(ctx context.Context, a *activeRun, idx int) (*rclone.JobStatus, *rclone.Stats, string) {
	run := a.run
	client := m.rc
	ticker := time.NewTicker(m.opts.PollInterval)
	defer ticker.Stop()

	var lastErr string
	for {
		select {
		case <-ctx.Done():
			return nil, nil, lastErr
		case <-ticker.C:
		}

		// 即使父 context 已取消，也要完成这一次状态查询以便拿到最终数据。
		queryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		stats, statsErr := client.Stats(queryCtx, a.curGroup)
		job, jobErr := client.JobStatus(queryCtx, a.jobID)
		cancel()

		if jobErr != nil {
			if ctx.Err() != nil {
				return nil, nil, lastErr
			}
			lastErr = jobErr.Error()
			m.logger.Warn("查询 job 状态失败",
				logging.Run(run.ID, "running"), logging.Job(a.jobID), "step", idx+1, logging.Err(jobErr))
			continue
		}
		if statsErr != nil {
			m.logger.Debug("查询传输统计失败", logging.Run(run.ID, "running"), logging.Err(statsErr))
		}

		cur := stepPercent(stats, job)
		snap := m.updateRun(run, func() {
			applyStepStats(run, a.acc, stats, job, m.stepPercentInChain(a, cur))
		})
		m.save(snap)

		if job.Finished {
			// 结束后再取一次统计，拿到最终字节数。
			statsCtx, cancel2 := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			if s, err := client.Stats(statsCtx, a.curGroup); err == nil {
				stats = s
			}
			cancel2()
			return job, stats, lastErr
		}
		if ctx.Err() != nil {
			return nil, nil, lastErr
		}
	}
}

// applyStepStats 把「已完成步骤的累计值 + 当前步骤的实时值」写进 run。
//
// 旧实现是整体覆盖（run.Bytes = stats.Bytes），多步下第二步会把第一步的数据抹掉，
// 所以这里改成 acc + 当前值。stats 为 nil 表示步骤已结束、累计值已并入 acc。
// 调用方需持有 m.mu。
func applyStepStats(run *store.Run, acc stepAccum, stats *rclone.Stats, job *rclone.JobStatus, percent float64) {
	var cur rclone.Stats
	if stats != nil {
		cur = *stats
	}
	run.Bytes = acc.Bytes + cur.Bytes
	run.TotalBytes = acc.TotalBytes + cur.TotalBytes
	run.Files = acc.Files + cur.Transfers
	run.TotalFiles = acc.TotalFiles + cur.TotalTransfers
	run.Checks = acc.Checks + cur.Checks
	run.Transfers = acc.Transfers + cur.Transfers
	run.Errors = acc.Errors + cur.Errors
	run.Renames = acc.Renames + cur.Renames
	run.Deletes = acc.Deletes + cur.Deletes
	run.ServerSideCopies = acc.ServerSideCopies + cur.ServerSideCopies
	run.ServerSideMoves = acc.ServerSideMoves + cur.ServerSideMoves
	run.FatalError = acc.FatalError || cur.FatalError
	run.Speed = cur.Speed
	if cur.ETA != nil {
		run.ETASeconds = int64(*cur.ETA)
	}
	if cur.LastError != nil && *cur.LastError != "" {
		run.Error = *cur.LastError
	}
	if job != nil && job.Error != "" {
		run.Error = job.Error
	}
	run.Percent = clampPercent(percent)
	run.DurationMS = time.Since(run.StartedAt).Milliseconds()
}

// stepPercent 计算单个步骤内部的百分比（按字节，退回按文件数）。
func stepPercent(stats *rclone.Stats, job *rclone.JobStatus) float64 {
	switch {
	case stats != nil && stats.TotalBytes > 0:
		return clampPercent(float64(stats.Bytes) / float64(stats.TotalBytes) * 100)
	case stats != nil && stats.TotalTransfers > 0:
		return clampPercent(float64(stats.Transfers) / float64(stats.TotalTransfers) * 100)
	case job != nil && job.Finished && job.Success:
		return 100
	}
	return 0
}

// stepPercentInChain 把"当前步骤的百分比"折算到整条链上：
// 已完成步骤占整数份，当前步骤占它自己那一份的一部分。
func (m *Manager) stepPercentInChain(a *activeRun, cur float64) float64 {
	total := len(a.task.Steps) - a.fromStep
	if total <= 0 {
		return 0
	}
	done := a.done
	if done < 0 {
		done = 0
	}
	return clampPercent((float64(done) + cur/100) / float64(total) * 100)
}

func clampPercent(p float64) float64 {
	if p < 0 {
		return 0
	}
	if p > 100 {
		return 100
	}
	return p
}

// finishInterrupted 处理被取消或整链中断的运行。
//
// 取消一律中止整条链：用户点了取消就是要停，不再看各步骤的 on_error。
func (m *Manager) finishInterrupted(ctx context.Context, a *activeRun) {
	m.mu.Lock()
	cancelled := a.cancelRequested
	m.mu.Unlock()

	status := store.StatusCanceled
	msg := "任务已取消"
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		status = store.StatusFailed
		msg = "任务执行超时，已强制停止"
	case !cancelled && errors.Is(ctx.Err(), context.Canceled):
		// 进程退出导致的取消。
		msg = "程序退出，任务已取消"
	}

	// 拉取最后一次统计，保留已传输进度。group 用当前步骤的，读不到就退回整链组。
	group := a.curGroup
	if group == "" {
		group = a.group
	}
	statsCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	var stats *rclone.Stats
	if s, err := m.rc.Stats(statsCtx, group); err == nil {
		stats = s
	}
	cancel()

	// 先把累计进度写进 run（和删除的 finalize 一样保留已传输量），再收敛终态。
	m.updateRun(a.run, func() {
		applyStepStats(a.run, a.acc, stats, nil, m.chainPercent(a))
	})
	m.finish(a.run, a, status, msg, nil, nil)
}

// finish 写入终态、附带 rclone 日志片段并落库。
func (m *Manager) finish(run *store.Run, a *activeRun, status store.RunStatus, message string, job *rclone.JobStatus, stats *rclone.Stats) {
	lines, overflow := m.journal.Since(a.mark)
	if overflow && len(lines) > 0 {
		lines = append([]string{"[提示] rclone 日志缓冲已滚动，以下为最近片段"}, lines...)
	}
	if len(lines) > m.opts.JournalTailLines {
		lines = lines[len(lines)-m.opts.JournalTailLines:]
	}

	now := time.Now().UTC()
	snap := m.updateRun(run, func() {
		if run.FinishedAt == nil {
			run.FinishedAt = &now
		}
		run.DurationMS = now.Sub(run.StartedAt).Milliseconds()
		run.Status = status
		run.Message = message
		switch {
		case job != nil && job.Error != "":
			run.Error = job.Error
		case status == store.StatusSuccess:
			run.Error = ""
		case run.Error == "" && message != "":
			run.Error = message
		}
		if status == store.StatusSuccess {
			// 多步骤链自带"3 个步骤全部完成"的文案，比泛泛的"同步完成"信息量大，保留。
			if message == "" {
				run.Message = "同步完成"
			}
			run.Percent = 100
		}
		run.LogTail = lines
	})
	m.save(snap)
}

// save 落库并广播。出错只记录日志，不影响传输本身。
func (m *Manager) save(snap *store.Run) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := m.store.UpdateRun(ctx, snap); err != nil {
		m.logger.Warn("写入运行记录失败", logging.Run(snap.ID, string(snap.Status)), logging.Err(err))
		return
	}
	m.publish(Event{Type: EventRunUpdated, Run: snap, At: time.Now().UTC()})
}

func errorFromString(s string) error {
	if s == "" {
		return nil
	}
	return errors.New(s)
}

// Progress 是给前端的进度视图。
type Progress struct {
	Percent    float64 `json:"percent"`
	Bytes      int64   `json:"bytes"`
	TotalBytes int64   `json:"total_bytes"`
	Files      int64   `json:"files"`
	TotalFiles int64   `json:"total_files"`
	Speed      float64 `json:"speed"`
	ETASeconds int64   `json:"eta_seconds"`
	Errors     int64   `json:"errors"`
}

// ProgressOf 从 Run 中提取进度视图。
func ProgressOf(r *store.Run) Progress {
	return Progress{
		Percent:    r.Percent,
		Bytes:      r.Bytes,
		TotalBytes: r.TotalBytes,
		Files:      r.Files,
		TotalFiles: r.TotalFiles,
		Speed:      r.Speed,
		ETASeconds: r.ETASeconds,
		Errors:     r.Errors,
	}
}
