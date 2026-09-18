// Package scheduler 基于 cron 表达式驱动任务定时执行。
//
// 每个启用的、配置了 cron 表达式的任务注册为独立条目；
// 任务的增删改通过 Sync/Remove 增量更新调度，无需重启进程。
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"

	"cloudsync/internal/config"
	"cloudsync/internal/logging"
	"cloudsync/internal/manager"
	"cloudsync/internal/store"
)

// Scheduler 是定时调度器。
type Scheduler struct {
	cfg    config.SchedulerConfig
	store  store.Store
	mgr    *manager.Manager
	logger *slog.Logger
	loc    *time.Location
	parser cron.Parser

	cron *cron.Cron

	mu      sync.Mutex
	entries map[int64]entryRef

	stopOnce  sync.Once
	stopped   chan struct{}
	refreshWg sync.WaitGroup
}

// entryRef 记录任务对应的 cron 条目。
type entryRef struct {
	id   cron.EntryID
	name string
}

// New 创建调度器。
func New(cfg config.SchedulerConfig, st store.Store, mgr *manager.Manager, logger *slog.Logger) (*Scheduler, error) {
	if logger == nil {
		logger = slog.Default()
	}
	loc := time.Local
	if cfg.Timezone != "" {
		l, err := time.LoadLocation(cfg.Timezone)
		if err != nil {
			return nil, fmt.Errorf("加载时区 %q: %w", cfg.Timezone, err)
		}
		loc = l
	}

	// 表达式语法档位由 scheduler.seconds 决定，二者严格互斥：
	//   5 段：分 时 日 月 周
	//   6 段：秒 分 时 日 月 周
	// 两者都支持 @every / @daily 等描述符。段数不匹配时报错而不是猜测意图，
	// 避免把 "0 0 3 * *" 静默解释成「每月 3 日 00:00」而非用户想要的「每天 03:00」。
	var parser cron.Parser
	if cfg.Seconds {
		parser = cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	} else {
		parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	}

	return &Scheduler{
		cfg:    cfg,
		store:  st,
		mgr:    mgr,
		logger: logger.With("component", "scheduler"),
		loc:    loc,
		parser: parser,
		cron: cron.New(
			cron.WithParser(parser),
			cron.WithLocation(loc),
			cron.WithChain(
				cron.Recover(cronLogger{logger: logger.With("component", "scheduler")}),
			),
		),
		entries: make(map[int64]entryRef),
		stopped: make(chan struct{}),
	}, nil
}

// parseExpr 解析 cron 表达式；段数与配置档位不符时给出可操作的提示。
func (s *Scheduler) parseExpr(expr string) (cron.Schedule, error) {
	spec, err := s.parser.Parse(expr)
	if err == nil {
		return spec, nil
	}
	if want, got := s.expectedFields(), len(strings.Fields(expr)); got != want {
		return nil, fmt.Errorf(
			"cron 表达式 %q 有 %d 段，当前配置需要 %d 段（%s）：%s",
			expr, got, want, s.segmentFormat(), s.switchHint(),
		)
	}
	return nil, fmt.Errorf("无效的 cron 表达式 %q: %w", expr, err)
}

// expectedFields 返回当前档位要求的字段数。
func (s *Scheduler) expectedFields() int {
	if s.cfg.Seconds {
		return 6
	}
	return 5
}

// segmentFormat 用中文描述当前档位的字段构成。
func (s *Scheduler) segmentFormat() string {
	if s.cfg.Seconds {
		return "秒 分 时 日 月 周"
	}
	return "分 时 日 月 周"
}

// switchHint 提示如何切换到另一种语法档位。
func (s *Scheduler) switchHint() string {
	if s.cfg.Seconds {
		return "如需 5 段语法，请设置 scheduler.seconds: false"
	}
	return "如需 6 段语法，请设置 scheduler.seconds: true"
}

// cronLogger 适配 robfig/cron 的日志接口到 slog。
type cronLogger struct{ logger *slog.Logger }

func (c cronLogger) Info(msg string, keysAndValues ...any) {
	c.logger.Info("cron: "+msg, keysAndValues...)
}

func (c cronLogger) Error(err error, msg string, keysAndValues ...any) {
	c.logger.Error("cron: "+msg, append([]any{logging.Err(err)}, keysAndValues...)...)
}

// Validate 校验 cron 表达式。
func (s *Scheduler) Validate(expr string) error {
	expr = trimSpace(expr)
	if expr == "" {
		return nil
	}
	spec, err := s.parseExpr(expr)
	if err != nil {
		return err
	}
	next := spec.Next(time.Now().In(s.loc))
	if next.IsZero() {
		return fmt.Errorf("cron 表达式 %q 永远不会触发", expr)
	}
	return nil
}

// DescribeNext 返回表达式在未来 3 次触发时间，供前端预览。
func (s *Scheduler) DescribeNext(expr string, n int) ([]time.Time, error) {
	expr = trimSpace(expr)
	if expr == "" {
		return nil, nil
	}
	spec, err := s.parseExpr(expr)
	if err != nil {
		return nil, err
	}
	out := make([]time.Time, 0, n)
	at := time.Now().In(s.loc)
	for i := 0; i < n; i++ {
		at = spec.Next(at)
		if at.IsZero() {
			break
		}
		out = append(out, at)
	}
	return out, nil
}

// Location 返回调度使用的时区。
func (s *Scheduler) Location() *time.Location { return s.loc }

// Start 加载全部任务并启动调度。
func (s *Scheduler) Start(ctx context.Context) error {
	if !s.cfg.Enabled {
		s.logger.Warn("定时调度已禁用（scheduler.enabled=false），cron 任务不会触发")
		return nil
	}

	if err := s.Reload(ctx); err != nil {
		return err
	}
	s.cron.Start()
	s.logger.Info("调度器已启动", "timezone", s.loc.String(), "seconds_precision", s.cfg.Seconds)

	s.refreshWg.Add(1)
	go s.refreshLoop()
	return nil
}

// Reload 全量重建调度条目。
func (s *Scheduler) Reload(ctx context.Context) error {
	tasks, err := s.store.ListTasks(ctx)
	if err != nil {
		return fmt.Errorf("加载任务列表: %w", err)
	}

	s.mu.Lock()
	existing := s.entries
	s.entries = make(map[int64]entryRef)
	s.mu.Unlock()

	for _, ref := range existing {
		s.cron.Remove(ref.id)
	}

	registered := 0
	for _, t := range tasks {
		if err := s.register(t); err != nil {
			s.logger.Error("注册定时任务失败",
				logging.Task(t.ID, t.Name), "cron", t.CronExpr, logging.Err(err))
			continue
		}
		if t.Enabled && trimSpace(t.CronExpr) != "" {
			registered++
		}
	}
	s.logger.Info("调度条目已刷新", "tasks", len(tasks), "scheduled", registered)
	return nil
}

// Sync 增量更新单个任务的调度条目。
func (s *Scheduler) Sync(ctx context.Context, task *store.Task) error {
	s.mu.Lock()
	if ref, ok := s.entries[task.ID]; ok {
		s.cron.Remove(ref.id)
		delete(s.entries, task.ID)
	}
	s.mu.Unlock()

	if err := s.register(task); err != nil {
		return err
	}
	s.syncNextRun(ctx, task.ID)
	return nil
}

// Remove 移除任务的调度条目。
func (s *Scheduler) Remove(ctx context.Context, taskID int64) {
	s.mu.Lock()
	ref, ok := s.entries[taskID]
	if ok {
		delete(s.entries, taskID)
	}
	s.mu.Unlock()
	if ok {
		s.cron.Remove(ref.id)
	}
	if err := s.store.ClearTaskNextRun(ctx, taskID); err != nil {
		s.logger.Warn("清空任务 next_run_at 失败", "task_id", taskID, logging.Err(err))
	}
}

// register 注册单个任务（内部方法，不处理已有条目）。
func (s *Scheduler) register(task *store.Task) error {
	if !s.cfg.Enabled {
		return nil
	}
	expr := trimSpace(task.CronExpr)
	if !task.Enabled || expr == "" {
		return nil
	}
	sched, err := s.parseExpr(expr)
	if err != nil {
		return err
	}

	taskID := task.ID
	taskName := task.Name
	// 用已解析的 Schedule 直接注册，绕开 cron 内部固定段数的解析器。
	entryID := s.cron.Schedule(sched, cron.FuncJob(func() { s.fire(taskID, taskName) }))

	s.mu.Lock()
	s.entries[taskID] = entryRef{id: entryID, name: taskName}
	s.mu.Unlock()

	s.logger.Info("已注册定时任务",
		logging.Task(taskID, taskName), "cron", expr, "next", s.nextOfEntry(entryID).Format(time.RFC3339))
	return nil
}

// fire 是 cron 触发入口。
func (s *Scheduler) fire(taskID int64, taskName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	task, err := s.store.GetTask(ctx, taskID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			s.logger.Warn("触发时任务已不存在，移除调度条目", "task_id", taskID, "task_name", taskName)
			s.Remove(ctx, taskID)
			return
		}
		s.logger.Error("加载任务失败，跳过本次触发", logging.Task(taskID, taskName), logging.Err(err))
		return
	}
	if !task.Enabled || trimSpace(task.CronExpr) == "" {
		s.logger.Info("任务已禁用或未配置 cron，跳过触发", logging.Task(task.ID, task.Name))
		s.Remove(ctx, task.ID)
		return
	}

	if s.cfg.SkipOverlap {
		if running := s.mgr.RunningByTask(task.ID); running != nil && !running.Status.Finished() {
			s.logger.Warn("上一轮尚未结束，跳过本次定时触发",
				logging.Task(task.ID, task.Name),
				"running_run_id", running.ID,
				"started_at", running.StartedAt.Format(time.RFC3339),
			)
			s.syncNextRun(ctx, task.ID)
			return
		}
	}

	run, err := s.mgr.Trigger(ctx, task, store.TriggerCron)
	switch {
	case err == nil:
		s.logger.Info("定时触发成功",
			logging.Task(task.ID, task.Name), logging.Run(run.ID, string(run.Status)))
	case errors.Is(err, manager.ErrTaskBusy), errors.Is(err, manager.ErrCapacity):
		s.logger.Warn("定时触发被跳过", logging.Task(task.ID, task.Name), logging.Err(err))
	case errors.Is(err, manager.ErrTaskDisabled):
		s.logger.Info("任务已禁用，忽略定时触发", logging.Task(task.ID, task.Name))
	default:
		s.logger.Error("定时触发失败", logging.Task(task.ID, task.Name), logging.Err(err))
	}
	s.syncNextRun(ctx, task.ID)
}

// Next 返回任务的下次触发时间（若未注册返回 nil）。
func (s *Scheduler) Next(taskID int64) *time.Time {
	s.mu.Lock()
	ref, ok := s.entries[taskID]
	s.mu.Unlock()
	if !ok {
		return nil
	}
	t := s.nextOfEntry(ref.id)
	if t.IsZero() {
		return nil
	}
	return &t
}

// nextOfEntry 返回条目的下次触发时间。
// cron 在 Start() 之前不会为条目计算 Next，此时退化为按 Schedule 直接推算，
// 保证「新增/修改任务后立即回写 next_run_at」也能拿到正确值。
func (s *Scheduler) nextOfEntry(entryID cron.EntryID) time.Time {
	entry := s.cron.Entry(entryID)
	if !entry.Next.IsZero() {
		return entry.Next
	}
	if entry.Schedule == nil {
		return time.Time{}
	}
	return entry.Schedule.Next(time.Now().In(s.loc))
}

// syncNextRun 把下次触发时间写回数据库。
func (s *Scheduler) syncNextRun(ctx context.Context, taskID int64) {
	next := s.Next(taskID)
	rt := store.TaskRuntime{NextRunAt: next}
	if next == nil {
		// TaskRuntime 使用 COALESCE，无法用该接口清空，这里直接调用 Clear。
		if err := s.store.ClearTaskNextRun(ctx, taskID); err != nil {
			s.logger.Debug("清空 next_run_at 失败", "task_id", taskID, logging.Err(err))
		}
		return
	}
	if err := s.store.UpdateTaskRuntime(ctx, taskID, rt); err != nil {
		s.logger.Debug("写入 next_run_at 失败", "task_id", taskID, logging.Err(err))
	}
}

// refreshLoop 定期刷新各任务的 next_run_at。
func (s *Scheduler) refreshLoop() {
	defer s.refreshWg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopped:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			s.mu.Lock()
			ids := make([]int64, 0, len(s.entries))
			for id := range s.entries {
				ids = append(ids, id)
			}
			s.mu.Unlock()
			for _, id := range ids {
				s.syncNextRun(ctx, id)
			}
			cancel()
		}
	}
}

// Stop 停止调度器，等待正在执行的触发逻辑结束。
func (s *Scheduler) Stop() {
	s.stopOnce.Do(func() { close(s.stopped) })
	ctx := s.cron.Stop()
	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Second):
		s.logger.Warn("等待调度器停止超时（任务执行由运行记录继续跟踪）")
	}
	s.refreshWg.Wait()
	s.logger.Info("调度器已停止")
}

// Stats 返回调度概览。
type Stats struct {
	Enabled  bool           `json:"enabled"`
	Timezone string         `json:"timezone"`
	Seconds  bool           `json:"seconds"`
	Entries  int            `json:"entries"`
	Next     *ScheduledItem `json:"next,omitempty"`
	Running  int            `json:"running"`
}

// ScheduledItem 是单个已注册条目的信息。
type ScheduledItem struct {
	TaskID   int64     `json:"task_id"`
	TaskName string    `json:"task_name,omitempty"`
	Next     time.Time `json:"next"`
}

// Stats 汇总调度器状态。
func (s *Scheduler) Stats() Stats {
	// 任务 ID 是 entries 的键，这里一并取出，否则前端拿不到 next 对应的任务。
	type item struct {
		taskID int64
		ref    entryRef
	}

	s.mu.Lock()
	items := make([]item, 0, len(s.entries))
	for id, ref := range s.entries {
		items = append(items, item{taskID: id, ref: ref})
	}
	s.mu.Unlock()

	out := Stats{
		Enabled:  s.cfg.Enabled,
		Timezone: s.loc.String(),
		Seconds:  s.cfg.Seconds,
		Entries:  len(items),
		Running:  s.mgr.ActiveCount(),
	}
	for _, it := range items {
		next := s.nextOfEntry(it.ref.id)
		if next.IsZero() {
			continue
		}
		if out.Next == nil || next.Before(out.Next.Next) {
			out.Next = &ScheduledItem{TaskID: it.taskID, TaskName: it.ref.name, Next: next}
		}
	}
	return out
}

func trimSpace(s string) string { return strings.TrimSpace(s) }
