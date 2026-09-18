package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"cloudsync/internal/logging"
	"cloudsync/internal/rclone"
	"cloudsync/internal/store"
)

// stubRC 是可编程的 rclone RC 替身，用于在无 rclone 进程的情况下验证执行流程。
type stubRC struct {
	mu sync.Mutex

	startCalls   []startCall
	stoppedJobs  []int64
	stoppedGroup []string

	// startFn 返回 job id；nil 时返回自增 id。
	startFn func(method string, params map[string]any, group string) (int64, error)
	// statsFn 返回统计；nil 时返回零值。
	statsFn func(group string) *rclone.Stats
	// jobFn 返回 job 状态；nil 时视为已完成且成功。
	jobFn func(jobID int64) (*rclone.JobStatus, error)
}

type startCall struct {
	method string
	params map[string]any
	group  string
}

func (s *stubRC) StartAsync(_ context.Context, method string, params map[string]any, group string) (int64, error) {
	s.mu.Lock()
	call := startCall{method: method, params: params, group: group}
	s.startCalls = append(s.startCalls, call)
	n := int64(len(s.startCalls))
	fn := s.startFn
	s.mu.Unlock()

	if fn != nil {
		return fn(method, params, group)
	}
	return n, nil
}

func (s *stubRC) Stats(_ context.Context, group string) (*rclone.Stats, error) {
	s.mu.Lock()
	fn := s.statsFn
	s.mu.Unlock()
	if fn == nil {
		return &rclone.Stats{Group: group}, nil
	}
	return fn(group), nil
}

func (s *stubRC) JobStatus(_ context.Context, jobID int64) (*rclone.JobStatus, error) {
	s.mu.Lock()
	fn := s.jobFn
	s.mu.Unlock()
	if fn == nil {
		return &rclone.JobStatus{ID: jobID, Finished: true, Success: true}, nil
	}
	return fn(jobID)
}

func (s *stubRC) StopJob(_ context.Context, jobID int64) error {
	s.mu.Lock()
	s.stoppedJobs = append(s.stoppedJobs, jobID)
	s.mu.Unlock()
	return nil
}

func (s *stubRC) StopGroup(_ context.Context, group string) error {
	s.mu.Lock()
	s.stoppedGroup = append(s.stoppedGroup, group)
	s.mu.Unlock()
	return nil
}

func (s *stubRC) firstCall(t *testing.T) startCall {
	t.Helper()
	calls := s.allCalls(t)
	return calls[0]
}

// allCalls 返回全部 StartAsync 调用（按发生顺序）。
func (s *stubRC) allCalls(t *testing.T) []startCall {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.startCalls) == 0 {
		t.Fatal("未调用 StartAsync")
	}
	out := make([]startCall, len(s.startCalls))
	copy(out, s.startCalls)
	return out
}

// ---------------------------------------------------------------------------

// fakeRunner 是 Runner 的测试替身：记录调用参数，避免测试里拉起真实 rclone 进程。
//
// 两条通道分开可编程：
//   - Run 用于写流式输出的子命令（deletefile），runErr 控制其成败；
//   - Capture 用于需要解析输出的子命令（lsjson），captureFn 按 args 返回内容。
type fakeRunner struct {
	mu    sync.Mutex
	calls [][]string

	// runErr 非 nil 时，Run 返回该错误，用于验证「清理失败不会误判为成功」。
	runErr error
	// captureFn 返回 Capture 的内容；nil 时返回空 JSON 数组。
	captureFn func(args []string) ([]byte, error)
}

func (f *fakeRunner) Run(_ context.Context, out io.Writer, args ...string) error {
	f.mu.Lock()
	f.calls = append(f.calls, append([]string(nil), args...))
	err := f.runErr
	f.mu.Unlock()
	if out != nil {
		_, _ = out.Write([]byte("fake runner output\n"))
	}
	return err
}

func (f *fakeRunner) Capture(_ context.Context, args ...string) ([]byte, error) {
	f.mu.Lock()
	f.calls = append(f.calls, append([]string(nil), args...))
	fn := f.captureFn
	f.mu.Unlock()
	if fn == nil {
		return []byte("[]"), nil
	}
	return fn(args)
}

func (f *fakeRunner) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// callsOf 返回以 cmd 开头的调用参数（不含 cmd 本身）。
func (f *fakeRunner) callsOf(cmd string) [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out [][]string
	for _, c := range f.calls {
		if len(c) > 0 && c[0] == cmd {
			out = append(out, append([]string(nil), c...))
		}
	}
	return out
}

func (f *fakeRunner) last() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return nil
	}
	return f.calls[len(f.calls)-1]
}

// listJSON 把条目序列化成 lsjson 的返回格式。
func listJSON(items ...rclone.ListItem) []byte {
	b, err := json.Marshal(items)
	if err != nil {
		panic(err)
	}
	return b
}

// item 构造一个 lsjson 条目。
func item(relPath string, size int64, modTime string) rclone.ListItem {
	return rclone.ListItem{
		Path:    relPath,
		Name:    path.Base(relPath),
		Size:    size,
		ModTime: modTime,
	}
}

func newTestManager(t *testing.T, rc RC, opts Options) (*Manager, *store.SQLiteStore) {
	return newTestManagerWithRunner(t, rc, &fakeRunner{}, opts)
}

// newTestManagerWithRunner 允许测试注入自己的 Runner（例如让它报错）。
func newTestManagerWithRunner(t *testing.T, rc RC, runner Runner, opts Options) (*Manager, *store.SQLiteStore) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "m.db"), logging.Discard())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if opts.PollInterval == 0 {
		opts.PollInterval = 20 * time.Millisecond
	}
	m := New(st, rc, runner, logging.NewRingBuffer(100), opts, logging.Discard())
	m.SetBaseContext(context.Background())
	return m, st
}

func createTask(t *testing.T, st store.Store, mutate func(*store.Task)) *store.Task {
	t.Helper()
	task := &store.Task{
		Name:    fmt.Sprintf("task-%d", time.Now().UnixNano()),
		Kind:    store.KindSync,
		Source:  "src:bucket",
		Dest:    "dst:bucket",
		Enabled: true,
	}
	if mutate != nil {
		mutate(task)
	}
	if err := st.CreateTask(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	return task
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, desc string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("等待超时: %s", desc)
}

func TestTriggerSuccess(t *testing.T) {
	rc := &stubRC{}
	rc.statsFn = func(group string) *rclone.Stats {
		return &rclone.Stats{
			Group: group, Bytes: 4096, TotalBytes: 4096,
			Transfers: 4, TotalTransfers: 4, Speed: 1024, Checks: 8, Renames: 1,
		}
	}
	m, st := newTestManager(t, rc, Options{MaxConcurrent: 2, SkipOverlap: true})
	task := createTask(t, st, func(task *store.Task) {
		task.ExtraFlags = map[string]any{"transfers": float64(8)}
	})

	// 记录事件
	var events []Event
	var evMu sync.Mutex
	unsub := m.Subscribe(func(e Event) {
		evMu.Lock()
		events = append(events, e)
		evMu.Unlock()
	})
	defer unsub()

	run, err := m.Trigger(context.Background(), task, store.TriggerManual)
	if err != nil {
		t.Fatalf("触发失败: %v", err)
	}
	if run.Status != store.StatusPending {
		t.Errorf("触发后状态应为 pending，得到 %q", run.Status)
	}

	waitFor(t, 3*time.Second, func() bool {
		got, err := st.GetRun(context.Background(), run.ID)
		return err == nil && got.Status.Finished()
	}, "运行结束")

	got, err := st.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != store.StatusSuccess {
		t.Fatalf("期望 success，得到 %q（message=%q error=%q）", got.Status, got.Message, got.Error)
	}
	if got.Bytes != 4096 || got.TotalBytes != 4096 || got.Percent != 100 {
		t.Errorf("进度统计错误: %+v", got)
	}
	if got.JobID == 0 || got.FinishedAt == nil || got.DurationMS < 0 {
		t.Errorf("终态字段缺失: job=%d finished=%v duration=%d", got.JobID, got.FinishedAt, got.DurationMS)
	}

	// 校验下发的 RC 参数
	call := rc.firstCall(t)
	if call.method != "sync/sync" {
		t.Errorf("方法名期望 sync/sync，得到 %s", call.method)
	}
	if call.params["srcFs"] != "src:bucket" || call.params["dstFs"] != "dst:bucket" {
		t.Errorf("源/目标参数错误: %+v", call.params)
	}
	if call.params["transfers"] != float64(8) {
		t.Errorf("extra_flags 未下发: %+v", call.params)
	}
	// 多步骤任务为每个步骤分配独立的 group（run-<runID>-<stepIndex>），
	// 否则 core/stats?group= 会把多个步骤的传输量混在一起。
	if !strings.HasPrefix(call.group, fmt.Sprintf("run-%d-", run.ID)) {
		t.Errorf("group 应以 run-%d- 开头，得到 %q", run.ID, call.group)
	}

	// 任务运行态回写
	waitFor(t, 2*time.Second, func() bool {
		t2, err := st.GetTask(context.Background(), task.ID)
		return err == nil && t2.LastStatus == string(store.StatusSuccess)
	}, "任务运行态回写")

	// 事件至少包含 created / updated / finished
	evMu.Lock()
	defer evMu.Unlock()
	var hasCreated, hasFinished bool
	for _, e := range events {
		if e.Type == EventRunCreated {
			hasCreated = true
		}
		if e.Type == EventRunFinished {
			hasFinished = true
		}
	}
	if !hasCreated || !hasFinished {
		t.Errorf("事件序列不完整: %+v", events)
	}
}

func TestTriggerFailureReportsRcloneError(t *testing.T) {
	rc := &stubRC{}
	rc.jobFn = func(jobID int64) (*rclone.JobStatus, error) {
		return &rclone.JobStatus{ID: jobID, Finished: true, Success: false,
			Error: "couldn't find section in config file"}, nil
	}
	rc.statsFn = func(group string) *rclone.Stats {
		return &rclone.Stats{Group: group, Errors: 2, FatalError: true, LastError: strPtr("boom")}
	}
	m, st := newTestManager(t, rc, Options{MaxConcurrent: 1})
	task := createTask(t, st, nil)

	run, err := m.Trigger(context.Background(), task, store.TriggerManual)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		got, err := st.GetRun(context.Background(), run.ID)
		return err == nil && got.Status.Finished()
	}, "运行结束")

	got, _ := st.GetRun(context.Background(), run.ID)
	if got.Status != store.StatusFailed {
		t.Fatalf("期望 failed，得到 %q", got.Status)
	}
	if got.Error == "" {
		t.Error("失败时应保留错误信息")
	}
	if got.Percent == 100 {
		t.Error("失败时不应把进度置为 100%")
	}
}

func TestStartAsyncErrorMarksRunFailed(t *testing.T) {
	rc := &stubRC{}
	rc.startFn = func(method string, params map[string]any, group string) (int64, error) {
		return 0, errors.New("failed to create file system for \"bad:\": didn't find section in config file")
	}
	m, st := newTestManager(t, rc, Options{MaxConcurrent: 1})
	task := createTask(t, st, nil)

	run, err := m.Trigger(context.Background(), task, store.TriggerManual)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		got, err := st.GetRun(context.Background(), run.ID)
		return err == nil && got.Status.Finished()
	}, "运行结束")

	got, _ := st.GetRun(context.Background(), run.ID)
	if got.Status != store.StatusFailed {
		t.Fatalf("期望 failed，得到 %q", got.Status)
	}
	if got.Message == "" || got.Error == "" {
		t.Errorf("应记录失败原因: message=%q error=%q", got.Message, got.Error)
	}
}

func TestCancelRunningTask(t *testing.T) {
	rc := &stubRC{}
	// job 永不结束，模拟长期运行的任务，由取消来终止。
	rc.jobFn = func(jobID int64) (*rclone.JobStatus, error) {
		return &rclone.JobStatus{ID: jobID, Finished: false}, nil
	}
	rc.statsFn = func(group string) *rclone.Stats {
		return &rclone.Stats{Group: group, Bytes: 512, TotalBytes: 4096, Speed: 100}
	}
	m, st := newTestManager(t, rc, Options{MaxConcurrent: 1})
	task := createTask(t, st, nil)

	run, err := m.Trigger(context.Background(), task, store.TriggerManual)
	if err != nil {
		t.Fatal(err)
	}
	// 等到 job id 回填才算"真的跑起来"：running 状态在提交 rclone job 之前就写了，
	// 只等状态会偶发地在 job 尚未提交时就取消，断言 stoppedJobs 就会飘。
	waitFor(t, 2*time.Second, func() bool {
		got, err := st.GetRun(context.Background(), run.ID)
		return err == nil && got.JobID > 0 && got.Status == store.StatusRunning
	}, "进入 running")

	if err := m.Cancel(run.ID); err != nil {
		t.Fatalf("取消失败: %v", err)
	}

	waitFor(t, 3*time.Second, func() bool {
		got, err := st.GetRun(context.Background(), run.ID)
		return err == nil && got.Status.Finished()
	}, "取消后收敛")

	got, _ := st.GetRun(context.Background(), run.ID)
	if got.Status != store.StatusCanceled {
		t.Fatalf("期望 canceled，得到 %q（%s）", got.Status, got.Message)
	}
	if got.Bytes != 512 {
		t.Errorf("取消时应保留已传输进度，得到 %d", got.Bytes)
	}

	rc.mu.Lock()
	defer rc.mu.Unlock()
	if len(rc.stoppedJobs) == 0 {
		t.Error("应调用 job/stop 停止 rclone 侧任务")
	}
	if len(rc.stoppedGroup) == 0 {
		t.Error("应调用 job/stopgroup 作为兜底")
	}
}

func TestCancelUnknownRun(t *testing.T) {
	rc := &stubRC{}
	m, _ := newTestManager(t, rc, Options{})
	if err := m.Cancel(12345); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("期望 ErrNotRunning，得到 %v", err)
	}
}

func TestTaskTimeoutMarksFailed(t *testing.T) {
	rc := &stubRC{}
	rc.jobFn = func(jobID int64) (*rclone.JobStatus, error) {
		return &rclone.JobStatus{ID: jobID, Finished: false}, nil
	}
	m, st := newTestManager(t, rc, Options{MaxConcurrent: 1})
	task := createTask(t, st, func(task *store.Task) {
		task.TimeoutSeconds = 1
	})

	run, err := m.Trigger(context.Background(), task, store.TriggerManual)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		got, err := st.GetRun(context.Background(), run.ID)
		return err == nil && got.Status.Finished()
	}, "超时收敛")

	got, _ := st.GetRun(context.Background(), run.ID)
	if got.Status != store.StatusFailed {
		t.Fatalf("超时应为 failed，得到 %q", got.Status)
	}
	if got.Message == "" {
		t.Error("应说明超时原因")
	}
}

func TestSkipOverlapRejectsSecondTrigger(t *testing.T) {
	rc := &stubRC{}
	release := make(chan struct{})
	rc.jobFn = func(jobID int64) (*rclone.JobStatus, error) {
		select {
		case <-release:
			return &rclone.JobStatus{ID: jobID, Finished: true, Success: true}, nil
		default:
			return &rclone.JobStatus{ID: jobID, Finished: false}, nil
		}
	}
	m, st := newTestManager(t, rc, Options{MaxConcurrent: 2, SkipOverlap: true})
	task := createTask(t, st, nil)

	if _, err := m.Trigger(context.Background(), task, store.TriggerManual); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return m.ActiveCount() == 1 }, "首个任务开始")

	_, err := m.Trigger(context.Background(), task, store.TriggerCron)
	if !errors.Is(err, ErrTaskBusy) {
		t.Fatalf("期望 ErrTaskBusy，得到 %v", err)
	}

	close(release)
	waitFor(t, 3*time.Second, func() bool { return m.ActiveCount() == 0 }, "任务结束")
}

func TestCapacityLimit(t *testing.T) {
	rc := &stubRC{}
	release := make(chan struct{})
	rc.jobFn = func(jobID int64) (*rclone.JobStatus, error) {
		select {
		case <-release:
			return &rclone.JobStatus{ID: jobID, Finished: true, Success: true}, nil
		default:
			return &rclone.JobStatus{ID: jobID, Finished: false}, nil
		}
	}
	// SkipOverlap=false 以便第二个任务也尝试抢占许可
	m, st := newTestManager(t, rc, Options{MaxConcurrent: 1, SkipOverlap: false})
	t1 := createTask(t, st, func(x *store.Task) { x.Name = "t1" })
	t2 := createTask(t, st, func(x *store.Task) { x.Name = "t2" })

	if _, err := m.Trigger(context.Background(), t1, store.TriggerManual); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return m.ActiveCount() == 1 }, "占用唯一许可")

	if _, err := m.Trigger(context.Background(), t2, store.TriggerManual); !errors.Is(err, ErrCapacity) {
		t.Fatalf("期望 ErrCapacity，得到 %v", err)
	}

	close(release)
	waitFor(t, 3*time.Second, func() bool { return m.ActiveCount() == 0 }, "任务结束")
}

func TestTriggerDisabledTask(t *testing.T) {
	rc := &stubRC{}
	m, st := newTestManager(t, rc, Options{})
	task := createTask(t, st, func(task *store.Task) { task.Enabled = false })
	if _, err := m.Trigger(context.Background(), task, store.TriggerManual); !errors.Is(err, ErrTaskDisabled) {
		t.Fatalf("期望 ErrTaskDisabled，得到 %v", err)
	}
}

func TestBuildRequestParamMapping(t *testing.T) {
	cases := []struct {
		kind   store.TaskKind
		method string
		params map[string]string
	}{
		{store.KindSync, "sync/sync", map[string]string{"srcFs": "a:", "dstFs": "b:"}},
		{store.KindCopy, "sync/copy", map[string]string{"srcFs": "a:", "dstFs": "b:"}},
		{store.KindMove, "sync/move", map[string]string{"srcFs": "a:", "dstFs": "b:"}},
		{store.KindCheck, "sync/check", map[string]string{"srcFs": "a:", "dstFs": "b:"}},
		{store.KindBisync, "sync/bisync", map[string]string{"path1": "a:", "path2": "b:"}},
		{store.KindDelete, "operations/delete", map[string]string{"fs": "a:"}},
		{store.KindPurge, "operations/purge", map[string]string{"fs": "a:"}},
		{store.KindMkdir, "operations/mkdir", map[string]string{"fs": "a:"}},
	}
	for _, tc := range cases {
		t.Run(string(tc.kind), func(t *testing.T) {
			// buildRequest 以步骤为单位；SingleKind/Source/Dest 由步骤自己携带。
			step := &store.TaskStep{Kind: tc.kind, Source: "a:", Dest: "b:",
				ExtraFlags: map[string]any{"dry_run": true}}
			method, params, err := buildRequest(step)
			if err != nil {
				t.Fatalf("buildRequest 失败: %v", err)
			}
			if method != tc.method {
				t.Errorf("方法期望 %q，得到 %q", tc.method, method)
			}
			for k, want := range tc.params {
				if params[k] != want {
					t.Errorf("参数 %s 期望 %q，得到 %v", k, want, params[k])
				}
			}
			if params["dry_run"] != true {
				t.Errorf("extra_flags 应合并进参数: %+v", params)
			}
			// 不应出现目标路径以外的参数键
			if tc.kind == store.KindPurge && len(params) != 2 {
				t.Errorf("purge 不应下发目标路径: %+v", params)
			}
		})
	}
}

func TestBuildRequestUnknownKind(t *testing.T) {
	if _, _, err := buildRequest(&store.TaskStep{Kind: "rsync", Source: "a:"}); err == nil {
		t.Fatal("未知类型应报错")
	}
}

func TestRedactHidesSecrets(t *testing.T) {
	out := redact(map[string]any{"pass": "secret", "password": "x", "srcFs": "a:"})
	if out["pass"] != "***" || out["password"] != "***" {
		t.Errorf("敏感字段应被脱敏: %+v", out)
	}
	if out["srcFs"] != "a:" {
		t.Errorf("普通字段不应被修改: %+v", out)
	}
}

func TestComputePercent(t *testing.T) {
	// 单步百分比：先按字节，没有字节总量时退化为文件数。
	if p := stepPercent(&rclone.Stats{Bytes: 50, TotalBytes: 200}, nil); p != 25 {
		t.Errorf("期望 25，得到 %v", p)
	}
	if p := stepPercent(&rclone.Stats{Transfers: 1, TotalTransfers: 4}, nil); p != 25 {
		t.Errorf("期望按文件数计算 25，得到 %v", p)
	}
	// 完成后成功固定为 100
	if p := stepPercent(nil, &rclone.JobStatus{Finished: true, Success: true}); p != 100 {
		t.Errorf("期望 100，得到 %v", p)
	}
	// 未开始时为 0
	if p := stepPercent(nil, &rclone.JobStatus{}); p != 0 {
		t.Errorf("期望 0，得到 %v", p)
	}
	// 越界收敛
	if p := stepPercent(&rclone.Stats{Bytes: 500, TotalBytes: 100}, nil); p != 100 {
		t.Errorf("应被截断到 100，得到 %v", p)
	}
}

// TestChainPercent 验证整链进度的折算：已完成步骤占整数份，当前步骤占它那一份的一部分。
func TestChainPercent(t *testing.T) {
	m := &Manager{}
	a := &activeRun{task: &store.Task{Steps: []*store.TaskStep{{}, {}, {}}}}
	if p := m.stepPercentInChain(a, 0); p != 0 {
		t.Errorf("第一步刚开始应为 0，得到 %v", p)
	}
	if p := m.stepPercentInChain(a, 50); math.Abs(p-50/3.0) > 0.01 {
		t.Errorf("第一步走了一半应为 50/3，得到 %v", p)
	}
	a.done = 1
	if p := m.stepPercentInChain(a, 50); p != 50 {
		t.Errorf("完成一步 + 当前一半应为 50，得到 %v", p)
	}
	a.done = 3
	if p := m.stepPercentInChain(a, 0); p != 100 {
		t.Errorf("全部完成应为 100，得到 %v", p)
	}
}

// ---------------------------------------------------------------------------
// 多步骤任务（任务链）
// ---------------------------------------------------------------------------

// chainSteps 返回三个步骤：第 2 步（下标 1）按其 onError 控制失败行为。
func chainSteps(onError store.StepOnError) []*store.TaskStep {
	return []*store.TaskStep{
		{Name: "音乐", Kind: store.KindSync, Source: "src:音乐", Dest: "dst:音乐"},
		{Name: "视频", Kind: store.KindCopy, Source: "src:视频", Dest: "dst:视频", OnError: onError},
		{Name: "文档", Kind: store.KindMove, Source: "src:文档", Dest: "dst:文档"},
	}
}

// createChainTask 用显式步骤创建任务（不走"顶层字段"的老格式）。
func createChainTask(t *testing.T, st store.Store, steps []*store.TaskStep) *store.Task {
	t.Helper()
	task := &store.Task{
		Name:    fmt.Sprintf("chain-%d", time.Now().UnixNano()),
		Kind:    steps[0].Kind,
		Source:  steps[0].Source,
		Dest:    steps[0].Dest,
		Steps:   steps,
		Enabled: true,
	}
	if err := st.CreateTask(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	// 回读一次：CreateTask 会把步骤落库并重新编号 position，测试要按库里的结果断言。
	got, err := st.GetTask(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func waitFinished(t *testing.T, st *store.SQLiteStore, runID int64) *store.Run {
	t.Helper()
	waitFor(t, 5*time.Second, func() bool {
		got, err := st.GetRun(context.Background(), runID)
		return err == nil && got.Status.Finished()
	}, "运行结束")
	got, err := st.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// TestMultiStepRunsAllStepsInOrder 是多步骤的基线：顺序执行、统计累加、逐步留痕。
func TestMultiStepRunsAllStepsInOrder(t *testing.T) {
	rc := &stubRC{}
	rc.statsFn = func(group string) *rclone.Stats {
		return &rclone.Stats{Group: group, Bytes: 100, TotalBytes: 100,
			Transfers: 1, TotalTransfers: 1, Checks: 2}
	}
	m, st := newTestManager(t, rc, Options{MaxConcurrent: 1})
	task := createChainTask(t, st, chainSteps(store.OnErrorContinue))

	run, err := m.Trigger(context.Background(), task, store.TriggerManual)
	if err != nil {
		t.Fatal(err)
	}
	got := waitFinished(t, st, run.ID)

	if got.Status != store.StatusSuccess {
		t.Fatalf("期望 success，得到 %q（message=%q error=%q）", got.Status, got.Message, got.Error)
	}
	if got.StepTotal != 3 {
		t.Errorf("StepTotal 应为 3，得到 %d", got.StepTotal)
	}
	if len(got.StepResults) != 3 {
		t.Fatalf("应记录 3 个步骤结果，得到 %d", len(got.StepResults))
	}
	for i, res := range got.StepResults {
		if res.Status != store.StatusSuccess {
			t.Errorf("步骤 %d 状态应为 success，得到 %q（%s）", i, res.Status, res.Error)
		}
		if res.Position != i {
			t.Errorf("步骤 %d 的 position 应为 %d，得到 %d", i, i, res.Position)
		}
	}

	// 顺序与方法：sync -> copy -> move，各自的源路径按声明顺序下发。
	calls := rc.allCalls(t)
	if len(calls) != 3 {
		t.Fatalf("应发起 3 次 rclone 调用，得到 %d", len(calls))
	}
	wantMethods := []string{"sync/sync", "sync/copy", "sync/move"}
	wantSrc := []string{"src:音乐", "src:视频", "src:文档"}
	for i, c := range calls {
		if c.method != wantMethods[i] {
			t.Errorf("第 %d 步方法应为 %s，得到 %s", i+1, wantMethods[i], c.method)
		}
		if c.params["srcFs"] != wantSrc[i] {
			t.Errorf("第 %d 步源路径应为 %s，得到 %v", i+1, wantSrc[i], c.params["srcFs"])
		}
		// 每个步骤独立的 group：混用会让 core/stats 读到跨步累计值。
		if want := fmt.Sprintf("run-%d-%d", run.ID, i); c.group != want {
			t.Errorf("第 %d 步 group 应为 %s，得到 %s", i+1, want, c.group)
		}
	}

	// 统计必须累加：每步 100 字节，不清零的话最终只会剩下最后一步的 100。
	if got.Bytes != 300 {
		t.Errorf("传输字节应累加为 300，得到 %d", got.Bytes)
	}
	if got.Files != 3 {
		t.Errorf("文件数应累加为 3，得到 %d", got.Files)
	}
	if got.Percent != 100 {
		t.Errorf("全部完成后进度应为 100，得到 %v", got.Percent)
	}
	if !strings.Contains(got.Message, "3 个步骤全部完成") {
		t.Errorf("成功文案应说明完成步数，得到 %q", got.Message)
	}
	// 日志里要有步骤分隔标记，否则用户只能看到一坨混合输出。
	if joined := strings.Join(got.LogTail, "\n"); !strings.Contains(joined, "[步骤 2/3]") {
		t.Errorf("日志缺少步骤分隔标记: %s", joined)
	}
}

// TestStepFailureWithContinue 验证默认策略：某步失败后继续跑完剩下的步骤，
// 整条链仍判失败，并指出是哪一步挂的。
func TestStepFailureWithContinue(t *testing.T) {
	rc := &stubRC{}
	// 第 2 步失败（job id 自增，第二次调用即 job 2）。
	rc.jobFn = func(jobID int64) (*rclone.JobStatus, error) {
		if jobID == 2 {
			return &rclone.JobStatus{ID: jobID, Finished: true, Success: false, Error: "permission denied"}, nil
		}
		return &rclone.JobStatus{ID: jobID, Finished: true, Success: true}, nil
	}
	m, st := newTestManager(t, rc, Options{MaxConcurrent: 1})
	task := createChainTask(t, st, chainSteps(store.OnErrorContinue))

	run, err := m.Trigger(context.Background(), task, store.TriggerManual)
	if err != nil {
		t.Fatal(err)
	}
	got := waitFinished(t, st, run.ID)

	if got.Status != store.StatusFailed {
		t.Fatalf("任一步失败整条链应判 failed，得到 %q", got.Status)
	}
	if len(got.StepResults) != 3 {
		t.Fatalf("continue 策略下后续步骤仍应执行，应有 3 条结果，得到 %d", len(got.StepResults))
	}
	if got.StepResults[1].Status != store.StatusFailed {
		t.Errorf("第 2 步应标 failed，得到 %q", got.StepResults[1].Status)
	}
	if got.StepResults[2].Status != store.StatusSuccess {
		t.Errorf("第 3 步应照常执行并成功，得到 %q", got.StepResults[2].Status)
	}
	if !strings.Contains(got.Message, "2/3") || !strings.Contains(got.Message, "视频") {
		t.Errorf("失败文案应指出完成数与失败步骤名，得到 %q", got.Message)
	}
	if !strings.Contains(got.Error, "permission denied") {
		t.Errorf("run.Error 应带上首个失败步骤的原因，得到 %q", got.Error)
	}
	if got.Percent == 100 {
		t.Errorf("存在失败步骤时进度不应为 100%%，得到 %v（StepResults=%+v）", got.Percent, got.StepResults)
	}
	if len(rc.allCalls(t)) != 3 {
		t.Errorf("应发起 3 次调用（含失败步骤之后的那一次）")
	}
}

// TestStepFailureWithAbort 验证 abort：失败即停，后续步骤不再执行。
func TestStepFailureWithAbort(t *testing.T) {
	rc := &stubRC{}
	rc.jobFn = func(jobID int64) (*rclone.JobStatus, error) {
		if jobID == 2 {
			return &rclone.JobStatus{ID: jobID, Finished: true, Success: false, Error: "boom"}, nil
		}
		return &rclone.JobStatus{ID: jobID, Finished: true, Success: true}, nil
	}
	m, st := newTestManager(t, rc, Options{MaxConcurrent: 1})
	task := createChainTask(t, st, chainSteps(store.OnErrorAbort))

	run, err := m.Trigger(context.Background(), task, store.TriggerManual)
	if err != nil {
		t.Fatal(err)
	}
	got := waitFinished(t, st, run.ID)

	if got.Status != store.StatusFailed {
		t.Fatalf("期望 failed，得到 %q", got.Status)
	}
	if len(got.StepResults) != 2 {
		t.Fatalf("abort 策略下只应有 2 条结果，得到 %d", len(got.StepResults))
	}
	if len(rc.allCalls(t)) != 2 {
		t.Errorf("abort 后不应再提交后续步骤，实际调用 %d 次", len(rc.allCalls(t)))
	}
	if !strings.Contains(got.Message, "已中止") {
		t.Errorf("文案应说明已中止，得到 %q", got.Message)
	}
	// 被跳过的步骤要在日志里说清楚，否则用户不知道是没跑还是跑了没记录。
	if joined := strings.Join(got.LogTail, "\n"); !strings.Contains(joined, "已跳过") {
		t.Errorf("日志应记录被跳过的步骤: %s", joined)
	}
}

// TestStepDelayIsAppliedBetweenSteps 验证间隔语义：步骤结束后等待，
// 且最后一秒的 delay 不再等待（否则白等一轮才收敛终态）。
func TestStepDelayIsAppliedBetweenSteps(t *testing.T) {
	rc := &stubRC{}
	m, st := newTestManager(t, rc, Options{MaxConcurrent: 1})
	steps := chainSteps(store.OnErrorContinue)
	steps[0].DelayAfter = 1
	steps[2].DelayAfter = 1 // 最后一步：应被忽略
	task := createChainTask(t, st, steps)

	start := time.Now()
	run, err := m.Trigger(context.Background(), task, store.TriggerManual)
	if err != nil {
		t.Fatal(err)
	}
	got := waitFinished(t, st, run.ID)

	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Errorf("步骤间隔未生效：耗时 %v", elapsed)
	}
	// 只应等待一次（第 1 步 -> 第 2 步），最后一步之后不再等待。
	var waits int
	for _, line := range got.LogTail {
		if strings.HasPrefix(line, "[步骤间隔]") {
			waits++
		}
	}
	if waits != 1 {
		t.Errorf("[步骤间隔] 标记应恰好出现 1 次（最后一步后不等待），得到 %d 次", waits)
	}
}

// TestTriggerFromStep 验证"从失败步骤重跑"：之前的步骤不再执行。
func TestTriggerFromStep(t *testing.T) {
	rc := &stubRC{}
	m, st := newTestManager(t, rc, Options{MaxConcurrent: 1})
	task := createChainTask(t, st, chainSteps(store.OnErrorContinue))

	run, err := m.TriggerFrom(context.Background(), task, store.TriggerManual, 1)
	if err != nil {
		t.Fatal(err)
	}
	got := waitFinished(t, st, run.ID)

	if got.Status != store.StatusSuccess {
		t.Fatalf("期望 success，得到 %q", got.Status)
	}
	calls := rc.allCalls(t)
	if len(calls) != 2 {
		t.Fatalf("从第 2 步开始应只发起 2 次调用，得到 %d", len(calls))
	}
	if calls[0].params["srcFs"] != "src:视频" {
		t.Errorf("应跳过第 1 步，首个调用源路径为 %v", calls[0].params["srcFs"])
	}
	if len(got.StepResults) != 2 || got.StepResults[0].Position != 1 {
		t.Errorf("应只记录后两步的结果，得到 %+v", got.StepResults)
	}
	// StepTotal 仍是任务的全部步骤数，否则"跑到第几步/共几步"会算错。
	if got.StepTotal != 3 {
		t.Errorf("StepTotal 应为任务的全部步数 3，得到 %d", got.StepTotal)
	}
	if got.StepIndex != 2 {
		t.Errorf("StepIndex 应停在最后一个执行的步骤，得到 %d", got.StepIndex)
	}
}

// TestTriggerFromStepFailureMessage 验证从中间重跑时，文案的分母是"本轮要跑的步数"
// 而不是任务的总步数：否则从第 3 步重跑失败会被说成"2/3 个步骤完成"。
func TestTriggerFromStepFailureMessage(t *testing.T) {
	rc := &stubRC{}
	// 本轮只提交两次 job：第 2 次（步骤下标 2）失败。
	rc.jobFn = func(jobID int64) (*rclone.JobStatus, error) {
		if jobID == 2 {
			return &rclone.JobStatus{ID: jobID, Finished: true, Success: false, Error: "boom"}, nil
		}
		return &rclone.JobStatus{ID: jobID, Finished: true, Success: true}, nil
	}
	m, st := newTestManager(t, rc, Options{MaxConcurrent: 1})
	task := createChainTask(t, st, chainSteps(store.OnErrorContinue))

	run, err := m.TriggerFrom(context.Background(), task, store.TriggerManual, 1)
	if err != nil {
		t.Fatal(err)
	}
	got := waitFinished(t, st, run.ID)

	if got.Status != store.StatusFailed {
		t.Fatalf("期望 failed，得到 %q", got.Status)
	}
	if !strings.Contains(got.Message, "1/2") {
		t.Errorf("分母应是本轮步数 2，得到 %q", got.Message)
	}
	// 步骤总数仍要报告任务的全部步数，前端靠它算"第几步/共几步"。
	if got.StepTotal != 3 {
		t.Errorf("StepTotal 应为 3，得到 %d", got.StepTotal)
	}
}

func TestTriggerFromStepOutOfRange(t *testing.T) {
	rc := &stubRC{}
	m, st := newTestManager(t, rc, Options{MaxConcurrent: 1})
	task := createChainTask(t, st, chainSteps(store.OnErrorContinue))

	if _, err := m.TriggerFrom(context.Background(), task, store.TriggerManual, 3); err == nil {
		t.Fatal("起始步骤越界应报错")
	}
	if _, err := m.TriggerFrom(context.Background(), task, store.TriggerManual, -1); err != nil {
		t.Fatalf("负数应被归一为 0，得到 %v", err)
	}
}

// TestChainForcesNoOverlap 验证多步骤任务默认不允许重叠：链时长可能超过触发周期。
func TestChainForcesNoOverlap(t *testing.T) {
	rc := &stubRC{}
	rc.jobFn = func(jobID int64) (*rclone.JobStatus, error) {
		return &rclone.JobStatus{ID: jobID, Finished: false}, nil
	}
	// SkipOverlap 显式关闭也不行：多步骤任务重叠会让"最近一次结果"失去意义。
	m, st := newTestManager(t, rc, Options{MaxConcurrent: 2, SkipOverlap: false})
	task := createChainTask(t, st, chainSteps(store.OnErrorContinue))

	if _, err := m.Trigger(context.Background(), task, store.TriggerManual); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return m.ActiveCount() == 1 }, "链启动")

	if _, err := m.Trigger(context.Background(), task, store.TriggerManual); !errors.Is(err, ErrTaskBusy) {
		t.Fatalf("多步骤任务应拒绝重复触发，得到 %v", err)
	}
	if err := m.Cancel(m.ActiveRunIDs()[0]); err != nil {
		t.Fatal(err)
	}
}

func TestCancelAllStopsEverything(t *testing.T) {
	rc := &stubRC{}
	rc.jobFn = func(jobID int64) (*rclone.JobStatus, error) {
		return &rclone.JobStatus{ID: jobID, Finished: false}, nil
	}
	m, st := newTestManager(t, rc, Options{MaxConcurrent: 3, SkipOverlap: false})
	t1 := createTask(t, st, func(x *store.Task) { x.Name = "a" })
	t2 := createTask(t, st, func(x *store.Task) { x.Name = "b" })
	for _, task := range []*store.Task{t1, t2} {
		if _, err := m.Trigger(context.Background(), task, store.TriggerManual); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 2*time.Second, func() bool { return m.ActiveCount() == 2 }, "两个任务启动")

	m.CancelAll()
	if !m.Wait(5 * time.Second) {
		t.Fatalf("CancelAll 后仍剩 %d 个任务", m.ActiveCount())
	}
	if m.ActiveCount() != 0 {
		t.Errorf("所有任务都应结束")
	}
}

func strPtr(s string) *string { return &s }

// ---------------------------------------------------------------------------
// 清理目标重名（dedupe_before）

const (
	srcModTime = "2026-09-01T10:00:00Z"
	oldModTime = "2026-01-01T10:00:00Z"
)

// dupeFixture 造一组固定的源/目标清单：
//
//	a.mp3  目标 2 份副本，都与源不同 -> 需要同步 -> 应清理
//	b.mp3  目标 2 份副本，其中 1 份与源一致 -> 不一定传输 -> 不应清理
//	c.mp3  目标只有 1 份 -> 不是重名 -> 不应清理
//	d.mp3  目标 2 份副本，源端不存在 -> 与目标独有文件无关 -> 不应清理
func dupeFixture() (dst, src []rclone.ListItem) {
	dst = []rclone.ListItem{
		item("a.mp3", 100, oldModTime),
		item("a.mp3", 101, oldModTime),
		item("b.mp3", 200, srcModTime),
		item("b.mp3", 200, oldModTime),
		item("c.mp3", 300, oldModTime),
		item("sub/d.mp3", 400, oldModTime),
		item("sub/d.mp3", 400, oldModTime),
	}
	src = []rclone.ListItem{
		item("a.mp3", 999, srcModTime),
		item("b.mp3", 200, srcModTime),
		item("c.mp3", 300, srcModTime),
	}
	return dst, src
}

// dupeRunner 按 lsjson 的目标分发固定清单：dest 用 dst，其余当源目录查询。
func dupeRunner(dst, src []rclone.ListItem) *fakeRunner {
	return &fakeRunner{captureFn: func(args []string) ([]byte, error) {
		for _, a := range args {
			if a == "dst:bucket" {
				return listJSON(dst...), nil
			}
			if strings.HasPrefix(a, "src:bucket") {
				return listJSON(src...), nil
			}
		}
		return listJSON(), nil
	}}
}

// TestDedupeBeforeClearsOnlyPathsThatWillTransfer 是这次改动的验收点：
// 只有"源确实与目标所有副本都不同"的路径才该被清理，其余一律不动。
func TestDedupeBeforeClearsOnlyPathsThatWillTransfer(t *testing.T) {
	rc := &stubRC{}
	dst, src := dupeFixture()
	runner := dupeRunner(dst, src)
	m, st := newTestManagerWithRunner(t, rc, runner, Options{MaxConcurrent: 1})
	task := createTask(t, st, func(x *store.Task) { x.DedupeBefore = true })

	run, err := m.Trigger(context.Background(), task, store.TriggerManual)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		got, err := st.GetRun(context.Background(), run.ID)
		return err == nil && got.Status.Finished()
	}, "运行结束")

	deletes := runner.callsOf("deletefile")
	if len(deletes) != 2 {
		t.Fatalf("只应清理 a.mp3 的两份副本（2 次 deletefile），实际 %d 次: %v", len(deletes), deletes)
	}
	for _, d := range deletes {
		if !slices.Equal(d, []string{"deletefile", "dst:bucket/a.mp3"}) {
			t.Errorf("不该清理该路径: %v", d)
		}
	}
	// 清理完必须照常同步。
	if len(rc.startCalls) != 1 {
		t.Fatalf("清理后应提交一次同步，实际 %d 次", len(rc.startCalls))
	}

	got, err := st.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != store.StatusSuccess {
		t.Fatalf("清理成功后应正常同步，得到 %q（%s）", got.Status, got.Error)
	}
	log := strings.Join(got.LogTail, "\n")
	if !strings.Contains(log, "[清理重名]") {
		t.Errorf("清理过程应写入运行日志: %v", got.LogTail)
	}
	if !strings.Contains(log, "已清理 1 个") {
		t.Errorf("日志应说明清理了几个路径: %v", got.LogTail)
	}
}

// TestDedupeBeforeKeepsUnchangedPaths 防止退化成"整目录无条件去重"。
func TestDedupeBeforeKeepsUnchangedPaths(t *testing.T) {
	rc := &stubRC{}
	// 目标 3 份副本，其中一份与源一致 -> 本次可能不传输 -> 一个都不许删。
	dst := []rclone.ListItem{
		item("x.mp3", 500, srcModTime),
		item("x.mp3", 500, oldModTime),
		item("x.mp3", 500, oldModTime),
	}
	src := []rclone.ListItem{item("x.mp3", 500, srcModTime)}
	runner := dupeRunner(dst, src)
	m, st := newTestManagerWithRunner(t, rc, runner, Options{MaxConcurrent: 1})
	task := createTask(t, st, func(x *store.Task) { x.DedupeBefore = true })

	run, err := m.Trigger(context.Background(), task, store.TriggerManual)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		got, err := st.GetRun(context.Background(), run.ID)
		return err == nil && got.Status.Finished()
	}, "运行结束")

	if n := len(runner.callsOf("deletefile")); n != 0 {
		t.Errorf("存在与源一致的副本时不应删除任何对象，实际删了 %d 次", n)
	}
	if len(rc.startCalls) != 1 {
		t.Errorf("未清理也应照常同步，实际 %d 次", len(rc.startCalls))
	}
}

func TestDedupeBeforeNoDuplicates(t *testing.T) {
	rc := &stubRC{}
	dst := []rclone.ListItem{item("a.mp3", 1, oldModTime), item("b.mp3", 2, oldModTime)}
	src := []rclone.ListItem{item("a.mp3", 9, srcModTime), item("b.mp3", 9, srcModTime)}
	runner := dupeRunner(dst, src)
	m, st := newTestManagerWithRunner(t, rc, runner, Options{MaxConcurrent: 1})
	task := createTask(t, st, func(x *store.Task) { x.DedupeBefore = true })

	run, err := m.Trigger(context.Background(), task, store.TriggerManual)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		got, err := st.GetRun(context.Background(), run.ID)
		return err == nil && got.Status.Finished()
	}, "运行结束")

	if n := len(runner.callsOf("deletefile")); n != 0 {
		t.Errorf("没有重名时不应删除任何对象，实际 %d 次", n)
	}
	// 源目录一次都不必查 —— 没有重名就没有比较的必要。
	if n := len(runner.callsOf("lsjson")); n != 1 {
		t.Errorf("无重名时应只列一次目标，实际 lsjson 调用 %d 次", n)
	}
}

func TestDedupeBeforeSkippedWhenDisabled(t *testing.T) {
	rc := &stubRC{}
	runner := &fakeRunner{}
	m, st := newTestManagerWithRunner(t, rc, runner, Options{MaxConcurrent: 1})
	task := createTask(t, st, nil) // DedupeBefore 默认 false

	run, err := m.Trigger(context.Background(), task, store.TriggerManual)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		got, err := st.GetRun(context.Background(), run.ID)
		return err == nil && got.Status.Finished()
	}, "运行结束")

	if runner.count() != 0 {
		t.Errorf("未开启时不应执行清理，实际 %d 次", runner.count())
	}
}

// TestDedupeBeforeListFailureAbortsRun 是这条链路最容易做错的地方：
// 扫描失败却继续同步，会让用户以为"目标已经没有重名了"，实际什么都没清。
func TestDedupeBeforeListFailureAbortsRun(t *testing.T) {
	rc := &stubRC{}
	runner := &fakeRunner{captureFn: func(args []string) ([]byte, error) {
		return nil, errors.New("lsjson: directory not found")
	}}
	m, st := newTestManagerWithRunner(t, rc, runner, Options{MaxConcurrent: 1})
	task := createTask(t, st, func(x *store.Task) { x.DedupeBefore = true })

	run, err := m.Trigger(context.Background(), task, store.TriggerManual)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		got, err := st.GetRun(context.Background(), run.ID)
		return err == nil && got.Status.Finished()
	}, "运行结束")

	got, err := st.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != store.StatusFailed {
		t.Fatalf("扫描失败应让整轮失败，得到 %q", got.Status)
	}
	if !strings.Contains(got.Error, "清理目标重名") {
		t.Errorf("错误信息应说明是清理步骤失败: %q", got.Error)
	}
	// 关键：清理没成功就不能提交同步任务。
	if len(rc.startCalls) != 0 {
		t.Errorf("清理失败后不应再提交 rclone 同步任务，实际提交了 %d 次", len(rc.startCalls))
	}
}

func TestDedupeBeforeRequiresRunner(t *testing.T) {
	rc := &stubRC{}
	// runner 传 nil：模拟未注入执行器的情况，必须失败而不是悄悄跳过。
	m, st := newTestManagerWithRunner(t, rc, nil, Options{MaxConcurrent: 1})
	task := createTask(t, st, func(x *store.Task) { x.DedupeBefore = true })

	run, err := m.Trigger(context.Background(), task, store.TriggerManual)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		got, err := st.GetRun(context.Background(), run.ID)
		return err == nil && got.Status.Finished()
	}, "运行结束")

	got, _ := st.GetRun(context.Background(), run.ID)
	if got.Status != store.StatusFailed {
		t.Fatalf("缺少执行器时应失败，得到 %q", got.Status)
	}
	if len(rc.startCalls) != 0 {
		t.Errorf("缺少执行器时不该提交同步任务")
	}
}

func TestSameContentModifyWindow(t *testing.T) {
	base := "2026-09-01T10:00:00.000Z"
	cases := []struct {
		name string
		a, b rclone.ListItem
		want bool
	}{
		{"大小不同", item("a", 10, base), item("a", 11, base), false},
		{"完全一致", item("a", 10, base), item("a", 10, base), true},
		{"远端截断到秒", item("a", 10, "2026-09-01T10:00:00.400Z"), item("a", 10, "2026-09-01T10:00:00Z"), true},
		{"真正被修改", item("a", 10, base), item("a", 10, "2026-09-02T10:00:00Z"), false},
		{"缺 modtime 视为不同", item("a", 10, ""), item("a", 10, base), false},
	}
	for _, tc := range cases {
		if got := sameContent(tc.a, tc.b); got != tc.want {
			t.Errorf("%s: sameContent = %v, 期望 %v", tc.name, got, tc.want)
		}
	}
}

func TestJoinRemote(t *testing.T) {
	cases := []struct{ root, rel, want string }{
		{"dst:bucket", "a.mp3", "dst:bucket/a.mp3"},
		{"dst:bucket/", "sub/a.mp3", "dst:bucket/sub/a.mp3"},
		{"115Drive:音乐", "a.mp3", "115Drive:音乐/a.mp3"},
		{"D:/data", "a.mp3", "D:/data/a.mp3"},
		{"dst:bucket", "", "dst:bucket"},
	}
	for _, tc := range cases {
		if got := joinRemote(tc.root, tc.rel); got != tc.want {
			t.Errorf("joinRemote(%q, %q) = %q, 期望 %q", tc.root, tc.rel, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// 实时日志（详情弹窗"边跑边看"）

// TestLiveLogTailWhileRunning：库里的 LogTail 要到终态才回写，进行中一直是空的。
// 详情页想边跑边看只能读 journal，这里锁住这个行为。
func TestLiveLogTailWhileRunning(t *testing.T) {
	rc := &stubRC{}
	// job 永不结束，把运行钉在进行中。
	rc.jobFn = func(jobID int64) (*rclone.JobStatus, error) {
		return &rclone.JobStatus{ID: jobID, Finished: false}, nil
	}
	rc.statsFn = func(group string) *rclone.Stats {
		return &rclone.Stats{Group: group, Bytes: 100, TotalBytes: 1000}
	}
	dst, src := dupeFixture()
	// 开「清理目标重名」是为了让 journal 里有确定性内容可断言。
	m, st := newTestManagerWithRunner(t, rc, dupeRunner(dst, src), Options{MaxConcurrent: 1})
	task := createTask(t, st, func(x *store.Task) { x.DedupeBefore = true })

	run, err := m.Trigger(context.Background(), task, store.TriggerManual)
	if err != nil {
		t.Fatal(err)
	}
	// job 提交前必然已经跑完清理（清理写在 StartAsync 之前），用 startCalls
	// 作为"journal 里已有确定性内容"的同步点，避免断言撞上时序。
	defer func() { _ = m.Cancel(run.ID) }()
	waitFor(t, 3*time.Second, func() bool {
		rc.mu.Lock()
		defer rc.mu.Unlock()
		return m.IsRunning(run.ID) && len(rc.startCalls) == 1
	}, "运行进入进行中且已完成清理")

	// 前提：库里那条记录的 LogTail 此刻确实是空的。
	db, err := st.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(db.LogTail) != 0 {
		t.Fatalf("进行中的记录库里不应有 LogTail，得到 %v", db.LogTail)
	}

	live := m.LiveLogTail(run.ID)
	if len(live) == 0 {
		t.Fatal("进行中应能取到实时日志片段")
	}
	if !strings.Contains(strings.Join(live, "\n"), "[清理重名]") {
		t.Errorf("实时日志应包含清理过程，得到 %v", live)
	}
}

// TestLiveLogTailAfterFinish：运行结束后必须返回 nil，调用方才会回落到库里的
// 终态 LogTail（否则详情会被实时片段覆盖成空）。
func TestLiveLogTailAfterFinish(t *testing.T) {
	rc := &stubRC{}
	dst, src := dupeFixture()
	m, st := newTestManagerWithRunner(t, rc, dupeRunner(dst, src), Options{MaxConcurrent: 1})
	task := createTask(t, st, func(x *store.Task) { x.DedupeBefore = true })

	run, err := m.Trigger(context.Background(), task, store.TriggerManual)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		got, err := st.GetRun(context.Background(), run.ID)
		return err == nil && got.Status.Finished()
	}, "运行结束")

	if live := m.LiveLogTail(run.ID); live != nil {
		t.Errorf("运行结束后应返回 nil，得到 %v", live)
	}
	if live := m.LiveLogTail(run.ID + 999); live != nil {
		t.Errorf("未知 run 应返回 nil，得到 %v", live)
	}

	got, err := st.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(got.LogTail, "\n"), "[清理重名]") {
		t.Errorf("终态 LogTail 应保留清理过程，得到 %v", got.LogTail)
	}
}
