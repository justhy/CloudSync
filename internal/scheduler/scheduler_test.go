package scheduler

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloudsync/internal/config"
	"cloudsync/internal/logging"
	"cloudsync/internal/manager"
	"cloudsync/internal/rclone"
	"cloudsync/internal/store"
)

// fakeRC 是满足 manager.RC 的最小实现：任务瞬间成功完成。
type fakeRC struct {
	mu    chan struct{}
	calls int
}

func (f *fakeRC) StartAsync(_ context.Context, method string, params map[string]any, group string) (int64, error) {
	f.mu <- struct{}{}
	f.calls++
	<-f.mu
	return 1, nil
}

func (f *fakeRC) Stats(_ context.Context, group string) (*rclone.Stats, error) {
	return &rclone.Stats{Group: group, Bytes: 100, TotalBytes: 100}, nil
}

func (f *fakeRC) JobStatus(_ context.Context, jobID int64) (*rclone.JobStatus, error) {
	return &rclone.JobStatus{ID: jobID, Finished: true, Success: true}, nil
}

func (f *fakeRC) StopJob(_ context.Context, jobID int64) error    { return nil }
func (f *fakeRC) StopGroup(_ context.Context, group string) error { return nil }

func (f *fakeRC) callCount() int {
	f.mu <- struct{}{}
	defer func() { <-f.mu }()
	return f.calls
}

func newScheduler(t *testing.T, mutate func(*config.SchedulerConfig)) (*Scheduler, *store.SQLiteStore, *manager.Manager, *fakeRC) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "s.db"), logging.Discard())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}

	rc := &fakeRC{mu: make(chan struct{}, 1)}
	// runner 传 nil：调度器测试不涉及「同步前去重」，且这里刻意避免拉起真实 rclone 进程。
	mgr := manager.New(st, rc, nil, logging.NewRingBuffer(50), manager.Options{
		MaxConcurrent: 2,
		PollInterval:  10 * time.Millisecond,
	}, logging.Discard())
	mgr.SetBaseContext(context.Background())

	cfg := config.SchedulerConfig{
		Enabled:           true,
		Seconds:           true,
		MaxConcurrentRuns: 2,
		SkipOverlap:       true,
	}
	if mutate != nil {
		mutate(&cfg)
	}

	sch, err := New(cfg, st, mgr, logging.Discard())
	if err != nil {
		t.Fatalf("创建调度器失败: %v", err)
	}
	return sch, st, mgr, rc
}

func TestValidateExpressions(t *testing.T) {
	sch, _, _, _ := newScheduler(t, func(c *config.SchedulerConfig) { c.Seconds = true })

	valid := []string{
		"* * * * * *", // 秒级（Seconds=true）
		"0 0 3 * * *", // 每天 03:00
		"@every 1m",   // 描述符
		"@daily",      // 描述符
	}
	for _, expr := range valid {
		if err := sch.Validate(expr); err != nil {
			t.Errorf("表达式 %q 应有效: %v", expr, err)
		}
	}
	if err := sch.Validate(""); err != nil {
		t.Errorf("空表达式应视为「仅手动」，得到 %v", err)
	}

	invalid := []string{"not a cron", "0 0 3 * *", "99 * * * * *"}
	for _, expr := range invalid {
		if err := sch.Validate(expr); err == nil {
			t.Errorf("表达式 %q 应无效", expr)
		}
	}
}

func TestFiveFieldModeRejectsSecondsExpression(t *testing.T) {
	sch, _, _, _ := newScheduler(t, func(c *config.SchedulerConfig) { c.Seconds = false })
	if err := sch.Validate("0 3 * * *"); err != nil {
		t.Errorf("5 段表达式应有效: %v", err)
	}
	if err := sch.Validate("*/5 0 3 * * *"); err == nil {
		t.Error("未开启秒级精度时 6 段表达式应被拒绝")
	}
}

// 段数与配置档位不符时，报错应说明期望段数并给出切换配置的提示，
// 而不是抛一句无法定位的解析错误。
func TestSegmentCountMismatchGivesActionableHint(t *testing.T) {
	cases := []struct {
		name        string
		seconds     bool
		expr        string
		wantContain []string
	}{
		{
			name:        "开启秒级时误用 5 段",
			seconds:     true,
			expr:        "0 0 3 * *",
			wantContain: []string{"有 5 段", "需要 6 段", "scheduler.seconds: false"},
		},
		{
			name:        "关闭秒级时误用 6 段",
			seconds:     false,
			expr:        "0 0 0 3 * *",
			wantContain: []string{"有 6 段", "需要 5 段", "scheduler.seconds: true"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sch, _, _, _ := newScheduler(t, func(c *config.SchedulerConfig) { c.Seconds = tc.seconds })

			err := sch.Validate(tc.expr)
			if err == nil {
				t.Fatalf("段数不匹配的表达式 %q 应被拒绝", tc.expr)
			}
			for _, want := range tc.wantContain {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("错误信息应包含 %q，得到 %q", want, err.Error())
				}
			}

			// DescribeNext 走同一解析路径，提示应保持一致。
			if _, derr := sch.DescribeNext(tc.expr, 3); derr == nil {
				t.Errorf("DescribeNext 也应拒绝 %q", tc.expr)
			} else if !strings.Contains(derr.Error(), tc.wantContain[0]) {
				t.Errorf("DescribeNext 错误信息应包含 %q，得到 %q", tc.wantContain[0], derr.Error())
			}
		})
	}
}

func TestDescribeNext(t *testing.T) {
	sch, _, _, _ := newScheduler(t, func(c *config.SchedulerConfig) { c.Seconds = false })

	next, err := sch.DescribeNext("0 3 * * *", 3)
	if err != nil {
		t.Fatalf("DescribeNext 失败: %v", err)
	}
	if len(next) != 3 {
		t.Fatalf("期望 3 个时间点，得到 %d", len(next))
	}
	for i, ts := range next {
		if ts.Hour() != 3 || ts.Minute() != 0 || ts.Second() != 0 {
			t.Errorf("第 %d 个时间点不是 03:00:00: %v", i, ts)
		}
		if i > 0 && !ts.After(next[i-1]) {
			t.Errorf("时间点应严格递增: %v", next)
		}
	}

	if _, err := sch.DescribeNext("bad expr", 3); err == nil {
		t.Error("非法表达式应报错")
	}
	if out, err := sch.DescribeNext("", 3); err != nil || len(out) != 0 {
		t.Errorf("空表达式应返回空列表: %v %v", out, err)
	}
}

func TestReloadRegistersOnlyEnabledScheduledTasks(t *testing.T) {
	sch, st, _, _ := newScheduler(t, nil)
	ctx := context.Background()

	scheduled := &store.Task{Name: "scheduled", Kind: store.KindSync, Source: "a:", Dest: "b:",
		CronExpr: "0 0 3 * * *", Enabled: true}
	manual := &store.Task{Name: "manual", Kind: store.KindSync, Source: "a:", Dest: "b:",
		CronExpr: "", Enabled: true}
	disabled := &store.Task{Name: "disabled", Kind: store.KindSync, Source: "a:", Dest: "b:",
		CronExpr: "0 0 4 * * *", Enabled: false}
	for _, task := range []*store.Task{scheduled, manual, disabled} {
		if err := st.CreateTask(ctx, task); err != nil {
			t.Fatal(err)
		}
	}

	if err := sch.Reload(ctx); err != nil {
		t.Fatalf("Reload 失败: %v", err)
	}

	if sch.Next(scheduled.ID) == nil {
		t.Error("启用且配置了 cron 的任务应被注册")
	}
	if sch.Next(manual.ID) != nil {
		t.Error("仅手动任务不应被注册")
	}
	if sch.Next(disabled.ID) != nil {
		t.Error("禁用任务不应被注册")
	}

	stats := sch.Stats()
	if stats.Entries != 1 {
		t.Errorf("注册条目数期望 1，得到 %d", stats.Entries)
	}
	if !stats.Enabled || !stats.Seconds {
		t.Errorf("调度器状态错误: %+v", stats)
	}
	// Next 必须带上任务 ID，否则前端无法从「下一次触发」跳到对应任务。
	if stats.Next == nil {
		t.Fatal("应给出最近一次触发")
	}
	if stats.Next.TaskID != scheduled.ID || stats.Next.TaskName != "scheduled" {
		t.Errorf("Next 应指向已注册任务 %d/scheduled，得到 %+v", scheduled.ID, *stats.Next)
	}
}

func TestSyncAndRemove(t *testing.T) {
	sch, st, _, _ := newScheduler(t, nil)
	ctx := context.Background()
	if err := sch.Reload(ctx); err != nil {
		t.Fatal(err)
	}

	task := &store.Task{Name: "t", Kind: store.KindSync, Source: "a:", Dest: "b:",
		CronExpr: "0 0 3 * * *", Enabled: true}
	if err := st.CreateTask(ctx, task); err != nil {
		t.Fatal(err)
	}

	if err := sch.Sync(ctx, task); err != nil {
		t.Fatalf("Sync 失败: %v", err)
	}
	if sch.Next(task.ID) == nil {
		t.Fatal("Sync 后应存在调度条目")
	}

	// 禁用后应移除条目
	task.Enabled = false
	if err := st.UpdateTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := sch.Sync(ctx, task); err != nil {
		t.Fatal(err)
	}
	if sch.Next(task.ID) != nil {
		t.Error("禁用后应移除调度条目")
	}

	// 非法表达式应报错
	task.Enabled = true
	task.CronExpr = "invalid"
	if err := sch.Sync(ctx, task); err == nil {
		t.Error("非法表达式应返回错误")
	}

	// Remove 应清理条目与 next_run_at
	task.CronExpr = "0 0 5 * * *"
	if err := sch.Sync(ctx, task); err != nil {
		t.Fatal(err)
	}
	sch.Remove(ctx, task.ID)
	if sch.Next(task.ID) != nil {
		t.Error("Remove 后不应存在调度条目")
	}
	reloaded, _ := st.GetTask(ctx, task.ID)
	if reloaded.NextRunAt != nil {
		t.Errorf("Remove 后 next_run_at 应被清空，得到 %v", reloaded.NextRunAt)
	}
}

func TestCronActuallyFires(t *testing.T) {
	// 秒级精度下每秒触发一次，验证完整链路：cron -> 触发 -> 运行记录生成。
	sch, st, mgr, rc := newScheduler(t, func(c *config.SchedulerConfig) {
		c.Seconds = true
		c.SkipOverlap = true
	})

	ctx := context.Background()
	task := &store.Task{Name: "每秒任务", Kind: store.KindSync, Source: "a:", Dest: "b:",
		CronExpr: "* * * * * *", Enabled: true}
	if err := st.CreateTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := sch.Sync(ctx, task); err != nil {
		t.Fatal(err)
	}

	startCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := sch.Start(startCtx); err != nil {
		t.Fatalf("Start 失败: %v", err)
	}
	defer sch.Stop()

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if rc.callCount() > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if rc.callCount() == 0 {
		t.Fatal("定时任务未在 4 秒内触发")
	}

	// 应有运行记录（cron 触发）
	runs, total, err := st.ListRuns(ctx, store.RunFilter{TaskID: task.ID, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if total == 0 {
		t.Fatal("应产生运行记录")
	}
	if runs[0].Trigger != store.TriggerCron {
		t.Errorf("触发来源应为 cron，得到 %q", runs[0].Trigger)
	}

	waitFor(t, 3*time.Second, func() bool { return mgr.ActiveCount() == 0 }, "任务结束")

	// next_run_at 应被回写
	waitFor(t, 3*time.Second, func() bool {
		got, err := st.GetTask(ctx, task.ID)
		return err == nil && got.NextRunAt != nil
	}, "回写 next_run_at")
}

func TestDisabledSchedulerDoesNotRegister(t *testing.T) {
	sch, st, _, _ := newScheduler(t, func(c *config.SchedulerConfig) { c.Enabled = false })
	ctx := context.Background()

	task := &store.Task{Name: "t", Kind: store.KindSync, Source: "a:", Dest: "b:",
		CronExpr: "0 0 3 * * *", Enabled: true}
	if err := st.CreateTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := sch.Start(ctx); err != nil {
		t.Fatalf("禁用状态下 Start 不应报错: %v", err)
	}
	defer sch.Stop()

	if sch.Next(task.ID) != nil {
		t.Error("调度器禁用时不应注册条目")
	}
	if sch.Stats().Entries != 0 {
		t.Errorf("条目数应为 0，得到 %d", sch.Stats().Entries)
	}
}

func TestNewRejectsBadTimezone(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "tz.db"), logging.Discard())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()

	rc := &fakeRC{mu: make(chan struct{}, 1)}
	mgr := manager.New(st, rc, nil, nil, manager.Options{}, logging.Discard())
	if _, err := New(config.SchedulerConfig{Timezone: "Nowhere/Unknown"}, st, mgr, logging.Discard()); err == nil {
		t.Fatal("非法时区应报错")
	}
}

func TestLocationConfiguredTimezone(t *testing.T) {
	sch, _, _, _ := newScheduler(t, func(c *config.SchedulerConfig) { c.Timezone = "Asia/Shanghai" })
	if sch.Location().String() != "Asia/Shanghai" {
		t.Errorf("时区应为 Asia/Shanghai，得到 %s", sch.Location().String())
	}
	if sch.Stats().Timezone != "Asia/Shanghai" {
		t.Errorf("Stats 时区错误: %s", sch.Stats().Timezone)
	}
}

// waitFor 轮询等待条件成立。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, desc string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("等待超时: %s", desc)
}
