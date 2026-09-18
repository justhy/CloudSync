package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloudsync/internal/logging"
)

func newTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	st, err := Open(path, logging.Discard())
	if err != nil {
		t.Fatalf("打开数据库失败: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	return st
}

func sampleTask(name string) *Task {
	return &Task{
		Name:        name,
		Description: "测试任务",
		Kind:        KindSync,
		Source:      "gdrive:photos",
		Dest:        "/mnt/backup/photos",
		ExtraFlags:  map[string]any{"transfers": float64(4), "dry_run": true},
		CronExpr:    "0 3 * * *",
		Enabled:     true,
	}
}

func TestTaskCRUD(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	task := sampleTask("每日相册备份")
	if err := st.CreateTask(ctx, task); err != nil {
		t.Fatalf("创建任务失败: %v", err)
	}
	if task.ID == 0 {
		t.Fatal("创建后应回填 ID")
	}

	got, err := st.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("查询任务失败: %v", err)
	}
	if got.Name != task.Name || got.Kind != KindSync || got.Source != task.Source {
		t.Errorf("任务字段不一致: %+v", got)
	}
	if got.ExtraFlags["dry_run"] != true {
		t.Errorf("extra_flags 未正确往返: %+v", got.ExtraFlags)
	}
	if !got.Enabled {
		t.Error("enabled 应为 true")
	}

	// 名称唯一约束
	if err := st.CreateTask(ctx, sampleTask("每日相册备份")); !errors.Is(err, ErrConflict) {
		t.Fatalf("同名任务应返回 ErrConflict，得到 %v", err)
	}

	// 更新
	got.Kind = KindCopy
	got.Dest = "/mnt/other"
	got.Enabled = false
	got.CronExpr = ""
	if err := st.UpdateTask(ctx, got); err != nil {
		t.Fatalf("更新任务失败: %v", err)
	}
	reloaded, err := st.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.Kind != KindCopy || reloaded.Enabled || reloaded.CronExpr != "" {
		t.Errorf("更新未生效: %+v", reloaded)
	}

	// 列表
	list, err := st.ListTasks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("任务列表长度期望 1，得到 %d", len(list))
	}

	// 删除
	if err := st.DeleteTask(ctx, task.ID); err != nil {
		t.Fatalf("删除任务失败: %v", err)
	}
	if _, err := st.GetTask(ctx, task.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("删除后应返回 ErrNotFound，得到 %v", err)
	}
	if err := st.DeleteTask(ctx, task.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("重复删除应返回 ErrNotFound，得到 %v", err)
	}
}

func TestTaskValidation(t *testing.T) {
	cases := []struct {
		name   string
		task   Task
		expect string
	}{
		{"缺名称", Task{Kind: KindSync, Source: "a:b"}, "名称"},
		{"非法类型", Task{Name: "x", Kind: "rsync", Source: "a:b", Dest: "c:d"}, "不支持的任务类型"},
		{"缺源", Task{Name: "x", Kind: KindSync, Dest: "c:d"}, "源路径"},
		{"缺目标", Task{Name: "x", Kind: KindSync, Source: "a:b"}, "目标路径"},
		{"负超时", Task{Name: "x", Kind: KindSync, Source: "a:b", Dest: "c:d", TimeoutSeconds: -1}, "超时"},
		{"保留参数", Task{Name: "x", Kind: KindSync, Source: "a:b", Dest: "c:d",
			ExtraFlags: map[string]any{"srcFs": "override"}}, "保留参数"},
		{"保留参数_group", Task{Name: "x", Kind: KindSync, Source: "a:b", Dest: "c:d",
			ExtraFlags: map[string]any{"_group": "g"}}, "保留参数"},
		{"未知下划线参数", Task{Name: "x", Kind: KindSync, Source: "a:b", Dest: "c:d",
			ExtraFlags: map[string]any{"_internal": "g"}}, "下划线"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.task.Validate()
			if err == nil {
				t.Fatalf("期望校验失败")
			}
			if !contains(err.Error(), tc.expect) {
				t.Fatalf("错误信息应包含 %q，得到 %q", tc.expect, err.Error())
			}
		})
	}

	// purge / mkdir 不需要目标路径
	ok := Task{Name: "清理", Kind: KindPurge, Source: "s3:tmp"}
	if err := ok.Validate(); err != nil {
		t.Fatalf("purge 任务不应要求目标路径: %v", err)
	}
}

func TestRunLifecycleAndFilters(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	task := sampleTask("t1")
	if err := st.CreateTask(ctx, task); err != nil {
		t.Fatal(err)
	}

	started := time.Now().UTC().Add(-time.Minute)
	run := &Run{
		TaskID: task.ID, TaskName: task.Name, Kind: task.Kind,
		Trigger: TriggerCron, Status: StatusRunning,
		StartedAt: started, JobID: 42,
	}
	if err := st.CreateRun(ctx, run); err != nil {
		t.Fatalf("创建运行记录失败: %v", err)
	}

	fin := time.Now().UTC()
	run.Status = StatusSuccess
	run.FinishedAt = &fin
	run.DurationMS = 1234
	run.Bytes = 2048
	run.TotalBytes = 2048
	run.Percent = 100
	run.LogTail = []string{"line1", "line2"}
	if err := st.UpdateRun(ctx, run); err != nil {
		t.Fatalf("更新运行记录失败: %v", err)
	}

	got, err := st.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusSuccess || got.Bytes != 2048 || got.JobID != 42 {
		t.Errorf("运行记录字段错误: %+v", got)
	}
	if len(got.LogTail) != 2 || got.LogTail[0] != "line1" {
		t.Errorf("log_tail 未正确往返: %+v", got.LogTail)
	}
	if got.FinishedAt == nil {
		t.Fatal("finished_at 应被保存")
	}
	// 时间戳精度为毫秒
	if diff := got.StartedAt.Sub(started).Abs(); diff > time.Millisecond {
		t.Errorf("started_at 精度损失过大: %v", diff)
	}

	// 非终态过滤
	active, _, err := st.ListRuns(ctx, RunFilter{Active: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 0 {
		t.Fatalf("不应有活跃运行记录，得到 %d", len(active))
	}

	// 状态过滤 + 分页
	items, total, err := st.ListRuns(ctx, RunFilter{Status: []RunStatus{StatusSuccess}, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(items) != 1 {
		t.Fatalf("状态过滤结果错误: total=%d len=%d", total, len(items))
	}

	// 任务维度过滤
	_, total, err = st.ListRuns(ctx, RunFilter{TaskID: task.ID})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("按任务过滤期望 1 条，得到 %d", total)
	}
}

func TestInterruptStaleRuns(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	task := sampleTask("t1")
	if err := st.CreateTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	running := &Run{TaskID: task.ID, TaskName: task.Name, Kind: task.Kind,
		Trigger: TriggerCron, Status: StatusRunning, StartedAt: time.Now().UTC()}
	if err := st.CreateRun(ctx, running); err != nil {
		t.Fatal(err)
	}

	n, err := st.InterruptStaleRuns(ctx)
	if err != nil {
		t.Fatalf("清理失败: %v", err)
	}
	if n != 1 {
		t.Fatalf("期望清理 1 条，得到 %d", n)
	}

	got, err := st.GetRun(ctx, running.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusFailed {
		t.Errorf("残留记录应标记为 failed，得到 %q", got.Status)
	}
	if got.FinishedAt == nil {
		t.Error("应补充 finished_at")
	}
	if got.Message == "" {
		t.Error("应补充说明信息")
	}
}

func TestTaskRuntimeAndCounts(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	t1 := sampleTask("t1")
	t2 := sampleTask("t2")
	t2.CronExpr = ""
	if err := st.CreateTask(ctx, t1); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateTask(ctx, t2); err != nil {
		t.Fatal(err)
	}

	last := time.Now().UTC()
	next := last.Add(3 * time.Hour)
	runID := int64(7)
	if err := st.UpdateTaskRuntime(ctx, t1.ID, TaskRuntime{
		LastRunAt: &last, NextRunAt: &next, LastRunID: &runID, LastStatus: string(StatusSuccess),
	}); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetTask(ctx, t1.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.LastRunID == nil || *got.LastRunID != 7 || got.LastStatus != string(StatusSuccess) {
		t.Errorf("运行态未写入: %+v", got)
	}
	if got.NextRunAt == nil || got.NextRunAt.Sub(next).Abs() > time.Millisecond {
		t.Errorf("next_run_at 未写入: %v", got.NextRunAt)
	}

	if err := st.ClearTaskNextRun(ctx, t1.ID); err != nil {
		t.Fatal(err)
	}
	got, _ = st.GetTask(ctx, t1.ID)
	if got.NextRunAt != nil {
		t.Errorf("next_run_at 应被清空，得到 %v", got.NextRunAt)
	}

	// 统计：只统计成功的运行
	fin := time.Now().UTC()
	okRun := &Run{TaskID: t1.ID, TaskName: t1.Name, Kind: t1.Kind, Trigger: TriggerCron,
		Status: StatusSuccess, StartedAt: time.Now().UTC(), FinishedAt: &fin}
	if err := st.CreateRun(ctx, okRun); err != nil {
		t.Fatal(err)
	}
	failedRun := &Run{TaskID: t1.ID, TaskName: t1.Name, Kind: t1.Kind, Trigger: TriggerManual,
		Status: StatusFailed, StartedAt: time.Now().UTC()}
	if err := st.CreateRun(ctx, failedRun); err != nil {
		t.Fatal(err)
	}
	pendingRun := &Run{TaskID: t2.ID, TaskName: t2.Name, Kind: t2.Kind, Trigger: TriggerManual,
		Status: StatusPending, StartedAt: time.Now().UTC()}
	if err := st.CreateRun(ctx, pendingRun); err != nil {
		t.Fatal(err)
	}

	counts, err := st.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Tasks != 2 || counts.EnabledTasks != 2 || counts.ScheduledTasks != 1 {
		t.Errorf("任务统计错误: %+v", counts)
	}
	if counts.Success24h != 1 || counts.Failed24h != 1 || counts.Pending != 1 {
		t.Errorf("运行统计错误: %+v", counts)
	}
	if counts.LastRunAt == nil {
		t.Error("应有最近运行时间")
	}
}

func TestPruneRuns(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	task := sampleTask("t1")
	if err := st.CreateTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		r := &Run{TaskID: task.ID, TaskName: task.Name, Kind: task.Kind,
			Trigger: TriggerCron, Status: StatusSuccess,
			StartedAt: time.Now().UTC().Add(time.Duration(i) * time.Second)}
		if err := st.CreateRun(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	deleted, err := st.PruneRuns(ctx, 5)
	if err != nil {
		t.Fatalf("清理失败: %v", err)
	}
	if deleted != 7 {
		t.Fatalf("期望清理 7 条，得到 %d", deleted)
	}

	items, total, err := st.ListRuns(ctx, RunFilter{TaskID: task.ID, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if total != 5 || len(items) != 5 {
		t.Fatalf("保留条数错误: total=%d", total)
	}
	// 应保留最新的 5 条
	if !items[0].StartedAt.After(items[4].StartedAt) {
		t.Error("应保留最新记录且按时间倒序返回")
	}

	// 0 表示不限制
	if n, err := st.PruneRuns(ctx, 0); err != nil || n != 0 {
		t.Fatalf("limit=0 时应跳过清理: n=%d err=%v", n, err)
	}
}

// ---------------------------------------------------------------------------
// 运行记录过期清理
// ---------------------------------------------------------------------------

// seedRun 插入一条指定状态与开始时间的运行记录。
func seedRun(t *testing.T, st *SQLiteStore, task *Task, status RunStatus, startedAt time.Time) int64 {
	t.Helper()
	r := &Run{TaskID: task.ID, TaskName: task.Name, Kind: task.Kind,
		Trigger: TriggerCron, Status: status, StartedAt: startedAt}
	if err := st.CreateRun(context.Background(), r); err != nil {
		t.Fatalf("插入运行记录失败: %v", err)
	}
	return r.ID
}

func TestPruneExpiredRunsOnlyRemovesFinishedOlderThanCutoff(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	task := sampleTask("保留策略")
	if err := st.CreateTask(ctx, task); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	old := seedRun(t, st, task, StatusSuccess, now.Add(-10*24*time.Hour))
	oldFailed := seedRun(t, st, task, StatusFailed, now.Add(-5*24*time.Hour))
	recent := seedRun(t, st, task, StatusSuccess, now.Add(-time.Hour))
	// 运行中的记录即便很老也不能动：它是任务的唯一观测窗口。
	active := seedRun(t, st, task, StatusRunning, now.Add(-30*24*time.Hour))
	pending := seedRun(t, st, task, StatusPending, now.Add(-30*24*time.Hour))

	cutoff := now.Add(-3 * 24 * time.Hour)

	pendingCount, err := st.CountExpiredRuns(ctx, cutoff)
	if err != nil {
		t.Fatalf("统计待清理失败: %v", err)
	}
	if pendingCount != 2 {
		t.Fatalf("应统计到 2 条待清理记录，得到 %d", pendingCount)
	}

	deleted, err := st.PruneExpiredRuns(ctx, cutoff)
	if err != nil {
		t.Fatalf("清理失败: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("应清理 2 条，得到 %d", deleted)
	}

	for _, id := range []int64{recent, active, pending} {
		if _, err := st.GetRun(ctx, id); err != nil {
			t.Errorf("运行记录 #%d 不应被清理: %v", id, err)
		}
	}
	for _, id := range []int64{old, oldFailed} {
		if _, err := st.GetRun(ctx, id); !errors.Is(err, ErrNotFound) {
			t.Errorf("过期记录 #%d 应被清理，得到 %v", id, err)
		}
	}

	// 清理后剩余 3 条（含 2 条非终态）。
	_, total, err := st.ListRuns(ctx, RunFilter{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if total != 3 {
		t.Errorf("应剩 3 条，得到 %d", total)
	}
}

// TestPruneExpiredRunsResetsCounterWhenEmptied：保留策略把记录清光时，
// 编号应从头开始（与「清空运行记录」的行为保持一致）。
func TestPruneExpiredRunsResetsCounterWhenEmptied(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	task := sampleTask("清光")
	if err := st.CreateTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	seedRun(t, st, task, StatusSuccess, time.Now().UTC().Add(-40*24*time.Hour))

	if _, err := st.PruneExpiredRuns(ctx, time.Now().UTC()); err != nil {
		t.Fatalf("清理失败: %v", err)
	}
	next := &Run{TaskID: task.ID, TaskName: task.Name, Kind: task.Kind, Status: StatusSuccess}
	if err := st.CreateRun(ctx, next); err != nil {
		t.Fatal(err)
	}
	if next.ID != 1 {
		t.Errorf("清空后编号应从 1 开始，得到 %d", next.ID)
	}
}

func TestRunStorageReportsUsage(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	empty, err := st.RunStorage(ctx)
	if err != nil {
		t.Fatalf("统计占用失败: %v", err)
	}
	if empty.Runs != 0 || empty.OldestRunAt != nil {
		t.Errorf("空库统计错误: %+v", empty)
	}

	task := sampleTask("占用")
	if err := st.CreateTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	oldest := time.Now().UTC().Add(-72 * time.Hour)
	seedRun(t, st, task, StatusSuccess, oldest)
	seedRun(t, st, task, StatusSuccess, time.Now().UTC())

	usage, err := st.RunStorage(ctx)
	if err != nil {
		t.Fatalf("统计占用失败: %v", err)
	}
	if usage.Runs != 2 {
		t.Errorf("记录数应为 2，得到 %d", usage.Runs)
	}
	if usage.OldestRunAt == nil {
		t.Fatal("应返回最早记录时间")
	}
	if diff := usage.OldestRunAt.Sub(oldest); diff > time.Second || diff < -time.Second {
		t.Errorf("最早记录时间错误: %v（期望 %v）", usage.OldestRunAt, oldest)
	}
	if usage.DBSizeBytes <= 0 {
		t.Errorf("应返回数据库文件大小，得到 %d", usage.DBSizeBytes)
	}
}

func TestSettingsRoundTrip(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	if _, ok, err := st.GetSetting(ctx, "run_retention_seconds"); err != nil || ok {
		t.Fatalf("未设置时应返回 ok=false: ok=%v err=%v", ok, err)
	}
	if err := st.SetSetting(ctx, "run_retention_seconds", "2592000"); err != nil {
		t.Fatalf("写入设置失败: %v", err)
	}
	v, ok, err := st.GetSetting(ctx, "run_retention_seconds")
	if err != nil || !ok || v != "2592000" {
		t.Fatalf("读取设置错误: v=%q ok=%v err=%v", v, ok, err)
	}
	// 覆盖写入
	if err := st.SetSetting(ctx, "run_retention_seconds", "0"); err != nil {
		t.Fatalf("覆盖设置失败: %v", err)
	}
	if v, _, _ = st.GetSetting(ctx, "run_retention_seconds"); v != "0" {
		t.Fatalf("覆盖后应读到 0，得到 %q", v)
	}
}

func TestRunFilterLimitClamping(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	task := sampleTask("t1")
	if err := st.CreateTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	items, _, err := st.ListRuns(ctx, RunFilter{TaskID: task.ID, Limit: 99999})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("空结果集应返回 0 条，得到 %d", len(items))
	}
}

func contains(haystack, needle string) bool { return strings.Contains(haystack, needle) }

// ---------------------------------------------------------------------------
// 运行记录删除

func TestDeleteRunAndDeleteAllRuns(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	task := sampleTask("t")
	if err := st.CreateTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for i := 0; i < 3; i++ {
		r := &Run{TaskID: task.ID, TaskName: task.Name, Kind: task.Kind,
			Status: StatusSuccess, StartedAt: time.Now().UTC()}
		if err := st.CreateRun(ctx, r); err != nil {
			t.Fatalf("写入运行记录失败: %v", err)
		}
		ids = append(ids, r.ID)
	}

	// 删除单条
	if err := st.DeleteRun(ctx, ids[0]); err != nil {
		t.Fatalf("删除运行记录失败: %v", err)
	}
	if _, err := st.GetRun(ctx, ids[0]); !errors.Is(err, ErrNotFound) {
		t.Errorf("删除后应返回 ErrNotFound，得到 %v", err)
	}
	// 不存在的记录要能被识别出来，否则 HTTP 层没法返回 404
	if err := st.DeleteRun(ctx, 99999); !errors.Is(err, ErrNotFound) {
		t.Errorf("删除不存在的记录应返回 ErrNotFound，得到 %v", err)
	}

	// 清空剩余两条
	n, err := st.DeleteAllRuns(ctx)
	if err != nil {
		t.Fatalf("清空失败: %v", err)
	}
	if n != 2 {
		t.Fatalf("期望清空 2 条，得到 %d", n)
	}
	_, total, err := st.ListRuns(ctx, RunFilter{TaskID: task.ID})
	if err != nil {
		t.Fatal(err)
	}
	if total != 0 {
		t.Errorf("清空后应为 0 条，得到 %d", total)
	}

	// 再清一次应删 0 条而不是报错
	if n, err := st.DeleteAllRuns(ctx); err != nil || n != 0 {
		t.Fatalf("空表清空应返回 0：n=%d err=%v", n, err)
	}

	// 全删之后编号要回到 1：runs 用 AUTOINCREMENT，不清 sqlite_sequence 的话
	// 下一条会接着历史最大值往下编，用户看到的就不是"从头开始"。
	again := &Run{TaskID: task.ID, TaskName: task.Name, Kind: task.Kind,
		Status: StatusSuccess, StartedAt: time.Now().UTC()}
	if err := st.CreateRun(ctx, again); err != nil {
		t.Fatalf("清空后写入运行记录失败: %v", err)
	}
	if again.ID != 1 {
		t.Errorf("清空后新记录应从 1 开始编号，得到 %d", again.ID)
	}
}

// TestDeleteAllRunsKeepsCounterWhenNotEmptied 覆盖另一种情况：只删一部分时
// 编号必须继续递增，不能被"清空重置"的分支误伤。
func TestDeleteAllRunsKeepsCounterWhenNotEmptied(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	task := sampleTask("t")
	if err := st.CreateTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for i := 0; i < 3; i++ {
		r := &Run{TaskID: task.ID, TaskName: task.Name, Kind: task.Kind,
			Status: StatusSuccess, StartedAt: time.Now().UTC()}
		if err := st.CreateRun(ctx, r); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, r.ID)
	}

	// 只删中间一条，不清空。
	if err := st.DeleteRun(ctx, ids[1]); err != nil {
		t.Fatal(err)
	}
	next := &Run{TaskID: task.ID, TaskName: task.Name, Kind: task.Kind,
		Status: StatusSuccess, StartedAt: time.Now().UTC()}
	if err := st.CreateRun(ctx, next); err != nil {
		t.Fatal(err)
	}
	if next.ID <= ids[len(ids)-1] {
		t.Errorf("未清空时编号应继续递增，历史最大 %d，新记录 %d", ids[len(ids)-1], next.ID)
	}
}

// ---------------------------------------------------------------------------
// 任务编号复用

// TestTaskIDReuseAfterDelete 覆盖「删掉中间的任务后编号要补洞」：
// 建 1/2/3 → 删 1 → 新建应拿回 1；再删 2 → 新建拿回 2。
func TestTaskIDReuseAfterDelete(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	var a, b, c *Task
	for i, x := range []**Task{&a, &b, &c} {
		task := sampleTask(fmt.Sprintf("任务-%d", i+1))
		if err := st.CreateTask(ctx, task); err != nil {
			t.Fatal(err)
		}
		*x = task
	}
	if a.ID != 1 || b.ID != 2 || c.ID != 3 {
		t.Fatalf("初始编号应为 1/2/3，得到 %d/%d/%d", a.ID, b.ID, c.ID)
	}

	if err := st.DeleteTask(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	reuse1 := sampleTask("补洞-1")
	if err := st.CreateTask(ctx, reuse1); err != nil {
		t.Fatal(err)
	}
	if reuse1.ID != 1 {
		t.Errorf("删除 1 号后新建应复用 1，得到 %d", reuse1.ID)
	}

	if err := st.DeleteTask(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	reuse2 := sampleTask("补洞-2")
	if err := st.CreateTask(ctx, reuse2); err != nil {
		t.Fatal(err)
	}
	if reuse2.ID != 2 {
		t.Errorf("删除 2 号后新建应复用 2，得到 %d", reuse2.ID)
	}

	// 没有空洞时继续往后发号。
	tail := sampleTask("顺延")
	if err := st.CreateTask(ctx, tail); err != nil {
		t.Fatal(err)
	}
	if tail.ID != 4 {
		t.Errorf("无空洞时应顺延到 4，得到 %d", tail.ID)
	}

	list, err := st.ListTasks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{1, 2, 3, 4}
	if len(list) != len(want) {
		t.Fatalf("任务数应为 %d，得到 %d", len(want), len(list))
	}
	for i, x := range list {
		if x.ID != want[i] {
			t.Errorf("列表第 %d 项 ID 应为 %d，得到 %d（列表未按 ID 排序？）", i, want[i], x.ID)
		}
	}
}

// TestDeleteTaskCascadesRuns：任务编号会被复用，如果历史运行记录留着，
// 复用后的新任务会凭空多出一堆"上一次运行"记录，必须一并删掉。
func TestDeleteTaskCascadesRuns(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	task := sampleTask("带历史的")
	if err := st.CreateTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	r := &Run{TaskID: task.ID, TaskName: task.Name, Kind: task.Kind,
		Status: StatusSuccess, StartedAt: time.Now().UTC()}
	if err := st.CreateRun(ctx, r); err != nil {
		t.Fatal(err)
	}

	if err := st.DeleteTask(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetRun(ctx, r.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("删除任务应连带清除其运行记录，得到 %v", err)
	}

	// 其它任务的历史不能被误删。
	other := sampleTask("另一个")
	if err := st.CreateTask(ctx, other); err != nil {
		t.Fatal(err)
	}
	keep := &Run{TaskID: other.ID, TaskName: other.Name, Kind: other.Kind,
		Status: StatusSuccess, StartedAt: time.Now().UTC()}
	if err := st.CreateRun(ctx, keep); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteTask(ctx, taskNextID(st, ctx)); err == nil {
		t.Error("删除不存在的任务应返回错误")
	}
	if _, err := st.GetRun(ctx, keep.ID); err != nil {
		t.Errorf("其它任务的运行记录不应被删除: %v", err)
	}
}

// taskNextID 返回一个当前不存在的任务编号，用于验证删除的 NotFound 分支。
func taskNextID(st *SQLiteStore, ctx context.Context) int64 {
	list, err := st.ListTasks(ctx)
	if err != nil {
		return 99999
	}
	var max int64
	for _, x := range list {
		if x.ID > max {
			max = x.ID
		}
	}
	return max + 100
}

// ---------------------------------------------------------------------------
// 清理目标重名（dedupe_before）

func TestTaskDedupeBeforeRoundTrip(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	task := sampleTask("去重开关")
	task.DedupeBefore = true
	if err := st.CreateTask(ctx, task); err != nil {
		t.Fatalf("创建任务失败: %v", err)
	}

	got, err := st.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("查询任务失败: %v", err)
	}
	if !got.DedupeBefore {
		t.Error("dedupe_before=true 未正确落库（新增字段若漏掉列清单，这里会静默变成 false）")
	}

	// 关闭后必须能回写为 false，否则用户永远关不掉这个开关。
	got.DedupeBefore = false
	if err := st.UpdateTask(ctx, got); err != nil {
		t.Fatalf("更新任务失败: %v", err)
	}
	got2, err := st.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("查询任务失败: %v", err)
	}
	if got2.DedupeBefore {
		t.Error("dedupe_before 关闭后未回写")
	}

	// ListTasks 走的是同一份列清单，也要覆盖。
	list, err := st.ListTasks(ctx)
	if err != nil {
		t.Fatalf("列表查询失败: %v", err)
	}
	for _, x := range list {
		if x.ID == task.ID && x.DedupeBefore {
			t.Error("列表接口返回的 dedupe_before 不一致")
		}
	}
}

// TestDedupeBeforeKindWhitelist 保证「清理目标重名」的类型白名单不只是前端约束。
// 前端隐藏可以被直接调 API 绕过，所以约束必须落在 Validate 上。
//
// 白名单是 sync/copy/move：这三种都会把源端对象写入目标端，才存在"目标同名
// 重复堆积"的问题；bisync/check/delete 等不写目标或没有源→目标比对语义，开这
// 个选项没有意义，必须被拒。
func TestDedupeBeforeKindWhitelist(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	rejected := []TaskKind{KindBisync, KindCheck, KindDelete}
	for _, kind := range rejected {
		task := sampleTask("去重-" + string(kind))
		task.Kind = kind
		task.DedupeBefore = true
		if err := st.CreateTask(ctx, task); err == nil {
			t.Errorf("任务类型 %s 不应允许「清理目标重名」", kind)
		}
	}

	// 对照组：sync/copy/move 必须放行。
	for _, kind := range []TaskKind{KindSync, KindCopy, KindMove} {
		ok := sampleTask("去重-" + string(kind))
		ok.Kind = kind
		ok.DedupeBefore = true
		if err := st.CreateTask(ctx, ok); err != nil {
			t.Errorf("%s 应允许「清理目标重名」，得到 %v", kind, err)
		}
	}
}

func TestAllowsDedupeBefore(t *testing.T) {
	allowed := map[TaskKind]bool{
		KindSync:   true,
		KindCopy:   true,
		KindMove:   true,
		KindBisync: false,
		KindCheck:  false,
		KindDelete: false,
	}
	for kind, want := range allowed {
		if got := kind.AllowsDedupeBefore(); got != want {
			t.Errorf("%s.AllowsDedupeBefore() = %v，期望 %v", kind, got, want)
		}
	}
}

// TestMigrateAddsDedupeBeforeToExistingDB 覆盖升级路径：
// CREATE TABLE IF NOT EXISTS 不会给已存在的表补列，漏了 ALTER 的话老库会直接
// 报 "no such column: dedupe_before"。
func TestMigrateAddsDedupeBeforeToExistingDB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	st, err := Open(path, logging.Discard())
	if err != nil {
		t.Fatalf("打开数据库失败: %v", err)
	}
	defer func() { _ = st.Close() }()
	ctx := context.Background()

	// 手工建一个"升级前"的 tasks 表：唯一差别是没有 dedupe_before。
	const legacyTasks = `CREATE TABLE IF NOT EXISTS tasks (
		id              INTEGER PRIMARY KEY AUTOINCREMENT,
		name            TEXT    NOT NULL UNIQUE,
		description     TEXT    NOT NULL DEFAULT '',
		kind            TEXT    NOT NULL,
		source          TEXT    NOT NULL,
		dest            TEXT    NOT NULL DEFAULT '',
		extra_flags     TEXT    NOT NULL DEFAULT '{}',
		cron_expr       TEXT    NOT NULL DEFAULT '',
		timeout_seconds INTEGER NOT NULL DEFAULT 0,
		enabled         INTEGER NOT NULL DEFAULT 1,
		created_at      INTEGER NOT NULL,
		updated_at      INTEGER NOT NULL,
		last_run_at     INTEGER,
		next_run_at     INTEGER,
		last_run_id     INTEGER,
		last_status     TEXT    NOT NULL DEFAULT ''
	);`
	if _, err := st.db.ExecContext(ctx, legacyTasks); err != nil {
		t.Fatalf("构造旧表失败: %v", err)
	}

	// 迁移必须幂等：连跑两次都不应报错。
	for i := 0; i < 2; i++ {
		if err := st.Migrate(ctx); err != nil {
			t.Fatalf("第 %d 次迁移失败: %v", i+1, err)
		}
	}

	task := sampleTask("升级后的任务")
	task.DedupeBefore = true
	if err := st.CreateTask(ctx, task); err != nil {
		t.Fatalf("老库升级后写入 dedupe_before 失败（迁移未生效）: %v", err)
	}
	got, err := st.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("查询任务失败: %v", err)
	}
	if !got.DedupeBefore {
		t.Error("老库升级后 dedupe_before 未正确读出")
	}
}
