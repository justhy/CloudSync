package maintenance

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloudsync/internal/logging"
	"cloudsync/internal/store"
)

func newTestService(t *testing.T, busy func() int) (*Service, *store.SQLiteStore) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "maint.db")
	st, err := store.Open(path, logging.Discard())
	if err != nil {
		t.Fatalf("打开数据库失败: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	return New(st, busy, logging.Discard()), st
}

func seedTask(t *testing.T, st *store.SQLiteStore, name string) *store.Task {
	t.Helper()
	task := &store.Task{
		Name: name, Kind: store.KindSync,
		Source: "gdrive:a", Dest: "/mnt/b", Enabled: true,
	}
	if err := st.CreateTask(context.Background(), task); err != nil {
		t.Fatalf("创建任务失败: %v", err)
	}
	return task
}

// seedRun 写入一条带日志片段的运行记录。logs 为 0 表示没有日志片段。
func seedRun(t *testing.T, st *store.SQLiteStore, task *store.Task, status store.RunStatus, logs int, startedAt time.Time) *store.Run {
	t.Helper()
	run := &store.Run{
		TaskID: task.ID, TaskName: task.Name, Kind: task.Kind,
		Status: status, StartedAt: startedAt, StepTotal: 1,
	}
	for i := 0; i < logs; i++ {
		run.LogTail = append(run.LogTail, fmt.Sprintf("2026/09/19 INFO  : 第 %d 行 rclone 输出（含中文，用于验证按字节统计）", i))
	}
	if err := st.CreateRun(context.Background(), run); err != nil {
		t.Fatalf("写入运行记录失败: %v", err)
	}
	return run
}

// 只清成功记录的日志片段：失败记录的日志必须原样留着排查用。
func TestCleanupDropsOnlySuccessLogs(t *testing.T) {
	svc, st := newTestService(t, nil)
	ctx := context.Background()
	task := seedTask(t, st, "t")
	ok := seedRun(t, st, task, store.StatusSuccess, 20, time.Now().UTC())
	bad := seedRun(t, st, task, store.StatusFailed, 20, time.Now().UTC())

	before, err := svc.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before.SuccessLogRuns != 1 || before.SuccessLogBytes <= 0 {
		t.Fatalf("应统计出 1 条成功记录的日志片段，实际 %+v", before)
	}
	if before.LogTailRuns != 2 {
		t.Fatalf("应统计出 2 条带日志片段的记录，实际 %d", before.LogTailRuns)
	}

	res, err := svc.Cleanup(ctx, Options{DropSuccessLogTail: true})
	if err != nil {
		t.Fatalf("瘦身失败: %v", err)
	}
	if res.LogsCleared != 1 {
		t.Fatalf("只应清掉成功记录的日志片段，实际 %d", res.LogsCleared)
	}
	if res.After.LogTailBytes >= res.Before.LogTailBytes {
		t.Fatalf("占用应下降: %d -> %d", res.Before.LogTailBytes, res.After.LogTailBytes)
	}

	got, err := st.GetRun(ctx, ok.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.LogTail) != 0 {
		t.Fatalf("成功记录的日志片段应被清空，实际 %d 行", len(got.LogTail))
	}
	got, err = st.GetRun(ctx, bad.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.LogTail) != 20 {
		t.Fatalf("失败记录的日志片段必须保留，实际 %d 行", len(got.LogTail))
	}
}

// 「清空全部日志片段」的范围要比「只清成功」大。
func TestCleanupDropAllLogs(t *testing.T) {
	svc, st := newTestService(t, nil)
	task := seedTask(t, st, "t")
	seedRun(t, st, task, store.StatusSuccess, 5, time.Now().UTC())
	seedRun(t, st, task, store.StatusFailed, 5, time.Now().UTC())

	res, err := svc.Cleanup(context.Background(), Options{DropAllLogTail: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.LogsCleared != 2 {
		t.Fatalf("应清掉 2 条记录的日志片段，实际 %d", res.LogsCleared)
	}
	if res.After.LogTailBytes != 0 {
		t.Fatalf("清空后日志片段占用应为 0，实际 %d", res.After.LogTailBytes)
	}
}

// 按任务裁剪旧记录：每个任务只留最近 N 条。
func TestCleanupRetainsPerTask(t *testing.T) {
	svc, st := newTestService(t, nil)
	ctx := context.Background()
	kept := seedTask(t, st, "kept")
	other := seedTask(t, st, "other")
	base := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < 5; i++ {
		seedRun(t, st, kept, store.StatusSuccess, 0, base.Add(time.Duration(i)*time.Minute))
	}
	seedRun(t, st, other, store.StatusSuccess, 0, base)

	res, err := svc.Cleanup(ctx, Options{RetainPerTask: 2})
	if err != nil {
		t.Fatal(err)
	}
	if res.RunsPruned != 3 {
		t.Fatalf("应裁掉 3 条（5 条留 2 条），实际 %d", res.RunsPruned)
	}
	_, total, err := st.ListRuns(ctx, store.RunFilter{TaskID: kept.ID})
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 {
		t.Fatalf("保留条数应为 2，实际 %d", total)
	}
	// 另一个任务只有 1 条，不该被连坐。
	_, total, err = st.ListRuns(ctx, store.RunFilter{TaskID: other.ID})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("其它任务的记录不应被裁剪，实际 %d", total)
	}
}

// 有任务在运行时必须跳过磁盘整理：VACUUM 会堵住所有进度回写。
func TestCleanupSkipsReclaimWhileBusy(t *testing.T) {
	svc, st := newTestService(t, func() int { return 2 })
	task := seedTask(t, st, "t")
	seedRun(t, st, task, store.StatusSuccess, 10, time.Now().UTC())

	res, err := svc.Cleanup(context.Background(), Options{DropSuccessLogTail: true, Reclaim: true})
	if err != nil {
		t.Fatal(err)
	}
	// 删日志片段照做（不阻塞运行），但整理磁盘要跳过。
	if res.LogsCleared != 1 {
		t.Fatalf("有任务运行时仍应清理日志片段，实际 %d", res.LogsCleared)
	}
	if res.Vacuumed || res.WalTruncated {
		t.Fatal("有任务在运行时不应做整理数据库")
	}
	if res.ReclaimSkipped == "" {
		t.Fatal("跳过的原因必须带回来给用户看")
	}
}

// 整理数据库要真的把空间还回去：删掉大量日志片段 → 空闲页变多 → VACUUM 后归零。
func TestCleanupReclaimShrinksFile(t *testing.T) {
	svc, st := newTestService(t, func() int { return 0 })
	ctx := context.Background()
	task := seedTask(t, st, "t")
	base := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < 40; i++ {
		seedRun(t, st, task, store.StatusSuccess, 60, base.Add(time.Duration(i)*time.Minute))
	}

	before, err := svc.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before.LogTailBytes < 10_000 {
		t.Fatalf("测试数据太小，无法验证回收效果: %d", before.LogTailBytes)
	}

	res, err := svc.Cleanup(ctx, Options{DropSuccessLogTail: true, Reclaim: true})
	if err != nil {
		t.Fatalf("瘦身失败: %v", err)
	}
	if !res.WalTruncated || !res.Vacuumed {
		t.Fatalf("整理数据库应完成: %+v", res)
	}
	if res.After.ReclaimableBytes != 0 {
		t.Fatalf("VACUUM 后空闲页应被回收干净，实际 %d 字节", res.After.ReclaimableBytes)
	}
	if res.ReclaimedBytes <= 0 {
		t.Fatalf("应报告回收了空间，实际 %d", res.ReclaimedBytes)
	}
	if res.After.LogTailBytes != 0 {
		t.Fatalf("日志片段应被清空，实际 %d", res.After.LogTailBytes)
	}
}

// 孤儿行（任务已删除但步骤/记录残留）能被清理出来。
func TestCleanupOrphans(t *testing.T) {
	svc, st := newTestService(t, nil)
	ctx := context.Background()

	// 直接用 SQL 造孤儿：正常路径下 DeleteTask 会在同一事务里删干净，
	// 所以这里模拟手工改库/旧版本残留。
	if _, err := st.DB().ExecContext(ctx,
		`INSERT INTO runs (task_id, task_name, kind, status, started_at, log_tail)
		 VALUES (9999, 'ghost', 'sync', 'success', ?, '["x"]')`, time.Now().UTC().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().ExecContext(ctx,
		`INSERT INTO task_steps (task_id, position, name, kind, source, dest)
		 VALUES (9999, 0, '', 'sync', 'a', 'b')`); err != nil {
		t.Fatal(err)
	}

	before, err := svc.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before.OrphanRuns != 1 || before.OrphanSteps != 1 {
		t.Fatalf("应统计出 1 条孤儿记录与 1 条孤儿步骤，实际 %+v", before)
	}

	res, err := svc.Cleanup(ctx, Options{CleanOrphans: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.OrphanRuns != 1 || res.OrphanSteps != 1 {
		t.Fatalf("应清掉 1 条孤儿记录与 1 条孤儿步骤，实际 %d/%d", res.OrphanRuns, res.OrphanSteps)
	}
	if res.After.OrphanRuns != 0 || res.After.OrphanSteps != 0 {
		t.Fatalf("清理后不应再有孤儿: %+v", res.After)
	}
}

// 选项校验：什么都不选要报错（否则用户点一下"瘦身"什么也没发生还以为是 bug），
// 超出范围的保留条数也要拒绝。
func TestOptionsValidate(t *testing.T) {
	cases := []struct {
		name string
		opt  Options
		ok   bool
	}{
		{"什么都不选", Options{}, false},
		{"只清成功日志", Options{DropSuccessLogTail: true}, true},
		{"只整理数据库", Options{Reclaim: true}, true},
		{"保留条数为负", Options{RetainPerTask: -1, Reclaim: true}, false},
		{"保留条数超上限", Options{RetainPerTask: MaxRetainPerTask + 1}, false},
		{"保留条数合法", Options{RetainPerTask: 500}, true},
	}
	for _, c := range cases {
		err := c.opt.Validate()
		if c.ok && err != nil {
			t.Errorf("%s: 不应报错，实际 %v", c.name, err)
		}
		if !c.ok {
			if err == nil {
				t.Errorf("%s: 应报错", c.name)
			} else if !errors.Is(err, ErrInvalidOptions) {
				t.Errorf("%s: 错误应可被 ErrInvalidOptions 识别，实际 %v", c.name, err)
			}
		}
	}
}

// 列表接口不再返回日志片段（否则 500 条记录就是几十 MB），详情接口仍返回。
func TestListRunsOmitsLogTail(t *testing.T) {
	_, st := newTestService(t, nil)
	ctx := context.Background()
	task := seedTask(t, st, "t")
	run := seedRun(t, st, task, store.StatusSuccess, 30, time.Now().UTC())

	items, _, err := st.ListRuns(ctx, store.RunFilter{TaskID: task.ID, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("应有 1 条记录，实际 %d", len(items))
	}
	if len(items[0].LogTail) != 0 {
		t.Fatalf("列表不应携带日志片段，实际 %d 行", len(items[0].LogTail))
	}
	detail, err := st.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(detail.LogTail) != 30 {
		t.Fatalf("详情应返回完整日志片段，实际 %d 行", len(detail.LogTail))
	}
}

// 统计口径：日志片段按字节算（中文一个字 3 字节），不能按字符数。
//
// 用 100 个汉字做样本：字符数是 100 出头，字节数是 300 出头，
// 两者差 3 倍，选错口径一眼就能看出来。
func TestDatabaseStatsCountsBytesNotChars(t *testing.T) {
	_, st := newTestService(t, nil)
	ctx := context.Background()
	task := seedTask(t, st, "t")
	run := &store.Run{
		TaskID: task.ID, TaskName: task.Name, Kind: task.Kind,
		Status: store.StatusSuccess, StartedAt: time.Now().UTC(),
		LogTail: []string{strings.Repeat("中", 100)},
	}
	if err := st.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}

	got, err := st.DatabaseStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.LogTailBytes < 300 {
		t.Fatalf("100 个汉字至少 300 字节，实际统计到 %d（疑似按字符数统计）", got.LogTailBytes)
	}
}
