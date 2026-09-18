package retention

import (
	"context"
	"testing"
	"time"

	"cloudsync/internal/logging"
)

func TestRetentionPrefersUIValueOverConfig(t *testing.T) {
	st := newRetentionTestStore(t)
	ctx := context.Background()

	svc := New(st, 7*24*time.Hour, time.Hour, logging.Discard())

	// 界面没设过：用配置的 7 天。
	if d, src := svc.Retention(ctx); d != 7*24*time.Hour || src != SourceConfig {
		t.Fatalf("未设置时应回落配置值: d=%v src=%s", d, src)
	}

	if err := svc.SetRetention(ctx, 30*24*time.Hour); err != nil {
		t.Fatalf("设置保留时长失败: %v", err)
	}
	if d, src := svc.Retention(ctx); d != 30*24*time.Hour || src != SourceUI {
		t.Fatalf("界面设置应优先: d=%v src=%s", d, src)
	}

	// 0 表示不限制，但仍属于界面设置的值 —— 否则界面上选了"不限"会被配置覆盖。
	if err := svc.SetRetention(ctx, 0); err != nil {
		t.Fatalf("设置不限制失败: %v", err)
	}
	if d, src := svc.Retention(ctx); d != 0 || src != SourceUI {
		t.Fatalf("界面设为不限制时应保留该意图: d=%v src=%s", d, src)
	}
	if n, err := svc.Pending(ctx); err != nil || n != 0 {
		t.Fatalf("不限制时不应有待清理记录: n=%d err=%v", n, err)
	}
	if svc.Cutoff(ctx) != nil {
		t.Error("不限制时 Cutoff 应为 nil")
	}
}

func TestRetentionRejectsOutOfRangeValues(t *testing.T) {
	st := newRetentionTestStore(t)
	svc := New(st, 0, time.Hour, logging.Discard())

	if err := svc.SetRetention(context.Background(), 30*time.Minute); err == nil {
		t.Error("短于 1 小时的保留时长应被拒绝")
	}
	if err := svc.SetRetention(context.Background(), 100*365*24*time.Hour); err == nil {
		t.Error("超过上限的保留时长应被拒绝")
	}
	if err := svc.SetRetention(context.Background(), 24*time.Hour); err != nil {
		t.Errorf("1 天应合法: %v", err)
	}
}

func TestPruneNowRemovesExpiredOnly(t *testing.T) {
	st := newRetentionTestStore(t)
	svc := New(st, 0, time.Hour, logging.Discard())
	ctx := context.Background()

	if err := svc.SetRetention(ctx, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	// 已过期 1 条、未过期 1 条、非终态 1 条。
	st.expired = 1
	if n, err := svc.Pending(ctx); err != nil || n != 1 {
		t.Fatalf("待清理应为 1: n=%d err=%v", n, err)
	}
	if cut := svc.Cutoff(ctx); cut == nil {
		t.Fatal("设置了保留时长时 Cutoff 不应为 nil")
	} else if time.Since(*cut) < 23*time.Hour {
		t.Errorf("Cutoff 应在约一天前: %v", cut)
	}

	deleted, err := svc.PruneNow(ctx)
	if err != nil {
		t.Fatalf("清理失败: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("应清理 1 条，得到 %d", deleted)
	}
	if d, _ := svc.Retention(ctx); d <= 0 {
		t.Error("清理后策略不应被改动")
	}
}

func TestRetentionDefaultSourceWhenUnset(t *testing.T) {
	st := newRetentionTestStore(t)
	svc := New(st, 0, 0, logging.Discard())
	if got := svc.Interval(); got != time.Hour {
		t.Errorf("间隔未设置时应回落 1h，得到 %v", got)
	}
	if d, src := svc.Retention(context.Background()); d != 0 || src != SourceDefault {
		t.Errorf("无配置无设置时应为不限: d=%v src=%s", d, src)
	}
}

// ---------------------------------------------------------------------------
// 测试替身：只实现本包需要的四个能力，避免依赖真实 SQLite。
// ---------------------------------------------------------------------------

type fakeStore struct {
	values  map[string]string
	expired int64
	pruned  int64
}

func (f *fakeStore) GetSetting(_ context.Context, key string) (string, bool, error) {
	v, ok := f.values[key]
	return v, ok, nil
}

func (f *fakeStore) SetSetting(_ context.Context, key, value string) error {
	if f.values == nil {
		f.values = map[string]string{}
	}
	f.values[key] = value
	return nil
}

func (f *fakeStore) PruneExpiredRuns(_ context.Context, _ time.Time) (int64, error) {
	f.pruned = f.expired
	return f.expired, nil
}

func (f *fakeStore) CountExpiredRuns(_ context.Context, _ time.Time) (int64, error) {
	return f.expired, nil
}

func newRetentionTestStore(t *testing.T) *fakeStore {
	t.Helper()
	return &fakeStore{}
}
