// Package retention 负责运行记录（含其中的 rclone 日志片段）的过期清理。
//
// 运行记录存在 SQLite 里，每条还带着一段 rclone 输出；长期不管的话数据库会
// 一直涨。这里提供两件事：
//  1. 可配置的保留时长（界面设置 > 配置文件，界面改过就以界面为准）；
//  2. 后台定时清理 + 手动立即清理。
//
// 硬约束：只清理已结束（success/failed/canceled）的记录。pending/running 是
// 正在执行任务的唯一观测窗口，把它删掉比多占几 MB 磁盘严重得多。
package retention

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"cloudsync/internal/config"
	"cloudsync/internal/logging"
)

// KeyRunRetention 是保留时长在 settings 表中的键，值为秒（字符串）。
const KeyRunRetention = "run_retention_seconds"

// ErrInvalidRetention 表示保留时长超出允许范围（HTTP 层据此返回 400）。
var ErrInvalidRetention = errors.New("保留时长超出允许范围")

// 保留时长的来源，用于界面提示"这个值是从哪来的"。
const (
	SourceUI      = "ui"      // 界面设置（存在数据库里）
	SourceConfig  = "config"  // 配置文件 / 环境变量
	SourceDefault = "default" // 未设置任何值
)

// Store 是本包需要的存储能力。
type Store interface {
	GetSetting(ctx context.Context, key string) (string, bool, error)
	SetSetting(ctx context.Context, key, value string) error
	PruneExpiredRuns(ctx context.Context, cutoff time.Time) (int64, error)
	CountExpiredRuns(ctx context.Context, cutoff time.Time) (int64, error)
}

// Service 是运行记录保留策略服务。
type Service struct {
	st       Store
	fallback time.Duration
	interval time.Duration
	logger   *slog.Logger
}

// New 创建服务。fallback 是数据库未设置过值时使用的配置值（<=0 表示不限制）。
func New(st Store, fallback, interval time.Duration, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	if interval <= 0 {
		interval = time.Hour
	}
	return &Service{st: st, fallback: fallback, interval: interval, logger: logger.With("component", "retention")}
}

// Interval 返回后台清理间隔。
func (s *Service) Interval() time.Duration { return s.interval }

// Retention 返回当前生效的保留时长（<=0 表示不限制）及其来源。
//
// 优先级：界面设置（数据库）> 配置文件。反过来的话，用户在界面上改的值会在
// 下次重启被配置文件覆盖回去，看起来就像"设置没保存"。
func (s *Service) Retention(ctx context.Context) (time.Duration, string) {
	raw, ok, err := s.st.GetSetting(ctx, KeyRunRetention)
	if err != nil {
		s.logger.Warn("读取保留策略失败，回落到配置值", logging.Err(err))
	}
	if ok && err == nil {
		if sec, cerr := strconv.ParseInt(raw, 10, 64); cerr == nil {
			return time.Duration(sec) * time.Second, SourceUI
		}
		s.logger.Warn("保留策略取值非法，回落到配置值", "raw", raw)
	}
	if s.fallback > 0 {
		return s.fallback, SourceConfig
	}
	return 0, SourceDefault
}

// SetRetention 写入保留时长；d<=0 表示不限制。
func (s *Service) SetRetention(ctx context.Context, d time.Duration) error {
	if d > 0 && d < config.MinRunRetention {
		return fmt.Errorf("保留时长最短为 %s: %w", config.HumanizeDuration(config.MinRunRetention), ErrInvalidRetention)
	}
	if d > config.MaxRunRetention {
		return fmt.Errorf("保留时长最长为 %s: %w", config.HumanizeDuration(config.MaxRunRetention), ErrInvalidRetention)
	}
	sec := int64(0)
	if d > 0 {
		sec = int64(d / time.Second)
	}
	return s.st.SetSetting(ctx, KeyRunRetention, strconv.FormatInt(sec, 10))
}

// Pending 返回按当前策略将会被清理的记录条数（不限制时为 0）。
func (s *Service) Pending(ctx context.Context) (int64, error) {
	d, _ := s.Retention(ctx)
	if d <= 0 {
		return 0, nil
	}
	return s.st.CountExpiredRuns(ctx, time.Now().UTC().Add(-d))
}

// Cutoff 返回当前策略对应的截止时间点；不限制时返回 nil。
func (s *Service) Cutoff(ctx context.Context) *time.Time {
	d, _ := s.Retention(ctx)
	if d <= 0 {
		return nil
	}
	t := time.Now().UTC().Add(-d)
	return &t
}

// PruneNow 立即按当前策略清理一次，返回删除条数。
func (s *Service) PruneNow(ctx context.Context) (int64, error) {
	d, src := s.Retention(ctx)
	if d <= 0 {
		return 0, nil
	}
	n, err := s.st.PruneExpiredRuns(ctx, time.Now().UTC().Add(-d))
	if err != nil {
		return 0, err
	}
	if n > 0 {
		s.logger.Info("已按保留策略清理运行记录",
			"deleted", n, "retention", d.String(), "source", src)
	}
	return n, nil
}

// Run 启动后台清理循环，阻塞直到 ctx 取消。
//
// 启动时先清一次：常见场景是停机很久后再开机，库里已经堆了一批过期记录，
// 不希望还要等一个 interval 才生效。
func (s *Service) Run(ctx context.Context) {
	s.sweep(ctx)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sweep(ctx)
		}
	}
}

func (s *Service) sweep(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, err := s.PruneNow(ctx); err != nil {
		s.logger.Warn("定期清理运行记录失败", logging.Err(err))
	}
}
