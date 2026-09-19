// Package maintenance 负责数据库的「瘦身」：在不破坏运行记录结构的前提下把
// 占住的字节还给操作系统。
//
// 与 retention 的分工：
//   - retention 解决"留多久"（策略问题）：按时间淘汰过期的运行记录；
//   - maintenance 解决"怎么把空间拿回来"（工程问题）：删行只是把页标记为空闲，
//     文件并不会变小，而且日志片段往往是整个库里最大的一块占用。
//
// 收益从大到小：
//  1. 丢弃运行记录里的 rclone 日志片段（log_tail，每条最多 200 行）；
//  2. 按任务裁剪旧记录、清理孤儿行；
//  3. VACUUM + 截断 WAL —— 不做这一步，前面删掉的空间不会体现在文件大小上。
//
// 硬约束：只动已结束的记录。pending/running 是正在执行任务的唯一观测窗口，
// 而且整理数据库（VACUUM）要重写整个文件，绝不能在有任务运行时做。
package maintenance

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"cloudsync/internal/store"
)

// MinRetainPerTask 是"每个任务保留条数"的下限。
//
// 允许 1 是有意的：用户想只留最后一次结果是完全合理的诉求，
// 但 0 与负数会被当成"不裁剪"（沿用 PruneRuns 的约定），避免误删全部。
const MinRetainPerTask = 1

// MaxRetainPerTask 是"每个任务保留条数"的上限，防止手滑填出一个天文数字
// 却以为清干净了。
const MaxRetainPerTask = 1_000_000

// ErrInvalidOptions 表示清理选项非法（HTTP 层据此返回 400）。
var ErrInvalidOptions = errors.New("清理选项非法")

// Options 描述一次瘦身要做什么。
type Options struct {
	// DropSuccessLogTail 丢弃成功记录的日志片段（保留失败/取消的，便于排查）。
	DropSuccessLogTail bool `json:"drop_success_log_tail"`
	// DropAllLogTail 丢弃全部记录的日志片段。与 DropSuccessLogTail 同时为真时
	// 以本项为准（范围更大）。
	DropAllLogTail bool `json:"drop_all_log_tail"`
	// RetainPerTask > 0 时，每个任务只保留最近这么多条运行记录。
	RetainPerTask int `json:"retain_per_task"`
	// CleanOrphans 清理指向已不存在任务的步骤定义与运行记录。
	CleanOrphans bool `json:"clean_orphans"`
	// Reclaim 整理数据库（截断 WAL + VACUUM）把空间真正还给系统。
	Reclaim bool `json:"reclaim"`
}

// Validate 校验选项。
func (o Options) Validate() error {
	if o.RetainPerTask < 0 {
		return fmt.Errorf("每个任务保留条数不能为负数: %w", ErrInvalidOptions)
	}
	if o.RetainPerTask > MaxRetainPerTask {
		return fmt.Errorf("每个任务保留条数最多 %d: %w", MaxRetainPerTask, ErrInvalidOptions)
	}
	if !o.DropSuccessLogTail && !o.DropAllLogTail && o.RetainPerTask == 0 && !o.CleanOrphans && !o.Reclaim {
		return fmt.Errorf("没有选择任何清理项: %w", ErrInvalidOptions)
	}
	return nil
}

// Result 是一次瘦身的结果与前后对比。
type Result struct {
	Before store.DBStats `json:"before"`
	After  store.DBStats `json:"after"`

	LogsCleared int64 `json:"logs_cleared"`
	RunsPruned  int64 `json:"runs_pruned"`
	OrphanSteps int64 `json:"orphan_steps_cleared"`
	OrphanRuns  int64 `json:"orphan_runs_cleared"`

	WalTruncated bool `json:"wal_truncated"`
	Vacuumed     bool `json:"vacuumed"`
	// ReclaimSkipped 非空说明"整理磁盘"这一步没做（有任务在运行或失败了），
	// 文字直接给用户看。
	ReclaimSkipped string `json:"reclaim_skipped,omitempty"`
	// ReclaimedBytes 是数据库总占用（主库 + WAL + SHM）的净减少量。
	ReclaimedBytes int64 `json:"reclaimed_bytes"`
	DurationMS     int64 `json:"duration_ms"`
}

// Store 是本包需要的存储能力。
type Store interface {
	DatabaseStats(ctx context.Context) (store.DBStats, error)
	ClearRunLogTail(ctx context.Context, onlySuccess bool) (int64, error)
	PruneRuns(ctx context.Context, perTaskLimit int) (int64, error)
	DeleteOrphanSteps(ctx context.Context) (int64, error)
	DeleteOrphanRuns(ctx context.Context) (int64, error)
	Vacuum(ctx context.Context) error
	CheckpointWAL(ctx context.Context) error
}

// Service 提供数据库瘦身。
type Service struct {
	st     Store
	busy   func() int
	logger *slog.Logger
}

// New 创建服务。busy 返回当前运行中的任务数，可为 nil（视为总是空闲）。
func New(st Store, busy func() int, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{st: st, busy: busy, logger: logger.With("component", "maintenance")}
}

// Stats 返回数据库占用构成的快照。
func (s *Service) Stats(ctx context.Context) (store.DBStats, error) {
	return s.st.DatabaseStats(ctx)
}

// Cleanup 执行一次瘦身。
//
// 删除类操作（清日志片段、裁剪记录、清孤儿）先做，回收类操作（WAL 截断、
// VACUUM）最后做——顺序反了的话 VACUUM 会把刚标记空闲的页再写一遍。
func (s *Service) Cleanup(ctx context.Context, opt Options) (Result, error) {
	var res Result
	if err := opt.Validate(); err != nil {
		return res, err
	}
	start := time.Now()

	before, err := s.st.DatabaseStats(ctx)
	if err != nil {
		return res, err
	}
	res.Before = before

	switch {
	case opt.DropAllLogTail:
		n, cerr := s.st.ClearRunLogTail(ctx, false)
		if cerr != nil {
			return res, cerr
		}
		res.LogsCleared = n
	case opt.DropSuccessLogTail:
		n, cerr := s.st.ClearRunLogTail(ctx, true)
		if cerr != nil {
			return res, cerr
		}
		res.LogsCleared = n
	}

	if opt.RetainPerTask > 0 {
		n, cerr := s.st.PruneRuns(ctx, opt.RetainPerTask)
		if cerr != nil {
			return res, cerr
		}
		res.RunsPruned = n
	}

	if opt.CleanOrphans {
		steps, cerr := s.st.DeleteOrphanSteps(ctx)
		if cerr != nil {
			return res, cerr
		}
		runs, cerr := s.st.DeleteOrphanRuns(ctx)
		if cerr != nil {
			return res, cerr
		}
		res.OrphanSteps, res.OrphanRuns = steps, runs
	}

	if opt.Reclaim {
		s.reclaim(ctx, &res)
	}

	after, err := s.st.DatabaseStats(ctx)
	if err != nil {
		return res, err
	}
	res.After = after
	if diff := before.TotalBytes - after.TotalBytes; diff > 0 {
		res.ReclaimedBytes = diff
	}
	res.DurationMS = time.Since(start).Milliseconds()

	s.logger.Info("数据库瘦身完成",
		"logs_cleared", res.LogsCleared,
		"runs_pruned", res.RunsPruned,
		"orphans", res.OrphanSteps+res.OrphanRuns,
		"wal_truncated", res.WalTruncated,
		"vacuumed", res.Vacuumed,
		"reclaimed_bytes", res.ReclaimedBytes,
		"duration_ms", res.DurationMS,
	)
	return res, nil
}

// reclaim 截断 WAL 并整理数据库。
//
// 有任务在运行时整段跳过：VACUUM 要重写整个库，而本程序只用一条数据库连接，
// 期间所有进度回写都会被堵住——用户看到的是"传输卡住了"，比多占几 MB 严重得多。
func (s *Service) reclaim(ctx context.Context, res *Result) {
	if s.busy != nil {
		if n := s.busy(); n > 0 {
			res.ReclaimSkipped = fmt.Sprintf("仍有 %d 个任务在运行，已跳过磁盘整理", n)
			return
		}
	}

	if err := s.st.CheckpointWAL(ctx); err != nil {
		s.logger.Warn("截断 WAL 失败", "err", err.Error())
		res.ReclaimSkipped = "截断 WAL 失败：" + err.Error()
	} else {
		res.WalTruncated = true
	}

	if err := s.st.Vacuum(ctx); err != nil {
		s.logger.Warn("整理数据库失败", "err", err.Error())
		if res.ReclaimSkipped == "" {
			res.ReclaimSkipped = "整理数据库失败：" + err.Error()
		}
		return
	}
	res.Vacuumed = true
}
