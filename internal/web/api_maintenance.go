package web

import (
	"context"
	"errors"
	"net/http"
	"time"

	"cloudsync/internal/maintenance"
)

// handleCleanupDatabase 执行一次数据库瘦身。
//
// 与 /api/runs/prune（按保留策略删过期记录）的分工：
//   - prune 解决"留多久"；
//   - 这里解决"把已经占住的字节还给系统"——包含丢弃日志片段、按任务裁剪记录、
//     清理孤儿行，以及最后整理数据库文件（截断 WAL + VACUUM）。
//
// 两者共同的硬约束：运行中的记录一律不动。
func (s *Server) handleCleanupDatabase(w http.ResponseWriter, r *http.Request) {
	if s.maint == nil {
		writeErr(w, http.StatusNotImplemented, "not_enabled", "数据库维护服务未启用")
		return
	}

	var opt maintenance.Options
	if err := decodeJSON(r, &opt); err != nil {
		writeStoreErr(w, err)
		return
	}
	if err := opt.Validate(); err != nil {
		if errors.Is(err, maintenance.ErrInvalidOptions) {
			writeErr(w, http.StatusBadRequest, "invalid_options", err.Error())
			return
		}
		writeStoreErr(w, err)
		return
	}

	// VACUUM 要重写整个库，大库上可能跑很久；按项目里其它长操作的先例给足超时，
	// 免得中途被请求上下文掐断留下半途状态。
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()

	res, err := s.maint.Cleanup(ctx, opt)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	s.logger.Info("数据库瘦身完成",
		"logs_cleared", res.LogsCleared,
		"runs_pruned", res.RunsPruned,
		"reclaimed_bytes", res.ReclaimedBytes,
	)
	writeJSON(w, http.StatusOK, res)
}
