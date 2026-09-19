package web

import (
	"errors"
	"net/http"
	"time"

	"cloudsync/internal/config"
	"cloudsync/internal/retention"
	"cloudsync/internal/store"
)

// handleGetSettings 返回运行记录的保留策略与占用情况。
func (s *Server) handleGetSettings(w http.ResponseWriter, r *http.Request) {
	payload, err := s.settingsPayload(r)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, payload)
}

// handleUpdateSettings 修改保留策略。
//
// 只接受 retention_hours 一个字段，且必须用指针区分"传了 0（不限）"与
// "没传这个字段" —— 后者是客户端 bug，静默当成不限会把用户的记录清空。
func (s *Server) handleUpdateSettings(w http.ResponseWriter, r *http.Request) {
	if s.retention == nil {
		writeErr(w, http.StatusNotImplemented, "not_enabled", "保留策略服务未启用")
		return
	}
	var body struct {
		RetentionHours *int `json:"retention_hours"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeStoreErr(w, err)
		return
	}
	if body.RetentionHours == nil {
		writeErr(w, http.StatusBadRequest, "missing_field", "缺少 retention_hours（0 表示不限制）")
		return
	}

	var d time.Duration
	if *body.RetentionHours > 0 {
		d = time.Duration(*body.RetentionHours) * time.Hour
	}
	if err := s.retention.SetRetention(r.Context(), d); err != nil {
		if errors.Is(err, retention.ErrInvalidRetention) {
			writeErr(w, http.StatusBadRequest, "invalid_retention", err.Error())
			return
		}
		writeStoreErr(w, err)
		return
	}
	s.logger.Info("运行记录保留策略已更新", "retention", config.HumanizeDuration(d))

	payload, err := s.settingsPayload(r)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, payload)
}

// handlePruneRuns 立即按当前策略清理过期运行记录。
//
// 清理后做一次 VACUUM：SQLite 删行只标记空闲页，文件不会变小，
// 用户看不到磁盘回落会以为没生效。
func (s *Server) handlePruneRuns(w http.ResponseWriter, r *http.Request) {
	if s.retention == nil {
		writeErr(w, http.StatusNotImplemented, "not_enabled", "保留策略服务未启用")
		return
	}
	deleted, err := s.retention.PruneNow(r.Context())
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	vacuumed := false
	if deleted > 0 {
		if verr := s.store.Vacuum(r.Context()); verr != nil {
			s.logger.Warn("整理数据库失败", "err", verr.Error())
		} else {
			vacuumed = true
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"deleted":  deleted,
		"vacuumed": vacuumed,
	})
}

// settingsPayload 汇总设置页需要的全部信息。
func (s *Server) settingsPayload(r *http.Request) (map[string]any, error) {
	d, source := time.Duration(0), retention.SourceDefault
	pending := int64(0)
	if s.retention != nil {
		d, source = s.retention.Retention(r.Context())
		n, err := s.retention.Pending(r.Context())
		if err != nil {
			return nil, err
		}
		pending = n
	}

	var usage store.RunStorage
	if st, err := s.store.RunStorage(r.Context()); err == nil {
		usage = st
	} else {
		s.logger.Warn("统计运行记录占用失败", "err", err.Error())
	}

	// 数据库占用构成：设置页的「数据库瘦身」面板要用它告诉用户空间到底花在哪。
	db, err := s.store.DatabaseStats(r.Context())
	if err != nil {
		s.logger.Warn("统计数据库占用失败", "err", err.Error())
	}

	interval := ""
	if s.retention != nil {
		interval = s.retention.Interval().String()
	}

	return map[string]any{
		"retention_hours":   int(d.Hours()),
		"retention_seconds": int64(d.Seconds()),
		"retention_text":    config.HumanizeDuration(d),
		"retention_source":  source,
		"pending":           pending,
		"storage":           usage,
		"db":                db,
		// 每个任务的记录条数上限目前只在配置文件里（storage.history_limit），
		// 这里把它带出去给瘦身面板当默认值用。
		"history_limit": s.cfg.Storage.HistoryLimit,
		"interval":      interval,
	}, nil
}
