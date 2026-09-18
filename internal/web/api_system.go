package web

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cloudsync/internal/manager"
	"cloudsync/internal/rclone"
	"cloudsync/internal/store"
)

// handleOverview 汇总仪表盘所需的全部信息。
func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	counts, err := s.store.Counts(ctx)
	if err != nil {
		writeStoreErr(w, err)
		return
	}

	info := s.rclone.Info()
	active := s.collectActive(ctx)

	payload := map[string]any{
		"counts":      counts,
		"scheduler":   s.scheduler.Stats(),
		"rclone":      info,
		"running":     active,
		"server_time": time.Now().UTC().Format(time.RFC3339),
		"timezone":    s.scheduler.Location().String(),
		"version":     Version,
	}
	writeJSON(w, http.StatusOK, payload)
}

// collectActive 汇总运行中的任务及其实时进度。
func (s *Server) collectActive(ctx context.Context) []map[string]any {
	ids := s.manager.ActiveRunIDs()
	out := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		run, err := s.store.GetRun(ctx, id)
		if err != nil {
			continue
		}
		out = append(out, map[string]any{
			"run":      run,
			"progress": progressOf(run),
		})
	}
	return out
}

// handleRcloneStatus 返回 rcd 子进程状态与实时统计。
func (s *Server) handleRcloneStatus(w http.ResponseWriter, r *http.Request) {
	info := s.rclone.Info()

	payload := map[string]any{
		"status": info,
	}
	if info.Ready {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if stats, err := s.rclone.Client().Stats(ctx, ""); err == nil {
			payload["stats"] = stats
		}
		if mem, err := s.rclone.Client().MemStats(ctx); err == nil {
			payload["memstats"] = mem
		}
		if groups, err := s.rclone.Client().GroupList(ctx); err == nil {
			payload["groups"] = groups
		}
	}
	writeJSON(w, http.StatusOK, payload)
}

// handleRcloneRestart 重启 rcd 子进程。
func (s *Server) handleRcloneRestart(w http.ResponseWriter, r *http.Request) {
	if n := s.manager.ActiveCount(); n > 0 {
		writeJSON(w, http.StatusConflict, apiError{
			Code:  "tasks_running",
			Error: "仍有 " + strconv.Itoa(n) + " 个任务在运行，请先取消后再重启 rclone",
		})
		return
	}

	s.logger.Info("手动重启 rclone rcd")
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	if err := s.rclone.Restart(ctx); err != nil {
		if errors.Is(err, rclone.ErrExternalRestart) {
			// 外部托管属于状态冲突，交给前端据 status.external 决定是否隐藏入口。
			writeErr(w, http.StatusConflict, "external_managed", err.Error())
			return
		}
		writeErr(w, http.StatusInternalServerError, "restart_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "status": s.rclone.Info()})
}

// handleRemotes 返回 rclone 配置中的 remote 列表。
func (s *Server) handleRemotes(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	remotes, err := s.rclone.Client().ListRemotes(ctx)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "rclone_error", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"remotes": remotes, "total": len(remotes)})
}

// handleRcloneLog 返回 rclone 子进程最近的输出。
func (s *Server) handleRcloneLog(w http.ResponseWriter, r *http.Request) {
	tail := queryInt(r, "tail", 300)
	if tail <= 0 {
		tail = 300
	}
	if tail > 5000 {
		tail = 5000
	}
	lines := s.rclone.Journal().Tail(tail)
	writeJSON(w, http.StatusOK, map[string]any{
		"lines": lines,
		"total": s.rclone.Journal().Len(),
		"tail":  tail,
	})
}

// handleBrowse 列出远端目录，便于在 UI 中选择源/目标路径。
func (s *Server) handleBrowse(w http.ResponseWriter, r *http.Request) {
	fsName := strings.TrimSpace(r.URL.Query().Get("fs"))
	if fsName == "" {
		writeErr(w, http.StatusBadRequest, "missing_fs", "缺少 fs 参数，例如 fs=myremote:bucket")
		return
	}
	remote := strings.TrimSpace(r.URL.Query().Get("remote"))
	recurse := queryBool(r, "recurse")

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	items, err := s.rclone.Client().List(ctx, fsName, remote, recurse)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "rclone_error", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items": items,
		"total": len(items),
		"fs":    fsName,
		"path":  remote,
	})
}

// handleAbout 查询远端容量。
func (s *Server) handleAbout(w http.ResponseWriter, r *http.Request) {
	fsName := strings.TrimSpace(r.URL.Query().Get("fs"))
	if fsName == "" {
		writeErr(w, http.StatusBadRequest, "missing_fs", "缺少 fs 参数")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	about, err := s.rclone.Client().About(ctx, fsName)
	if err != nil {
		// 部分后端不支持 about，属于可预期情况。
		if errors.Is(err, context.DeadlineExceeded) {
			writeErr(w, http.StatusGatewayTimeout, "timeout", "查询容量超时")
			return
		}
		writeErr(w, http.StatusBadGateway, "unsupported", "该远端不支持容量查询："+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"about": about, "fs": fsName})
}

// handleRcloneOptions 返回当前生效的全局选项（裁剪为常用项，避免响应过大）。
func (s *Server) handleRcloneOptions(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	all, err := s.rclone.Client().GetOptions(ctx, "main")
	if err != nil {
		writeErr(w, http.StatusBadGateway, "rclone_error", err.Error())
		return
	}
	interesting := []string{
		"transfers", "checkers", "retries", "low-level-retries", "bwlimit",
		"max-age", "max-size", "min-size", "exclude", "include",
		"dry-run", "checksum", "ignore-existing", "no-traverse", "size-only",
		"log-level", "use-json-log", "stats", "buffer-size", "multi-thread-streams",
	}
	picked := map[string]any{}
	for _, key := range interesting {
		if v, ok := all[key]; ok {
			picked[key] = v
		}
		// rclone 使用下划线/短横线两种写法，做一次兼容查找。
		alt := strings.ReplaceAll(key, "-", "_")
		if v, ok := all[alt]; ok {
			picked[alt] = v
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"options": picked, "available": len(all)})
}

func progressOf(run *store.Run) map[string]any {
	p := manager.ProgressOf(run)
	return map[string]any{
		"percent":     p.Percent,
		"bytes":       p.Bytes,
		"total_bytes": p.TotalBytes,
		"files":       p.Files,
		"total_files": p.TotalFiles,
		"speed":       p.Speed,
		"eta_seconds": p.ETASeconds,
		"errors":      p.Errors,
		"speed_text":  rclone.FormatBytes(int64(p.Speed)) + "/s",
		"bytes_text":  rclone.FormatBytes(p.Bytes),
		"total_text":  rclone.FormatBytes(p.TotalBytes),
	}
}
