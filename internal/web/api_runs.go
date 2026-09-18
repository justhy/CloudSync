package web

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"cloudsync/internal/logging"
	"cloudsync/internal/manager"
	"cloudsync/internal/store"
)

func (s *Server) handleListRuns(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	filter := store.RunFilter{
		TaskID: int64(queryInt(r, "task_id", 0)),
		Limit:  queryInt(r, "limit", 50),
		Offset: queryInt(r, "offset", 0),
		Active: queryBool(r, "active"),
	}
	if raw := strings.TrimSpace(q.Get("status")); raw != "" {
		for _, part := range strings.Split(raw, ",") {
			part = strings.TrimSpace(part)
			switch store.RunStatus(part) {
			case store.StatusPending, store.StatusRunning, store.StatusSuccess, store.StatusFailed, store.StatusCanceled:
				filter.Status = append(filter.Status, store.RunStatus(part))
			default:
				writeErr(w, http.StatusBadRequest, "bad_status", fmt.Sprintf("无效的状态值 %q", part))
				return
			}
		}
	}

	runs, total, err := s.store.ListRuns(r.Context(), filter)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":  runs,
		"total":  total,
		"limit":  filter.Limit,
		"offset": filter.Offset,
	})
}

// handleActiveRuns 返回当前运行中的任务（含实时进度快照）。
func (s *Server) handleActiveRuns(w http.ResponseWriter, r *http.Request) {
	ids := s.manager.ActiveRunIDs()
	items := make([]*store.Run, 0, len(ids))
	for _, id := range ids {
		run, err := s.store.GetRun(r.Context(), id)
		if err != nil {
			continue
		}
		items = append(items, run)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": len(items)})
}

func (s *Server) handleGetRun(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	run, err := s.store.GetRun(r.Context(), id)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	// 进行中的记录库里的 LogTail 是空的（终态才回写），改切 journal 的实时片段，
	// 详情页才能做到"边跑边看输出"。
	if s.manager.IsRunning(id) {
		if live := s.manager.LiveLogTail(id); live != nil {
			snap := *run
			snap.LogTail = live
			run = &snap
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"run":      run,
		"progress": manager.ProgressOf(run),
	})
}

func (s *Server) handleCancelRun(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	if err := s.manager.Cancel(id); err != nil {
		if errors.Is(err, manager.ErrNotRunning) {
			writeErr(w, http.StatusNotFound, "not_running", "该运行记录不在运行中")
			return
		}
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "run_id": id})
}

// handleDeleteRun 删除单条运行记录。
//
// 运行中的记录拒绝删除：这条记录是任务执行的唯一观测窗口（取消、进度、
// 日志切片都挂在它上面），删掉之后 rclone 侧的 job 还在跑，却没人看得见。
// 判断依据用 manager 的 active 表而不是库里的 status —— 库里的记录可能是
// 上次异常退出残留的，未必可信。
func (s *Server) handleDeleteRun(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	if s.manager.IsRunning(id) {
		writeJSON(w, http.StatusConflict, apiError{
			Code:  "run_running",
			Error: "该运行正在执行，请先取消再删除",
		})
		return
	}
	if err := s.store.DeleteRun(r.Context(), id); err != nil {
		writeStoreErr(w, err)
		return
	}
	s.logger.Info("运行记录已删除", logging.Run(id, "deleted"))
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "id": id})
}

// handleDeleteAllRuns 清空全部运行记录。
//
// 有任务在跑时拒绝：清空会把正在执行的那条也抹掉，观测窗口直接消失。
// 任务的 last_run_id/last_status 故意保留——它们描述任务本身的最近结果，
// 清掉会让任务列表出现"从没跑过"的假象。
func (s *Server) handleDeleteAllRuns(w http.ResponseWriter, r *http.Request) {
	if n := s.manager.ActiveCount(); n > 0 {
		writeJSON(w, http.StatusConflict, apiError{
			Code:  "runs_running",
			Error: fmt.Sprintf("有 %d 个任务正在运行，请先取消或等待结束", n),
		})
		return
	}
	deleted, err := s.store.DeleteAllRuns(r.Context())
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	s.logger.Info("运行记录已清空", "deleted", deleted)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "deleted": deleted})
}

// handleEvents 通过 SSE 推送运行事件。
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "no_stream", "服务端不支持流式响应")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	events := make(chan manager.Event, 64)
	unsub := s.manager.Subscribe(func(e manager.Event) {
		select {
		case events <- e:
		default:
			// 客户端消费不过来时丢弃中间事件，避免阻塞任务执行。
		}
	})
	defer unsub()

	// 首帧告知客户端连接已建立及当前运行快照。
	s.writeSSE(w, "hello", map[string]any{
		"time":    time.Now().UTC().Format(time.RFC3339),
		"active":  s.manager.ActiveCount(),
		"running": s.manager.ActiveRunIDs(),
	})
	flusher.Flush()

	heartbeat := time.NewTicker(20 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case e := <-events:
			s.writeSSE(w, string(e.Type), e)
			flusher.Flush()
		case <-heartbeat.C:
			// 注释帧用于保活，防止中间代理断开连接。
			_, _ = w.Write([]byte(": keepalive\n\n"))
			flusher.Flush()
		}
	}
}

func (s *Server) writeSSE(w http.ResponseWriter, event string, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		s.logger.Warn("序列化 SSE 数据失败", logging.Err(err))
		return
	}
	// 事件名与数据均为单行 JSON，避免出现裸换行破坏 SSE 帧结构。
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
}
