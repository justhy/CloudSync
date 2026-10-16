package web

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"cloudsync/internal/store"
)

// taskExportFormat 是导出文件的 format 标识，导入时用它做基本校验。
const taskExportFormat = "cloudsync.tasks"

// maxImportTasks 是一次导入允许的任务条数上限。
//
// 导入接口复用 decodeJSON，请求体上限 1 MiB；单任务 JSON 通常几百字节，
// 1000 条已远超实际迁移规模，同时避免一次请求把调度器刷爆。
const maxImportTasks = 1000

// taskExportDoc 是任务导出文件的结构，也是导入接口接受的格式。
//
// tasks 里每一项与 POST /api/tasks 的请求体同构（taskPayload），所以导出的文件
// 可以原样再导入，不需要任何额外转换 —— 也让导出/导入天然共用同一套字段校验。
type taskExportDoc struct {
	Format     string        `json:"format"`
	Version    int           `json:"version"`
	ExportedAt string        `json:"exported_at"`
	Count      int           `json:"count"`
	Tasks      []taskPayload `json:"tasks"`
}

// 导入模式。
const (
	// importModeAppend 追加（默认）：保留现有任务，只新增文件里的，同名跳过。
	importModeAppend = "append"
	// importModeReplace 覆盖：先删除现有全部任务，再导入文件里的任务。
	importModeReplace = "replace"
)

// taskImportRequest 是导入请求体：与导出文档同构，另加一个导入模式。
//
// 用嵌入而不是重复一遍字段：导出文档将来加字段时导入自动跟上。mode 只在导入时
// 有意义，所以不放进 taskExportDoc —— 导出的文件里不会出现它。
type taskImportRequest struct {
	taskExportDoc
	Mode string `json:"mode"`
}

// taskImportItem 是单个任务的导入结果。
type taskImportItem struct {
	Name   string `json:"name"`
	Status string `json:"status"` // created | skipped | failed
	ID     int64  `json:"id,omitempty"`
	Error  string `json:"error,omitempty"`
}

// taskImportResult 是导入接口的汇总响应。
type taskImportResult struct {
	Mode    string `json:"mode"`
	Total   int    `json:"total"`
	Created int    `json:"created"`
	Skipped int    `json:"skipped"`
	Failed  int    `json:"failed"`
	// Removed 是 replace 模式下被删除的现有任务数。
	Removed int `json:"removed"`
	// Warn 记录"已导入但定时注册失败"的任务，非空时前端应提示用户。
	Warn  string           `json:"warn,omitempty"`
	Items []taskImportItem `json:"items"`
}

// taskToPayload 把任务实体转成可再导入的请求体形态。
//
// 只导出任务定义，不带 ID / 运行记录 / 运行态字段：迁移到另一实例时编号重新分配，
// 历史运行记录属于旧库，搬过去只会张冠李戴。
func taskToPayload(t *store.Task) taskPayload {
	enabled := t.Enabled
	p := taskPayload{
		Name:           t.Name,
		Description:    t.Description,
		Kind:           string(t.Kind),
		Source:         t.Source,
		Dest:           t.Dest,
		ExtraFlags:     t.ExtraFlags,
		CronExpr:       t.CronExpr,
		TimeoutSeconds: t.TimeoutSeconds,
		Enabled:        &enabled,
		DedupeBefore:   t.DedupeBefore,
	}
	for _, st := range t.Steps {
		if st == nil {
			continue
		}
		p.Steps = append(p.Steps, stepPayload{
			Name:           st.Name,
			Kind:           string(st.Kind),
			Source:         st.Source,
			Dest:           st.Dest,
			ExtraFlags:     st.ExtraFlags,
			TimeoutSeconds: st.TimeoutSeconds,
			DedupeBefore:   st.DedupeBefore,
			DelayAfter:     st.DelayAfter,
			OnError:        string(st.OnError),
		})
	}
	return p
}

// handleExportTasks 导出全部任务定义（不含运行记录）为可再导入的 JSON 文档。
func (s *Server) handleExportTasks(w http.ResponseWriter, r *http.Request) {
	tasks, err := s.store.ListTasks(r.Context())
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	items := make([]taskPayload, 0, len(tasks))
	for _, t := range tasks {
		items = append(items, taskToPayload(t))
	}
	now := time.Now().UTC()
	doc := taskExportDoc{
		Format:     taskExportFormat,
		Version:    1,
		ExportedAt: now.Format(time.RFC3339),
		Count:      len(items),
		Tasks:      items,
	}
	// 让浏览器直接存成文件；文件名带时间戳，多次导出不会互相覆盖。
	w.Header().Set("Content-Disposition",
		fmt.Sprintf("attachment; filename=%q", "cloudsync-tasks-"+now.Format("20060102-150405")+".json"))
	writeJSON(w, http.StatusOK, doc)
	s.logger.Info("导出任务", "count", len(items))
}

// handleImportTasks 从导出的 JSON 文档导入任务。
//
// mode 决定与现有任务的关系：
//   - append（默认）：保留现有任务，只新增文件里的，同名跳过；
//   - replace：先删除现有全部任务（含其运行记录），再导入文件里的任务。
//
// 两种模式都逐条处理，单条失败不影响其余（便于"改好再导"，配合同名跳过天然幂等）；
// 导入的任务一律重新分配 ID，不带入源实例的编号与运行记录。
func (s *Server) handleImportTasks(w http.ResponseWriter, r *http.Request) {
	var req taskImportRequest
	if err := decodeJSON(r, &req); err != nil {
		writeStoreErr(w, err)
		return
	}
	doc := req.taskExportDoc

	mode := strings.ToLower(strings.TrimSpace(req.Mode))
	switch mode {
	case "":
		mode = importModeAppend
	case importModeAppend, importModeReplace:
	default:
		writeErr(w, http.StatusBadRequest, "bad_mode",
			fmt.Sprintf("未知的导入模式 %q（可选：append / replace）", req.Mode))
		return
	}

	if doc.Format != "" && doc.Format != taskExportFormat {
		writeErr(w, http.StatusBadRequest, "bad_format",
			fmt.Sprintf("不支持的文件格式 %q（期望 %q）", doc.Format, taskExportFormat))
		return
	}
	if len(doc.Tasks) == 0 {
		writeErr(w, http.StatusBadRequest, "empty", "文件中没有任务")
		return
	}
	if len(doc.Tasks) > maxImportTasks {
		writeErr(w, http.StatusBadRequest, "too_many",
			fmt.Sprintf("一次最多导入 %d 个任务，当前 %d 个", maxImportTasks, len(doc.Tasks)))
		return
	}

	existing, err := s.store.ListTasks(r.Context())
	if err != nil {
		writeStoreErr(w, err)
		return
	}

	res := taskImportResult{
		Mode:  mode,
		Total: len(doc.Tasks),
		Items: make([]taskImportItem, 0, len(doc.Tasks)),
	}

	// 覆盖模式：先清空现有任务。运行中的任务删不掉，直接整体拒绝而不是悄悄取消 ——
	// 用户以为"只是导入"，实际却中断了正在跑的同步，这种意外不能有。
	if mode == importModeReplace {
		for _, t := range existing {
			if run := s.manager.RunningByTask(t.ID); run != nil {
				writeJSON(w, http.StatusConflict, apiError{
					Code:  "task_running",
					Error: fmt.Sprintf("任务 #%d「%s」正在运行，无法覆盖导入；请先取消再试", t.ID, t.Name),
				})
				return
			}
		}
		for _, t := range existing {
			s.scheduler.Remove(r.Context(), t.ID)
			if err := s.store.DeleteTask(r.Context(), t.ID); err != nil {
				writeStoreErr(w, err)
				return
			}
			res.Removed++
		}
		existing = nil
	}

	names := make(map[string]struct{}, len(existing)+len(doc.Tasks))
	for _, t := range existing {
		names[t.Name] = struct{}{}
	}

	var warns []string
	for _, p := range doc.Tasks {
		item := taskImportItem{Name: strings.TrimSpace(p.Name)}

		task, err := s.payloadToTask(p, nil)
		if err != nil {
			item.Status = "failed"
			item.Error = err.Error()
			res.Failed++
			res.Items = append(res.Items, item)
			continue
		}
		if _, dup := names[task.Name]; dup {
			item.Status = "skipped"
			item.Error = "同名任务已存在，已跳过"
			res.Skipped++
			res.Items = append(res.Items, item)
			continue
		}
		if err := s.store.CreateTask(r.Context(), task); err != nil {
			// 并发导入时靠唯一约束兜底：同样按跳过处理，不当作失败。
			if errors.Is(err, store.ErrConflict) {
				item.Status = "skipped"
				item.Error = "同名任务已存在，已跳过"
				res.Skipped++
			} else {
				item.Status = "failed"
				item.Error = err.Error()
				res.Failed++
			}
			res.Items = append(res.Items, item)
			continue
		}
		names[task.Name] = struct{}{}
		item.Status = "created"
		item.ID = task.ID
		res.Created++
		res.Items = append(res.Items, item)

		if err := s.scheduler.Sync(r.Context(), task); err != nil {
			warns = append(warns, fmt.Sprintf("#%d %s：%v", task.ID, task.Name, err))
		}
	}
	if len(warns) > 0 {
		res.Warn = "以下任务已导入，但定时注册失败：" + strings.Join(warns, "；")
	}

	s.logger.Info("导入任务",
		"mode", res.Mode, "total", res.Total, "removed", res.Removed,
		"created", res.Created, "skipped", res.Skipped, "failed", res.Failed)
	writeJSON(w, http.StatusOK, res)
}
