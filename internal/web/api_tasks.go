package web

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cloudsync/internal/logging"
	"cloudsync/internal/manager"
	"cloudsync/internal/store"
)

// stepPayload 是请求体中的一个步骤。
type stepPayload struct {
	Name       string         `json:"name"`
	Kind       string         `json:"kind"`
	Source     string         `json:"source"`
	Dest       string         `json:"dest"`
	ExtraFlags map[string]any `json:"extra_flags"`
	// TimeoutSeconds 为本步骤超时；<=0 时回落任务级超时。
	TimeoutSeconds int `json:"timeout_seconds"`
	// DedupeBefore 仅 sync/copy/move 可用，其余类型会被 TaskStep.Validate 拒绝。
	DedupeBefore bool `json:"dedupe_before"`
	// DelayAfter 是本步骤结束后等待多少秒再执行下一步。
	DelayAfter int `json:"delay_after"`
	// OnError 为本步骤失败后的行为：continue（默认）/ abort。
	OnError string `json:"on_error"`
}

// taskPayload 是创建/更新任务的请求体。
//
// steps 为任务的真值；顶层的 kind/source/dest/dedupe_before 只是"只有一个步骤时"
// 的简写，供老调用方使用（见 store.Task.normalizeSteps）。
type taskPayload struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Kind        string         `json:"kind"`
	Source      string         `json:"source"`
	Dest        string         `json:"dest"`
	ExtraFlags  map[string]any `json:"extra_flags"`
	CronExpr    string         `json:"cron_expr"`
	Steps       []stepPayload  `json:"steps"`
	// TimeoutSeconds 是各步骤的默认超时：步骤自己配了就用步骤的。
	TimeoutSeconds int   `json:"timeout_seconds"`
	Enabled        *bool `json:"enabled"`
	// DedupeBefore 为 true 时，sync 任务在同步前先对目标执行 rclone dedupe。
	// 仅 sync 可用，其他类型会被 store.Task.Validate 拒绝（见 store.TaskKind.AllowsDedupeBefore）。
	DedupeBefore bool `json:"dedupe_before"`
}

// taskView 是返回给前端的任务视图（附加派生字段）。
type taskView struct {
	*store.Task
	NextRuns []string   `json:"next_runs,omitempty"`
	Running  *store.Run `json:"running,omitempty"`
	Method   string     `json:"method"`
}

func (s *Server) toView(t *store.Task) taskView {
	v := taskView{Task: t}
	if m, err := methodOf(t); err == nil {
		v.Method = m
	}
	if t.CronExpr != "" && s.scheduler != nil {
		if next, err := s.scheduler.DescribeNext(t.CronExpr, 3); err == nil {
			for _, n := range next {
				v.NextRuns = append(v.NextRuns, n.Format(time.RFC3339))
			}
		}
	}
	v.Running = s.manager.RunningByTask(t.ID)
	return v
}

func methodOf(t *store.Task) (string, error) {
	m := map[store.TaskKind]string{
		store.KindSync:   "sync/sync",
		store.KindCopy:   "sync/copy",
		store.KindMove:   "sync/move",
		store.KindBisync: "sync/bisync",
		store.KindCheck:  "sync/check",
		store.KindDelete: "operations/delete",
		store.KindPurge:  "operations/purge",
		store.KindMkdir:  "operations/mkdir",
	}
	method, ok := m[t.Kind]
	if !ok {
		return "", errors.New("未知任务类型")
	}
	return method, nil
}

func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	tasks, err := s.store.ListTasks(r.Context())
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	items := make([]taskView, 0, len(tasks))
	for _, t := range tasks {
		items = append(items, s.toView(t))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": len(items)})
}

func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	t, err := s.store.GetTask(r.Context(), id)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	runs, total, err := s.store.ListRuns(r.Context(), store.RunFilter{TaskID: id, Limit: 10})
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"task":        s.toView(t),
		"recent_runs": runs,
		"runs_total":  total,
	})
}

func (s *Server) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	var payload taskPayload
	if err := decodeJSON(r, &payload); err != nil {
		writeStoreErr(w, err)
		return
	}

	task, err := s.payloadToTask(payload, nil)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	if err := s.store.CreateTask(r.Context(), task); err != nil {
		writeStoreErr(w, err)
		return
	}
	if err := s.scheduler.Sync(r.Context(), task); err != nil {
		// 任务已落库，调度注册失败要如实反馈但不回滚，便于用户修正表达式。
		s.logger.Error("任务已创建但调度注册失败", logging.Task(task.ID, task.Name), logging.Err(err))
		writeJSON(w, http.StatusCreated, map[string]any{
			"task": s.toView(task),
			"warn": "任务已保存，但定时注册失败：" + err.Error(),
		})
		return
	}

	s.logger.Info("任务已创建",
		logging.Task(task.ID, task.Name),
		"kind", string(task.Kind),
		"cron", task.CronExpr,
		"enabled", task.Enabled,
	)
	writeJSON(w, http.StatusCreated, map[string]any{"task": s.toView(task)})
}

func (s *Server) handleUpdateTask(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	existing, err := s.store.GetTask(r.Context(), id)
	if err != nil {
		writeStoreErr(w, err)
		return
	}

	// 运行中的任务不允许编辑：改路径/参数只对"下一轮"生效，用户却以为对当前
	// 这轮也生效，进而得出错误的结论。这里从服务端拦，前端隐藏按钮只是体验层。
	if running := s.manager.RunningByTask(id); running != nil {
		writeJSON(w, http.StatusConflict, apiError{
			Code:  "task_running",
			Error: fmt.Sprintf("任务正在运行中（运行 #%d），请先取消再编辑", running.ID),
		})
		return
	}

	var payload taskPayload
	if err := decodeJSON(r, &payload); err != nil {
		writeStoreErr(w, err)
		return
	}

	task, err := s.payloadToTask(payload, existing)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	if err := s.store.UpdateTask(r.Context(), task); err != nil {
		writeStoreErr(w, err)
		return
	}
	if err := s.scheduler.Sync(r.Context(), task); err != nil {
		s.logger.Error("任务已更新但调度注册失败", logging.Task(task.ID, task.Name), logging.Err(err))
		writeJSON(w, http.StatusOK, map[string]any{
			"task": s.toView(task),
			"warn": "任务已保存，但定时注册失败：" + err.Error(),
		})
		return
	}

	s.logger.Info("任务已更新",
		logging.Task(task.ID, task.Name),
		"kind", string(task.Kind),
		"cron", task.CronExpr,
		"enabled", task.Enabled,
	)
	writeJSON(w, http.StatusOK, map[string]any{"task": s.toView(task)})
}

// handleSetTaskEnabled 只切换任务的启用状态。
//
// 独立于 PUT /api/tasks/{id}：列表页的开关是一次轻量操作，不应要求前端回传
// 整张表单 —— 部分字段的缺省在 payloadToTask 里意味着"清空"，直接复用会把
// 未回传的字段抹掉。
func (s *Server) handleSetTaskEnabled(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	var req struct {
		Enabled bool `json:"enabled"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeStoreErr(w, err)
		return
	}

	task, err := s.store.GetTask(r.Context(), id)
	if err != nil {
		writeStoreErr(w, err)
		return
	}

	// 与编辑/删除同一条规则：任务运行中不允许改动它，只能先取消。
	// 列表里的开关是 UI，直接调 API 一样要被拦住。
	if running := s.manager.RunningByTask(id); running != nil {
		writeJSON(w, http.StatusConflict, apiError{
			Code:  "task_running",
			Error: fmt.Sprintf("任务正在运行中（运行 #%d），请先取消再修改启用状态", running.ID),
		})
		return
	}

	// 幂等：重复设置同一状态直接返回当前视图，不产生无意义的写库与日志。
	if task.Enabled == req.Enabled {
		writeJSON(w, http.StatusOK, map[string]any{"task": s.toView(task)})
		return
	}

	task.Enabled = req.Enabled
	if err := s.store.UpdateTask(r.Context(), task); err != nil {
		writeStoreErr(w, err)
		return
	}
	// 停用要从调度器摘除，启用要重新注册，二者都走 Sync。
	if err := s.scheduler.Sync(r.Context(), task); err != nil {
		s.logger.Error("任务已更新但调度注册失败", logging.Task(task.ID, task.Name), logging.Err(err))
		writeJSON(w, http.StatusOK, map[string]any{
			"task": s.toView(task),
			"warn": "任务已保存，但定时注册失败：" + err.Error(),
		})
		return
	}

	s.logger.Info("任务启用状态已切换",
		logging.Task(task.ID, task.Name),
		"enabled", task.Enabled,
	)
	writeJSON(w, http.StatusOK, map[string]any{"task": s.toView(task)})
}

func (s *Server) handleDeleteTask(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	task, err := s.store.GetTask(r.Context(), id)
	if err != nil {
		writeStoreErr(w, err)
		return
	}

	if running := s.manager.RunningByTask(id); running != nil {
		if !queryBool(r, "force") {
			writeJSON(w, http.StatusConflict, apiError{
				Code:  "task_running",
				Error: "任务正在运行中，请先取消或使用 ?force=1 强制删除",
			})
			return
		}
		if err := s.manager.Cancel(running.ID); err != nil && !errors.Is(err, manager.ErrNotRunning) {
			s.logger.Warn("删除前取消任务失败", logging.Run(running.ID, "running"), logging.Err(err))
		}
		s.manager.Wait(10 * time.Second)
	}

	s.scheduler.Remove(r.Context(), id)
	if err := s.store.DeleteTask(r.Context(), id); err != nil {
		writeStoreErr(w, err)
		return
	}
	s.logger.Info("任务已删除", logging.Task(task.ID, task.Name))
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "id": id})
}

func (s *Server) handleRunTask(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	task, err := s.store.GetTask(r.Context(), id)
	if err != nil {
		writeStoreErr(w, err)
		return
	}

	// from_step 用于"从失败的那一步重跑"：在此之前的步骤不再执行。
	fromStep := 0
	if raw := r.URL.Query().Get("from_step"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			writeErr(w, http.StatusBadRequest, "bad_from_step", "from_step 必须是非负整数")
			return
		}
		if n >= len(task.Steps) {
			writeErr(w, http.StatusBadRequest, "bad_from_step",
				fmt.Sprintf("起始步骤 %d 超出范围（共 %d 步）", n+1, len(task.Steps)))
			return
		}
		fromStep = n
	}

	run, err := s.manager.TriggerFrom(r.Context(), task, store.TriggerManual, fromStep)
	switch {
	case err == nil:
		s.logger.Info("手动触发任务", logging.Task(task.ID, task.Name), logging.Run(run.ID, string(run.Status)))
		writeJSON(w, http.StatusAccepted, map[string]any{"run": run})
	case errors.Is(err, manager.ErrTaskBusy):
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": err.Error(),
			"code":  "task_busy",
			"run":   run,
		})
	case errors.Is(err, manager.ErrCapacity):
		writeErr(w, http.StatusTooManyRequests, "capacity", err.Error())
	case errors.Is(err, manager.ErrTaskDisabled):
		writeErr(w, http.StatusConflict, "task_disabled", err.Error())
	default:
		writeStoreErr(w, err)
	}
}

func (s *Server) handleCancelTask(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	running := s.manager.RunningByTask(id)
	if running == nil {
		writeErr(w, http.StatusNotFound, "not_running", "该任务当前没有运行中的实例")
		return
	}
	if err := s.manager.Cancel(running.ID); err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "run_id": running.ID})
}

// handleValidateCron 校验 cron 表达式并返回未来触发时间。
func (s *Server) handleValidateCron(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CronExpr string `json:"cron_expr"`
		Count    int    `json:"count"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeStoreErr(w, err)
		return
	}
	if req.Count <= 0 || req.Count > 10 {
		req.Count = 5
	}

	if strings.TrimSpace(req.CronExpr) == "" {
		writeJSON(w, http.StatusOK, map[string]any{"valid": true, "next": []string{}, "message": "未设置定时表达式，仅支持手动触发"})
		return
	}
	next, err := s.scheduler.DescribeNext(req.CronExpr, req.Count)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"valid": false, "error": err.Error()})
		return
	}
	out := make([]string, 0, len(next))
	for _, n := range next {
		out = append(out, n.Format(time.RFC3339))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"valid":    true,
		"next":     out,
		"timezone": s.scheduler.Location().String(),
	})
}

// payloadToTask 把请求体转换为任务实体；base 非空时在其上做更新。
func (s *Server) payloadToTask(p taskPayload, base *store.Task) (*store.Task, error) {
	task := &store.Task{}
	if base != nil {
		*task = *base
		task.ExtraFlags = nil
	}

	task.Name = strings.TrimSpace(p.Name)
	task.Description = strings.TrimSpace(p.Description)
	task.CronExpr = strings.TrimSpace(p.CronExpr)
	task.TimeoutSeconds = p.TimeoutSeconds

	kind := store.TaskKind(strings.ToLower(strings.TrimSpace(p.Kind)))
	if kind == "" {
		// 请求体不带 kind 时沿用原值，没有原值才兜到 sync。
		// 一律兜 sync 会把"只提交 steps"的更新变成把第一步类型改成 sync。
		kind = store.KindSync
		if base != nil && base.Kind != "" {
			kind = base.Kind
		}
	}
	task.Kind = kind
	task.Source = strings.TrimSpace(p.Source)
	task.Dest = strings.TrimSpace(p.Dest)
	task.DedupeBefore = p.DedupeBefore

	if p.ExtraFlags != nil {
		task.ExtraFlags = p.ExtraFlags
	} else if base != nil {
		task.ExtraFlags = base.ExtraFlags
	}

	if len(p.Steps) > 0 {
		steps := make([]*store.TaskStep, 0, len(p.Steps))
		for _, sp := range p.Steps {
			kind := store.TaskKind(strings.ToLower(strings.TrimSpace(sp.Kind)))
			if kind == "" {
				kind = store.KindSync
			}
			steps = append(steps, &store.TaskStep{
				Name:           strings.TrimSpace(sp.Name),
				Kind:           kind,
				Source:         strings.TrimSpace(sp.Source),
				Dest:           strings.TrimSpace(sp.Dest),
				ExtraFlags:     sp.ExtraFlags,
				TimeoutSeconds: sp.TimeoutSeconds,
				DedupeBefore:   sp.DedupeBefore,
				DelayAfter:     sp.DelayAfter,
				OnError:        store.StepOnError(strings.ToLower(strings.TrimSpace(sp.OnError))),
			})
		}
		task.Steps = steps
		// 顶层字段跟着第一步走。单步任务在 normalizeSteps 里会把顶层值写回步骤，
		// 顶层若留着请求里的旧值（或缺省值），等于悄悄改掉用户刚提交的第一步。
		task.Kind = steps[0].Kind
		task.Source = steps[0].Source
		task.Dest = steps[0].Dest
		task.DedupeBefore = steps[0].DedupeBefore
		if len(steps[0].ExtraFlags) > 0 {
			task.ExtraFlags = steps[0].ExtraFlags
		}
	}
	// p.Steps 为空时保留原样：老调用方只发顶层字段，单步任务由 normalizeSteps
	// 把它们写回唯一的步骤，多步任务则用自己原有的步骤。

	if p.Enabled != nil {
		task.Enabled = *p.Enabled
	} else if base == nil {
		task.Enabled = true
	}

	// 路径归一化：去掉误输入的多余空白。
	if err := task.Validate(); err != nil {
		return nil, invalid("%s", err.Error())
	}
	if task.CronExpr != "" {
		if err := s.scheduler.Validate(task.CronExpr); err != nil {
			return nil, invalid("%s", err.Error())
		}
	}
	return task, nil
}
