package store

import (
	"fmt"
	"strings"
	"time"
)

// TaskKind 是 rclone 操作类型。
type TaskKind string

// 支持的任务类型，取值对应 rclone RC API 的方法名（去掉 sync/ 等前缀）。
const (
	KindSync   TaskKind = "sync"   // 单向同步：使 dst 与 src 一致（会删除 dst 多余文件）
	KindCopy   TaskKind = "copy"   // 单向复制：只增不删
	KindMove   TaskKind = "move"   // 移动：复制后删除源
	KindBisync TaskKind = "bisync" // 双向同步（需先 --resync 初始化）
	KindCheck  TaskKind = "check"  // 校验 src/dst 一致性，不做传输
	KindDelete TaskKind = "delete" // 删除目标路径下的文件
	KindPurge  TaskKind = "purge"  // 清空目录（含目录本身）
	KindMkdir  TaskKind = "mkdir"  // 创建目录
)

// AllKinds 返回全部支持的任务类型。
func AllKinds() []TaskKind {
	return []TaskKind{KindSync, KindCopy, KindMove, KindBisync, KindCheck, KindDelete, KindPurge, KindMkdir}
}

// Valid 判断任务类型是否受支持。
func (k TaskKind) Valid() bool {
	for _, x := range AllKinds() {
		if k == x {
			return true
		}
	}
	return false
}

// NeedsDest 表示该类型是否必须提供目标路径。
func (k TaskKind) NeedsDest() bool {
	switch k {
	case KindPurge, KindMkdir:
		return false
	default:
		return true
	}
}

// AllowsDedupeBefore 表示该类型是否支持「清理目标重名」。
//
// sync/copy/move 都会把源端对象写到目标端：一旦目标允许同名，重复对象既不参与
// 比对（rclone 挑一份、其余忽略），还会随着每次传输继续累积 —— 所以这三种类型
// 都需要"先消除目标同名歧义"的前置步骤。check 不写目标，purge/mkdir/delete
// 没有源到目标的比对语义，开这个选项没有意义。
// 注意这里刻意写成白名单而不是黑名单：将来新增任务类型时，默认不开放，
// 必须显式确认过语义才能放行，避免新类型被静默卷入。
func (k TaskKind) AllowsDedupeBefore() bool {
	switch k {
	case KindSync, KindCopy, KindMove:
		return true
	default:
		return false
	}
}

// MaxTaskSteps 是单个任务允许配置的步骤数上限。
//
// 步骤串行执行且共享一次运行记录，步骤过多会让「一次运行」的语义变得模糊，
// 也给超时与间隔叠加出难以预估的总时长；50 已经远超实际编排需求。
const MaxTaskSteps = 50

// StepOnError 描述某个步骤失败后整条链的行为。
type StepOnError string

// 失败策略取值。
const (
	// OnErrorContinue 记录失败后继续执行后续步骤（默认）。
	OnErrorContinue StepOnError = "continue"
	// OnErrorAbort 立即中止，后续步骤不再执行。
	OnErrorAbort StepOnError = "abort"
)

// normalize 把空值归一为默认策略 continue。
func (e StepOnError) normalize() StepOnError {
	switch strings.ToLower(strings.TrimSpace(string(e))) {
	case string(OnErrorAbort):
		return OnErrorAbort
	default:
		return OnErrorContinue
	}
}

// Valid 判断取值是否合法（空值视为合法，会被归一为 continue）。
func (e StepOnError) Valid() bool {
	switch strings.ToLower(strings.TrimSpace(string(e))) {
	case "", string(OnErrorContinue), string(OnErrorAbort):
		return true
	default:
		return false
	}
}

// TaskStep 是任务中的一个执行步骤。
//
// 一个任务按顺序执行它的全部步骤；定时表达式属于任务本身，因此链的触发时间
// 就是第一个步骤的开始时间，步与步之间用 DelayAfter 拉出间隔。
type TaskStep struct {
	ID       int64 `json:"id,omitempty"`
	Position int   `json:"position"`
	// Name 是步骤的可选备注，用于失败时指出"哪一步挂了"。
	Name string   `json:"name"`
	Kind TaskKind `json:"kind"`
	// Source 与 Dest 是 rclone 路径。
	Source string `json:"source"`
	Dest   string `json:"dest"`
	// ExtraFlags 直接作为 RC API 请求体参数下发，作用于本步骤。
	ExtraFlags map[string]any `json:"extra_flags,omitempty"`
	// TimeoutSeconds 为本步骤的超时；<=0 时回落任务级超时。
	TimeoutSeconds int  `json:"timeout_seconds"`
	DedupeBefore   bool `json:"dedupe_before"`
	// DelayAfter 是本步骤进入终态后、开始下一步之前等待的秒数。
	// 第一个步骤之前不等待，最后一个步骤之后也不等待。
	DelayAfter int `json:"delay_after"`
	// OnError 决定本步骤失败后是继续还是中止；空值按 continue 处理。
	OnError StepOnError `json:"on_error"`
}

// Validate 校验单个步骤。
func (s *TaskStep) Validate() error {
	if s == nil {
		return fmt.Errorf("步骤为空")
	}
	if len(strings.TrimSpace(s.Name)) > 128 {
		return fmt.Errorf("步骤名称过长（最多 128 字符）")
	}
	if !s.Kind.Valid() {
		return fmt.Errorf("不支持的任务类型 %q，可选值: %s", s.Kind, joinKinds())
	}
	if strings.TrimSpace(s.Source) == "" {
		return fmt.Errorf("源路径不能为空")
	}
	if s.Kind.NeedsDest() && strings.TrimSpace(s.Dest) == "" {
		return fmt.Errorf("任务类型 %s 需要目标路径", s.Kind)
	}
	if s.DedupeBefore && !s.Kind.AllowsDedupeBefore() {
		return fmt.Errorf("任务类型 %s 不支持「清理目标重名」，仅 %s/%s/%s 支持", s.Kind, KindSync, KindCopy, KindMove)
	}
	if s.TimeoutSeconds < 0 {
		return fmt.Errorf("超时时间不能为负数")
	}
	if s.DelayAfter < 0 {
		return fmt.Errorf("间隔时间不能为负数")
	}
	if s.DelayAfter > 86400 {
		return fmt.Errorf("间隔时间过长（最多 86400 秒）")
	}
	if !s.OnError.Valid() {
		return fmt.Errorf("未知的失败策略 %q，可选值: continue / abort", s.OnError)
	}
	if err := validateFlags(s.ExtraFlags); err != nil {
		return err
	}
	return nil
}

// RunStatus 是运行记录的状态。
type RunStatus string

// 运行状态取值。
const (
	StatusPending  RunStatus = "pending"
	StatusRunning  RunStatus = "running"
	StatusSuccess  RunStatus = "success"
	StatusFailed   RunStatus = "failed"
	StatusCanceled RunStatus = "canceled"
)

// Finished 判断状态是否为终态。
func (s RunStatus) Finished() bool {
	switch s {
	case StatusSuccess, StatusFailed, StatusCanceled:
		return true
	default:
		return false
	}
}

// TriggerSource 描述任务是被谁触发的。
type TriggerSource string

// 触发来源取值。
const (
	TriggerManual TriggerSource = "manual"
	TriggerCron   TriggerSource = "cron"
	TriggerAPI    TriggerSource = "api"
)

// Task 是一条同步任务定义。
type Task struct {
	ID          int64    `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Kind        TaskKind `json:"kind"`
	// Source 与 Dest 是 rclone 路径，形如 myremote:bucket/path 或 /local/path。
	Source string `json:"source"`
	Dest   string `json:"dest"`
	// ExtraFlags 直接作为 RC API 请求体参数下发，如 transfers/checkers/dry_run。
	ExtraFlags map[string]any `json:"extra_flags,omitempty"`
	// CronExpr 为空表示仅手动触发。
	CronExpr string `json:"cron_expr"`
	// TimeoutSeconds 为 0 时使用全局默认超时。
	TimeoutSeconds int  `json:"timeout_seconds"`
	Enabled        bool `json:"enabled"`
	// DedupeBefore 为 true 时，传输开始前先清理目标端与源端同名但内容不一致的
	// 重复对象。仅 sync/copy/move 可用。
	//
	// 该字段与 Kind/Source/Dest 一样，是 Steps[0] 的只读镜像（见 normalizeSteps）：
	// 真值在 Steps 里，保留顶层字段是为了列表展示与老客户端兼容。
	DedupeBefore bool `json:"dedupe_before"`

	// Steps 是任务的执行步骤，按顺序依次执行，至少一步。
	//
	// cron 表达式只存在于任务级，所以链的触发时间天然等于第一个步骤的开始时间，
	// 不存在"子任务各自配了定时却不生效"的二义状态。
	Steps []*TaskStep `json:"steps"`

	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
	LastRunAt  *time.Time `json:"last_run_at,omitempty"`
	NextRunAt  *time.Time `json:"next_run_at,omitempty"`
	LastRunID  *int64     `json:"last_run_id,omitempty"`
	LastStatus string     `json:"last_status,omitempty"`
}

// Validate 校验任务字段合法性。
func (t *Task) Validate() error {
	if strings.TrimSpace(t.Name) == "" {
		return fmt.Errorf("任务名称不能为空")
	}
	if len(t.Name) > 128 {
		return fmt.Errorf("任务名称过长（最多 128 字符）")
	}
	if t.TimeoutSeconds < 0 {
		return fmt.Errorf("超时时间不能为负数")
	}
	if err := t.normalizeSteps(); err != nil {
		return err
	}
	if len(t.Steps) == 0 {
		return fmt.Errorf("任务至少需要配置一个步骤（源路径不能为空）")
	}
	if len(t.Steps) > MaxTaskSteps {
		return fmt.Errorf("步骤数量过多（最多 %d 个）", MaxTaskSteps)
	}
	for i, s := range t.Steps {
		if err := s.Validate(); err != nil {
			return fmt.Errorf("步骤 %d：%w", i+1, err)
		}
	}
	return nil
}

// normalizeSteps 归一化步骤：补齐老格式、排序、回填顶层镜像字段。
//
// 两件事必须在这里做而不能只在 API 层做：
//  1. 旧客户端（以及历史代码）只发 kind/source/dest 三个顶层字段，这里把它们
//     包成单步骤，老调用方零改动继续可用；
//  2. 顶层 Kind/Source/Dest/DedupeBefore 是 Steps[0] 的镜像，写出来才能保证
//     列表页、method 推导、老读取方看到的值与真正被执行的第一步一致。
//     反过来不成立：这些顶层字段被直接修改时，不做反向同步 —— 真值只有 Steps。
func (t *Task) normalizeSteps() error {
	if len(t.Steps) == 0 {
		if strings.TrimSpace(t.Source) == "" {
			return nil // 没有可补齐的依据，由调用方报"至少需要一个步骤"。
		}
		kind := t.Kind
		if kind == "" {
			kind = KindSync
		}
		t.Steps = []*TaskStep{{
			Kind:           kind,
			Source:         t.Source,
			Dest:           t.Dest,
			ExtraFlags:     t.ExtraFlags,
			TimeoutSeconds: t.TimeoutSeconds,
			DedupeBefore:   t.DedupeBefore,
			OnError:        OnErrorContinue,
		}}
	}
	if len(t.Steps) == 1 {
		// 只有一个步骤时，顶层字段就是这个步骤的另一种写法：调用方改顶层等于改这一步。
		// 多步时不成立（"顶层 kind 指哪一步"没有答案），所以只在单步时做，且只做非空覆盖。
		t.syncLegacyIntoStep(t.Steps[0])
	}
	for i, s := range t.Steps {
		if s == nil {
			return fmt.Errorf("步骤 %d 为空", i+1)
		}
		// 非法策略必须在这里挡掉：下面这行 normalize 会把任何未知值抹成 continue，
		// 先归一再校验的话用户填错的 "retry" 会被静默接受。
		if !s.OnError.Valid() {
			return fmt.Errorf("步骤 %d：未知的失败策略 %q，可选值: continue / abort", i+1, s.OnError)
		}
		s.Position = i
		s.OnError = s.OnError.normalize()
	}
	first := t.Steps[0]
	t.Kind = first.Kind
	t.Source = first.Source
	t.Dest = first.Dest
	t.DedupeBefore = first.DedupeBefore
	t.ExtraFlags = first.ExtraFlags
	return nil
}

// syncLegacyIntoStep 把顶层字段写回唯一的步骤（见 normalizeSteps 的注释）。
func (t *Task) syncLegacyIntoStep(st *TaskStep) {
	if t.Kind != "" {
		st.Kind = t.Kind
	}
	if strings.TrimSpace(t.Source) != "" {
		st.Source = t.Source
	}
	if strings.TrimSpace(t.Dest) != "" {
		st.Dest = t.Dest
	}
	st.DedupeBefore = t.DedupeBefore
	if len(t.ExtraFlags) > 0 {
		st.ExtraFlags = t.ExtraFlags
	}
}

// 保留参数由宿主程序注入，禁止用户在 extra_flags 中覆盖。
var reservedFlags = map[string]string{
	"_async":  "由程序内部管理",
	"_config": "由程序内部管理",
	"_group":  "由程序内部管理",
	"srcFs":   "请使用任务字段 source",
	"dstFs":   "请使用任务字段 dest",
	"fs":      "请使用任务字段 source",
	"remote":  "请使用任务字段 dest",
	"path1":   "请使用任务字段 source",
	"path2":   "请使用任务字段 dest",
	"_filter": "请在 extra_flags 中直接使用 filter 相关键",
}

func validateFlags(flags map[string]any) error {
	for k := range flags {
		if reason, ok := reservedFlags[k]; ok {
			return fmt.Errorf("extra_flags 中的 %q 为保留参数（%s）", k, reason)
		}
		if strings.HasPrefix(k, "_") {
			return fmt.Errorf("extra_flags 中的 %q 以下划线开头，属于 rclone 内部参数", k)
		}
	}
	return nil
}

// Run 是一次任务执行记录。
type Run struct {
	ID       int64  `json:"id"`
	TaskID   int64  `json:"task_id"`
	TaskName string `json:"task_name"`
	// Kind 冗余保存，便于任务被删除后仍能展示历史。
	Kind    TaskKind      `json:"kind"`
	Trigger TriggerSource `json:"trigger"`
	// JobID 是 rclone 侧的 job id，0 表示尚未拿到。
	JobID  int64     `json:"job_id"`
	Status RunStatus `json:"status"`

	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	DurationMS int64      `json:"duration_ms"`

	// 进度统计（来自 job/status 或 core/stats）。
	Bytes            int64   `json:"bytes"`
	TotalBytes       int64   `json:"total_bytes"`
	Files            int64   `json:"files"`
	TotalFiles       int64   `json:"total_files"`
	Checks           int64   `json:"checks"`
	Transfers        int64   `json:"transfers"`
	Errors           int64   `json:"errors"`
	Renames          int64   `json:"renames"`
	Deletes          int64   `json:"deletes"`
	Speed            float64 `json:"speed"`
	ETASeconds       int64   `json:"eta_seconds"`
	Percent          float64 `json:"percent"`
	ServerSideCopies int64   `json:"server_side_copies"`
	ServerSideMoves  int64   `json:"server_side_moves"`
	FatalError       bool    `json:"fatal_error"`

	Error string `json:"error,omitempty"`
	// StepIndex / StepTotal 标记本次运行执行到第几个步骤（0 基）与总步骤数。
	// 单步骤任务恒为 0 / 1，老数据读出来也是这个语义。
	StepIndex int `json:"step_index"`
	StepTotal int `json:"step_total"`
	// StepResults 是每个已完成步骤的结果摘要，用于详情页逐步展示"哪一步挂了"。
	StepResults []StepResult `json:"step_results,omitempty"`
	// LogTail 保存本次运行窗口内的 rclone 输出尾部，便于前端排查。
	LogTail []string `json:"log_tail,omitempty"`
	// LogMark 是运行开始时 rclone 日志缓冲的游标，仅内存使用，用于切分日志片段。
	LogMark int64 `json:"-"`

	// Message 是给用户看的一句话描述（如“已取消”）。
	Message string `json:"message,omitempty"`
}

// StepResult 是单个步骤的执行结果摘要。
//
// 只存"够用来回答哪一步失败"的最小集合，不存完整统计——完整统计仍以运行为
// 单位聚合，逐步明细可以从日志里的步骤分隔标记读到。
type StepResult struct {
	Position   int       `json:"position"`
	Name       string    `json:"name"`
	Kind       TaskKind  `json:"kind"`
	Status     RunStatus `json:"status"`
	Error      string    `json:"error,omitempty"`
	DurationMS int64     `json:"duration_ms"`
	Bytes      int64     `json:"bytes"`
	Files      int64     `json:"files"`
}

// RunFilter 是运行记录查询条件。
type RunFilter struct {
	TaskID int64
	Status []RunStatus
	// Limit 默认 50，最大 500。
	Limit int
	// Offset 分页偏移。
	Offset int
	// Active 为 true 时只查询非终态记录。
	Active bool
}

// Counts 是概览统计。
type Counts struct {
	Tasks          int        `json:"tasks"`
	EnabledTasks   int        `json:"enabled_tasks"`
	ScheduledTasks int        `json:"scheduled_tasks"`
	Running        int        `json:"running"`
	Pending        int        `json:"pending"`
	Success24h     int        `json:"success_24h"`
	Failed24h      int        `json:"failed_24h"`
	LastRunAt      *time.Time `json:"last_run_at,omitempty"`
}

func joinKinds() string {
	parts := make([]string, 0, len(AllKinds()))
	for _, k := range AllKinds() {
		parts = append(parts, string(k))
	}
	return strings.Join(parts, ", ")
}
