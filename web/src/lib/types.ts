/** 后端返回结构的 TypeScript 描述（字段名与 Go 结构体 json tag 一一对应）。 */

export type TaskKind =
  | "sync"
  | "copy"
  | "move"
  | "bisync"
  | "check"
  | "delete"
  | "purge"
  | "mkdir"

export type RunStatus = "pending" | "running" | "success" | "failed" | "canceled"

/** 步骤失败后的整链行为。 */
export type StepOnError = "continue" | "abort"

/** 任务中的一个执行步骤；一个任务按顺序执行它的全部步骤。 */
export interface TaskStep {
  id?: number
  position: number
  /** 步骤备注，失败时用它指出"哪一步挂了"，可为空。 */
  name: string
  kind: TaskKind
  source: string
  dest: string
  extra_flags?: Record<string, unknown>
  /** 本步超时（秒）；0 表示用任务级超时。 */
  timeout_seconds: number
  dedupe_before: boolean
  /** 本步进入终态后、开始下一步之前等待的秒数。 */
  delay_after: number
  on_error: StepOnError
}

/** 单个步骤的执行结果摘要。 */
export interface StepResult {
  position: number
  name: string
  kind: TaskKind
  status: RunStatus
  error?: string
  duration_ms: number
  bytes: number
  files: number
}

export interface Task {
  id: number
  name: string
  description: string
  kind: TaskKind
  source: string
  dest: string
  extra_flags?: Record<string, unknown>
  cron_expr: string
  timeout_seconds: number
  enabled: boolean
  dedupe_before: boolean
  /** 执行步骤，至少一步；定时表达式属于任务本身，链的触发时间＝第一步的开始时间。 */
  steps: TaskStep[]
  created_at: string
  updated_at: string
  last_run_at?: string
  next_run_at?: string
  last_run_id?: number
  last_status?: RunStatus
}

export interface TaskView extends Task {
  next_runs?: string[]
  running?: Run
  method: string
}

export interface Run {
  id: number
  task_id: number
  task_name: string
  kind: TaskKind
  trigger: string
  job_id: number
  status: RunStatus
  started_at: string
  finished_at?: string
  duration_ms: number
  bytes: number
  total_bytes: number
  files: number
  total_files: number
  checks: number
  transfers: number
  errors: number
  renames: number
  deletes: number
  speed: number
  eta_seconds: number
  percent: number
  server_side_copies: number
  server_side_moves: number
  fatal_error: boolean
  error?: string
  /** 本次执行到第几个步骤（0 基）；单步任务恒为 0。 */
  step_index: number
  /** 任务的步骤总数；单步任务恒为 1。 */
  step_total: number
  /** 每个已完成步骤的结果摘要。 */
  step_results?: StepResult[]
  log_tail?: string[]
  message?: string
}

export interface ActiveRun {
  run: Run
  progress: {
    percent: number
    bytes: number
    total_bytes: number
    files: number
    total_files: number
    speed: number
    eta_seconds: number
    errors: number
  }
}

export interface Counts {
  tasks: number
  enabled_tasks: number
  scheduled_tasks: number
  running: number
  pending: number
  success_24h: number
  failed_24h: number
  last_run_at?: string
}

export interface SchedulerStats {
  enabled: boolean
  timezone: string
  seconds: boolean
  entries: number
  running: number
  next?: { task_id: number; task_name: string; next: string }
}

export interface RcloneStatus {
  state: string
  ready: boolean
  pid: number
  version: string
  endpoint: string
  binary: string
  config_file?: string
  restarts: number
  last_error?: string
  started_at?: string
  uptime_seconds: number
  auto_start: boolean
  auto_restart: boolean
  journal_lines: number
  external: boolean
}

export interface Overview {
  counts: Counts
  scheduler: SchedulerStats
  rclone: RcloneStatus
  running: ActiveRun[]
  server_time: string
  timezone: string
  version: string
}

export interface StepPayload {
  name: string
  kind: TaskKind
  source: string
  dest: string
  extra_flags?: Record<string, unknown>
  timeout_seconds: number
  dedupe_before: boolean
  delay_after: number
  on_error: StepOnError
}

/**
 * 创建/更新任务的请求体。
 *
 * steps 是真值；顶层的 kind/source/dest 只是给老调用方的简写，前端一律不依赖。
 */
export interface TaskPayload {
  name: string
  description: string
  cron_expr: string
  timeout_seconds: number
  enabled: boolean
  steps: StepPayload[]
  kind: TaskKind
  source: string
  dest: string
  dedupe_before: boolean
  extra_flags: Record<string, unknown>
}

/** 设置页：运行记录保留策略与占用情况。 */
export interface SettingsInfo {
  /** 保留时长（小时）；0 表示不限制。 */
  retention_hours: number
  retention_seconds: number
  retention_text: string
  /** ui = 界面设置（存数据库），config = 配置文件，default = 未设置。 */
  retention_source: string
  /** 按当前策略将会被清理的条数。 */
  pending: number
  interval?: string
  storage: {
    runs: number
    oldest_run_at?: string
    db_size_bytes: number
  }
}
