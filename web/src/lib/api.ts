import type {
  ActiveRun,
  CleanupOptions,
  CleanupResult,
  Overview,
  Run,
  RunStatus,
  SettingsInfo,
  Task,
  TaskPayload,
  TaskView,
} from "./types"

/**
 * 请求基址。
 *
 * 后端支持 server.base_path 子路径部署（如 /cloudsync/），所以不能写死
 * "/api"。产物里脚本位于 <base>assets/index-xxx.js，往前退一层就是站点根。
 */
function apiBase(): string {
  const path = new URL(import.meta.url).pathname
  const idx = path.lastIndexOf("/assets/")
  return idx >= 0 ? path.slice(0, idx + 1) : "/"
}

const BASE = apiBase()

export class ApiError extends Error {
  status: number
  code?: string

  constructor(message: string, status: number, code?: string) {
    super(message)
    this.name = "ApiError"
    this.status = status
    this.code = code
  }
}

interface RequestOptions {
  method?: string
  body?: unknown
  signal?: AbortSignal
}

async function request<T>(path: string, opts: RequestOptions = {}): Promise<T> {
  const init: RequestInit = {
    method: opts.method ?? "GET",
    credentials: "same-origin",
    headers: { Accept: "application/json" },
    signal: opts.signal,
  }
  if (opts.body !== undefined) {
    init.headers = { ...init.headers, "Content-Type": "application/json" }
    init.body = JSON.stringify(opts.body)
  }

  const res = await fetch(BASE + path.replace(/^\//, ""), init)
  const text = await res.text()
  let data: unknown = null
  if (text) {
    try {
      data = JSON.parse(text)
    } catch {
      data = { error: text }
    }
  }
  if (!res.ok) {
    const payload = (data ?? {}) as { error?: string; code?: string }
    throw new ApiError(payload.error ?? `HTTP ${res.status}`, res.status, payload.code)
  }
  return data as T
}

export const api = {
  me: () => request<{ authenticated: boolean; username?: string }>("api/me"),
  login: (username: string, password: string) =>
    request<{ ok: boolean }>("api/login", { method: "POST", body: { username, password } }),
  logout: () => request<{ ok: boolean }>("api/logout", { method: "POST" }),

  overview: () => request<Overview>("api/overview"),

  tasks: () => request<{ items: TaskView[] }>("api/tasks"),
  task: (id: number) => request<{ task: TaskView }>(`api/tasks/${id}`),
  createTask: (payload: TaskPayload) =>
    request<{ task: Task; warn?: string }>("api/tasks", { method: "POST", body: payload }),
  updateTask: (id: number, payload: TaskPayload) =>
    request<{ task: Task; warn?: string }>(`api/tasks/${id}`, { method: "PUT", body: payload }),
  setTaskEnabled: (id: number, enabled: boolean) =>
    request<{ task: Task }>(`api/tasks/${id}/enabled`, { method: "POST", body: { enabled } }),
  deleteTask: (id: number) => request<{ ok: boolean }>(`api/tasks/${id}`, { method: "DELETE" }),
  /** fromStep 为 0 基步骤下标，用于"从失败的那一步重跑"。 */
  runTask: (id: number, fromStep?: number) =>
    request<{ run: Run }>(`api/tasks/${id}/run${fromStep ? `?from_step=${fromStep}` : ""}`, {
      method: "POST",
    }),
  cancelTask: (id: number) =>
    request<{ ok: boolean }>(`api/tasks/${id}/cancel`, { method: "POST" }),
  validateCron: (cron_expr: string, count = 5) =>
    request<{ valid: boolean; error?: string; message?: string; next?: string[]; timezone?: string }>(
      "api/tasks/validate",
      { method: "POST", body: { cron_expr, count } },
    ),

  runs: (params: { limit: number; offset: number; status?: string }) => {
    const q = new URLSearchParams({ limit: String(params.limit), offset: String(params.offset) })
    if (params.status) q.set("status", params.status)
    return request<{ items: Run[]; total: number }>(`api/runs?${q.toString()}`)
  },
  run: (id: number) => request<{ run: Run; progress: ActiveRun["progress"] }>(`api/runs/${id}`),
  cancelRun: (id: number) =>
    request<{ ok: boolean }>(`api/runs/${id}/cancel`, { method: "POST" }),
  deleteRun: (id: number) => request<{ ok: boolean }>(`api/runs/${id}`, { method: "DELETE" }),
  deleteAllRuns: () => request<{ deleted: number }>("api/runs", { method: "DELETE" }),
  /** 按当前保留策略立即清理过期运行记录。 */
  pruneRuns: () => request<{ ok: boolean; deleted: number; vacuumed: boolean }>("api/runs/prune", {
    method: "POST",
  }),

  settings: () => request<SettingsInfo>("api/settings"),
  updateSettings: (retention_hours: number) =>
    request<SettingsInfo>("api/settings", { method: "PUT", body: { retention_hours } }),
  /** 数据库瘦身：丢弃日志片段 / 裁剪记录 / 清孤儿 / 整理数据库文件。 */
  cleanupDatabase: (opt: CleanupOptions) =>
    request<CleanupResult>("api/maintenance/cleanup", { method: "POST", body: opt }),

  rclone: () =>
    request<{
      status: Overview["rclone"]
      stats?: {
        bytes: number
        totalBytes: number
        transfers: number
        errors: number
        speed: number
      }
      memstats?: { Alloc: number; NumGC: number }
      groups?: string[]
    }>("api/rclone"),
  rcloneRestart: () => request<{ ok: boolean }>("api/rclone/restart", { method: "POST" }),
  rcloneRemotes: () => request<{ remotes: string[] }>("api/rclone/remotes"),
  rcloneLog: (tail = 400) => request<{ lines: string[] }>(`api/rclone/log?tail=${tail}`),
  /** 清空内存里的 rclone 输出缓冲（不影响任何运行记录）。 */
  rcloneLogClear: () =>
    request<{ ok: boolean; cleared: number }>("api/rclone/log/clear", { method: "POST", body: {} }),
}

/** SSE 事件名。 */
export type EventName = "hello" | "run.created" | "run.updated" | "run.finished"

export function eventsURL(): string {
  return BASE + "api/events"
}

export function isActive(status: RunStatus | undefined): boolean {
  return status === "running" || status === "pending"
}
