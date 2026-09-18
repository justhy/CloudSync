import * as React from "react"
import { ArrowDown, ArrowUp, Copy, Loader2, Plus, ShieldAlert, Trash2 } from "lucide-react"
import { toast } from "sonner"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import { Textarea } from "@/components/ui/textarea"
import { api } from "@/lib/api"
import { KIND_TEXT, fmtTime } from "@/lib/format"
import type { StepOnError, StepPayload, TaskKind, TaskPayload, TaskView } from "@/lib/types"

/** 「清理目标重名」只对会把源端对象写到目标端的类型有意义。白名单，避免新增类型被意外放行。 */
const DEDUPE_KINDS: TaskKind[] = ["sync", "copy", "move"]

/** purge / mkdir 只作用于一个路径，没有"源 → 目标"的语义。 */
function needsDest(kind: TaskKind): boolean {
  return kind !== "purge" && kind !== "mkdir"
}

const MAX_STEPS = 50

interface StepForm {
  name: string
  kind: TaskKind
  source: string
  dest: string
  dedupe_before: boolean
  delay_after: string
  on_error: StepOnError
  timeout_seconds: string
  flags: string
}

interface FormState {
  name: string
  description: string
  cron_expr: string
  timeout_seconds: string
  enabled: boolean
  steps: StepForm[]
}

function emptyStep(kind: TaskKind = "sync"): StepForm {
  return {
    name: "",
    kind,
    source: "",
    dest: "",
    dedupe_before: false,
    delay_after: "0",
    on_error: "continue",
    timeout_seconds: "0",
    flags: "",
  }
}

function flagsText(v?: Record<string, unknown>): string {
  return v && Object.keys(v).length ? JSON.stringify(v, null, 2) : ""
}

function stepToForm(s: TaskView["steps"][number]): StepForm {
  return {
    name: s.name ?? "",
    kind: s.kind,
    source: s.source ?? "",
    dest: s.dest ?? "",
    dedupe_before: Boolean(s.dedupe_before),
    delay_after: String(s.delay_after ?? 0),
    on_error: s.on_error === "abort" ? "abort" : "continue",
    timeout_seconds: String(s.timeout_seconds ?? 0),
    flags: flagsText(s.extra_flags),
  }
}

const EMPTY: FormState = {
  name: "",
  description: "",
  cron_expr: "",
  timeout_seconds: "0",
  enabled: true,
  steps: [emptyStep()],
}

function toForm(task: TaskView | null): FormState {
  if (!task) return { ...EMPTY, steps: [emptyStep()] }
  // 老数据（或升级前建的任务）可能没有 steps，退化成顶层字段的单步任务。
  const steps = task.steps?.length
    ? task.steps.map(stepToForm)
    : [{ ...emptyStep(task.kind ?? "sync"), source: task.source ?? "", dest: task.dest ?? "" }]
  return {
    name: task.name,
    description: task.description,
    cron_expr: task.cron_expr,
    timeout_seconds: String(task.timeout_seconds ?? 0),
    enabled: task.enabled,
    steps,
  }
}

export function TaskFormDialog({
  open,
  task,
  onOpenChange,
  onSaved,
}: {
  open: boolean
  task: TaskView | null
  onOpenChange: (open: boolean) => void
  onSaved: () => void
}) {
  const [form, setForm] = React.useState<FormState>(() => toForm(task))
  const [error, setError] = React.useState("")
  const [busy, setBusy] = React.useState(false)
  const [cronResult, setCronResult] = React.useState<React.ReactNode>(null)

  React.useEffect(() => {
    if (open) {
      setForm(toForm(task))
      setError("")
      setCronResult(null)
    }
  }, [open, task])

  const set = <K extends keyof FormState>(key: K, value: FormState[K]) =>
    setForm((f) => ({ ...f, [key]: value }))

  const patchStep = (i: number, patch: Partial<StepForm>) =>
    setForm((f) => ({
      ...f,
      steps: f.steps.map((s, idx) => (idx === i ? { ...s, ...patch } : s)),
    }))

  function addStep() {
    setForm((f) => {
      if (f.steps.length >= MAX_STEPS) return f
      // 新步骤沿用上一步的类型：编排链里多数步骤是同一种操作，省一次选择。
      return { ...f, steps: [...f.steps, emptyStep(f.steps[f.steps.length - 1]?.kind ?? "sync")] }
    })
  }

  function removeStep(i: number) {
    setForm((f) => (f.steps.length <= 1 ? f : { ...f, steps: f.steps.filter((_, idx) => idx !== i) }))
  }

  function moveStep(i: number, delta: number) {
    setForm((f) => {
      const j = i + delta
      if (j < 0 || j >= f.steps.length) return f
      const steps = [...f.steps]
      ;[steps[i], steps[j]] = [steps[j], steps[i]]
      return { ...f, steps }
    })
  }

  function duplicateStep(i: number) {
    setForm((f) => {
      if (f.steps.length >= MAX_STEPS) return f
      const steps = [...f.steps]
      steps.splice(i + 1, 0, { ...steps[i] })
      return { ...f, steps }
    })
  }

  async function validateCron() {
    try {
      const res = await api.validateCron(form.cron_expr, 5)
      if (!res.valid) {
        setCronResult(<span className="text-destructive">{res.error || "表达式无效"}</span>)
        return
      }
      if (!res.next?.length) {
        setCronResult(<span className="text-muted-foreground">{res.message || "未设置定时"}</span>)
        return
      }
      setCronResult(
        <span className="block space-y-0.5">
          <span className="text-muted-foreground">
            未来 {res.next.length} 次触发{res.timezone ? `（${res.timezone}）` : ""}：
          </span>
          {res.next.map((x) => (
            <span key={x} className="tabular block text-muted-foreground">
              {fmtTime(x)}
            </span>
          ))}
        </span>,
      )
    } catch (err) {
      setCronResult(
        <span className="text-destructive">{err instanceof Error ? err.message : "校验失败"}</span>,
      )
    }
  }

  function parseFlags(raw: string, where: string): Record<string, unknown> | null {
    const text = raw.trim()
    if (!text) return {}
    let parsed: unknown
    try {
      parsed = JSON.parse(text)
    } catch (err) {
      setError(`${where}的 extra_flags 不是合法 JSON：${err instanceof Error ? err.message : String(err)}`)
      return null
    }
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      setError(`${where}的 extra_flags 必须是 JSON 对象`)
      return null
    }
    return parsed as Record<string, unknown>
  }

  async function save() {
    setError("")
    if (!form.name.trim()) {
      setError("任务名称不能为空")
      return
    }

    const steps: StepPayload[] = []
    for (let i = 0; i < form.steps.length; i++) {
      const s = form.steps[i]
      const label = form.steps.length > 1 ? `步骤 ${i + 1}` : "任务"
      if (!s.source.trim()) {
        setError(`${label}：源路径不能为空`)
        return
      }
      if (needsDest(s.kind) && !s.dest.trim()) {
        setError(`${label}：类型 ${s.kind} 需要目标路径`)
        return
      }
      const delay = Number.parseInt(s.delay_after, 10) || 0
      if (delay < 0 || delay > 86400) {
        setError(`${label}：间隔时间需在 0 ~ 86400 秒之间`)
        return
      }
      const flags = parseFlags(s.flags, label)
      if (!flags) return
      steps.push({
        name: s.name.trim(),
        kind: s.kind,
        source: s.source.trim(),
        dest: s.dest.trim(),
        timeout_seconds: Math.max(0, Number.parseInt(s.timeout_seconds, 10) || 0),
        dedupe_before: DEDUPE_KINDS.includes(s.kind) && s.dedupe_before,
        delay_after: delay,
        on_error: s.on_error,
        extra_flags: flags,
      })
    }

    // 顶层字段按第一步推导：它们只是给老调用方/列表展示的镜像，真值在 steps。
    const first = steps[0]
    const payload: TaskPayload = {
      name: form.name.trim(),
      description: form.description.trim(),
      cron_expr: form.cron_expr.trim(),
      timeout_seconds: Math.max(0, Number.parseInt(form.timeout_seconds, 10) || 0),
      enabled: form.enabled,
      steps,
      kind: first.kind,
      source: first.source,
      dest: first.dest,
      dedupe_before: first.dedupe_before,
      extra_flags: first.extra_flags ?? {},
    }

    setBusy(true)
    try {
      const res = task ? await api.updateTask(task.id, payload) : await api.createTask(payload)
      toast.success(task ? "任务已更新" : "任务已创建", res.warn ? { description: res.warn } : {})
      onOpenChange(false)
      onSaved()
    } catch (err) {
      setError(err instanceof Error ? err.message : "保存失败")
    } finally {
      setBusy(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[90svh] overflow-y-auto sm:max-w-3xl">
        <DialogHeader>
          <DialogTitle>{task ? `编辑任务 #${task.id}` : "新建任务"}</DialogTitle>
          <DialogDescription>
            一个任务按顺序执行它的全部步骤；定时表达式属于任务本身，链的触发时间就是第一步的开始时间。
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          {/* 「启用」是任务级开关，放在表单最上方：排在 cron 校验下面会被误读成校验的附属选项。 */}
          <div className="flex items-center justify-between rounded-lg border bg-muted/40 px-3 py-2.5">
            <div className="space-y-0.5">
              <Label htmlFor="f-enabled">启用</Label>
              <p className="text-xs text-muted-foreground">停用后定时与手动触发都会被拒绝</p>
            </div>
            <Switch
              id="f-enabled"
              checked={form.enabled}
              onCheckedChange={(v) => set("enabled", v)}
            />
          </div>

          <div className="grid gap-4 sm:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor="f-name">名称 *</Label>
              <Input
                id="f-name"
                value={form.name}
                onChange={(e) => set("name", e.target.value)}
                placeholder="例如 相册备份"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="f-desc">描述</Label>
              <Input
                id="f-desc"
                value={form.description}
                onChange={(e) => set("description", e.target.value)}
                placeholder="可选"
              />
            </div>
          </div>

          <div className="grid gap-4 sm:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor="f-cron">
                cron 表达式 <span className="text-muted-foreground">（留空表示仅手动执行）</span>
              </Label>
              <Input
                id="f-cron"
                value={form.cron_expr}
                onChange={(e) => set("cron_expr", e.target.value)}
                placeholder="0 3 * * *"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="f-timeout">
                任务超时 <span className="text-muted-foreground">（秒，各步骤的默认超时；0 表示不限制）</span>
              </Label>
              <Input
                id="f-timeout"
                type="number"
                min={0}
                value={form.timeout_seconds}
                onChange={(e) => set("timeout_seconds", e.target.value)}
              />
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2 text-sm">
            <Button variant="outline" size="sm" onClick={validateCron}>
              校验表达式
            </Button>
            <div className="min-w-0 flex-1">{cronResult}</div>
          </div>

          {/* ---------------------------------------------------------------- */}
          {/* 步骤                                                              */}
          {/* ---------------------------------------------------------------- */}
          <div className="space-y-2">
            <div className="flex items-center justify-between gap-2">
              <div className="flex items-center gap-2">
                <Label className="text-sm">执行步骤</Label>
                <Badge variant="secondary">{form.steps.length} 步</Badge>
              </div>
              <Button
                variant="outline"
                size="sm"
                onClick={addStep}
                disabled={form.steps.length >= MAX_STEPS}
              >
                <Plus /> 添加步骤
              </Button>
            </div>
            <p className="text-xs text-muted-foreground">
              从上到下依次执行；「间隔」是本步结束后、下一步开始前的等待秒数（最后一步之后不等待）。
            </p>

            <div className="space-y-3">
              {form.steps.map((s, i) => {
                const showDedupe = DEDUPE_KINDS.includes(s.kind)
                const prefix = `s${i}`
                return (
                  <div key={i} className="space-y-3 rounded-lg border p-3">
                    <div className="flex flex-wrap items-center gap-2">
                      <Badge variant="outline">步骤 {i + 1}</Badge>
                      <Input
                        className="h-8 max-w-[12rem] flex-1"
                        value={s.name}
                        onChange={(e) => patchStep(i, { name: e.target.value })}
                        placeholder="备注（可选，失败时用来定位）"
                        aria-label={`步骤 ${i + 1} 备注`}
                      />
                      <div className="flex items-center gap-1">
                        <Button
                          variant="ghost"
                          size="icon"
                          className="size-8"
                          onClick={() => moveStep(i, -1)}
                          disabled={i === 0}
                          aria-label={`上移步骤 ${i + 1}`}
                        >
                          <ArrowUp />
                        </Button>
                        <Button
                          variant="ghost"
                          size="icon"
                          className="size-8"
                          onClick={() => moveStep(i, 1)}
                          disabled={i === form.steps.length - 1}
                          aria-label={`下移步骤 ${i + 1}`}
                        >
                          <ArrowDown />
                        </Button>
                        <Button
                          variant="ghost"
                          size="icon"
                          className="size-8"
                          onClick={() => duplicateStep(i)}
                          disabled={form.steps.length >= MAX_STEPS}
                          aria-label={`复制步骤 ${i + 1}`}
                        >
                          <Copy />
                        </Button>
                        <Button
                          variant="ghost"
                          size="icon"
                          className="size-8 text-destructive hover:bg-destructive/10 hover:text-destructive"
                          onClick={() => removeStep(i)}
                          disabled={form.steps.length <= 1}
                          aria-label={`删除步骤 ${i + 1}`}
                        >
                          <Trash2 />
                        </Button>
                      </div>
                    </div>

                    <div className="grid gap-3 sm:grid-cols-[9rem_1fr_1fr]">
                      <div className="space-y-1.5">
                        <Label htmlFor={`${prefix}-kind`} className="text-xs">
                          类型
                        </Label>
                        <Select
                          value={s.kind}
                          onValueChange={(v) => {
                            const kind = v as TaskKind
                            // 切到不支持的类型时把开关关掉，避免提交一个服务端会拒绝的组合。
                            patchStep(i, {
                              kind,
                              dedupe_before: DEDUPE_KINDS.includes(kind) && s.dedupe_before,
                            })
                          }}
                        >
                          <SelectTrigger id={`${prefix}-kind`} className="h-8 w-full">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            {Object.keys(KIND_TEXT).map((k) => (
                              <SelectItem key={k} value={k}>
                                {KIND_TEXT[k]}（{k}）
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                      <div className="space-y-1.5">
                        <Label htmlFor={`${prefix}-src`} className="text-xs">
                          源路径 *
                        </Label>
                        <Input
                          id={`${prefix}-src`}
                          className="h-8 font-mono text-xs"
                          value={s.source}
                          onChange={(e) => patchStep(i, { source: e.target.value })}
                          placeholder="remote:bucket/path"
                        />
                      </div>
                      <div className="space-y-1.5">
                        <Label htmlFor={`${prefix}-dst`} className="text-xs">
                          目标路径{needsDest(s.kind) ? " *" : ""}
                        </Label>
                        <Input
                          id={`${prefix}-dst`}
                          className="h-8 font-mono text-xs"
                          value={s.dest}
                          onChange={(e) => patchStep(i, { dest: e.target.value })}
                          placeholder={needsDest(s.kind) ? "remote:bucket/path" : "purge / mkdir 可留空"}
                        />
                      </div>
                    </div>

                    <div className="grid gap-3 sm:grid-cols-[7rem_1fr]">
                      <div className="space-y-1.5">
                        <Label htmlFor={`${prefix}-delay`} className="text-xs">
                          间隔（秒）
                        </Label>
                        <Input
                          id={`${prefix}-delay`}
                          className="h-8"
                          type="number"
                          min={0}
                          value={s.delay_after}
                          onChange={(e) => patchStep(i, { delay_after: e.target.value })}
                        />
                      </div>
                      <div className="space-y-1.5">
                        <Label htmlFor={`${prefix}-onerr`} className="text-xs">
                          失败后
                        </Label>
                        <Select
                          value={s.on_error}
                          onValueChange={(v) => patchStep(i, { on_error: v as StepOnError })}
                        >
                          <SelectTrigger id={`${prefix}-onerr`} className="h-8 w-full">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="continue">继续执行后续步骤（默认）</SelectItem>
                            <SelectItem value="abort">中止，后续步骤不再执行</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                    </div>

                    {showDedupe ? (
                      <div className="flex items-center justify-between gap-3 rounded-md border bg-muted/40 px-3 py-2">
                        <div className="space-y-0.5">
                          <Label htmlFor={`${prefix}-dedupe`} className="text-xs">
                            清理目标重名
                          </Label>
                          <p className="flex gap-1.5 text-xs text-muted-foreground">
                            <ShieldAlert className="mt-0.5 size-3.5 shrink-0" />
                            <span>
                              只有源与目标<strong>所有</strong>同名副本都不同（这次一定会传输）时才删目标副本再写入
                            </span>
                          </p>
                        </div>
                        <Switch
                          id={`${prefix}-dedupe`}
                          checked={s.dedupe_before}
                          onCheckedChange={(v) => patchStep(i, { dedupe_before: v })}
                        />
                      </div>
                    ) : null}

                    <details className="group">
                      <summary className="cursor-pointer select-none text-xs text-muted-foreground hover:text-foreground">
                        高级（本步超时 / 本步参数）
                      </summary>
                      <div className="mt-2 grid gap-3 sm:grid-cols-2">
                        <div className="space-y-1.5">
                          <Label htmlFor={`${prefix}-timeout`} className="text-xs">
                            本步超时（秒，0 表示用任务级超时）
                          </Label>
                          <Input
                            id={`${prefix}-timeout`}
                            className="h-8"
                            type="number"
                            min={0}
                            value={s.timeout_seconds}
                            onChange={(e) => patchStep(i, { timeout_seconds: e.target.value })}
                          />
                        </div>
                        <div className="space-y-1.5">
                          <Label htmlFor={`${prefix}-flags`} className="text-xs">
                            本步 extra_flags（JSON 对象）
                          </Label>
                          <Textarea
                            id={`${prefix}-flags`}
                            rows={2}
                            className="font-mono text-xs"
                            value={s.flags}
                            onChange={(e) => patchStep(i, { flags: e.target.value })}
                            placeholder='{"transfers": 8}'
                          />
                        </div>
                      </div>
                    </details>
                  </div>
                )
              })}
            </div>
          </div>

          {error ? <p className="text-sm text-destructive">{error}</p> : null}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            取消
          </Button>
          <Button onClick={save} disabled={busy}>
            {busy ? <Loader2 className="animate-spin" /> : null} 保存
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
