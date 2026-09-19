import * as React from "react"
import { Database, HardDrive, Info, Loader2, RotateCcw, Save, Trash2 } from "lucide-react"
import { toast } from "sonner"

import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import { Label } from "@/components/ui/label"
import { useConfirm } from "@/components/confirm-provider"
import { api } from "@/lib/api"
import { fmtBytes, fmtTime } from "@/lib/format"
import type { CleanupResult, SettingsInfo } from "@/lib/types"

/** 保留时长选项（小时）：0 表示不限制。 */
const RETENTION_OPTIONS: { value: string; label: string }[] = [
  { value: "0", label: "不限制" },
  { value: "12", label: "12 小时" },
  { value: "24", label: "1 天" },
  { value: "72", label: "3 天" },
  { value: "168", label: "7 天" },
  { value: "360", label: "15 天" },
  { value: "720", label: "30 天" },
  { value: "2160", label: "90 天" },
  { value: "4320", label: "180 天" },
  { value: "8760", label: "365 天" },
]

/** 每个任务保留的运行记录条数：0 表示不裁剪。 */
const RETAIN_OPTIONS: { value: string; label: string }[] = [
  { value: "0", label: "不裁剪" },
  { value: "100", label: "100 条" },
  { value: "500", label: "500 条" },
  { value: "1000", label: "1000 条" },
  { value: "5000", label: "5000 条" },
]

/** 日志片段（rclone 输出）的处理策略。 */
const LOG_POLICY_OPTIONS: { value: string; label: string }[] = [
  { value: "keep", label: "全部保留" },
  { value: "success", label: "只保留失败与取消的（推荐）" },
  { value: "all", label: "全部丢弃（不推荐）" },
]

const SOURCE_TEXT: Record<string, string> = {
  ui: "界面设置（已保存，重启后仍然生效）",
  config: "配置文件 storage.run_retention",
  default: "未设置（默认不限制）",
}

/** 占比文案；分母为 0 时返回 0。 */
function pct(part: number, total: number): string {
  if (!total) return "0"
  return ((part / total) * 100).toFixed(0)
}

export function SettingsPage() {
  const confirm = useConfirm()

  const [info, setInfo] = React.useState<SettingsInfo | null>(null)
  const [loading, setLoading] = React.useState(true)
  const [hours, setHours] = React.useState("0")
  const [saving, setSaving] = React.useState(false)
  const [pruning, setPruning] = React.useState(false)

  // 瘦身面板
  const [cleanupBusy, setCleanupBusy] = React.useState(false)
  const [logPolicy, setLogPolicy] = React.useState("success")
  const [retainPerTask, setRetainPerTask] = React.useState("0")
  const [cleanOrphans, setCleanOrphans] = React.useState(true)
  const [reclaim, setReclaim] = React.useState(true)
  const [cleanupResult, setCleanupResult] = React.useState<CleanupResult | null>(null)

  const load = React.useCallback(async () => {
    try {
      const res = await api.settings()
      setInfo(res)
      // 下拉里没有的自定义值（比如配置文件写了 36h）也要能显示出来。
      setHours((prev) => (prev === "" ? String(res.retention_hours) : prev))
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "加载设置失败")
    } finally {
      setLoading(false)
    }
  }, [])

  React.useEffect(() => {
    void load()
  }, [load])

  // 后端返回值变化（首次加载 / 保存后）时同步下拉。
  React.useEffect(() => {
    if (info) setHours(String(info.retention_hours))
  }, [info])

  const db = info?.db

  // 首次拿到设置后，把「每个任务保留条数」预填成配置文件里的值。
  React.useEffect(() => {
    if (!info) return
    const v = String(info.history_limit)
    setRetainPerTask(RETAIN_OPTIONS.some((o) => o.value === v) ? v : "0")
  }, [info])

  const current = info?.retention_hours ?? 0
  const dirty = String(current) !== hours

  async function save() {
    setSaving(true)
    try {
      const res = await api.updateSettings(Number(hours) || 0)
      setInfo(res)
      toast.success(
        res.retention_hours > 0 ? `已设置保留 ${res.retention_text}` : "已设置为不限制",
      )
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "保存失败")
    } finally {
      setSaving(false)
    }
  }

  async function pruneNow() {
    const pending = info?.pending ?? 0
    const ok = await confirm({
      title: `确认清理 ${pending} 条过期运行记录？`,
      description: "将连同其中的 rclone 日志片段一并删除，不可恢复；运行中的记录不受影响。",
      confirmText: "清理",
    })
    if (!ok) return
    setPruning(true)
    try {
      const res = await api.pruneRuns()
      toast.success(
        res.deleted > 0 ? `已清理 ${res.deleted} 条过期记录` : "没有需要清理的记录",
      )
      await load()
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "清理失败")
    } finally {
      setPruning(false)
    }
  }

  async function cleanup() {
    const parts: string[] = []
    if (logPolicy === "success" && db) {
      parts.push(
        `清空 ${db.success_log_runs} 条成功记录的日志片段（约 ${fmtBytes(db.success_log_bytes)}）`,
      )
    }
    if (logPolicy === "all" && db) {
      parts.push(
        `清空全部 ${db.log_tail_runs} 条记录的日志片段（约 ${fmtBytes(db.log_tail_bytes)}）`,
      )
    }
    if (Number(retainPerTask) > 0) {
      parts.push(`每个任务只保留最近 ${retainPerTask} 条运行记录`)
    }
    if (cleanOrphans && db && db.orphan_steps + db.orphan_runs > 0) {
      parts.push(`清理 ${db.orphan_steps} 条孤儿步骤与 ${db.orphan_runs} 条孤儿记录`)
    }
    if (reclaim) {
      parts.push("截断 WAL 并整理数据库文件（有任务在运行时会自动跳过）")
    }
    if (parts.length === 0) {
      toast.error("没有选择任何清理项")
      return
    }

    const ok = await confirm({
      title: "确认执行数据库瘦身？",
      description: `${parts.join("；")}。删除不可恢复；运行中的记录不受影响。`,
      confirmText: "执行",
    })
    if (!ok) return

    setCleanupBusy(true)
    try {
      const res = await api.cleanupDatabase({
        drop_success_log_tail: logPolicy === "success",
        drop_all_log_tail: logPolicy === "all",
        retain_per_task: Number(retainPerTask) || 0,
        clean_orphans: cleanOrphans,
        reclaim,
      })
      setCleanupResult(res)
      const bits: string[] = []
      if (res.logs_cleared > 0) bits.push(`${res.logs_cleared} 条日志片段`)
      if (res.runs_pruned > 0) bits.push(`${res.runs_pruned} 条运行记录`)
      if (res.orphan_steps_cleared + res.orphan_runs_cleared > 0) {
        bits.push(`${res.orphan_steps_cleared + res.orphan_runs_cleared} 条孤儿数据`)
      }
      const freed = res.reclaimed_bytes > 0 ? `，释放 ${fmtBytes(res.reclaimed_bytes)}` : ""
      toast.success(bits.length ? `已清理 ${bits.join("、")}${freed}` : `已整理数据库${freed}`)
      if (res.reclaim_skipped) toast.warning(res.reclaim_skipped)
      await load()
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "瘦身失败")
    } finally {
      setCleanupBusy(false)
    }
  }

  const storage = info?.storage

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle className="text-base">运行记录保留</CardTitle>
          <CardDescription>
            运行记录及其中的 rclone 日志片段保存在本地数据库里。设置保留时长后，后台会定期清理
            {info?.interval ? `（每 ${info.interval} 检查一次）` : ""}
            已结束且超过保留期的记录；运行中的记录永不清。
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-3 sm:grid-cols-3">
            <Stat label="运行记录" value={storage ? `${storage.runs} 条` : "—"} />
            <Stat
              label="最早一条"
              value={storage?.oldest_run_at ? fmtTime(storage.oldest_run_at) : "—"}
            />
            <Stat
              label="数据库占用"
              value={
                db ? fmtBytes(db.total_bytes) : storage ? fmtBytes(storage.db_size_bytes) : "—"
              }
              icon={Database}
            />
          </div>

          <div className="flex flex-wrap items-end gap-2">
            <div className="space-y-1.5">
              <label className="text-xs text-muted-foreground" htmlFor="retention">
                保留时长
              </label>
              <Select value={hours} onValueChange={setHours} disabled={loading}>
                <SelectTrigger id="retention" className="w-36">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {RETENTION_OPTIONS.map((o) => (
                    <SelectItem key={o.value} value={o.value}>
                      {o.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <Button onClick={save} disabled={!dirty || saving}>
              {saving ? <Loader2 className="animate-spin" /> : <Save />}
              保存
            </Button>
            {dirty ? (
              <Button variant="ghost" onClick={() => setHours(String(current))}>
                <RotateCcw /> 撤销
              </Button>
            ) : null}
          </div>

          <p className="text-xs text-muted-foreground">
            当前生效：{info?.retention_text ?? "—"}
            {info ? ` · 来源：${SOURCE_TEXT[info.retention_source] ?? info.retention_source}` : ""}
          </p>

          <div className="flex flex-wrap items-center gap-2 rounded-lg border border-dashed p-3">
            <span className="text-sm text-muted-foreground">
              当前策略下待清理 {info?.pending ?? 0} 条
            </span>
            <Button
              variant="destructive"
              size="sm"
              className="ml-auto"
              disabled={!info || info.pending === 0 || pruning}
              onClick={pruneNow}
            >
              {pruning ? <Loader2 className="animate-spin" /> : <Trash2 />}
              立即清理
            </Button>
          </div>
          <p className="text-xs text-muted-foreground">
            清理后会整理数据库文件，磁盘占用才会真正回落。
          </p>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">数据库瘦身</CardTitle>
          <CardDescription>
            删除只标记空闲页，文件并不会立刻变小；运行记录里的 rclone
            日志片段通常是整个库最大的一块占用。这里的操作只影响已结束的记录，运行中的任务永远不动。
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <Stat
              label="数据库总计"
              value={db ? fmtBytes(db.total_bytes) : "—"}
              icon={Database}
            />
            <Stat
              label="主文件 / WAL"
              value={db ? `${fmtBytes(db.main_bytes)} / ${fmtBytes(db.wal_bytes)}` : "—"}
              icon={HardDrive}
            />
            <Stat label="可回收空间" value={db ? fmtBytes(db.reclaimable_bytes) : "—"} />
            <Stat
              label="运行记录"
              value={db ? `${db.runs} 条（运行中 ${db.active_runs}）` : "—"}
            />
          </div>

          <div className="flex gap-2 rounded-lg border border-dashed p-3 text-sm">
            <Info className="mt-0.5 size-4 shrink-0 text-muted-foreground" />
            <div className="min-w-0 space-y-0.5">
              <div>
                日志片段占用 <span className="tabular">{db ? fmtBytes(db.log_tail_bytes) : "—"}</span>
                {db && db.total_bytes > 0 ? `（占数据库 ${pct(db.log_tail_bytes, db.total_bytes)}%）` : ""}
                ，共 {db?.log_tail_runs ?? 0} 条记录带日志。
              </div>
              <div className="text-muted-foreground">
                其中成功记录 {db ? fmtBytes(db.success_log_bytes) : "—"}（
                {db?.success_log_runs ?? 0} 条）——这部分几乎没人回看，清理收益最大。
              </div>
            </div>
          </div>

          <div className="grid gap-4 sm:grid-cols-2">
            <div className="space-y-1.5">
              <label className="text-xs text-muted-foreground" htmlFor="log-policy">
                日志片段处理
              </label>
              <Select value={logPolicy} onValueChange={setLogPolicy} disabled={loading}>
                <SelectTrigger id="log-policy" className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {LOG_POLICY_OPTIONS.map((o) => (
                    <SelectItem key={o.value} value={o.value}>
                      {o.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <p className="text-xs text-muted-foreground">
                失败与取消记录的日志留在库里，便于事后排查。
              </p>
            </div>

            <div className="space-y-1.5">
              <label className="text-xs text-muted-foreground" htmlFor="retain-per-task">
                每个任务保留的运行记录
              </label>
              <Select value={retainPerTask} onValueChange={setRetainPerTask} disabled={loading}>
                <SelectTrigger id="retain-per-task" className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {RETAIN_OPTIONS.map((o) => (
                    <SelectItem key={o.value} value={o.value}>
                      {o.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <p className="text-xs text-muted-foreground">
                超出部分按时间从旧到新删除；默认值取自配置文件的 storage.history_limit。
              </p>
            </div>
          </div>

          <div className="space-y-3 rounded-lg border p-3">
            <div className="flex items-center justify-between gap-3">
              <div className="min-w-0">
                <Label htmlFor="clean-orphans" className="text-sm">
                  清理孤儿数据
                </Label>
                <p className="text-xs text-muted-foreground">
                  任务已删除但残留的步骤定义与运行记录（当前 {db?.orphan_steps ?? 0} 条步骤、
                  {db?.orphan_runs ?? 0} 条记录）。
                </p>
              </div>
              <Switch
                id="clean-orphans"
                checked={cleanOrphans}
                onCheckedChange={setCleanOrphans}
                disabled={loading}
              />
            </div>
            <div className="flex items-center justify-between gap-3">
              <div className="min-w-0">
                <Label htmlFor="reclaim" className="text-sm">
                  整理数据库文件
                </Label>
                <p className="text-xs text-muted-foreground">
                  截断 WAL 并回收空闲页（预计可释放 {db ? fmtBytes(db.reclaimable_bytes) : "—"}
                  ）。会重写整个库，有任务在运行时自动跳过。
                </p>
              </div>
              <Switch
                id="reclaim"
                checked={reclaim}
                onCheckedChange={setReclaim}
                disabled={loading}
              />
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <Button variant="destructive" onClick={cleanup} disabled={loading || cleanupBusy}>
              {cleanupBusy ? <Loader2 className="animate-spin" /> : <Trash2 />}
              执行瘦身
            </Button>
            {cleanupResult ? (
              <span className="text-xs text-muted-foreground">
                上次：清理 {cleanupResult.logs_cleared} 条日志、
                {cleanupResult.runs_pruned} 条记录，
                {cleanupResult.reclaimed_bytes > 0
                  ? `释放 ${fmtBytes(cleanupResult.reclaimed_bytes)}`
                  : "未回收空间"}
                （耗时 {cleanupResult.duration_ms} ms）
              </span>
            ) : null}
          </div>
          {cleanupResult?.reclaim_skipped ? (
            <p className="text-xs text-amber-600">{cleanupResult.reclaim_skipped}</p>
          ) : null}
        </CardContent>
      </Card>
    </div>
  )
}

function Stat({
  label,
  value,
  icon: Icon,
}: {
  label: string
  value: string
  icon?: React.ComponentType<{ className?: string }>
}) {
  return (
    <div className="rounded-lg border p-3">
      <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
        {Icon ? <Icon className="size-3.5" /> : null}
        {label}
      </div>
      <div className="tabular mt-1 text-sm font-medium">{value}</div>
    </div>
  )
}
