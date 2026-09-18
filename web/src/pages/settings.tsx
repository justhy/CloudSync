import * as React from "react"
import { Database, Loader2, RotateCcw, Save, Trash2 } from "lucide-react"
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
import { useConfirm } from "@/components/confirm-provider"
import { api } from "@/lib/api"
import { fmtBytes, fmtTime } from "@/lib/format"
import type { SettingsInfo } from "@/lib/types"

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

const SOURCE_TEXT: Record<string, string> = {
  ui: "界面设置（已保存，重启后仍然生效）",
  config: "配置文件 storage.run_retention",
  default: "未设置（默认不限制）",
}

export function SettingsPage() {
  const confirm = useConfirm()

  const [info, setInfo] = React.useState<SettingsInfo | null>(null)
  const [loading, setLoading] = React.useState(true)
  const [hours, setHours] = React.useState("0")
  const [saving, setSaving] = React.useState(false)
  const [pruning, setPruning] = React.useState(false)

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
              value={storage ? fmtBytes(storage.db_size_bytes) : "—"}
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
