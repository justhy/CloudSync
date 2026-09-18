import * as React from "react"
import { Activity, Clock, Hash, RefreshCw, RotateCw, Server, Tag } from "lucide-react"
import { toast } from "sonner"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Label } from "@/components/ui/label"
import { Switch } from "@/components/ui/switch"
import { useConfirm } from "@/components/confirm-provider"
import { api } from "@/lib/api"
import { fmtBytes, fmtDuration, fmtSpeed, fmtTime } from "@/lib/format"

interface RcloneState {
  status: {
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
  stats?: { bytes: number; totalBytes: number; transfers: number; errors: number; speed: number }
  memstats?: { Alloc: number; NumGC: number }
  groups?: string[]
}

export function RclonePage() {
  const confirm = useConfirm()
  const [data, setData] = React.useState<RcloneState | null>(null)
  const [log, setLog] = React.useState<string[]>([])
  const [remotes, setRemotes] = React.useState<string[] | null>(null)
  const [autoLog, setAutoLog] = React.useState(true)

  const load = React.useCallback(async () => {
    try {
      setData((await api.rclone()) as RcloneState)
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "加载 rclone 状态失败")
    }
  }, [])

  const loadLog = React.useCallback(async () => {
    try {
      const res = await api.rcloneLog(400)
      setLog(res.lines ?? [])
    } catch {
      /* 日志拉不到不影响其它面板 */
    }
  }, [])

  React.useEffect(() => {
    void load()
    void loadLog()
  }, [load, loadLog])

  // 日志自动刷新：这一页的日志是全局的 rclone 输出，5s 一次足够。
  React.useEffect(() => {
    if (!autoLog) return
    const timer = window.setInterval(() => {
      if (!document.hidden) void loadLog()
    }, 5000)
    return () => window.clearInterval(timer)
  }, [autoLog, loadLog])

  async function restart() {
    const ok = await confirm({
      title: "确认重启 rclone 子进程？",
      description: "重启期间任务无法执行。",
      confirmText: "重启",
    })
    if (!ok) return
    try {
      await api.rcloneRestart()
      toast.success("rclone 已重启")
      await load()
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "重启失败")
    }
  }

  async function loadRemotes() {
    try {
      const res = await api.rcloneRemotes()
      setRemotes(res.remotes ?? [])
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "加载远端失败")
    }
  }

  const s = data?.status
  const cards = [
    {
      label: "状态",
      value: s ? (s.ready ? "就绪" : s.state || "未知") : "—",
      icon: Activity,
      tone: s?.ready ? "text-success" : "text-destructive",
    },
    { label: "版本", value: s?.version || "—", icon: Tag },
    { label: "PID", value: s?.pid ? String(s.pid) : "—", icon: Hash },
    {
      label: "运行时长",
      value: s?.uptime_seconds ? fmtDuration(s.uptime_seconds * 1000) : "—",
      icon: Clock,
    },
    { label: "重启次数", value: String(s?.restarts ?? 0), icon: RotateCw },
    { label: "日志行数", value: String(s?.journal_lines ?? 0), icon: Server },
  ]

  const rows: [string, string][] = s
    ? [
        ["RC 地址", s.endpoint || "—"],
        ["可执行文件", s.binary || "—"],
        ["配置文件", s.config_file || "（rclone 默认）"],
        [
          "托管方式",
          s.external ? "外部实例（不由本程序管理）" : s.auto_start ? "本程序托管" : "未托管",
        ],
        ["自动重启", s.auto_restart ? `开启（已重启 ${s.restarts ?? 0} 次）` : "关闭"],
        ["启动时间", fmtTime(s.started_at)],
        ["最近错误", s.last_error || "无"],
        [
          "传输统计",
          data?.stats
            ? `已传 ${fmtBytes(data.stats.bytes)} / ${fmtBytes(data.stats.totalBytes)}，${data.stats.transfers} 文件，${data.stats.errors} 错误，速度 ${fmtSpeed(data.stats.speed)}`
            : "—",
        ],
        [
          "内存占用",
          data?.memstats?.Alloc
            ? `${fmtBytes(data.memstats.Alloc)} / GC ${data.memstats.NumGC} 次`
            : "—",
        ],
        ["活跃统计组", data?.groups?.length ? data.groups.join(", ") : "无"],
      ]
    : []

  return (
    <div className="space-y-6">
      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
        {cards.map((c) => {
          const Icon = c.icon
          return (
            <Card key={c.label} className="transition-shadow hover:shadow-md">
              <CardContent className="flex items-center justify-between gap-3 p-4">
                <div className="min-w-0">
                  <div className="text-xs text-muted-foreground">{c.label}</div>
                  <div className={`tabular mt-1 truncate text-xl font-semibold ${c.tone ?? ""}`}>
                    {c.value}
                  </div>
                </div>
                <div className="flex size-10 shrink-0 items-center justify-center rounded-lg bg-muted text-muted-foreground">
                  <Icon className="size-5" />
                </div>
              </CardContent>
            </Card>
          )
        })}
      </div>

      <Card>
        <CardHeader className="flex-row items-center justify-between space-y-0">
          <div>
            <CardTitle className="text-base">子进程控制</CardTitle>
            <CardDescription>由本程序托管的 rclone rcd 实例</CardDescription>
          </div>
          {s && !s.external ? (
            <Button variant="outline" onClick={restart}>
              <RotateCw /> 重启 rclone
            </Button>
          ) : null}
        </CardHeader>
        <CardContent className="grid gap-2 lg:grid-cols-2">
          {rows.length ? (
            rows.map(([k, v]) => (
              <div key={k} className="flex gap-3 border-b py-1.5 text-sm last:border-0">
                <span className="w-24 shrink-0 text-muted-foreground">{k}</span>
                <span className="tabular min-w-0 break-all">{v}</span>
              </div>
            ))
          ) : (
            <p className="text-sm text-muted-foreground">加载中…</p>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex-row items-center justify-between space-y-0">
          <div>
            <CardTitle className="text-base">远端列表（rclone.conf）</CardTitle>
          </div>
          <Button variant="outline" onClick={loadRemotes}>
            <RefreshCw /> 刷新
          </Button>
        </CardHeader>
        <CardContent>
          {remotes === null ? (
            <p className="text-sm text-muted-foreground">点击「刷新」加载</p>
          ) : remotes.length === 0 ? (
            <p className="text-sm text-muted-foreground">rclone.conf 中还没有配置任何 remote</p>
          ) : (
            <div className="flex flex-wrap gap-2">
              {remotes.map((r) => (
                <Badge key={r} variant="secondary" className="font-mono">
                  {r}
                  {r.includes(";") ? "（本地路径）" : ""}
                </Badge>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex-row items-center justify-between space-y-0 gap-2">
          <div>
            <CardTitle className="text-base">rclone 输出日志</CardTitle>
          </div>
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-2">
              <Switch id="log-auto" checked={autoLog} onCheckedChange={setAutoLog} />
              <Label htmlFor="log-auto" className="text-xs text-muted-foreground">
                自动刷新
              </Label>
            </div>
            <Button variant="outline" size="sm" onClick={() => void loadLog()}>
              <RefreshCw /> 刷新
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          <pre className="log-view max-h-96 overflow-auto rounded-lg border bg-muted/40 p-3 text-xs">
            {log.length ? log.join("\n") : "（暂无输出）"}
          </pre>
        </CardContent>
      </Card>
    </div>
  )
}
