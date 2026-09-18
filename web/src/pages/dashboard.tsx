import { Activity, CalendarClock, CheckCircle2, ListChecks, PlayCircle, XCircle } from "lucide-react"
import { toast } from "sonner"

import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { ProgressCell } from "@/components/progress-cell"
import { api } from "@/lib/api"
import { fmtETA, fmtSpeed, fmtTime, fromNow, triggerText } from "@/lib/format"
import { useNow } from "@/hooks/use-now"
import { useSession } from "@/state/session"

export function DashboardPage({ onOpenRun }: { onOpenRun: (id: number) => void }) {
  const { overview, refresh } = useSession()
  const now = useNow()

  const counts = overview?.counts
  const running = overview?.running ?? []
  const scheduler = overview?.scheduler

  const cards = [
    { label: "任务总数", value: counts?.tasks ?? 0, icon: ListChecks },
    { label: "已启用", value: counts?.enabled_tasks ?? 0, icon: PlayCircle },
    {
      label: "定时任务",
      value: counts?.scheduled_tasks ?? 0,
      icon: CalendarClock,
      hint: scheduler?.timezone,
    },
    { label: "运行中", value: counts?.running ?? 0, icon: Activity, tone: "text-primary" },
    { label: "24h 成功", value: counts?.success_24h ?? 0, icon: CheckCircle2, tone: "text-success" },
    { label: "24h 失败", value: counts?.failed_24h ?? 0, icon: XCircle, tone: "text-destructive" },
  ]

  async function cancelRun(id: number) {
    try {
      await api.cancelRun(id)
      toast.warning(`已请求取消运行 #${id}`)
      refresh()
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "取消失败")
    }
  }

  const schedRows: [string, string][] = [
    ["调度器", scheduler?.enabled ? "已启用" : "已禁用"],
    ["时区", scheduler?.timezone || "—"],
    ["表达式精度", scheduler?.seconds ? "6 段（秒级）" : "5 段（分钟级）"],
    ["已注册条目", String(scheduler?.entries ?? 0)],
    [
      "下次触发",
      scheduler?.next
        ? `${scheduler.next.task_name} @ ${fmtTime(scheduler.next.next)}`
        : "—",
    ],
    ["并发上限", `${scheduler?.running ?? 0} 运行中`],
  ]

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
                  <div className={`tabular mt-1 text-2xl font-semibold ${c.tone ?? ""}`}>
                    {c.value}
                  </div>
                  {c.hint ? (
                    <div className="mt-0.5 truncate text-xs text-muted-foreground">{c.hint}</div>
                  ) : null}
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
            <CardTitle className="text-base">正在运行</CardTitle>
            <CardDescription>{running.length} 个任务</CardDescription>
          </div>
        </CardHeader>
        <CardContent className="px-0 pb-0">
          {running.length === 0 ? (
            <p className="px-6 pb-6 text-sm text-muted-foreground">当前没有运行中的任务</p>
          ) : (
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-16">#</TableHead>
                    <TableHead>任务</TableHead>
                    <TableHead className="w-28">触发</TableHead>
                    <TableHead className="w-56">进度</TableHead>
                    <TableHead className="w-40">速度 / 剩余</TableHead>
                    <TableHead className="w-32">耗时</TableHead>
                    <TableHead className="w-20 text-right">操作</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {running.map(({ run, progress }) => (
                    <TableRow key={run.id}>
                      <TableCell className="tabular text-muted-foreground">{run.id}</TableCell>
                      <TableCell>
                        <button
                          type="button"
                          className="font-medium hover:underline"
                          onClick={() => onOpenRun(run.id)}
                        >
                          {run.task_name}
                        </button>
                      </TableCell>
                      <TableCell className="text-muted-foreground">
                        {triggerText(run.trigger)}
                      </TableCell>
                      <TableCell>
                        <ProgressCell
                          percent={progress.percent}
                          bytes={progress.bytes}
                          totalBytes={progress.total_bytes}
                          files={progress.files}
                          totalFiles={progress.total_files}
                          active
                        />
                      </TableCell>
                      <TableCell className="tabular text-muted-foreground">
                        {fmtSpeed(progress.speed)} / {fmtETA(progress.eta_seconds)}
                      </TableCell>
                      <TableCell className="tabular text-muted-foreground">
                        {/* now 只用于让相对时间每秒自增 */}
                        <span data-now={now}>{fromNow(run.started_at)}</span>
                      </TableCell>
                      <TableCell className="text-right">
                        <Button
                          variant="destructive"
                          size="sm"
                          onClick={() => cancelRun(run.id)}
                        >
                          取消
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">调度状态</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-2 sm:grid-cols-2">
          {schedRows.map(([k, v]) => (
            <div key={k} className="flex items-baseline gap-3 text-sm">
              <span className="w-24 shrink-0 text-muted-foreground">{k}</span>
              <span className="min-w-0 break-all">{v}</span>
            </div>
          ))}
        </CardContent>
      </Card>
    </div>
  )
}
