import * as React from "react"
import { Loader2, RefreshCw, RotateCcw } from "lucide-react"
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
import { StatusBadge } from "@/components/status-badge"
import { useAutoPoll } from "@/hooks/use-poll"
import { api, isActive } from "@/lib/api"
import { KIND_TEXT, fmtBytes, fmtDuration, fmtSpeed, fmtTime, fromNow, triggerText } from "@/lib/format"
import type { Run } from "@/lib/types"

export function RunDetailDialog({
  runId,
  open,
  onOpenChange,
}: {
  runId: number | null
  open: boolean
  onOpenChange: (open: boolean) => void
}) {
  const [run, setRun] = React.useState<Run | null>(null)
  const [loading, setLoading] = React.useState(false)
  const logRef = React.useRef<HTMLPreElement>(null)
  // 自动刷新会重绘日志区：记住滚动位置，别把正在阅读的用户拽回顶部。
  const scrollTop = React.useRef(0)

  const active = run ? isActive(run.status) : false

  const load = React.useCallback(async () => {
    if (runId === null) return
    try {
      const res = await api.run(runId)
      setRun(res.run)
    } catch {
      /* 详情刷新失败静默处理：弹窗自己有刷新按钮 */
    } finally {
      setLoading(false)
    }
  }, [runId])

  React.useEffect(() => {
    if (!open || runId === null) return
    setLoading(true)
    void load()
  }, [open, runId, load])

  useAutoPoll(load, 2000, open && active)

  // 重绘后恢复滚动位置（在浏览器绘制前执行，用户看不到跳动）。
  React.useLayoutEffect(() => {
    const el = logRef.current
    if (el) el.scrollTop = scrollTop.current
  })

  if (!run) {
    return (
      <Dialog open={open} onOpenChange={onOpenChange}>
        <DialogContent className="sm:max-w-3xl">
          <DialogHeader>
            <DialogTitle>运行详情</DialogTitle>
          </DialogHeader>
          <div className="flex items-center gap-2 py-8 text-sm text-muted-foreground">
            {loading ? <Loader2 className="animate-spin" /> : null} 加载中…
          </div>
        </DialogContent>
      </Dialog>
    )
  }

  const known = run.total_bytes > 0 || run.total_files > 0
  const stepTotal = run.step_total ?? 0
  const results = run.step_results ?? []
  // 「从失败步骤重跑」只对多步骤任务有意义：单步任务重跑就是整任务重跑。
  const failedStep = stepTotal > 1 ? results.find((r) => r.status === "failed") : undefined

  async function rerunFromFailed() {
    if (!run || failedStep === undefined) return
    try {
      const res = await api.runTask(run.task_id, failedStep.position)
      toast.success(`已从步骤 ${failedStep.position + 1} 重新触发，运行 #${res.run.id}`)
    } catch (err) {
      const msg = err instanceof Error ? err.message : "触发失败"
      ;(msg.includes("并发") || msg.includes("冷却") ? toast.warning : toast.error)(msg)
    }
  }

  const rows: [string, React.ReactNode][] = [
    ["任务", `${run.task_name} (#${run.task_id})`],
    ...(stepTotal > 1
      ? ([
          [
            "步骤",
            <span className="tabular">
              {Math.min(run.step_index + 1, stepTotal)} / {stepTotal}
              <span className="ml-2 text-xs text-muted-foreground">
                已记录 {results.length} 条结果
              </span>
            </span>,
          ],
        ] as [string, React.ReactNode][])
      : []),
    ["类型", KIND_TEXT[run.kind] ?? run.kind],
    ["触发方式", triggerText(run.trigger)],
    [
      "状态",
      <span className="flex items-center gap-2">
        <StatusBadge status={run.status} />
        {run.message ? <span className="text-xs text-muted-foreground">{run.message}</span> : null}
      </span>,
    ],
    ["rclone job", run.job_id ? String(run.job_id) : "—"],
    [
      "开始 / 结束",
      `${fmtTime(run.started_at)} → ${run.finished_at ? fmtTime(run.finished_at) : "进行中"}`,
    ],
    [
      "耗时",
      run.duration_ms ? fmtDuration(run.duration_ms) : active ? fromNow(run.started_at) : "—",
    ],
    [
      "已传输",
      `${fmtBytes(run.bytes)} / ${
        known ? `${fmtBytes(run.total_bytes)}（${(run.percent || 0).toFixed(1)}%）` : "未知总量"
      }`,
    ],
    ["文件", `${run.files} / ${run.total_files}`],
    ["速度", fmtSpeed(run.speed)],
    ["错误数 / 致命", `${run.errors} / ${run.fatal_error ? "是" : "否"}`],
    ["重命名 / 删除", `${run.renames} / ${run.deletes}`],
  ]

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[90svh] overflow-y-auto sm:max-w-3xl">
        <DialogHeader>
          <DialogTitle>运行 #{run.id} 详情</DialogTitle>
          <DialogDescription>
            {active ? "任务进行中，日志每 2 秒自动刷新" : "本次运行窗口"}
          </DialogDescription>
        </DialogHeader>

        <div className="grid gap-x-6 gap-y-1.5 text-sm sm:grid-cols-2">
          {rows.map(([k, v]) => (
            <div key={k} className="flex gap-3 border-b py-1.5 last:border-0">
              <span className="w-24 shrink-0 text-muted-foreground">{k}</span>
              <span className="tabular min-w-0 break-all">{v}</span>
            </div>
          ))}
        </div>

        {results.length ? (
          <div className="space-y-1.5">
            <div className="text-xs text-muted-foreground">步骤结果</div>
            <div className="divide-y rounded-lg border">
              {results.map((r) => (
                <div
                  key={r.position}
                  className="flex flex-wrap items-center gap-x-3 gap-y-1 px-3 py-2 text-xs"
                >
                  <Badge variant="outline" className="tabular">
                    {r.position + 1}
                  </Badge>
                  <StatusBadge status={r.status} />
                  <span className="font-medium">{r.name || "未命名步骤"}</span>
                  <span className="text-muted-foreground">{KIND_TEXT[r.kind] ?? r.kind}</span>
                  <span className="tabular text-muted-foreground">{fmtDuration(r.duration_ms)}</span>
                  <span className="tabular text-muted-foreground">
                    {fmtBytes(r.bytes)} · {r.files} 文件
                  </span>
                  {r.error ? (
                    <span className="w-full break-all text-destructive">{r.error}</span>
                  ) : null}
                </div>
              ))}
            </div>
          </div>
        ) : null}

        {run.error ? (
          <div className="space-y-1.5">
            <div className="text-xs text-muted-foreground">错误信息</div>
            <pre className="log-view max-h-40 overflow-auto rounded-lg border bg-muted/40 p-3 text-xs text-destructive">
              {run.error}
            </pre>
          </div>
        ) : null}

        <div className="space-y-1.5">
          <div className="text-xs text-muted-foreground">rclone 输出</div>
          <pre
            ref={logRef}
            onScroll={(e) => {
              scrollTop.current = e.currentTarget.scrollTop
            }}
            className="log-view max-h-72 overflow-auto rounded-lg border bg-muted/40 p-3 text-xs"
          >
            {run.log_tail?.length ? run.log_tail.join("\n") : "（暂无输出）"}
          </pre>
        </div>

        <DialogFooter>
          {failedStep !== undefined ? (
            <Button
              variant="outline"
              onClick={() => void rerunFromFailed()}
              title="前面的步骤刚跑过，重跑要重新比对整棵树，没必要"
            >
              <RotateCcw /> 从步骤 {failedStep.position + 1} 重跑
            </Button>
          ) : null}
          <Button variant="outline" onClick={() => void load()}>
            <RefreshCw /> 刷新
          </Button>
          <Button variant="ghost" onClick={() => onOpenChange(false)}>
            关闭
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
