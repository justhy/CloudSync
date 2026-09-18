import * as React from "react"
import { Settings2, Trash2, X } from "lucide-react"
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
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { ProgressCell } from "@/components/progress-cell"
import { StatusBadge } from "@/components/status-badge"
import { RunDetailDialog } from "@/components/run-detail"
import { useConfirm } from "@/components/confirm-provider"
import { useAutoPoll } from "@/hooks/use-poll"
import { useNow } from "@/hooks/use-now"
import { api, isActive } from "@/lib/api"
import { KIND_TEXT, fmtBytes, fmtDuration, fmtTime, fromNow, triggerText } from "@/lib/format"
import { useSession } from "@/state/session"
import type { Run, RunStatus } from "@/lib/types"

const PAGE_SIZE = 50

const STATUS_OPTIONS: { value: string; label: string }[] = [
  { value: "all", label: "全部状态" },
  { value: "running", label: "运行中" },
  { value: "pending", label: "排队中" },
  { value: "success", label: "成功" },
  { value: "failed", label: "失败" },
  { value: "canceled", label: "已取消" },
]

export function RunsPage({ onOpenSettings }: { onOpenSettings?: () => void }) {
  const { activeCount, eventTick, refresh } = useSession()
  const confirm = useConfirm()
  const now = useNow()

  const [items, setItems] = React.useState<Run[]>([])
  const [total, setTotal] = React.useState(0)
  const [offset, setOffset] = React.useState(0)
  const [status, setStatus] = React.useState("all")
  const [loading, setLoading] = React.useState(true)
  const [viewRun, setViewRun] = React.useState<number | null>(null)

  const load = React.useCallback(async () => {
    try {
      const res = await api.runs({
        limit: PAGE_SIZE,
        offset,
        status: status === "all" ? undefined : status,
      })
      setItems(res.items ?? [])
      setTotal(res.total ?? 0)
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "加载运行记录失败")
    } finally {
      setLoading(false)
    }
  }, [offset, status])

  React.useEffect(() => {
    void load()
  }, [load, eventTick])

  useAutoPoll(load, activeCount > 0 ? 2000 : 15000)

  async function cancelRun(run: Run) {
    try {
      await api.cancelRun(run.id)
      toast.warning(`已请求取消运行 #${run.id}`)
      await load()
      refresh()
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "取消失败")
    }
  }

  async function deleteRun(run: Run) {
    const ok = await confirm({
      title: `确认删除运行记录 #${run.id}？`,
      description: "此操作不可恢复。",
      confirmText: "删除",
    })
    if (!ok) return
    try {
      await api.deleteRun(run.id)
      toast.success(`运行记录 #${run.id} 已删除`)
      await load()
      refresh()
    } catch (err) {
      const msg = err instanceof Error ? err.message : "删除失败"
      ;(msg.includes("运行") ? toast.warning : toast.error)(msg)
    }
  }

  async function clearAll() {
    const ok = await confirm({
      title: "确认清空全部运行记录？",
      description: "此操作不可恢复；任务本身的最近状态不受影响。清空后编号会从头开始。",
      confirmText: "清空",
    })
    if (!ok) return
    try {
      const res = await api.deleteAllRuns()
      toast.success(`已清空 ${res.deleted ?? 0} 条运行记录`)
      setOffset(0)
      await load()
      refresh()
    } catch (err) {
      const msg = err instanceof Error ? err.message : "清空失败"
      ;(msg.includes("运行") ? toast.warning : toast.error)(msg)
    }
  }

  const range = total
    ? `${offset + 1} - ${Math.min(offset + items.length, total)}`
    : "0"

  return (
    <>
      <Card>
        <CardHeader className="flex-row flex-wrap items-center justify-between gap-2 space-y-0">
          <div>
            <CardTitle className="text-base">运行记录</CardTitle>
            <CardDescription>共 {total} 条，当前显示第 {range} 条</CardDescription>
          </div>
          <div className="flex items-center gap-2">
            <Select
              value={status}
              onValueChange={(v) => {
                setStatus(v)
                setOffset(0)
              }}
            >
              <SelectTrigger className="w-32">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {STATUS_OPTIONS.map((o) => (
                  <SelectItem key={o.value} value={o.value}>
                    {o.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            {onOpenSettings ? (
              <Button variant="ghost" onClick={onOpenSettings}>
                <Settings2 /> 保留策略
              </Button>
            ) : null}
            <Button variant="destructive" onClick={clearAll}>
              <Trash2 /> 删除全部
            </Button>
          </div>
        </CardHeader>
        <CardContent className="px-0 pb-0">
          {!loading && items.length === 0 ? (
            <p className="px-6 pb-6 text-sm text-muted-foreground">暂无运行记录</p>
          ) : (
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-14">#</TableHead>
                    <TableHead>任务</TableHead>
                    <TableHead className="hidden w-20 md:table-cell">类型</TableHead>
                    <TableHead className="hidden w-20 sm:table-cell">触发</TableHead>
                    <TableHead className="w-28">状态</TableHead>
                    <TableHead className="w-56">进度</TableHead>
                    <TableHead className="hidden w-28 lg:table-cell">数据量</TableHead>
                    <TableHead className="w-28">耗时</TableHead>
                    <TableHead className="hidden w-44 xl:table-cell">开始时间</TableHead>
                    <TableHead className="w-40 text-right">操作</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {items.map((r) => {
                    const active = isActive(r.status)
                    return (
                      <TableRow key={r.id}>
                        <TableCell className="tabular text-muted-foreground">{r.id}</TableCell>
                        <TableCell className="max-w-[12rem] truncate font-medium">
                          {r.task_name}
                        </TableCell>
                        <TableCell className="hidden text-muted-foreground md:table-cell">
                          {KIND_TEXT[r.kind] ?? r.kind}
                        </TableCell>
                        <TableCell className="hidden text-muted-foreground sm:table-cell">
                          {triggerText(r.trigger)}
                        </TableCell>
                        <TableCell>
                          <StatusBadge status={r.status as RunStatus} />
                        </TableCell>
                        <TableCell>
                          <ProgressCell
                            percent={r.percent}
                            bytes={r.bytes}
                            totalBytes={r.total_bytes}
                            files={r.files}
                            totalFiles={r.total_files}
                            active={active}
                          />
                        </TableCell>
                        <TableCell className="tabular hidden text-muted-foreground lg:table-cell">
                          {fmtBytes(r.bytes)}
                        </TableCell>
                        <TableCell className="tabular text-muted-foreground">
                          <span data-now={now}>
                            {r.duration_ms
                              ? fmtDuration(r.duration_ms)
                              : active
                                ? fromNow(r.started_at)
                                : "—"}
                          </span>
                        </TableCell>
                        <TableCell className="tabular hidden text-muted-foreground xl:table-cell">
                          {fmtTime(r.started_at)}
                        </TableCell>
                        <TableCell>
                          {/* 详情固定在最左，右侧才是随状态变化的破坏性操作。 */}
                          <div className="flex justify-end gap-1">
                            <Button variant="outline" size="sm" onClick={() => setViewRun(r.id)}>
                              详情
                            </Button>
                            {active ? (
                              <Button
                                variant="destructive"
                                size="sm"
                                onClick={() => cancelRun(r)}
                              >
                                <X /> 取消
                              </Button>
                            ) : (
                              <Button
                                variant="ghost"
                                size="sm"
                                className="text-destructive hover:bg-destructive/10 hover:text-destructive"
                                onClick={() => deleteRun(r)}
                              >
                                <Trash2 /> 删除
                              </Button>
                            )}
                          </div>
                        </TableCell>
                      </TableRow>
                    )
                  })}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>

      <div className="mt-3 flex items-center justify-between gap-2">
        <span className="text-xs text-muted-foreground">
          第 {Math.floor(offset / PAGE_SIZE) + 1} 页 / 共 {Math.max(1, Math.ceil(total / PAGE_SIZE))} 页
        </span>
        <div className="flex gap-2">
          <Button
            variant="outline"
            size="sm"
            disabled={offset <= 0}
            onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
          >
            上一页
          </Button>
          <Button
            variant="outline"
            size="sm"
            disabled={offset + PAGE_SIZE >= total}
            onClick={() => setOffset(offset + PAGE_SIZE)}
          >
            下一页
          </Button>
        </div>
      </div>

      <RunDetailDialog
        runId={viewRun}
        open={viewRun !== null}
        onOpenChange={(open) => {
          if (!open) setViewRun(null)
        }}
      />
    </>
  )
}
