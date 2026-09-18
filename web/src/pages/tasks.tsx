import * as React from "react"
import { Pencil, Play, Plus, Trash2, X } from "lucide-react"
import { toast } from "sonner"

import { Badge } from "@/components/ui/badge"
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
import { Switch } from "@/components/ui/switch"
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"
import { StatusBadge } from "@/components/status-badge"
import { TaskFormDialog } from "@/components/task-form"
import { useConfirm } from "@/components/confirm-provider"
import { useAutoPoll } from "@/hooks/use-poll"
import { api } from "@/lib/api"
import { KIND_TEXT, fmtTime, fromNow } from "@/lib/format"
import { useSession } from "@/state/session"
import type { TaskView } from "@/lib/types"

export function TasksPage() {
  const { activeCount, eventTick, refresh } = useSession()
  const confirm = useConfirm()
  const [items, setItems] = React.useState<TaskView[]>([])
  const [loading, setLoading] = React.useState(true)
  const [editing, setEditing] = React.useState<TaskView | null>(null)
  const [formOpen, setFormOpen] = React.useState(false)

  const load = React.useCallback(async () => {
    try {
      const res = await api.tasks()
      setItems(res.items ?? [])
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "加载任务失败")
    } finally {
      setLoading(false)
    }
  }, [])

  React.useEffect(() => {
    void load()
  }, [load, eventTick])

  useAutoPoll(load, activeCount > 0 ? 2000 : 15000)

  async function toggleEnabled(task: TaskView, next: boolean) {
    try {
      await api.setTaskEnabled(task.id, next)
      toast.success(`任务 #${task.id} 已${next ? "启用" : "停用"}`)
      await load()
      refresh()
    } catch (err) {
      const msg = err instanceof Error ? err.message : "操作失败"
      // 409（任务运行中）等失败场景：开关保持原状由服务端状态为准，重新拉一次。
      toast.error(msg.includes("运行") ? msg : `设置失败：${msg}`)
      await load()
    }
  }

  async function runTask(task: TaskView) {
    try {
      const res = await api.runTask(task.id)
      toast.success(`已触发任务，运行 #${res.run.id}`)
      await load()
      refresh()
    } catch (err) {
      const msg = err instanceof Error ? err.message : "触发失败"
      // 409 并发上限 / 429 冷却中：属于预期内的业务拒绝，用警告语气。
      ;(msg.includes("并发") || msg.includes("冷却") ? toast.warning : toast.error)(msg)
      await load()
    }
  }

  async function removeTask(task: TaskView) {
    const ok = await confirm({
      title: `确认删除任务 #${task.id}？`,
      description: "其历史运行记录会一并删除（任务编号会被复用，留着旧记录会让新任务凭空多出历史）。",
      confirmText: "删除",
    })
    if (!ok) return
    try {
      await api.deleteTask(task.id)
      toast.success(`任务 #${task.id} 已删除`)
      await load()
      refresh()
    } catch (err) {
      // 运行中删除会被服务端拒绝：只提示先取消，不提供"强制删除"旁路。
      const msg = err instanceof Error ? err.message : "删除失败"
      ;(msg.includes("运行") ? toast.warning : toast.error)(msg)
      await load()
    }
  }

  async function cancelTask(task: TaskView) {
    if (!task.running) return
    try {
      await api.cancelRun(task.running.id)
      toast.warning(`已请求取消运行 #${task.running.id}`)
      await load()
      refresh()
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "取消失败")
    }
  }

  function openCreate() {
    setEditing(null)
    setFormOpen(true)
  }

  async function openEdit(task: TaskView) {
    try {
      const res = await api.task(task.id)
      setEditing(res.task)
      setFormOpen(true)
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "加载任务失败")
    }
  }

  return (
    <TooltipProvider delayDuration={300}>
      <Card>
        <CardHeader className="flex-row items-center justify-between space-y-0 gap-2">
          <div>
            <CardTitle className="text-base">同步任务</CardTitle>
            <CardDescription>按任务 ID 排序；删除后编号会被复用</CardDescription>
          </div>
          <Button onClick={openCreate}>
            <Plus /> 新建任务
          </Button>
        </CardHeader>
        <CardContent className="px-0 pb-0">
          {!loading && items.length === 0 ? (
            <p className="px-6 pb-6 text-sm text-muted-foreground">
              还没有任务，点击右上角「新建任务」开始
            </p>
          ) : (
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-14">ID</TableHead>
                    <TableHead>名称</TableHead>
                    <TableHead className="w-24">类型</TableHead>
                    <TableHead>源 → 目标</TableHead>
                    <TableHead className="w-32">定时</TableHead>
                    <TableHead className="hidden w-44 xl:table-cell">下次触发</TableHead>
                    <TableHead className="w-28">最近状态</TableHead>
                    <TableHead className="w-20">启用</TableHead>
                    <TableHead className="w-52 text-right">操作</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {items.map((t) => {
                    const running = Boolean(t.running)
                    const next = t.next_runs?.[0] ?? t.next_run_at
                    const stepCount = t.steps?.length ?? 0
                    return (
                      <TableRow key={t.id}>
                        <TableCell className="tabular text-muted-foreground">{t.id}</TableCell>
                        <TableCell>
                          <div className="font-medium">{t.name}</div>
                          {t.description ? (
                            <div className="text-xs text-muted-foreground">{t.description}</div>
                          ) : null}
                        </TableCell>
                        <TableCell>
                          <Badge variant="secondary">{KIND_TEXT[t.kind] ?? t.kind}</Badge>
                          {stepCount > 1 ? (
                            <Badge variant="outline" className="mt-1 block w-fit">
                              {stepCount} 步
                            </Badge>
                          ) : null}
                        </TableCell>
                        <TableCell>
                          {stepCount > 1 ? (
                            // 多步骤任务的路径不可能一行放下：折叠成"等 N 步"，
                            // 完整清单放 tooltip，避免列表被长路径撑爆。
                            <Tooltip>
                              <TooltipTrigger asChild>
                                <span className="block max-w-[22rem] cursor-default">
                                  <div className="truncate">
                                    {t.steps[0].source}
                                    {t.steps[0].dest ? (
                                      <span className="text-muted-foreground">
                                        {" "}
                                        → {t.steps[0].dest}
                                      </span>
                                    ) : null}
                                  </div>
                                  <div className="truncate text-xs text-muted-foreground">
                                    等 {stepCount} 步
                                  </div>
                                </span>
                              </TooltipTrigger>
                              <TooltipContent className="max-w-md space-y-1">
                                {t.steps.map((s, i) => (
                                  <div key={s.id ?? i} className="text-xs">
                                    {i + 1}. {s.name ? `${s.name} · ` : ""}
                                    {KIND_TEXT[s.kind] ?? s.kind} {s.source}
                                    {s.dest ? ` → ${s.dest}` : ""}
                                    {s.delay_after ? ` · 间隔 ${s.delay_after}s` : ""}
                                  </div>
                                ))}
                              </TooltipContent>
                            </Tooltip>
                          ) : (
                            <>
                              <div className="max-w-[22rem] truncate">
                                {t.source}
                                {t.dest ? (
                                  <span className="text-muted-foreground"> → {t.dest}</span>
                                ) : null}
                              </div>
                              <div className="truncate text-xs text-muted-foreground">
                                {t.method}
                              </div>
                            </>
                          )}
                        </TableCell>
                        <TableCell className="tabular text-muted-foreground">
                          {t.cron_expr || "手动"}
                        </TableCell>
                        <TableCell className="hidden xl:table-cell">
                          {next ? (
                            <>
                              <div className="tabular text-sm">{fmtTime(next)}</div>
                              <div className="text-xs text-muted-foreground">{fromNow(next)}</div>
                            </>
                          ) : (
                            <span className="text-muted-foreground">—</span>
                          )}
                        </TableCell>
                        <TableCell>
                          {running ? (
                            <StatusBadge status="running" />
                          ) : t.last_status ? (
                            <StatusBadge status={t.last_status} />
                          ) : (
                            <span className="text-xs text-muted-foreground">未运行</span>
                          )}
                        </TableCell>
                        <TableCell>
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span>
                                <Switch
                                  checked={t.enabled}
                                  disabled={running}
                                  onCheckedChange={(v) => toggleEnabled(t, v)}
                                  aria-label={`任务 ${t.id} 启用开关`}
                                />
                              </span>
                            </TooltipTrigger>
                            <TooltipContent>
                              {running
                                ? "任务正在运行中，请先取消再操作"
                                : "停用后定时与手动触发都会被拒绝"}
                            </TooltipContent>
                          </Tooltip>
                        </TableCell>
                        <TableCell>
                          <div className="flex justify-end gap-1">
                            {running ? (
                              <>
                                <Button
                                  variant="destructive"
                                  size="sm"
                                  onClick={() => cancelTask(t)}
                                >
                                  <X /> 取消
                                </Button>
                                <DisabledButton label="编辑" why="请先取消再编辑" />
                                <DisabledButton label="删除" why="请先取消再删除" />
                              </>
                            ) : (
                              <>
                                <Button size="sm" onClick={() => runTask(t)}>
                                  <Play /> 执行
                                </Button>
                                <Button
                                  variant="outline"
                                  size="sm"
                                  onClick={() => openEdit(t)}
                                >
                                  <Pencil /> 编辑
                                </Button>
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  className="text-destructive hover:bg-destructive/10 hover:text-destructive"
                                  onClick={() => removeTask(t)}
                                >
                                  <Trash2 /> 删除
                                </Button>
                              </>
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

      <TaskFormDialog
        open={formOpen}
        task={editing}
        onOpenChange={setFormOpen}
        onSaved={async () => {
          await load()
          refresh()
        }}
      />
    </TooltipProvider>
  )
}

/** 运行中置灰的按钮：保留 tooltip 说明为什么不能点（disabled 按钮不触发 hover 事件）。 */
function DisabledButton({ label, why }: { label: string; why: string }) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span>
          <Button variant="outline" size="sm" disabled>
            {label}
          </Button>
        </span>
      </TooltipTrigger>
      <TooltipContent>任务正在运行中，{why}</TooltipContent>
    </Tooltip>
  )
}
