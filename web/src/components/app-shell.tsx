import * as React from "react"
import {
  Cloud,
  History,
  LayoutDashboard,
  ListChecks,
  LogOut,
  Menu,
  Moon,
  RefreshCw,
  Settings,
  Sun,
  Wifi,
  WifiOff,
  type LucideIcon,
} from "lucide-react"

import { Button } from "@/components/ui/button"
import { Separator } from "@/components/ui/separator"
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet"
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"
import { cn } from "@/lib/utils"
import { useConfirm } from "@/components/confirm-provider"
import { api } from "@/lib/api"
import { useSession } from "@/state/session"
import type { Theme } from "@/hooks/use-theme"

export type PageKey = "dash" | "tasks" | "runs" | "rclone" | "settings"

const NAV: { key: PageKey; label: string; icon: LucideIcon }[] = [
  { key: "dash", label: "总览", icon: LayoutDashboard },
  { key: "tasks", label: "任务", icon: ListChecks },
  { key: "runs", label: "运行记录", icon: History },
  { key: "rclone", label: "rclone", icon: Cloud },
  { key: "settings", label: "设置", icon: Settings },
]

export function AppShell({
  page,
  onPage,
  theme,
  onToggleTheme,
  children,
}: {
  page: PageKey
  onPage: (p: PageKey) => void
  theme: Theme
  onToggleTheme: () => void
  children: React.ReactNode
}) {
  const { overview, sse, version, refresh, setAuth } = useSession()
  const confirm = useConfirm()
  const [sheetOpen, setSheetOpen] = React.useState(false)

  const rc = overview?.rclone
  const rcLabel = rc
    ? rc.ready
      ? `rclone ${rc.version || "就绪"}${rc.pid ? ` · PID ${rc.pid}` : ""}`
      : rc.state === "restarting" || rc.state === "starting"
        ? `rclone ${rc.state === "restarting" ? "重启中" : "启动中"}`
        : rc.state === "failed"
          ? "rclone 异常"
          : "rclone 已停止"
    : "rclone 检测中"
  const rcTone = rc?.ready
    ? "text-success"
    : rc && (rc.state === "restarting" || rc.state === "starting")
      ? "text-warning"
      : rc?.state === "failed"
        ? "text-destructive"
        : "text-muted-foreground"

  async function logout() {
    if (!(await confirm({ title: "退出登录？", confirmText: "退出" }))) return
    try {
      await api.logout()
    } catch {
      /* 退出失败也要回到登录页 */
    }
    setAuth("anon")
  }

  const navButtons = NAV.map((item) => {
    const Icon = item.icon
    const active = page === item.key
    return (
      <button
        key={item.key}
        type="button"
        onClick={() => {
          onPage(item.key)
          setSheetOpen(false)
        }}
        className={cn(
          "flex w-full items-center gap-3 rounded-lg px-3 py-2 text-sm transition-colors",
          active
            ? "bg-sidebar-accent font-medium text-foreground"
            : "text-muted-foreground hover:bg-sidebar-accent/60 hover:text-foreground",
        )}
      >
        <Icon className="size-4 shrink-0" />
        {item.label}
      </button>
    )
  })

  const sidebar = (
    <div className="flex h-full flex-col gap-2 border-r border-sidebar-border bg-sidebar px-3 py-4">
      <div className="flex items-center gap-2 px-2 pb-2">
        <div className="flex size-8 items-center justify-center rounded-lg bg-primary/10 text-primary">
          <Cloud className="size-4" />
        </div>
        <div className="min-w-0">
          <div className="truncate text-sm font-semibold">CloudSync</div>
          <div className="truncate text-xs text-muted-foreground">
            {version ? `v${version}` : "rclone 管理台"}
          </div>
        </div>
      </div>
      <nav className="flex flex-col gap-1">{navButtons}</nav>
      <div className="mt-auto space-y-2">
        <Separator />
        <Button variant="ghost" size="sm" className="w-full justify-start" onClick={onToggleTheme}>
          {theme === "dark" ? <Sun /> : <Moon />}
          {theme === "dark" ? "浅色模式" : "深色模式"}
        </Button>
        <Button
          variant="ghost"
          size="sm"
          className="w-full justify-start text-muted-foreground"
          onClick={logout}
        >
          <LogOut /> 退出登录
        </Button>
      </div>
    </div>
  )

  return (
    <TooltipProvider delayDuration={300}>
      <div className="flex min-h-svh bg-background">
        <aside className="hidden w-60 shrink-0 lg:block">{sidebar}</aside>

        <div className="flex min-w-0 flex-1 flex-col">
          <header className="sticky top-0 z-30 flex h-14 items-center gap-2 border-b bg-background/80 px-3 backdrop-blur lg:px-6">
            <Sheet open={sheetOpen} onOpenChange={setSheetOpen}>
              <SheetTrigger asChild>
                <Button variant="ghost" size="icon" className="lg:hidden">
                  <Menu />
                </Button>
              </SheetTrigger>
              <SheetContent side="left" className="w-60 p-0">
                <SheetHeader className="sr-only">
                  <SheetTitle>导航</SheetTitle>
                </SheetHeader>
                {sidebar}
              </SheetContent>
            </Sheet>

            <div className="min-w-0 flex-1">
              <div className="truncate text-sm font-medium lg:text-base">
                {NAV.find((n) => n.key === page)?.label}
              </div>
            </div>

            <Tooltip>
              <TooltipTrigger asChild>
                <span
                  className={cn(
                    "hidden items-center gap-1.5 rounded-full border px-2 py-1 text-xs sm:inline-flex",
                    rcTone,
                  )}
                  title={[rc?.endpoint, rc?.last_error].filter(Boolean).join(" / ")}
                >
                  <span className="size-1.5 rounded-full bg-current" />
                  {rcLabel}
                </span>
              </TooltipTrigger>
              <TooltipContent>
                {[rc?.endpoint, rc?.last_error].filter(Boolean).join(" / ") || "rclone 状态"}
              </TooltipContent>
            </Tooltip>

            <Tooltip>
              <TooltipTrigger asChild>
                <span
                  className={cn(
                    "hidden items-center gap-1.5 rounded-full border px-2 py-1 text-xs sm:inline-flex",
                    sse === "open" ? "text-success" : "text-warning",
                  )}
                >
                  {sse === "open" ? <Wifi className="size-3" /> : <WifiOff className="size-3" />}
                  {sse === "open" ? "实时连接" : "连接中断"}
                </span>
              </TooltipTrigger>
              <TooltipContent>
                SSE 用于实时推送进度；断开时会由轮询兜底刷新。
              </TooltipContent>
            </Tooltip>

            <Tooltip>
              <TooltipTrigger asChild>
                <Button variant="ghost" size="icon" onClick={refresh}>
                  <RefreshCw />
                </Button>
              </TooltipTrigger>
              <TooltipContent>刷新</TooltipContent>
            </Tooltip>
          </header>

          <main className="mx-auto w-full max-w-[1400px] flex-1 p-4 lg:p-6">{children}</main>
        </div>
      </div>
    </TooltipProvider>
  )
}
