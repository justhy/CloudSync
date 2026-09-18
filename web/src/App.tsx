import * as React from "react"
import { Loader2 } from "lucide-react"

import { AppShell, type PageKey } from "@/components/app-shell"
import { ConfirmProvider } from "@/components/confirm-provider"
import { RunDetailDialog } from "@/components/run-detail"
import { Toaster } from "@/components/ui/sonner"
import { useTheme, type Theme } from "@/hooks/use-theme"
import { DashboardPage } from "@/pages/dashboard"
import { LoginView } from "@/pages/login"
import { RclonePage } from "@/pages/rclone"
import { RunsPage } from "@/pages/runs"
import { SettingsPage } from "@/pages/settings"
import { TasksPage } from "@/pages/tasks"
import { SessionProvider, useSession } from "@/state/session"

export default function App() {
  const { theme, toggle } = useTheme()
  return (
    <ConfirmProvider>
      <SessionProvider>
        <Toaster theme={theme} />
        <Gate theme={theme} onToggleTheme={toggle} />
      </SessionProvider>
    </ConfirmProvider>
  )
}

function Gate({ theme, onToggleTheme }: { theme: Theme; onToggleTheme: () => void }) {
  const { auth } = useSession()
  const [page, setPage] = React.useState<PageKey>("dash")
  const [openRun, setOpenRun] = React.useState<number | null>(null)

  if (auth === "unknown") {
    return (
      <div className="flex min-h-svh items-center justify-center gap-2 text-sm text-muted-foreground">
        <Loader2 className="animate-spin" /> 正在连接…
      </div>
    )
  }
  if (auth === "anon") return <LoginView />

  return (
    <AppShell page={page} onPage={setPage} theme={theme} onToggleTheme={onToggleTheme}>
      {page === "dash" ? <DashboardPage onOpenRun={setOpenRun} /> : null}
      {page === "tasks" ? <TasksPage /> : null}
      {page === "runs" ? <RunsPage onOpenSettings={() => setPage("settings")} /> : null}
      {page === "rclone" ? <RclonePage /> : null}
      {page === "settings" ? <SettingsPage /> : null}

      {/* 总览页点任务名也能看详情，弹窗挂在壳层避免每个页面各挂一个。 */}
      <RunDetailDialog
        runId={openRun}
        open={openRun !== null}
        onOpenChange={(next) => {
          if (!next) setOpenRun(null)
        }}
      />
    </AppShell>
  )
}
