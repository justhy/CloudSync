import * as React from "react"
import { Cloud, Loader2, LogIn } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { api } from "@/lib/api"
import { useSession } from "@/state/session"

export function LoginView() {
  const { setAuth } = useSession()
  const [user, setUser] = React.useState("")
  const [pass, setPass] = React.useState("")
  const [busy, setBusy] = React.useState(false)
  const [error, setError] = React.useState("")

  async function submit(e: React.FormEvent) {
    e.preventDefault()
    setBusy(true)
    setError("")
    try {
      await api.login(user, pass)
      setAuth("authed")
    } catch (err) {
      setError(err instanceof Error ? err.message : "登录失败")
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="relative flex min-h-svh items-center justify-center overflow-hidden bg-background p-4">
      {/* 背景光晕：纯装饰，不参与交互 */}
      <div
        aria-hidden
        className="pointer-events-none absolute -top-40 left-1/2 size-[36rem] -translate-x-1/2 rounded-full bg-primary/20 blur-3xl"
      />
      <div
        aria-hidden
        className="pointer-events-none absolute -bottom-48 -left-24 size-[28rem] rounded-full bg-info/15 blur-3xl"
      />

      <Card className="relative w-full max-w-sm shadow-xl">
        <CardHeader className="space-y-3 text-center">
          <div className="mx-auto flex size-12 items-center justify-center rounded-xl bg-primary/10 text-primary">
            <Cloud className="size-6" />
          </div>
          <div className="space-y-1">
            <CardTitle className="text-xl">CloudSync</CardTitle>
            <CardDescription>rclone 云端同步管理台</CardDescription>
          </div>
        </CardHeader>
        <CardContent>
          <form className="space-y-4" onSubmit={submit}>
            <div className="space-y-2">
              <Label htmlFor="login-user">用户名</Label>
              <Input
                id="login-user"
                autoComplete="username"
                autoFocus
                value={user}
                onChange={(e) => setUser(e.target.value)}
                required
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="login-pass">密码</Label>
              <Input
                id="login-pass"
                type="password"
                autoComplete="current-password"
                value={pass}
                onChange={(e) => setPass(e.target.value)}
                required
              />
            </div>
            {error ? <p className="text-sm text-destructive">{error}</p> : null}
            <Button type="submit" className="w-full" disabled={busy}>
              {busy ? (
                <Loader2 className="animate-spin" />
              ) : (
                <>
                  <LogIn /> 登 录
                </>
              )}
            </Button>
          </form>
        </CardContent>
      </Card>
    </div>
  )
}
