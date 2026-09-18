import * as React from "react"

import { ApiError, api, eventsURL } from "@/lib/api"
import { useAutoPoll } from "@/hooks/use-poll"
import type { Overview } from "@/lib/types"

export type AuthState = "unknown" | "authed" | "anon"
export type SSEState = "connecting" | "open" | "closed"

interface SessionValue {
  auth: AuthState
  setAuth: (next: AuthState) => void
  overview: Overview | null
  /** 进行中的记录数（运行中 + 排队中），决定各页面的轮询节奏。 */
  activeCount: number
  sse: SSEState
  /** 每收到一个 SSE 事件自增，页面把它作为"该刷新了"的信号。 */
  eventTick: number
  version: string
  refresh: () => void
}

const SessionContext = React.createContext<SessionValue | null>(null)

export function useSession(): SessionValue {
  const value = React.useContext(SessionContext)
  if (!value) throw new Error("useSession 必须在 SessionProvider 内使用")
  return value
}

export function SessionProvider({ children }: { children: React.ReactNode }) {
  const [auth, setAuth] = React.useState<AuthState>("unknown")
  const [overview, setOverview] = React.useState<Overview | null>(null)
  const [sse, setSse] = React.useState<SSEState>("connecting")
  const [eventTick, setEventTick] = React.useState(0)
  const [manualTick, setManualTick] = React.useState(0)

  const load = React.useCallback(async () => {
    try {
      setOverview(await api.overview())
    } catch (err) {
      // 会话过期：把登录态打回去，由外层渲染登录页。
      if (err instanceof ApiError && err.status === 401) setAuth("anon")
    }
  }, [])

  React.useEffect(() => {
    api
      .me()
      .then((me) => setAuth(me.authenticated ? "authed" : "anon"))
      .catch(() => setAuth("anon"))
  }, [])

  // SSE：连接正常时由服务端推动刷新，轮询只作为它失效时的兜底。
  React.useEffect(() => {
    if (auth !== "authed") return
    const es = new EventSource(eventsURL())
    const bump = () => setEventTick((t) => t + 1)
    const onOpen = () => setSse("open")
    const onError = () => setSse("closed")
    es.addEventListener("open", onOpen)
    es.addEventListener("error", onError)
    for (const name of ["hello", "run.created", "run.updated", "run.finished"]) {
      es.addEventListener(name, bump)
    }
    return () => {
      es.removeEventListener("open", onOpen)
      es.removeEventListener("error", onError)
      es.close()
    }
  }, [auth])

  React.useEffect(() => {
    if (auth !== "authed") return
    void load()
  }, [auth, eventTick, manualTick, load])

  const activeCount = React.useMemo(() => {
    const c = overview?.counts
    return (c?.running ?? 0) + (c?.pending ?? 0)
  }, [overview])

  useAutoPoll(load, activeCount > 0 ? 2000 : 15000, auth === "authed")

  const value = React.useMemo<SessionValue>(
    () => ({
      auth,
      setAuth,
      overview,
      activeCount,
      sse,
      eventTick,
      version: overview?.version ?? "",
      refresh: () => setManualTick((t) => t + 1),
    }),
    [auth, overview, activeCount, sse, eventTick],
  )

  return <SessionContext.Provider value={value}>{children}</SessionContext.Provider>
}
