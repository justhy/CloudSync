import { useEffect, useRef } from "react"

/**
 * 自适应轮询。
 *
 * SSE 是"尽力而为"的：代理断流、服务端重启都可能让它静默失效，表现为
 * "任务已经成功、进度条还停在原地"。所以每个页面都保留一层轮询兜底：
 * 有运行中的记录时快刷，空闲时慢刷；页面切到后台则完全不发请求。
 */
export function useAutoPoll(fn: () => void | Promise<void>, delay: number, enabled = true) {
  const saved = useRef(fn)
  saved.current = fn

  useEffect(() => {
    if (!enabled) return
    let timer: number | undefined
    let stopped = false
    let busy = false

    const schedule = () => {
      if (stopped) return
      timer = window.setTimeout(run, delay)
    }

    const run = async () => {
      if (stopped) return
      if (document.hidden || busy) {
        schedule()
        return
      }
      busy = true
      try {
        await saved.current()
      } catch {
        /* 轮询失败不弹提示，下一轮继续 */
      } finally {
        busy = false
        schedule()
      }
    }

    schedule()
    return () => {
      stopped = true
      if (timer) window.clearTimeout(timer)
    }
  }, [delay, enabled])
}

/** 文档可见性变化时回调，用于"切回前台立刻刷新一次"。 */
export function useOnVisible(fn: () => void) {
  const saved = useRef(fn)
  saved.current = fn
  useEffect(() => {
    const handler = () => {
      if (!document.hidden) saved.current()
    }
    document.addEventListener("visibilitychange", handler)
    return () => document.removeEventListener("visibilitychange", handler)
  }, [])
}
