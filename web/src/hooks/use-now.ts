import { useEffect, useState } from "react"

/** 每秒回传当前时间戳，用于"耗时 / 多久之后"这类相对时间自增。 */
export function useNow(intervalMs = 1000): number {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), intervalMs)
    return () => window.clearInterval(timer)
  }, [intervalMs])
  return now
}
