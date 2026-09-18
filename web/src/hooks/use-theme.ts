import { useLayoutEffect, useState } from "react"

export type Theme = "light" | "dark"

const KEY = "cloudsync-theme"

function initial(): Theme {
  if (typeof document === "undefined") return "light"
  return document.documentElement.classList.contains("dark") ? "dark" : "light"
}

export function useTheme() {
  const [theme, setTheme] = useState<Theme>(initial)

  useLayoutEffect(() => {
    document.documentElement.classList.toggle("dark", theme === "dark")
    try {
      localStorage.setItem(KEY, theme)
    } catch {
      /* 隐私模式下写不了 localStorage，忽略即可 */
    }
  }, [theme])

  return { theme, toggle: () => setTheme((t) => (t === "dark" ? "light" : "dark")) }
}
