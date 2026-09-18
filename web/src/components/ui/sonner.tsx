import { Toaster as Sonner, type ToasterProps } from "sonner"

/**
 * shadcn 原版用 next-themes 取主题，这里换成本项目的 useTheme：
 * 不为一个取值引入额外依赖，主题状态也和页面保持一致。
 */
export function Toaster({
  theme = "light",
  ...props
}: ToasterProps & { theme?: "light" | "dark" | "system" }) {
  return (
    <Sonner
      theme={theme}
      className="toaster group"
      position="top-right"
      toastOptions={{
        classNames: {
          toast:
            "group toast group-[.toaster]:bg-background group-[.toaster]:text-foreground group-[.toaster]:border-border group-[.toaster]:shadow-lg",
          description: "group-[.toast]:text-muted-foreground",
          actionButton: "group-[.toast]:bg-primary group-[.toast]:text-primary-foreground",
          cancelButton: "group-[.toast]:bg-muted group-[.toast]:text-muted-foreground",
        },
      }}
      {...props}
    />
  )
}
