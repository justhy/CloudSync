import { cn } from "@/lib/utils"
import { STATUS_TEXT } from "@/lib/format"
import type { RunStatus } from "@/lib/types"

const STYLES: Record<string, string> = {
  pending: "border-info/30 bg-info/10 text-info",
  running: "border-primary/30 bg-primary/10 text-primary",
  success: "border-success/30 bg-success/10 text-success",
  failed: "border-destructive/30 bg-destructive/10 text-destructive",
  canceled: "border-border bg-muted text-muted-foreground",
}

export function StatusBadge({
  status,
  className,
}: {
  status?: RunStatus | string
  className?: string
}) {
  const key = String(status ?? "")
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 rounded-full border px-2 py-0.5 text-xs font-medium",
        STYLES[key] ?? STYLES.canceled,
        className,
      )}
    >
      {key === "running" ? (
        <span className="relative flex size-1.5">
          <span className="absolute inline-flex size-full animate-ping rounded-full bg-current opacity-75" />
          <span className="relative inline-flex size-1.5 rounded-full bg-current" />
        </span>
      ) : (
        <span className="size-1.5 rounded-full bg-current" />
      )}
      {STATUS_TEXT[key] ?? key ?? "—"}
    </span>
  )
}
