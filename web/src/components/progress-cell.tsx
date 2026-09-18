import { Progress } from "@/components/ui/progress"
import { cn } from "@/lib/utils"
import { fmtBytes } from "@/lib/format"

interface Props {
  percent?: number
  bytes?: number
  totalBytes?: number
  files?: number
  totalFiles?: number
  /** 是否仍在进行中：只有在运行中且总量未知时才画滚动条。 */
  active?: boolean
}

/**
 * 进度单元格。
 *
 * 已结束的记录即使计数为 0 也要按最终百分比画静态条 —— 否则"同步完成、
 * 无需传输任何文件"的任务会永远显示一条滚动的进度条，与"成功"自相矛盾。
 */
export function ProgressCell({ percent, bytes, totalBytes, files, totalFiles, active }: Props) {
  const pct = Math.min(100, Math.max(0, Number(percent) || 0))
  const known = (totalBytes ?? 0) > 0 || (totalFiles ?? 0) > 0
  const text = known
    ? `${fmtBytes(bytes)} / ${fmtBytes(totalBytes)} · ${files ?? 0}/${totalFiles ?? 0} 文件`
    : `${fmtBytes(bytes)} / 未知总量`

  return (
    <div className="min-w-[160px] space-y-1">
      {known || !active ? (
        <Progress value={pct} className="h-1.5" />
      ) : (
        <div className={cn("relative h-1.5 w-full overflow-hidden rounded-full bg-muted")}>
          <div className="indeterminate-bar h-full w-1/3 rounded-full bg-primary" />
        </div>
      )}
      <div className="tabular text-xs text-muted-foreground">{text}</div>
    </div>
  )
}
