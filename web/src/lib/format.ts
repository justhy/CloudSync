/** 展示层格式化：全部与后端单位一致（字节按 1024 进制、耗时传毫秒）。 */

export function fmtBytes(n: number | undefined | null): string {
  const v = Number(n) || 0
  if (v < 1024) return `${v} B`
  const units = ["KiB", "MiB", "GiB", "TiB", "PiB"]
  let i = -1
  let x = v
  do {
    x /= 1024
    i++
  } while (x >= 1024 && i < units.length - 1)
  return `${x.toFixed(x >= 100 ? 0 : 1)} ${units[i]}`
}

export function fmtSpeed(n: number | undefined | null): string {
  const v = Number(n) || 0
  return v <= 0 ? "—" : `${fmtBytes(v)}/s`
}

export function fmtDuration(ms: number | undefined | null): string {
  const v = Number(ms) || 0
  if (v < 1000) return `${v} ms`
  const s = Math.floor(v / 1000)
  if (s < 60) return `${s} s`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m} m ${s % 60} s`
  const h = Math.floor(m / 60)
  if (h < 24) return `${h} h ${m % 60} m`
  return `${Math.floor(h / 24)} d ${h % 24} h`
}

export function fmtETA(sec: number | undefined | null): string {
  const v = Number(sec) || 0
  return v <= 0 ? "—" : fmtDuration(v * 1000)
}

const pad = (x: number) => (x < 10 ? `0${x}` : `${x}`)

export function fmtTime(v?: string | null): string {
  if (!v) return "—"
  const d = new Date(v)
  if (Number.isNaN(d.getTime()) || d.getFullYear() < 2000) return "—"
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(
    d.getMinutes(),
  )}:${pad(d.getSeconds())}`
}

/** 相对时间："3 分 12 秒后" / "5 秒前"。 */
export function fromNow(v?: string | null): string {
  if (!v) return ""
  const t = new Date(v).getTime()
  if (Number.isNaN(t)) return ""
  const diff = t - Date.now()
  return diff >= 0 ? `${fmtDuration(Math.abs(diff))}后` : `${fmtDuration(Math.abs(diff))}前`
}

export const STATUS_TEXT: Record<string, string> = {
  pending: "排队中",
  running: "运行中",
  success: "成功",
  failed: "失败",
  canceled: "已取消",
}

export const KIND_TEXT: Record<string, string> = {
  sync: "同步",
  copy: "复制",
  move: "移动",
  bisync: "双向同步",
  check: "校验",
  delete: "删除",
  purge: "清空",
  mkdir: "创建目录",
}

/** 触发方式的可读文案。 */
export function triggerText(t?: string): string {
  if (t === "cron") return "定时"
  if (t === "manual") return "手动"
  return t || "—"
}
