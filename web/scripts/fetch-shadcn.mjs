/**
 * 拉取 shadcn/ui 官方组件源码并落盘到 src/components/ui/。
 *
 * 不走 shadcn CLI 是因为它在非交互环境里会卡在提示上；
 * registry 本身就是这些文件的唯一来源，直接取 JSON 等价且可复现。
 *
 *   node scripts/fetch-shadcn.mjs [component...]
 */
import { mkdir, writeFile } from "node:fs/promises"
import { dirname, join, resolve } from "node:path"

const STYLE = process.env.SHADCN_STYLE ?? "new-york"
const OUT = resolve(process.cwd(), "src/components/ui")
const DEFAULT = [
  "button",
  "card",
  "badge",
  "input",
  "label",
  "textarea",
  "select",
  "switch",
  "table",
  "dialog",
  "alert-dialog",
  "sheet",
  "dropdown-menu",
  "tooltip",
  "progress",
  "separator",
  "scroll-area",
  "skeleton",
  "sonner",
]

const names = process.argv.slice(2).length ? process.argv.slice(2) : DEFAULT
const deps = new Set()

for (const name of names) {
  const url = `https://ui.shadcn.com/r/styles/${STYLE}/${name}.json`
  const res = await fetch(url)
  if (!res.ok) {
    console.error(`✗ ${name}: HTTP ${res.status}`)
    process.exitCode = 1
    continue
  }
  const item = await res.json()
  for (const dep of item.dependencies ?? []) deps.add(dep)
  for (const dep of item.registryDependencies ?? []) deps.add(`(registry) ${dep}`)

  for (const file of item.files ?? []) {
    // registry 返回的 path 形如 "components/ui/button.tsx" 或 "ui/button.tsx"。
    const rel = file.path.startsWith("components/ui/")
      ? file.path.slice("components/ui/".length)
      : file.path.replace(/^ui\//, "")
    const target = join(OUT, rel)
    await mkdir(dirname(target), { recursive: true })
    await writeFile(target, file.content, "utf8")
    console.log(`✓ ${name} -> src/components/ui/${rel}`)
  }
}

if (deps.size) console.log("\n依赖:", [...deps].join(", "))
