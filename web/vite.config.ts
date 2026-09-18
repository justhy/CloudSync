import { fileURLToPath, URL } from "node:url"
import { defineConfig } from "vite"
import react from "@vitejs/plugin-react"
import tailwindcss from "@tailwindcss/vite"

// 构建产物直接写入 Go 的 embed 目录（internal/web/static），
// 这样 `go build` 不需要任何 Node 工具链就能产出完整二进制。
export default defineConfig({
  // 相对路径：部署在子路径（server.base_path）下也能正确加载资源。
  base: "./",
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": fileURLToPath(new URL("./src", import.meta.url)),
    },
  },
  build: {
    outDir: "../internal/web/static",
    emptyOutDir: true,
    // 产物要提交进仓库并被 embed 进二进制，不生成 sourcemap。
    sourcemap: false,
  },
  server: {
    port: 5173,
    // 开发时把 /api 代理到本机的 cloudsync（默认 8080）。
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8080",
        changeOrigin: true,
      },
    },
  },
})
