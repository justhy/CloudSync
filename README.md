# CloudSync

基于 [rclone RC API](https://rclone.org/rc/) 的云端文件同步管理器：单个 Go 二进制，自带 Web 管理端，支持一次性执行与 cron 定时调度。

程序启动时自动拉起 `rclone rcd` 子进程，退出时自动关闭；所有传输交由 rclone 执行，本程序负责编排、调度、进度跟踪与可观测性。

---

## 目录

- [特性](#特性)
- [架构](#架构)
- [快速开始](#快速开始)
- [配置](#配置)
- [环境变量](#环境变量)
- [Web 管理端与 API](#web-管理端与-api)
- [任务类型与参数](#任务类型与参数)
- [cron 表达式](#cron-表达式)
- [部署](#部署)
- [发版](#发版)
- [开发](#开发)
- [已知限制](#已知限制)

---

## 特性

| 能力 | 说明 |
| --- | --- |
| 子进程托管 | 启动时拉起 `rclone rcd`，退出时 `core/quit` 优雅关闭并强杀兜底；意外退出按指数退避自动重启 |
| 防孤儿进程 | Linux 用 `Pdeathsig` + `Setpgid`；macOS/BSD 用 `Setpgid`；Windows 用 Job Object（`KILL_ON_JOB_CLOSE`），宿主进程被强杀也不会留下 rclone |
| 定时调度 | `robfig/cron/v3`，支持 5 段 / 6 段（秒级）两档语法与 `@every`/`@daily` 描述符；任务增删改即时生效，无需重启 |
| 异步执行 + 进度 | 通过 `_async=true` + `_group` 提交，轮询 `job/status` 与 `core/stats?group=` 汇总进度；REST 查询 + SSE 实时推送 |
| 多步骤任务 | 一个任务可编排多组「类型 + 源 + 目标」，串行执行、共享一次运行记录；每步可单独设失败策略、超时与步间间隔，失败后支持「从该步重跑」 |
| 持久化 | SQLite（`modernc.org/sqlite`，纯 Go，无 CGO），单连接避免 `SQLITE_BUSY`；WAL 模式。运行记录及其中的 rclone 日志片段都落在这一个库里 |
| 日志保留 | 运行记录可设保留时长（界面 / `storage.run_retention`），后台定期清理过期记录；只清已结束的记录，支持在设置页查看占用并立即清理 |
| 崩溃自愈 | 启动时把上次异常退出遗留的 `running` 记录收敛为 `failed`，不会留下永久卡住的运行记录 |
| 结构化日志 | `log/slog`，JSON / text 双格式，可选输出到文件；内存环形缓冲保存 rclone 输出并按单次运行切片 |
| 认证 | 自包含 HMAC 签名会话 Cookie（无外部依赖）、常量时间凭据比较、按 IP 登录限流 |
| 单二进制交付 | 前端（React + Tailwind + shadcn/ui）构建产物通过 `embed` 打进二进制；产物已随仓库提交，使用者无需 Node |

---

## 架构

```
                        ┌──────────────────────────────────────────┐
                        │           cloudsync (单二进制)             │
                        │                                          │
   浏览器 ─── HTTP ───▶ │  internal/web     REST + SSE + 静态前端    │
                        │        │                                 │
                        │        ├── internal/manager   任务执行编排 │
                        │        │        │  _async / job/status    │
                        │        ├── internal/scheduler  cron 调度   │
                        │        └── internal/store      SQLite      │
                        │                 │                        │
                        │        internal/rclone  子进程 + RC 客户端  │
                        └─────────────────┼────────────────────────┘
                                          │ RC HTTP API
                                          ▼
                                  rclone rcd 子进程 ──▶ 云端 / 本地存储
```

| 包 | 职责 |
| --- | --- |
| `cmd/cloudsync` | 入口：装配依赖、信号处理、优雅关闭 |
| `cmd/rclone-mock` | 假 rclone RC 服务，用于无 rclone 环境下的联调与压测 |
| `internal/config` | YAML + 环境变量 + 命令行合并，校验与默认值派生 |
| `internal/logging` | slog 初始化、字段助手、rclone 输出环形缓冲 |
| `internal/store` | 任务 / 运行记录的 SQLite 持久化 |
| `internal/rclone` | RC HTTP 客户端 + 子进程监管（跨平台进程组 / Job Object） |
| `internal/manager` | 异步运行生命周期、并发闸门、进度聚合、SSE 广播 |
| `internal/scheduler` | cron 注册、触发、`next_run_at` 回写 |
| `internal/web` | 路由、认证、REST 处理器、SSE、embed 前端 |

---

## 快速开始

### 前置条件

- Go 1.24+（仅从源码构建时需要）
- [`rclone`](https://rclone.org/downloads/) 在 `PATH` 中，或用 `rclone.path` 指定绝对路径

### 从源码构建

```bash
git clone <repo> cloudsync && cd cloudsync
make build            # 产出 bin/cloudsync（含版本号注入）
```

不需要 Node：前端构建产物已随仓库提交，直接 `go build` 也能得到完整管理端。

国内网络如拉取依赖超时：

```bash
export GOPROXY=https://goproxy.cn,direct
```

### 配置并启动

```bash
cp config.example.yaml config.yaml
# 至少填写 server.password（或用环境变量注入）
export CLOUDSYNC_SERVER_PASSWORD='请改成足够强的密码'

./bin/cloudsync -config config.yaml
```

只校验配置、不启动：

```bash
CLOUDSYNC_SERVER_PASSWORD=x ./bin/cloudsync -config config.yaml -check
```

启动后打开 <http://127.0.0.1:8080/>，用配置中的用户名/密码登录。

### 不装 rclone 先跑通

仓库自带 `cmd/rclone-mock`，可模拟 rclone RC 服务：

```bash
make build
./bin/rclone-mock rcd --rc-addr 127.0.0.1:5572 --rc-user admin --rc-pass dev
# 另开一个终端
./bin/cloudsync -config config.yaml        # 配置里设 rclone.auto_start: false
```

mock 支持用环境变量控制模拟行为：

| 变量 | 含义 |
| --- | --- |
| `MOCK_DURATION_MS` | 单次传输耗时（默认约 3000ms） |
| `MOCK_TOTAL_BYTES` | 报告的总字节数 |
| `MOCK_FAIL` | 非空则本次任务判定为失败 |
| `MOCK_DELAY_MS` | 提交后到开始传输的延迟 |

mock 只实现少数 RC 端点，对不认识的命令行参数一律忽略。因此也可以直接让
CloudSync 托管它（`rclone.path` 指向 mock、`rclone.auto_start: true`），
无需 `auto_start: false` 手工分两步——宿主追加的 `--rc-job-expire-duration`
这类真实 rclone 参数不会让 mock 启动失败。该行为由
`cmd/rclone-mock/main_test.go` 的 `TestStripUnknownFlagsSupervisorCommandLine` 守护。

---

## 配置

完整注释见 [`config.example.yaml`](config.example.yaml)。配置来源优先级：

```
内置默认值  ->  YAML 文件  ->  环境变量 (CLOUDSYNC_*)  ->  命令行参数
```

要点：

- **严格模式**：未知字段会直接报错，拼错字段名不会被静默忽略。
- **时长字段**：既接受 `"30s"` / `"5m"` / `"1h30m"`，也接受整数（单位秒）；天/周写作 `"30d"` / `"2w"`（标准库只认到 `h`，这里额外补了两个单位）。
- **必填项**：`server.addr`、`server.username`、`server.password`、`rclone.rc_addr`。
- **留空即随机**：`server.session_secret` 与 `rclone.rc_pass` 留空时会生成随机值。前者重启后使所有会话失效；后者仅对程序自己拉起的 rcd 有意义。
- **环境变量不能置空**：空字符串视为"未设置"。
- **默认不限时**：`scheduler.default_timeout` 的内置值是 `0`，即任务不写 `timeout_seconds` 时不会因超时被中断。理由是超大目录的首次同步常超过一天，被上限砍断等于白跑一轮，比卡住更难接受；"别堆积"由 `scheduler.skip_overlap` 与 cron 兜住，真卡死了还能手动取消。要限制就显式写 `"24h"` 这类值。
- **时区无需宿主支持**：IANA 时区库已通过 `import _ "time/tzdata"` 编入二进制（代价约 450KB）。这是必需的——发布构建用了 `-trimpath`，它会让 `runtime.GOROOT()` 返回空字符串，而 Windows 不读系统时区库，两者叠加会使 `scheduler.timezone: "Asia/Shanghai"` 直接报 `unknown time zone` 导致进程无法启动。精简容器同理。回归防线见 `internal/config/config_test.go` 的 `TestTimezoneDatabaseIsEmbedded`。

### 命令行参数

| 参数 | 说明 |
| --- | --- |
| `-config <path>` | 配置文件路径（等价于 `CLOUDSYNC_CONFIG`） |
| `-check` | 仅校验配置后退出 |
| `-version` | 打印版本后退出 |
| `-addr <host:port>` | 覆盖 `server.addr` |
| `-log-level <lvl>` | 覆盖 `log.level`：`debug`/`info`/`warn`/`error` |
| `-log-format <fmt>` | 覆盖 `log.format`：`json`/`text` |
| `-rclone <path>` | 覆盖 `rclone.path` |
| `-no-rclone` | 等价于 `rclone.auto_start: false`，只连接已运行的 rcd |

---

## 环境变量

| 变量 | 对应配置 |
| --- | --- |
| `CLOUDSYNC_CONFIG` | 配置文件路径 |
| `CLOUDSYNC_SERVER_ADDR` | `server.addr` |
| `CLOUDSYNC_SERVER_BASE_PATH` | `server.base_path` |
| `CLOUDSYNC_SERVER_USERNAME` | `server.username` |
| `CLOUDSYNC_SERVER_PASSWORD` | `server.password` |
| `CLOUDSYNC_SERVER_SESSION_SECRET` | `server.session_secret` |
| `CLOUDSYNC_SERVER_TRUSTED_PROXY` | `server.trusted_proxy` |
| `CLOUDSYNC_RCLONE_PATH` | `rclone.path` |
| `CLOUDSYNC_RCLONE_CONFIG_FILE` | `rclone.config_file` |
| `CLOUDSYNC_RCLONE_RC_ADDR` | `rclone.rc_addr` |
| `CLOUDSYNC_RCLONE_RC_USER` | `rclone.rc_user` |
| `CLOUDSYNC_RCLONE_RC_PASS` | `rclone.rc_pass` |
| `CLOUDSYNC_RCLONE_NO_AUTH` | `rclone.rc_no_auth` |
| `CLOUDSYNC_RCLONE_AUTO_START` | `rclone.auto_start` |
| `CLOUDSYNC_RCLONE_AUTO_RESTART` | `rclone.auto_restart` |
| `CLOUDSYNC_RCLONE_MAX_RESTARTS` | `rclone.max_restarts` |
| `CLOUDSYNC_RCLONE_LOG_LEVEL` | `rclone.log_level` |
| `CLOUDSYNC_RCLONE_JOURNAL_SIZE` | `rclone.journal_size` |
| `CLOUDSYNC_RCLONE_STARTUP_TIMEOUT` | `rclone.startup_timeout` |
| `CLOUDSYNC_RCLONE_SHUTDOWN_TIMEOUT` | `rclone.shutdown_timeout` |
| `CLOUDSYNC_STORAGE_DRIVER` | `storage.driver` |
| `CLOUDSYNC_STORAGE_DSN` | `storage.dsn` |
| `CLOUDSYNC_STORAGE_HISTORY_LIMIT` | `storage.history_limit` |
| `CLOUDSYNC_STORAGE_RUN_RETENTION` | `storage.run_retention` |
| `CLOUDSYNC_STORAGE_RETENTION_INTERVAL` | `storage.retention_interval` |
| `CLOUDSYNC_SCHEDULER_ENABLED` | `scheduler.enabled` |
| `CLOUDSYNC_SCHEDULER_TIMEZONE` | `scheduler.timezone` |
| `CLOUDSYNC_SCHEDULER_SECONDS` | `scheduler.seconds` |
| `CLOUDSYNC_SCHEDULER_SKIP_OVERLAP` | `scheduler.skip_overlap` |
| `CLOUDSYNC_SCHEDULER_MAX_CONCURRENT_RUNS` | `scheduler.max_concurrent_runs` |
| `CLOUDSYNC_SCHEDULER_DEFAULT_TIMEOUT` | `scheduler.default_timeout` |
| `CLOUDSYNC_LOG_LEVEL` | `log.level` |
| `CLOUDSYNC_LOG_FORMAT` | `log.format` |
| `CLOUDSYNC_LOG_FILE` | `log.file` |

---

## Web 管理端与 API

所有 `/api/*` 接口（`/api/health`、`/api/login` 除外）都需要登录会话 Cookie。响应统一为 JSON；错误体形如：

```json
{ "error": "任务名称不能为空", "code": "validation_failed" }
```

### 认证

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/api/health` | 存活探测，无需认证 |
| `POST` | `/api/login` | 登录，下发会话 Cookie |
| `POST` | `/api/logout` | 登出 |
| `GET` | `/api/me` | 当前会话信息 |

### 任务

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/api/tasks` | 任务列表（含 `next_runs` 预览与运行中实例） |
| `POST` | `/api/tasks` | 创建任务 |
| `POST` | `/api/tasks/validate` | 校验 cron 表达式并返回未来触发时间 |
| `GET` | `/api/tasks/{id}` | 任务详情 + 最近运行记录 |
| `PUT` | `/api/tasks/{id}` | 更新任务；任务运行中返回 `409 task_running` |
| `POST` | `/api/tasks/{id}/enabled` | 只切换启用状态，body 为 `{"enabled": true}`；重复设置幂等 |
| `DELETE` | `/api/tasks/{id}` | 删除任务；正在运行需加 `?force=1`。会连带清除该任务的历史运行记录——任务编号会被复用，留着旧记录会让新任务凭空多出"上次运行结果" |
| `POST` | `/api/tasks/{id}/run` | 手动触发，返回 `202` 与运行记录；可选 `?from_step=N` 从第 `N+1` 步开始（`N` 为 0 基下标），见[从失败的那一步重跑](#从失败的那一步重跑) |
| `POST` | `/api/tasks/{id}/cancel` | 取消该任务当前运行中的实例 |

**运行中锁定**：任务处于运行态时，`PUT` / `DELETE` / `POST /enabled` 一律返回 `409 task_running`，唯一可用的操作是取消——改定义只对下一轮生效，用户却常误以为对当前这轮也生效。`?force=1` 是留给脚本的运维后门（先取消再删除），界面不提供。

另外，**多步骤任务无条件禁止并发**（不看 `scheduler.skip_overlap`）：一条链的时长是各步之和加间隔，很容易超过触发周期，允许重叠会让几轮链同时跑，互相抢带宽，且"最近一次结果"变得不可解释。重叠时再次触发返回 `409 task_busy`。

### 运行记录

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/api/runs` | 运行记录列表，支持 `task_id` / `status` / `limit` / `offset` |
| `GET` | `/api/runs/active` | 当前运行中的记录 |
| `GET` | `/api/runs/{id}` | 单条记录 + 实时进度 |
| `POST` | `/api/runs/{id}/cancel` | 取消指定运行 |
| `GET` | `/api/events` | SSE 事件流，实时推送状态与进度变化 |
| `DELETE` | `/api/runs/{id}` | 删除单条运行记录；运行中的记录需先取消 |
| `DELETE` | `/api/runs` | 清空全部运行记录；清空后编号从 `1` 重新开始 |
| `POST` | `/api/runs/prune` | 按当前保留策略立即清理过期记录，返回 `{deleted, vacuumed}` |

### 设置（保留策略）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/api/settings` | 保留时长（`retention_hours`，`0`=不限）、来源、待清理条数、记录占用与数据库大小 |
| `PUT` | `/api/settings` | 修改保留时长，body 为 `{"retention_hours": 720}`（`0` 表示不限） |

保留时长允许 `1 小时 ~ 3650 天`，超出返回 `400 invalid_retention`。取值来源优先级：**界面设置（存在数据库 `settings` 表）> 配置文件**——反过来会让界面上改过的值在重启后被配置文件覆盖回去。

后台每 `storage.retention_interval`（默认 `1h`，最短 `5m`）检查一次，启动时也会先清一次。清理只针对**已结束**（`success`/`failed`/`canceled`）且 `started_at` 早于截止点的记录；`pending`/`running` 是任务的唯一观测窗口，永不清。用 `started_at` 而不是 `finished_at` 判断，是为了避免"跑了三天才结束的任务刚落地就被清掉"。清理后 `POST /api/runs/prune` 会执行一次 `VACUUM`，否则 SQLite 只是把页标记为空闲，磁盘占用不会回落。

**编号规则**：任务 ID 取当前**最小空闲编号**（删掉 2 号后新建的任务会补到 2 号），所以列表里的编号始终连续；运行记录 ID 只增不减，只有"清空全部"这一个入口才会把它重置回 `1`——单条删除中间某条不会让后续编号回退。

**运行中的日志**：`GET /api/runs/{id}` 在记录进行中时返回的是 journal 里的实时片段（库里的 `log_tail` 要到运行结束才回写），详情页因此能做到边跑边看输出。

### 系统与 rclone

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/api/overview` | 仪表盘：计数、运行中列表、rclone 状态、调度概览 |
| `GET` | `/api/rclone` | rclone 托管状态 + `core/stats` |
| `POST` | `/api/rclone/restart` | 重启托管中的 rcd；外部托管时返回 `409` |
| `GET` | `/api/rclone/remotes` | `config/listremotes` |
| `GET` | `/api/rclone/log` | rclone 输出尾部（`?tail=N`） |
| `GET` | `/api/rclone/list` | 目录浏览（`operations/list`） |
| `GET` | `/api/rclone/about` | 空间用量（`operations/about`） |
| `GET` | `/api/rclone/options` | 全局选项（`options/get`） |

### 示例

```bash
BASE=http://127.0.0.1:8080
JAR=/tmp/cloudsync.jar

# 登录（会话写入 cookie jar）
curl -sS -c $JAR -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"'"$CLOUDSYNC_SERVER_PASSWORD"'"}' \
  $BASE/api/login

# 创建任务：每天 03:00 增量同步
curl -sS -b $JAR -H 'Content-Type: application/json' -d '{
  "name": "照片备份",
  "kind": "sync",
  "source": "gdrive:photos",
  "dest": "/mnt/backup/photos",
  "cron_expr": "0 3 * * *",
  "extra_flags": {"transfers": 8, "checkers": 16}
}' $BASE/api/tasks

# 创建多步骤任务：先同步照片（失败即中止、跑完等 60 秒），再复制文档
# steps 是任务的真值，顶层无需再写 kind/source/dest
curl -sS -b $JAR -H 'Content-Type: application/json' -d '{
  "name": "夜间归档",
  "cron_expr": "0 3 * * *",
  "steps": [
    {"name": "照片", "kind": "sync", "source": "gdrive:photos",
     "dest": "/mnt/backup/photos", "dedupe_before": true,
     "extra_flags": {"transfers": 8}, "delay_after": 60, "on_error": "abort"},
    {"name": "文档", "kind": "copy", "source": "gdrive:docs",
     "dest": "/mnt/backup/docs", "on_error": "continue"}
  ]
}' $BASE/api/tasks

# 多步骤任务失败后，从失败的那一步重跑
curl -sS -b $JAR -X POST "$BASE/api/tasks/2/run?from_step=1"

# 手动触发并查看进度
RUN=$(curl -sS -b $JAR -X POST $BASE/api/tasks/1/run | jq -r .run.id)
curl -sS -b $JAR $BASE/api/runs/$RUN | jq '.run.status, .progress.percent'

# 实时事件流
curl -sS -b $JAR -N $BASE/api/events
```

---

## 任务类型与参数

| `kind` | 对应 RC 方法 | 路径参数 |
| --- | --- | --- |
| `sync` | `sync/sync` | `srcFs` / `dstFs` |
| `copy` | `sync/copy` | `srcFs` / `dstFs` |
| `move` | `sync/move` | `srcFs` / `dstFs` |
| `bisync` | `sync/bisync` | `path1` / `path2` |
| `check` | `sync/check` | `srcFs` / `dstFs`（比对两端，**需要目标路径**） |
| `delete` | `operations/delete` | `fs` |
| `purge` | `operations/purge` | `fs` |
| `mkdir` | `operations/mkdir` | `fs` |

任务字段：

| 字段 | 说明 |
| --- | --- |
| `name` | 任务名，唯一，最长 128 字符 |
| `steps` | 执行步骤数组，至少 1 个、最多 50 个，见[多步骤任务](#多步骤任务)。**这是任务的真值**，下表中的 `kind`/`source`/`dest`/`dedupe_before`/`extra_flags` 都是 `steps[0]` 的只读镜像 |
| `kind` | 上表中的类型 |
| `source` | 源路径，映射为 `srcFs` / `fs` / `path1` |
| `dest` | 目标路径，映射为 `dstFs` / `path2`；`purge`/`mkdir` 不需要 |
| `cron_expr` | 定时表达式；留空表示仅手动触发 |
| `enabled` | 是否启用定时调度；停用后定时**与手动**触发都会被拒绝（`409 task_disabled`） |
| `timeout_seconds` | 单次运行超时；`0` 表示使用 `scheduler.default_timeout`。**默认不限制**（`scheduler.default_timeout` 内置值为 `0`） |
| `dedupe_before` | 传输前先清理目标重名，**仅 `sync`/`copy`/`move` 可用**，见下方说明。它不是 rclone 参数，因此**不能**写进 `extra_flags` |
| `extra_flags` | 透传给 rclone RC 的额外参数，如 `{"transfers": 8}` |

> **顶层字段是镜像，不是入参**：请求体里带了 `steps` 时，程序不读这些顶层字段，而是反过来用 `steps[0]` 覆盖它们，避免"顶层留着旧值、悄悄改掉用户刚提交的第一步"。只有**单步骤**任务（历史数据，或只发 `kind`/`source`/`dest` 的老客户端）才允许用顶层字段改写那一步——多步骤时"顶层 kind 指哪一步"没有答案，所以不成立。

**保留参数**：`srcFs`/`dstFs`/`fs`/`remote`/`path1`/`path2` 以及以下划线开头的键（`_async`/`_group`/`_config` 等）由程序内部管理，写进 `extra_flags` 会被拒绝——它们会破坏任务语义与进度分组。

### 清理目标重名（`dedupe_before`）

部分网盘（如 115）允许**同一个目录下存在多个同名对象**。rclone 对这种情况写得很明确：

> Duplicate objects (files with the same name, on those providers that support it) are **not yet handled**.

即 `sync` 只会随便挑一份参与比较，其余的打一条 `NOTICE: ...: Duplicate object found in destination - ignoring` 后忽略——**既不删除也不比较**。后果是：目标端的重复永远不会消失，且"留下的是不是源里最新的那份"没有保证。

开启 `dedupe_before` 后，同步开始前会先做一次**精准清理**，只对"这次确实会发生传输"的路径动手：

1. `rclone lsjson --recursive <dest>` 列出目标全部对象，找出同名重复的**相对路径**；
2. 对每个重名路径，按目录缓存地读取源端同名文件（`rclone lsjson <source>/<dir>`）；
3. 用与 `sync` 默认一致的判据（size + modtime，容差 1 秒以吸收远端截断到秒的精度损失）比较：
   - 源与目标的**所有**副本都不同 → 这次一定会传 → 用 `rclone deletefile` 把目标端该名字的副本逐个删掉，随后 `sync` 从源重写一份进去；
   - 目标已存在一份与源一致的副本 → 这次大概率不会动它 → **不删**，只记日志，避免把本可跳过的传输变成强制重传；
   - 源端不存在该文件 → 与本次传输无关 → 不删。

**为什么不是整目录 `rclone dedupe`**：`dedupe` 会扫过整个目标树删掉所有重名，包括那些永远不会被同步的文件；而且它的 `--dedupe-mode newest` 并不等于"留下与源一致的那份"，反而可能把刚传上去的新文件当成"旧"的删掉。按路径精准清理既避免了误删，也保证了「删完必然回填」（目标没了这个名字，`sync` 就一定重新传）。

注意事项：

- 只在会把源端对象写入目标端的类型下有意义（`sync`/`copy`/`move`），其他类型携带该字段会被服务端拒绝（`Task.Validate`）。前端也只在类型选到这三种时才显示该选项，但**前端隐藏不是约束**，直接调 API 一样会被拦。
- 扫描目标失败会让整轮运行直接失败，不会"跳过清理继续同步"——否则用户会误以为目标已经没有重名。单个路径删除失败只记日志、继续同步，不会中断整轮。
- 该步骤的过程与结果会写进本次运行的日志片段（形如 `[清理重名] 目标重名路径 N 个：已清理 X 个…`），可在运行详情里看到。
- 清理用的是 rclone **子进程**（`lsjson` / `deletefile` 需要读取输出与逐个删除，RC 侧不便表达），二进制与 `--config` 与托管的 `rclone rcd` 保持一致。
- 代价：开启后会多一次目标全量列举。目标对象很多时这一步本身就要花时间，属于预期开销。

### 多步骤任务

一个任务可以编排多组「类型 + 源 + 目标」，按顺序**串行**执行，共享同一条运行记录。

**为什么不是「把几个任务串成链」**：若链由独立任务组成，每个任务的 `cron_expr` 就必须"失效"，用户会面对"配了定时却不生效"的二义状态，还要额外定义"谁的定时说了算"。把步骤收进任务内部后，`cron_expr` 只属于任务一级——链的触发时间天然等于第一个步骤的开始时间。

步骤字段：

| 字段 | 说明 |
| --- | --- |
| `name` | 步骤备注，仅用于失败时指出"哪一步挂了"，可留空 |
| `kind` / `source` / `dest` | 与任务级同义，作用于本步骤 |
| `extra_flags` | 本步骤的 rclone 参数，同样受保留参数限制 |
| `timeout_seconds` | 本步骤超时；`0` 回落到任务级 `timeout_seconds`，再回落到 `scheduler.default_timeout`。三层全为 `0` 时该步不限时 |
| `dedupe_before` | 本步骤传输前先清理目标重名，规则与任务级一致 |
| `delay_after` | 本步骤**结束之后**、下一步开始**之前**等待的秒数（`0~86400`）。第一个步骤之前不等，最后一个步骤之后也不等 |
| `on_error` | 本步骤失败后的行为：`continue`（默认）记录失败并继续执行后续步骤；`abort` 立即中止，后续步骤不再执行。填其他值直接报错，不会被静默归一 |

执行与状态语义：

- **失败默认不中断**：某一步失败后，后面的步骤照常执行——"先补元数据、再传文件"这类编排里，第 3 步失败不该让第 6 步跟着不跑。整条链有任何一步失败，本次运行最终状态就是 `failed`，文案形如 `2/3 个步骤完成，失败：文档`。
- **进度按步骤数折算，不是按字节比**：各步总量在开始前不可知，中途还会随新步骤开始而增长，用字节比会让百分比往回跳。折算是「已完成步骤数 / 本轮步骤数」加上「当前步骤进度 / 本轮步骤数」，单步骤任务的进度与升级前完全一致；失败的步骤不计入完成数，整链没走通进度就不会到顶。
- **每个步骤一个 rclone 分组** `run-<runID>-<stepIdx>`：分组混用会让 `core/stats?group=` 读到跨步累计值。字节/文件数等统计仍在运行记录上按整条链累加。
- **逐步结果可见**：运行记录带 `step_index`（0 基）、`step_total` 与 `step_results`（每步的状态、耗时、字节/文件数、错误），详情页逐步展示是哪一步挂的。
- **多步骤任务无条件禁止并发**：链的时长是各步之和加间隔，很容易超过触发周期，允许重叠会让几轮链同时跑、互相抢带宽，"最近一次结果"也失去意义。

#### 从失败的那一步重跑

`POST /api/tasks/{id}/run?from_step=N`（`N` 为 0 基下标，`N=0` 等价于整体重跑）跳过前 `N` 步，直接从第 `N+1` 步开始：

```bash
curl -sS -b $JAR -X POST "$BASE/api/tasks/7/run?from_step=2"   # 3 步的链，只跑第 3 步
```

越界或非整数返回 `400 bad_from_step`，文案形如 `起始步骤 4 超出范围（共 3 步）`。

之所以值得单独开一个入口：重跑前缀毫无收益——前面的步骤刚成功过，再跑一遍要重新比对整棵树，代价与首次执行相同。相应地，进度与文案里的分母都是**本轮实际执行的步骤数**：3 步的链从第 3 步重跑且失败，显示的是 `0/1 个步骤完成`，而不是误导性的 `2/3`。详情页在链存在失败步骤时会直接给出「从步骤 N 重跑」按钮。

#### 兼容性

- 只发顶层 `kind`/`source`/`dest` 的请求（含历史数据）会被当作**单步骤**任务，行为与升级前一致。
- 迁移会把存量任务补成一条 `task_steps` 记录，且**不复制** `timeout_seconds`——把任务级超时固化成的步骤级超时，会让日后调整任务超时不再生效。
- 步骤上限 50：步骤串行且共享一次运行记录，再多会让"一次运行"的语义变模糊，超时与间隔叠加出的总时长也难以预估。

---

## cron 表达式

语法档位由 `scheduler.seconds` 决定，**两档严格互斥，不做猜测**：

| `scheduler.seconds` | 字段数 | 构成 | 示例 |
| --- | --- | --- | --- |
| `false`（默认） | 5 | 分 时 日 月 周 | `0 3 * * *` = 每天 03:00 |
| `true` | 6 | 秒 分 时 日 月 周 | `0 0 3 * * *` = 每天 03:00:00 |

两档都支持 `@every 1h`、`@daily`、`@midnight` 等描述符。

段数与档位不符时会直接报错，并说明期望段数与切换方式：

```
cron 表达式 "0 3 * * *" 有 5 段，当前配置需要 6 段（秒 分 时 日 月 周）：
如需 5 段语法，请设置 scheduler.seconds: false
```

> 这里刻意不做"自动回退解析"。若允许回退，用户在 `seconds: true` 下写 `0 3 * * *` 会被静默解释成
> 「每月 3 日 00:00」，而用户很可能想要「每天 03:00」——静默的错误调度比直接报错危险得多。

`next_run_at` 由调度器回写并在管理端展示；任务增删改立即生效（增量更新条目），无需重启进程。

---

## 部署

### Docker

镜像基于多阶段构建，运行层包含 `rclone` 与 `cloudsync`：

```bash
docker build -t cloudsync:latest .

docker run -d --name cloudsync \
  -p 8080:8080 \
  -e CLOUDSYNC_SERVER_PASSWORD='请改成足够强的密码' \
  -v cloudsync-data:/data \
  -v "$HOME/.config/rclone:/config/rclone:ro" \
  cloudsync:latest
```

容器内约定：

- 配置文件 `/etc/cloudsync/config.yaml`
- 数据目录 `/data`（SQLite 落在 `/data/cloudsync.db`）
- rclone 配置挂载到 `/config/rclone/rclone.conf`
- 以非 root 用户 `cloudsync` 运行
- 已声明 `HEALTHCHECK`，探测 `/api/health`

### systemd

```bash
sudo install -Dm755 bin/cloudsync /usr/local/bin/cloudsync
sudo install -Dm644 config.example.yaml /etc/cloudsync/config.yaml
sudo install -Dm644 deploy/cloudsync.service /etc/systemd/system/cloudsync.service
sudo useradd --system --home /var/lib/cloudsync --create-home cloudsync

sudo install -d -o cloudsync -g cloudsync /var/lib/cloudsync
sudo install -d -o cloudsync -g cloudsync /var/log/cloudsync

sudo chown cloudsync:cloudsync /etc/cloudsync/config.yaml
sudo chmod 600 /etc/cloudsync/config.yaml      # 内含管理端密码
```

在 `/etc/cloudsync/config.yaml` 中至少设置：

```yaml
rclone:
  config_file: "/etc/rclone/rclone.conf"     # 不要依赖运行用户的 HOME
storage:
  dsn: "/var/lib/cloudsync/cloudsync.db"
log:
  file: "/var/log/cloudsync/cloudsync.log"
scheduler:
  timezone: "Asia/Shanghai"
```

> 单元文件以 `ProtectSystem=strict` 运行，`/` 为只读，仅 `/var/lib/cloudsync` 与
> `/var/log/cloudsync` 通过 `ReadWritePaths` 放行。配置文件落在其他可写路径会导致启动失败。

若还没有 rclone 配置：

```bash
sudo install -d -o cloudsync -g cloudsync /etc/rclone
sudo -u cloudsync rclone --config /etc/rclone/rclone.conf config
sudo chmod 600 /etc/rclone/rclone.conf
```

```bash
# 可选：把管理端密码放在环境文件里，避免写进配置文件
sudo install -m 600 -o root -g root /dev/null /etc/cloudsync/cloudsync.env
echo 'CLOUDSYNC_SERVER_PASSWORD=请改成足够强的密码' | sudo tee -a /etc/cloudsync/cloudsync.env

sudo systemctl daemon-reload
sudo systemctl enable --now cloudsync
sudo systemctl status cloudsync
journalctl -u cloudsync -f
```

> **关于 `KillMode`**：单元文件使用 `KillMode=mixed`，`systemctl stop` 时先只向主进程发
> `SIGTERM`，让它走 `core/quit` 优雅关闭 rclone，超时后才对整个 cgroup 发 `SIGKILL`。
> 若保持默认的 `control-group`，systemd 会直接给 rclone 子进程发信号，绕过程序内的优雅关闭路径。

### 反向代理

放在 Nginx / Caddy 之后时：

- 传入 `X-Forwarded-For`，并在配置中设 `server.trusted_proxy: true`（否则按 IP 限流可被伪造头绕过）。
- **`/api/events` 是 SSE 长连接**：需关闭代理缓冲（Nginx `proxy_buffering off;`）并放宽读超时。
- 程序侧 `server.write_timeout` 保持 `0`，否则长连接会被自身截断。
- 如需部署在子路径，设置 `server.base_path: /cloudsync/`，前端会自动按该前缀请求接口。

### 优雅关闭顺序

收到 `SIGINT` / `SIGTERM` 后依次执行：

1. 停止接收新的 HTTP 请求（`Shutdown`）
2. 停止调度器，避免关停过程中触发新任务
3. 取消并等待运行中的任务（超时 30s）
4. `core/quit` 关闭 rclone rcd，超时后强杀

---

## 发版

发版走 GitHub Actions（`.github/workflows/release.yml`）：推一个 tag 上去，自动
「校验 → 交叉编译 → 打包 → 建 Release 草稿」。构建参数只有一份 —— 在 Makefile 的
`dist` 目标里，CI 只负责编排，不会出现 CI 与本地两套参数悄悄漂移的情况。

### 触发方式

**正式发版**，推一个 `v*` tag：

```bash
git tag -a v0.1.0 -m "v0.1.0"
git push origin v0.1.0
```

**只试跑不发布**：仓库 Actions 页面选 `release` → Run workflow，填一个版本号
（默认 `v0.0.0-dev`）。只会产出 Actions artifact 供下载检查，不会建 Release。

### 前置条件

不满足时表现为「什么都没发生」，而且**没有任何报错**，是最容易踩的坑：

1. 仓库已经推到 GitHub，`.github/workflows/release.yml` 已经进了仓库；
2. tag 触发取的是 **tag 指向的那个提交**里的 workflow 文件 —— 先提交 workflow，
   再打 tag，顺序反了就永远不会触发；
3. 手动触发的 Run workflow 按钮只认**默认分支**上的 workflow 文件。

### 产物命名

`cloudsync-<版本>-<系统>-<架构>[vN].<tar.gz|zip>`（32 位 ARM 带 `vN` 区分 GOARM）：

| 系统 | 架构 | 归档 |
| --- | --- | --- |
| Linux | amd64（64 位 x86） | `cloudsync-v0.1.0-linux-amd64.tar.gz` |
| Linux | 386（32 位 x86） | `cloudsync-v0.1.0-linux-386.tar.gz` |
| Linux | armv6（32 位 ARM） | `cloudsync-v0.1.0-linux-armv6.tar.gz` |
| Linux | armv7（32 位 ARM） | `cloudsync-v0.1.0-linux-armv7.tar.gz` |
| Linux | arm64（64 位 ARM） | `cloudsync-v0.1.0-linux-arm64.tar.gz` |
| Linux | riscv64 | `cloudsync-v0.1.0-linux-riscv64.tar.gz` |
| Windows | amd64（64 位 x86） | `cloudsync-v0.1.0-windows-amd64.zip` |
| Windows | 386（32 位 x86） | `cloudsync-v0.1.0-windows-386.zip` |
| Windows | arm64（64 位 ARM） | `cloudsync-v0.1.0-windows-arm64.zip` |
| macOS | amd64（Intel） | `cloudsync-v0.1.0-darwin-amd64.tar.gz` |
| macOS | arm64（Apple Silicon） | `cloudsync-v0.1.0-darwin-arm64.tar.gz` |

另附一份 `SHA256SUMS`（只覆盖归档，不含中间产物）：

```bash
sha256sum -c SHA256SUMS
```

归档内含 `cloudsync`（Unix 平台带可执行位）+ `README.md` + `config.example.yaml`。Unix 平台
刻意用 `tar.gz` 而不是裸二进制 —— 裸文件从 GitHub 下载后会丢掉可执行位，`tar.gz` 能保留。

32 位 ARM 为什么出两个：`armv6` 能跑在 v6 与 v7 硬件上（兼容性最好），`armv7` 是硬浮点、
性能更好。两者文件名不歧义，多一个归档而已。

#### 为什么没有 MIPS

`mips` / `mipsle` / `mips64` / `mips64le` **全部编不出来**。不是 Makefile 少配了什么，
而是 SQLite 驱动不支持：本项目用纯 Go 的 `modernc.org/sqlite`（这正是 `CGO_ENABLED=0`
能交叉编译的前提），它依赖的 `modernc.org/libc` 没有 MIPS 实现 ——

- `mips` / `mipsle` / `mips64`：libc 在该架构下一个文件都没有，报
  `build constraints exclude all Go files`；
- `mips64le`：libc 有半套，但 sqlite 侧绑定缺失，报一堆
  `undefined: sqlite3_index_constraint` / `Xsqlite3_config` / `SQLITE_OK`。

这两个库的版本是 `modernc.org/sqlite v1.34.5` / `modernc.org/libc v1.55.3`；升级后
可以再验证一次 MIPS 是否被补上。真需要 MIPS 只能换 CGO 版驱动
（`mattn/go-sqlite3`），那等于放弃无 CGO 交叉编译、给每个目标配 C 交叉工具链。

### Release 是草稿

工作流建的是**草稿** Release（`draft: true`），需要人工过一眼产物与自动生成的
Release notes，再点 Publish。注意：草稿状态下 `GET /releases/latest` 取不到这份
Release，别误以为发布失败。

### 本地打包

```bash
make dist VERSION=v0.1.0    # → dist/*.tar.gz|*.zip 与 SHA256SUMS
```

`dist` 依赖 `cross`，所以编译参数只有 `cross` 一份，不存在「本地与 CI 两套参数漂移」。
跑完之后 `dist/` 里**同时有裸二进制和归档**（前者是 `cross` 的产物）—— 上传 Release 时
只取归档与 `SHA256SUMS`，workflow 里已按 `*.tar.gz` / `*.zip` 明确限定。

`make dist` 依赖 `zip` / `tar` / `sha256sum`，所以适合在 Linux/WSL 或 CI 上跑。
Windows 的 Git Bash 一般不带 `zip`，该目标会**直接报错退出**，而不是产出一份
残缺的 `dist/`。另外 Mac 上没有 `sha256sum`，会自动退回 `shasum -a 256`。

### 版本号从哪来

`main.version` 由 `-ldflags -X` 注入，内置回退值是 `dev`：

- CI：用 tag 名（如 `v0.1.0`）；
- 本地：`git describe --tags --always --dirty` —— 有 tag 就是 `v0.1.0`，没 tag
  就是 commit 短哈希（如 `5ea7dbb`），工作区脏则带 `-dirty` 后缀。

查当前二进制的版本：

```bash
./cloudsync -version
```

---

## 开发

```bash
make help          # 列出全部目标
make build         # 构建 bin/cloudsync
make build-mock    # 构建 bin/rclone-mock
make web-build     # 构建前端产物（改了 web/src 之后必跑）
make test          # go test ./...
make vet           # go vet ./...
make fmt           # gofmt -l -w
make run           # 本地运行（读取 config.yaml）
make cross         # 交叉编译 11 个平台到 dist/（裸二进制，本地自用）
make dist          # cross + 打包归档 + SHA256SUMS（发版用，见「发版」）
make clean         # 清理 bin/ 与 dist/
```

### 代码结构

```
cmd/cloudsync/          入口：装配、信号、优雅关闭
cmd/rclone-mock/        假 rclone RC 服务（联调 / 压测）
internal/config/        YAML + env + flag 合并与校验
internal/logging/       slog 初始化、字段助手、环形缓冲
internal/store/         任务 / 运行记录持久化
internal/rclone/        RC 客户端 + 子进程监管（proc_*.go 跨平台）
internal/manager/       异步运行生命周期与进度聚合
internal/scheduler/     cron 注册与触发
internal/retention/     运行记录保留策略与过期清理
internal/web/           HTTP 路由、认证、REST、SSE
internal/web/static/    前端构建产物（已提交，embed 进二进制）
web/                    前端源码：React + Vite + Tailwind + shadcn/ui
deploy/                 systemd 单元等部署文件
```

### 前端

管理端是 React 19 + Vite + Tailwind CSS 4 + shadcn/ui（Radix  primitives），源码在 `web/`。

```bash
make web-install     # 首次：npm install
make web-dev         # 开发服务器（:5173，/api 代理到本机 :8080 的 cloudsync）
make web-build       # 构建到 internal/web/static（提交前必跑）
make web-typecheck   # tsc --noEmit
```

一个刻意的设计：**构建产物 `internal/web/static/` 是提交进仓库的**，所以 `go build` / `make build` 不需要 Node，clone 下来就能编译出带完整管理端的二进制。代价是改前端后仓库里会多一份压缩产物的 diff。改了 `web/src` 记得跑 `make web-build` 并把产物一起提交，否则二进制里还是旧界面。

`web/node_modules/` 不入库，`package-lock.json` 入库以保证依赖可复现。shadcn 组件源码（含后续新增组件）可以跑：

```bash
cd web && node scripts/fetch-shadcn.mjs button card dialog   # 从官方 registry 取源码
```

### 测试

```bash
make test                        # 全量
go test ./internal/web/... -v    # 单包
```

测试策略：

- `config`、`store`、`rclone`、`scheduler`、`manager` 是**单元测试**，其中 rclone 客户端与 supervisor 用 `httptest` 假服务驱动。
- `web` 是**集成测试**：起真实的 `httptest` 服务 + 假 rclone RC 服务 + 真实 SQLite，覆盖认证、限流、任务 CRUD、触发、进度、SSE、权限边界。
- 需要真实 rclone 二进制的场景用 `cmd/rclone-mock` 替代，CI 无需安装 rclone。

---

## 已知限制

- **未做 `-race` 验证**：竞态检测需要 CGO 与 C 编译器，当前开发机未安装 `gcc`，因此 `go test -race` 未能运行。并发关键路径（manager 的活跃表、SSE 订阅、调度器条目）已通过"快照拷贝"而非共享指针暴露状态来规避数据竞争，但这属于设计保证而非工具验证——**在具备 C 编译器的环境上补跑 `make test-race` 是推荐的下一步**。
- **传输能力完全取决于 rclone**：`srcFs` / `dstFs` 的写法、支持的 remote 类型、传输参数都以 rclone 为准，本程序不做校验。
- **单机编排**：SQLite + 内存态活跃表，不支持多实例共享同一份数据目录。多实例请各自使用独立的 `storage.dsn` 与 `rclone.rc_addr`。
- **日志保留的最小粒度是 1 小时**：想保留"最近 30 分钟"做不到，再短的窗口对排查也没意义。另外 `log.file`（程序自身日志）没有轮转与过期策略，长期运行请交给 logrotate 之类的外部工具；会被清理的只有运行记录里的 rclone 输出片段。
- **`bisync` 的首次运行**仍需人工按 rclone 要求准备 `--resync`，本程序只负责调度与跟踪。
- **Windows 服务**：未提供 Windows 服务包装（`os.Interrupt` 覆盖 Ctrl+C；作为服务运行时请确认 SIGTERM 等价信号能被投递）。
