# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# 构建阶段：编译 cloudsync（纯 Go，CGO 关闭）
# ---------------------------------------------------------------------------
FROM golang:1.24-alpine AS builder

# 允许注入国内代理以加速依赖拉取，例如：
#   docker build --build-arg GOPROXY=https://goproxy.cn,direct .
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY} \
    CGO_ENABLED=0 \
    GOOS=linux

WORKDIR /src

# 先只拷依赖清单，利用层缓存：仅依赖变化时才重新下载模块。
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
# -trimpath 去掉构建机路径，产出可复现；-s -w 减小体积。
RUN go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
        -o /out/cloudsync ./cmd/cloudsync

# ---------------------------------------------------------------------------
# 运行阶段
# ---------------------------------------------------------------------------
FROM alpine:3.20

# rclone 是必需的外部依赖；ca-certificates 供 HTTPS remote 校验证书。
# tzdata：cloudsync 自身已通过 import _ "time/tzdata" 内嵌 IANA 时区库，
# 不依赖系统；这里装一份主要是给 rclone 等外部工具使用。
RUN apk add --no-cache rclone ca-certificates tzdata \
 && addgroup -S cloudsync \
 && adduser -S -G cloudsync -h /data -s /sbin/nologin cloudsync \
 && install -d -o cloudsync -g cloudsync /data /config/rclone

COPY --from=builder /out/cloudsync /usr/local/bin/cloudsync
COPY config.example.yaml /etc/cloudsync/config.yaml

# 容器内约定：
#   - 数据目录 /data（SQLite 落在 /data/cloudsync.db）
#   - rclone 配置 /config/rclone/rclone.conf（建议只读挂载）
#   - server.password 必须由环境变量 CLOUDSYNC_SERVER_PASSWORD 注入
ENV CLOUDSYNC_STORAGE_DSN=/data/cloudsync.db \
    CLOUDSYNC_RCLONE_CONFIG_FILE=/config/rclone/rclone.conf \
    CLOUDSYNC_LOG_FORMAT=json \
    CLOUDSYNC_LOG_LEVEL=info

VOLUME ["/data"]

USER cloudsync
WORKDIR /data

EXPOSE 8080

# 默认监听 0.0.0.0:8080（见 config.example.yaml 的 server.addr）。
# 如改过监听地址/端口，请同步调整这里的探测地址。
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8080/api/health || exit 1

ENTRYPOINT ["/usr/local/bin/cloudsync"]
CMD ["-config", "/etc/cloudsync/config.yaml"]
