# CloudSync 构建脚本
#
# 常用：
#   make build        构建 bin/cloudsync
#   make web-build    构建前端产物（改了 web/src 之后要跑）
#   make test         运行全部测试
#   make cross        交叉编译多平台（裸二进制，本地自用）
#   make dist         交叉编译 + 打包归档 + 校验和（发版用，CI 调的就是它）

SHELL      := /bin/sh

BIN_DIR    := bin
DIST_DIR   := dist
APP        := cloudsync
MOCK       := rclone-mock
PKG_MAIN   := ./cmd/cloudsync
PKG_MOCK   := ./cmd/rclone-mock

# 版本号优先取 git tag/commit，取不到则回退 dev。
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT     ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_TIME ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

GO         ?= go
GOFLAGS    ?=
# -s -w 去掉符号表与调试信息；-X 注入版本号。
LDFLAGS    := -s -w -X main.version=$(VERSION)

# 交叉编译目标。
PLATFORMS  := linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64

.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# 构建
# ---------------------------------------------------------------------------

.PHONY: build
build: ## 构建当前平台的 cloudsync
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) -trimpath -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$(APP) $(PKG_MAIN)
	@echo "已构建 $(BIN_DIR)/$(APP) ($(VERSION))"

.PHONY: build-mock
build-mock: ## 构建 rclone-mock（无 rclone 时的联调用假服务）
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) -trimpath -o $(BIN_DIR)/$(MOCK) $(PKG_MOCK)
	@echo "已构建 $(BIN_DIR)/$(MOCK)"

.PHONY: build-all
build-all: build build-mock ## 构建全部二进制

.PHONY: cross
cross: ## 交叉编译多平台到 dist/（裸二进制，不打包）
	@mkdir -p $(DIST_DIR)
	@set -e; for p in $(PLATFORMS); do \
		os=$${p%/*}; arch=$${p#*/}; \
		ext=""; [ "$$os" = "windows" ] && ext=".exe"; \
		out="$(DIST_DIR)/$(APP)-$$os-$$arch$$ext"; \
		echo "-> $$out"; \
		GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 \
			$(GO) build $(GOFLAGS) -trimpath -ldflags "$(LDFLAGS)" -o "$$out" $(PKG_MAIN); \
	done
	@echo "交叉编译完成，产物在 $(DIST_DIR)/"

# 发版打包：交叉编译 → 每平台打成归档 → 生成 SHA256SUMS。CI 走的就是这个目标，
# 本地和 CI 共用一份编译参数，避免两边悄悄漂移。
#
# 归档命名：cloudsync-<版本>-<系统>-<架构>.<tar.gz|zip>
#   cloudsync-v0.1.0-linux-amd64.tar.gz
#   cloudsync-v0.1.0-windows-amd64.zip
# 归档内含：可执行文件 + README.md + config.example.yaml。
#
# 为什么 Unix 用 tar.gz 而不是直接丢裸二进制：裸文件下载后会丢掉可执行位，
# 用户还得自己 chmod +x；tar.gz 能保留。Windows 用 zip，双击就能解压。
#
# 所以打包前后要保证二进制带可执行位：go build 在 Linux 上默认 0755，但值受
# umask 影响，这里显式 chmod 0755 抹平差异。（在 Windows 盘上 chmod 是空操作，
# 本地打出来的 tar 里会是 0644 —— 属正常现象，CI 在 Linux 上打不会有这问题。）
#
# 依赖 zip / tar / sha256sum（或 macOS 的 shasum）。CI 的 ubuntu runner 全部自带；
# Windows 的 Git Bash 通常没有 zip，会在此处直接报错退出，而不是产出一份残缺的
# dist —— 本地要打包请用 WSL/Linux，或走 workflow_dispatch 试跑。
.PHONY: dist
dist: ## 交叉编译并打包发版归档（产出 dist/*.tar.gz|*.zip 与 SHA256SUMS）
	@command -v zip >/dev/null 2>&1 || { \
		echo "缺少 zip 命令：Windows 的 Git Bash 一般不带 zip。"; \
		echo "请在 Linux/WSL 或 CI 上执行 make dist，或安装 zip 后重试。"; \
		exit 1; }
	@rm -rf $(DIST_DIR)
	@mkdir -p $(DIST_DIR)
	@set -e; for p in $(PLATFORMS); do \
		os=$${p%/*}; arch=$${p#*/}; \
		ext=""; [ "$$os" = "windows" ] && ext=".exe"; \
		name="$(APP)-$(VERSION)-$$os-$$arch"; \
		stage="$(DIST_DIR)/.stage/$$name"; \
		mkdir -p "$$stage"; \
		echo "-> $$name"; \
		GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 \
			$(GO) build $(GOFLAGS) -trimpath -ldflags "$(LDFLAGS)" \
				-o "$$stage/$(APP)$$ext" $(PKG_MAIN); \
		chmod 0755 "$$stage/$(APP)$$ext"; \
		cp README.md config.example.yaml "$$stage/"; \
		if [ "$$os" = "windows" ]; then \
			( cd "$$stage" && zip -qr "../../$$name.zip" "$(APP)$$ext" README.md config.example.yaml ); \
		else \
			tar -C "$$stage" -czf "$(DIST_DIR)/$$name.tar.gz" "$(APP)" README.md config.example.yaml; \
		fi; \
	done
	@rm -rf $(DIST_DIR)/.stage
	@cd $(DIST_DIR) && if command -v sha256sum >/dev/null 2>&1; then \
		sha256sum * > SHA256SUMS; \
	else \
		shasum -a 256 * > SHA256SUMS; \
	fi
	@echo "打包完成：$(DIST_DIR)/（版本 $(VERSION)）"
	@ls -lh $(DIST_DIR)

.PHONY: install
install: build ## 安装到 GOBIN（或 GOPATH/bin）
	$(GO) install $(GOFLAGS) -trimpath -ldflags "$(LDFLAGS)" $(PKG_MAIN)

# ---------------------------------------------------------------------------
# 前端
#
# 前端产物（internal/web/static/）已提交进仓库，所以纯 Go 构建不需要 Node。
# 只有改 web/src 下的源码时才需要跑这几个目标。
# ---------------------------------------------------------------------------

WEB_DIR := web
NPM     ?= npm

.PHONY: web-install
web-install: ## 安装前端依赖
	cd $(WEB_DIR) && $(NPM) install

.PHONY: web-dev
web-dev: ## 启动开发服务器（/api 代理到本机 8080）
	cd $(WEB_DIR) && $(NPM) run dev

.PHONY: web-build
web-build: ## 构建前端产物到 internal/web/static（提交前必跑）
	cd $(WEB_DIR) && $(NPM) run build

.PHONY: web-typecheck
web-typecheck: ## 前端类型检查
	cd $(WEB_DIR) && $(NPM) run typecheck

# ---------------------------------------------------------------------------
# 质量
# ---------------------------------------------------------------------------

.PHONY: test
test: ## 运行全部测试
	$(GO) test $(GOFLAGS) ./... -count=1

.PHONY: test-v
test-v: ## 运行全部测试（详细输出）
	$(GO) test $(GOFLAGS) ./... -count=1 -v

.PHONY: test-race
test-race: ## 运行测试并开启竞态检测（需要 CGO 与 C 编译器）
	CGO_ENABLED=1 $(GO) test $(GOFLAGS) -race ./... -count=1

.PHONY: cover
cover: ## 生成覆盖率报告 coverage.html
	$(GO) test $(GOFLAGS) ./... -count=1 -coverprofile=coverage.out -covermode=atomic
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "已生成 coverage.html"

.PHONY: vet
vet: ## go vet 静态检查
	$(GO) vet $(GOFLAGS) ./...

.PHONY: fmt
fmt: ## 格式化代码
	$(GO) fmt ./...

.PHONY: fmt-check
fmt-check: ## 检查是否存在未格式化的文件（CI 用）
	@out=$$(gofmt -l .); \
	if [ -n "$$out" ]; then echo "以下文件未格式化:"; echo "$$out"; exit 1; fi; \
	echo "格式检查通过"

.PHONY: tidy
tidy: ## 整理 go.mod / go.sum
	$(GO) mod tidy

.PHONY: check
check: fmt-check vet test ## 提交前一键校验

# ---------------------------------------------------------------------------
# 运行
# ---------------------------------------------------------------------------

.PHONY: run
run: ## 本地运行（读取 config.yaml）
	$(GO) run $(PKG_MAIN) -config config.yaml -log-format text

.PHONY: run-mock
run-mock: ## 启动 rclone-mock，供 rclone.auto_start=false 时联调
	$(GO) run $(PKG_MOCK) rcd --rc-addr 127.0.0.1:5572 --rc-user admin --rc-pass dev

.PHONY: check-config
check-config: ## 校验 config.yaml
	$(GO) run $(PKG_MAIN) -config config.yaml -check

# ---------------------------------------------------------------------------
# 容器
# ---------------------------------------------------------------------------

.PHONY: docker
docker: ## 构建 Docker 镜像
	docker build --build-arg VERSION=$(VERSION) -t cloudsync:$(VERSION) -t cloudsync:latest .

# ---------------------------------------------------------------------------
# 清理
# ---------------------------------------------------------------------------

.PHONY: clean
clean: ## 清理构建产物
	rm -rf $(BIN_DIR) $(DIST_DIR) coverage.out coverage.html

.PHONY: help
help: ## 显示本帮助
	@echo "CloudSync 构建目标："
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'
