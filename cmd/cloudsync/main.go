// Command cloudsync 是一个以 rclone 为传输引擎的云端文件同步管理器。
//
// 特性：
//   - 启动时自动拉起 rclone rcd，退出时优雅关闭（含信号/强杀兜底）
//   - cron 表达式定时调度，支持秒级精度
//   - 异步任务执行 + 实时进度查询（REST + SSE）
//   - 结构化日志（log/slog，JSON/文本）
//   - 内置 Web 管理端（单二进制，前端已 embed）
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"cloudsync/internal/config"
	"cloudsync/internal/logging"
	"cloudsync/internal/maintenance"
	"cloudsync/internal/manager"
	"cloudsync/internal/rclone"
	"cloudsync/internal/retention"
	"cloudsync/internal/scheduler"
	"cloudsync/internal/store"
	"cloudsync/internal/web"
)

// version 由构建脚本通过 -ldflags 注入。
var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "启动失败: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		configPath  = flag.String("config", "", "配置文件路径（YAML）；不指定时先看 CLOUDSYNC_CONFIG，再看当前目录的 config.yaml")
		showVersion = flag.Bool("version", false, "打印版本并退出")
		checkOnly   = flag.Bool("check", false, "仅校验配置文件后退出")
		addr        = flag.String("addr", "", "覆盖 Web 监听地址，如 0.0.0.0:8080")
		logLevel    = flag.String("log-level", "", "覆盖日志级别：debug/info/warn/error")
		logFormat   = flag.String("log-format", "", "覆盖日志格式：json/text")
		rclonePath  = flag.String("rclone", "", "覆盖 rclone 可执行文件路径")
		noRclone    = flag.Bool("no-rclone", false, "不拉起 rclone，仅连接已运行的 rcd")
	)
	flag.Usage = usage
	flag.Parse()

	if *showVersion {
		fmt.Printf("cloudsync %s\n", version)
		return nil
	}

	// 配置文件定位：-config > CLOUDSYNC_CONFIG > 当前目录下的 config.yaml。
	// 先解析出来，是为了让下面的启动日志与 -check 能报出**真正**用了哪个文件
	// （否则自动加载了 config.yaml 却显示"未指定"，排查时会误判）。
	cfgPath := config.ResolvePath(*configPath)

	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}
	// 命令行参数优先级最高。
	if *addr != "" {
		cfg.Server.Addr = *addr
	}
	if *logLevel != "" {
		cfg.Log.Level = *logLevel
	}
	if *logFormat != "" {
		cfg.Log.Format = *logFormat
	}
	if *rclonePath != "" {
		cfg.Rclone.Path = *rclonePath
	}
	if *noRclone {
		cfg.Rclone.AutoStart = false
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	if *checkOnly {
		fmt.Printf("配置校验通过（配置文件: %s）\n", orDefault(cfgPath, "内置默认值 + 环境变量"))
		return nil
	}

	logger, closeLog, err := logging.Setup(logging.Options{
		Level:     cfg.Log.Level,
		Format:    cfg.Log.Format,
		File:      cfg.Log.File,
		AddSource: cfg.Log.AddSource,
		Service:   "cloudsync",
	})
	if err != nil {
		return err
	}
	defer func() {
		if cerr := closeLog(); cerr != nil {
			fmt.Fprintf(os.Stderr, "关闭日志失败: %v\n", cerr)
		}
	}()

	web.Version = version

	logger.Info("CloudSync 启动中",
		"version", version,
		"config_file", orDefault(cfgPath, "(未指定)"),
		"web_addr", cfg.Server.Addr,
		"base_path", cfg.Server.BasePath,
		"rclone_binary", cfg.Rclone.Path,
		"rclone_rc_addr", cfg.Rclone.RCAddr,
		"rclone_autostart", cfg.Rclone.AutoStart,
		"scheduler_enabled", cfg.Scheduler.Enabled,
		"scheduler_timezone", orDefault(cfg.Scheduler.Timezone, "local"),
		"storage", cfg.Storage.DSN,
		"run_retention", retentionText(cfg.Storage.RunRetention.D()),
	)

	if err := cfg.EnsureDataDir(); err != nil {
		return fmt.Errorf("准备数据目录: %w", err)
	}

	// 根 context：随进程信号取消，驱动所有后台 goroutine 停止。
	rootCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	// 1) 存储
	st, err := store.Open(cfg.Storage.DSN, logger)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := st.Close(); cerr != nil {
			logger.Error("关闭数据库失败", logging.Err(cerr))
		}
	}()

	migrateCtx, cancelMigrate := context.WithTimeout(rootCtx, 30*time.Second)
	err = st.Migrate(migrateCtx)
	cancelMigrate()
	if err != nil {
		return err
	}
	logger.Info("数据库已就绪", "dsn", cfg.Storage.DSN, "driver", cfg.Storage.Driver)

	// 上一次进程被强杀时可能残留 running 记录，这里统一收敛为失败。
	cleanupCtx, cancelCleanup := context.WithTimeout(rootCtx, 20*time.Second)
	stale, err := st.InterruptStaleRuns(cleanupCtx)
	cancelCleanup()
	if err != nil {
		logger.Warn("清理残留运行记录失败", logging.Err(err))
	} else if stale > 0 {
		logger.Warn("发现上次异常退出遗留的运行记录，已标记为失败", "count", stale)
	}

	// 2) rclone rcd 子进程
	sup := rclone.NewSupervisor(cfg.Rclone, logger)
	startCtx, cancelStart := context.WithTimeout(rootCtx, cfg.Rclone.StartupTimeout.D()+time.Second)
	err = sup.Start(startCtx)
	cancelStart()
	if err != nil {
		return err
	}

	// 3) 任务管理器
	mgr := manager.New(st, sup.Client(), rclone.NewRunner(cfg.Rclone), sup.Journal(), manager.Options{
		MaxConcurrent:    cfg.Scheduler.MaxConcurrentRuns,
		DefaultTimeout:   cfg.Scheduler.DefaultTimeout.D(),
		SkipOverlap:      cfg.Scheduler.SkipOverlap,
		HistoryLimit:     cfg.Storage.HistoryLimit,
		JournalTailLines: 200,
		PollInterval:     2 * time.Second,
	}, logger)
	mgr.SetBaseContext(rootCtx)

	// 3.5) 运行记录保留策略（过期清理）
	//
	// 配置里的 storage.run_retention 只是"出厂默认值"：用户在设置页改过之后
	// 值存在数据库里，优先级更高，避免重启后被配置文件覆盖回去。
	ret := retention.New(st,
		cfg.Storage.RunRetention.D(),
		cfg.Storage.RetentionInterval.D(),
		logger,
	)
	go ret.Run(rootCtx)

	// 3.6) 数据库瘦身（丢弃日志片段、裁剪记录、整理文件）
	//
	// busy 回调用来避开"有任务在运行时整理数据库"：VACUUM 要重写整个库，
	// 而本程序只用一条数据库连接，期间所有进度回写都会被堵住。
	maint := maintenance.New(st, mgr.ActiveCount, logger)

	// 4) 调度器
	sch, err := scheduler.New(cfg.Scheduler, st, mgr, logger)
	if err != nil {
		sup.Stop(context.Background())
		return err
	}
	schedCtx, cancelSched := context.WithTimeout(rootCtx, 30*time.Second)
	err = sch.Start(schedCtx)
	cancelSched()
	if err != nil {
		sup.Stop(context.Background())
		return err
	}

	// 5) Web 管理端
	srv := web.New(cfg, st, mgr, sch, sup, ret, maint, logger)
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()
	go srv.StartGC(rootCtx)

	// 等待退出信号或服务异常。
	exitReason := "收到退出信号"
	select {
	case <-rootCtx.Done():
	case err := <-serveErr:
		if err != nil {
			exitReason = "Web 服务异常退出: " + err.Error()
			logger.Error("Web 服务异常退出", logging.Err(err))
		} else {
			exitReason = "Web 服务已停止"
		}
	}

	logger.Info("开始优雅关闭", "reason", exitReason)
	shutdownTimeout := 30 * time.Second

	// a) 停止接收新请求
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("关闭 Web 服务超时", logging.Err(err))
	}
	cancelShutdown()

	// b) 停止调度，避免关停过程中触发新任务
	sch.Stop()

	// c) 取消并等待运行中的任务
	if n := mgr.ActiveCount(); n > 0 {
		logger.Info("正在取消运行中的任务", "count", n)
		mgr.CancelAll()
		if !mgr.Wait(shutdownTimeout) {
			logger.Warn("仍有任务未在超时内结束", "remaining", mgr.ActiveCount())
		}
	}

	// d) 关闭 rclone rcd
	rcloneStopCtx, cancelRclone := context.WithTimeout(context.Background(), cfg.Rclone.ShutdownTimeout.D()+10*time.Second)
	if err := sup.Stop(rcloneStopCtx); err != nil {
		logger.Warn("关闭 rclone rcd 时出错", logging.Err(err))
	}
	cancelRclone()

	logger.Info("CloudSync 已退出", "reason", exitReason)
	return nil
}

func orDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

// retentionText 把保留时长渲染成可读文本；0 为不限制。
func retentionText(d time.Duration) string {
	if d <= 0 {
		return "不限制"
	}
	return d.String()
}

func usage() {
	out := flag.CommandLine.Output()
	fmt.Fprintf(out, `CloudSync - 基于 rclone 的云端文件同步管理器

用法:
  cloudsync [选项]

  不指定 -config 时，按此顺序找配置文件：CLOUDSYNC_CONFIG -> 当前目录下的 config.yaml。
  都没有则只用内置默认值（+ 环境变量）启动。

选项:
`)
	flag.PrintDefaults()
	fmt.Fprintf(out, `
示例:
  cloudsync                                          # 当前目录有 config.yaml 就用它
  cloudsync -config /etc/cloudsync/config.yaml
  CLOUDSYNC_SERVER_PASSWORD=secret cloudsync -addr 127.0.0.1:8080 -log-level debug

常用环境变量:
  CLOUDSYNC_CONFIG                 配置文件路径（优先级高于当前目录的 config.yaml）
  CLOUDSYNC_SERVER_PASSWORD        管理端登录密码（必填，或用配置文件）
  CLOUDSYNC_RCLONE_PATH            rclone 可执行文件路径
  CLOUDSYNC_RCLONE_RC_ADDR         rcd 监听地址，默认 127.0.0.1:5572
  CLOUDSYNC_STORAGE_DSN            SQLite 文件路径
  CLOUDSYNC_SCHEDULER_TIMEZONE     调度时区，如 Asia/Shanghai

注意: 若使用 -no-rclone，请自行确保 rclone rcd 已在本机运行。
`)
}
