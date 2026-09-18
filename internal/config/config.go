// Package config 负责加载、校验并合并应用配置。
//
// 配置来源优先级（后者覆盖前者）：
//
//	内置默认值 -> YAML 配置文件 -> 环境变量 (CLOUDSYNC_*) -> 命令行参数
//
// 其中"YAML 配置文件"自己要读哪个文件，由 ResolvePath 决定（后者覆盖前者）：
//
//	-config 参数 -> CLOUDSYNC_CONFIG 环境变量 -> 当前目录下的 config.yaml（存在才用）
package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	// 把 IANA 时区数据库编进二进制。
	//
	// 必要性：Windows 上 time.platformZoneSources 为空（见 go/src/time/zoneinfo_windows.go），
	// LoadLocation 只能靠 ZONEINFO 环境变量或 $GOROOT/lib/time/zoneinfo.zip。而发布构建用了
	// -trimpath（可复现构建），它会让 runtime.GOROOT() 返回空字符串，于是 GOROOT 兜底也失效，
	// scheduler.timezone 一旦写成 "Asia/Shanghai" 就报 "unknown time zone"。
	// 精简容器（未装 tzdata）同理。
	//
	// 代价约 450KB，换来时区解析不依赖宿主环境。
	// 对应的回归防线见 config_test.go 的 TestTimezoneDatabaseIsEmbedded。
	_ "time/tzdata"

	"gopkg.in/yaml.v3"
)

// EnvPrefix 是所有环境变量的前缀。
const EnvPrefix = "CLOUDSYNC"

// DefaultFile 是未显式指定配置文件时自动探测的文件名，相对当前工作目录。
const DefaultFile = "config.yaml"

// 保留策略的取值范围。
const (
	// minRetentionInterval 是后台清理的最短间隔：太频繁会白白多出一堆全表扫描。
	minRetentionInterval = 5 * time.Minute
	// MinRunRetention 是允许设置的最短保留时长（低于它会把还在排查的记录删掉）。
	MinRunRetention = time.Hour
	// MaxRunRetention 是允许设置的最长保留时长（10 年）。
	MaxRunRetention = 240 * time.Hour * 365
)

// Duration 是对 time.Duration 的包装，使其可以从 YAML 中以 "30s"、"5m" 或整数秒解析。
type Duration time.Duration

// UnmarshalYAML 支持 `30s` / `5m` / `1h30m` / `7d` / `2w` 字符串以及纯整数（单位秒）。
//
// 额外支持 d/w 是因为保留时长这类配置用天/周描述更自然，而 time.ParseDuration
// 只认 h 及以下的单位，"7d" 会直接报 unknown unit。
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err == nil {
		if s == "" {
			*d = 0
			return nil
		}
		if n, err := strconv.Atoi(s); err == nil {
			*d = Duration(time.Duration(n) * time.Second)
			return nil
		}
		parsed, err := parseDuration(s)
		if err != nil {
			return fmt.Errorf("无效的时长 %q: %w", s, err)
		}
		*d = Duration(parsed)
		return nil
	}
	var n int64
	if err := value.Decode(&n); err != nil {
		return fmt.Errorf("无效的时长: %w", err)
	}
	*d = Duration(time.Duration(n) * time.Second)
	return nil
}

// MarshalYAML 以可读字符串输出。
func (d Duration) MarshalYAML() (any, error) {
	return time.Duration(d).String(), nil
}

// parseDuration 在 time.ParseDuration 之上补上 d（天）与 w（周）两个单位。
//
// 只接受 "数字+单位" 的形式；复合写法（如 "1d12h"）交给标准库时会失败，
// 保持报错比悄悄解析出错误结果好。
func parseDuration(s string) (time.Duration, error) {
	unit := s[len(s)-1:]
	num := s[:len(s)-1]
	switch unit {
	case "d", "w":
		n, err := strconv.ParseFloat(strings.TrimSpace(num), 64)
		if err != nil {
			return 0, fmt.Errorf("无效的时长 %q", s)
		}
		hours := n * 24
		if unit == "w" {
			hours *= 7
		}
		return time.Duration(hours * float64(time.Hour)), nil
	}
	return time.ParseDuration(s)
}

// D 返回标准库的 time.Duration。
func (d Duration) D() time.Duration { return time.Duration(d) }

// String 实现 fmt.Stringer。
func (d Duration) String() string { return time.Duration(d).String() }

// HumanizeDuration 把时长渲染成中文可读串，用于错误提示与界面展示。
//
// 直接用 time.Duration.String() 会得到 "87600h0m0s" 这种机器串，
// 出现在"保留时长最长为…"的报错里没人看得懂。
func HumanizeDuration(d time.Duration) string {
	if d <= 0 {
		return "不限制"
	}
	switch {
	case d >= 24*time.Hour && d%(24*time.Hour) == 0:
		return fmt.Sprintf("%d 天", int(d/(24*time.Hour)))
	case d >= time.Hour && d%time.Hour == 0:
		return fmt.Sprintf("%d 小时", int(d/time.Hour))
	case d >= time.Hour:
		return fmt.Sprintf("%.1f 小时", d.Hours())
	default:
		return fmt.Sprintf("%d 分钟", int(d/time.Minute))
	}
}

// Config 是应用总配置。
type Config struct {
	Server    ServerConfig    `yaml:"server"`
	Rclone    RcloneConfig    `yaml:"rclone"`
	Storage   StorageConfig   `yaml:"storage"`
	Scheduler SchedulerConfig `yaml:"scheduler"`
	Log       LogConfig       `yaml:"log"`
}

// ServerConfig 是 Web 管理端配置。
type ServerConfig struct {
	Addr          string   `yaml:"addr"`
	BasePath      string   `yaml:"base_path"`
	Username      string   `yaml:"username"`
	Password      string   `yaml:"password"`
	SessionSecret string   `yaml:"session_secret"`
	SessionTTL    Duration `yaml:"session_ttl"`
	ReadTimeout   Duration `yaml:"read_timeout"`
	WriteTimeout  Duration `yaml:"write_timeout"`
	// TrustedProxy 为 true 时信任 X-Forwarded-For 中的客户端 IP。
	TrustedProxy bool `yaml:"trusted_proxy"`
}

// RcloneConfig 描述 rclone rcd 子进程与 RC API 连接参数。
type RcloneConfig struct {
	// Path 是 rclone 可执行文件路径，可为绝对路径或 PATH 中的名字。
	Path string `yaml:"path"`
	// ConfigFile 对应 rclone 的 --config，为空时使用 rclone 默认位置。
	ConfigFile string `yaml:"config_file"`
	// RCAddr 是 rcd 监听地址，宿主程序据此访问 RC API。
	RCAddr string `yaml:"rc_addr"`
	RCUser string `yaml:"rc_user"`
	RCPass string `yaml:"rc_pass"`
	// RCNoAuth 为 true 时以 --rc-no-auth 启动，不校验 RC 认证。
	RCNoAuth bool `yaml:"rc_no_auth"`
	// ExtraArgs 追加到 rclone rcd 命令行的原始参数。
	ExtraArgs []string `yaml:"extra_args"`
	// GlobalOptions 在 rcd 就绪后通过 options/set 下发的全局传输参数。
	GlobalOptions map[string]any `yaml:"global_options"`

	// AutoStart 为 false 时不拉起 rcd，仅作为已运行实例的客户端使用。
	AutoStart bool `yaml:"auto_start"`
	// AutoRestart 为 true 时子进程意外退出会自动重启。
	AutoRestart  bool     `yaml:"auto_restart"`
	MaxRestarts  int      `yaml:"max_restarts"`
	RestartDelay Duration `yaml:"restart_backoff"`

	StartupTimeout  Duration `yaml:"startup_timeout"`
	ShutdownTimeout Duration `yaml:"shutdown_timeout"`

	// LogLevel 对应 rclone 的 --log-level。
	LogLevel string `yaml:"log_level"`
	// JournalSize 是内存中保留的 rclone 输出行数上限。
	JournalSize int `yaml:"journal_size"`
}

// StorageConfig 描述持久化配置。
type StorageConfig struct {
	// Driver 目前仅支持 sqlite。
	Driver string `yaml:"driver"`
	// DSN 为 SQLite 文件路径；":memory:" 表示内存库。
	DSN string `yaml:"dsn"`
	// HistoryLimit 是单个任务保留的运行记录条数上限，<=0 表示不限制。
	HistoryLimit int `yaml:"history_limit"`
	// RunRetention 是运行记录（含其中的 rclone 日志片段）的保留时长。
	//
	// 只清理已结束的记录；<=0 表示不限制。这里给的是"出厂默认值"，
	// 用户在设置页改过之后以数据库里的值为准，避免每次重启被配置覆盖回去。
	RunRetention Duration `yaml:"run_retention"`
	// RetentionInterval 是后台清理任务的检查间隔，默认 1h，最短 5m。
	RetentionInterval Duration `yaml:"retention_interval"`
}

// SchedulerConfig 描述定时调度配置。
type SchedulerConfig struct {
	Enabled bool `yaml:"enabled"`
	// Timezone 为 IANA 时区名，如 Asia/Shanghai；为空表示本地时区。
	Timezone string `yaml:"timezone"`
	// Seconds 为 true 时 cron 表达式支持 6 段（含秒）。
	Seconds bool `yaml:"seconds"`
	// MaxConcurrentRuns 是全局同时运行的任务数上限。
	MaxConcurrentRuns int `yaml:"max_concurrent_runs"`
	// DefaultTimeout 是任务未显式设置超时时的默认值，<=0 表示不限制。
	DefaultTimeout Duration `yaml:"default_timeout"`
	// SkipOverlap 为 true 时，同一任务上一轮未结束则跳过本次触发。
	SkipOverlap bool `yaml:"skip_overlap"`
}

// LogConfig 描述结构化日志配置。
type LogConfig struct {
	// Level 取值 debug/info/warn/error。
	Level string `yaml:"level"`
	// Format 取值 json/text。
	Format string `yaml:"format"`
	// File 非空时同时写入该文件（追加模式）。
	File string `yaml:"file"`
	// AddSource 为 true 时记录调用位置。
	AddSource bool `yaml:"add_source"`
}

// Default 返回内置默认配置。
func Default() *Config {
	return &Config{
		Server: ServerConfig{
			Addr:         "0.0.0.0:8080",
			BasePath:     "/",
			Username:     "admin",
			SessionTTL:   Duration(12 * time.Hour),
			ReadTimeout:  Duration(30 * time.Second),
			WriteTimeout: Duration(0),
		},
		Rclone: RcloneConfig{
			Path:            "rclone",
			RCAddr:          "127.0.0.1:5572",
			RCUser:          "admin",
			AutoStart:       true,
			AutoRestart:     true,
			MaxRestarts:     5,
			RestartDelay:    Duration(2 * time.Second),
			StartupTimeout:  Duration(30 * time.Second),
			ShutdownTimeout: Duration(15 * time.Second),
			LogLevel:        "INFO",
			JournalSize:     2000,
		},
		Storage: StorageConfig{
			Driver:       "sqlite",
			DSN:          "data/cloudsync.db",
			HistoryLimit: 5000,
		},
		Scheduler: SchedulerConfig{
			Enabled:           true,
			MaxConcurrentRuns: 4,
			// DefaultTimeout 刻意留成 0（不限制）。曾默认 24h，但大目录的首次同步
			// 动辄十几小时，被上限砍断时用户看到的是"跑了大半天白跑"，比卡住更难接受；
			// 而"别堆积"由 skip_overlap 与 cron 兜住，真卡死了还能手动取消。
			SkipOverlap: true,
		},
		Log: LogConfig{
			Level:  "info",
			Format: "json",
		},
	}
}

// ResolvePath 按优先级选出实际要读取的配置文件路径，空串表示"不读文件"
// （此时配置完全来自内置默认值 + 环境变量）。
//
//  1. 显式传入的 path（-config 参数，或调用方直接给的路径）
//  2. 环境变量 CLOUDSYNC_CONFIG
//  3. 当前工作目录下的 config.yaml —— 存在才用
//
// 故意只看**当前工作目录**，不看可执行文件所在目录：从别处启动时悄悄加载
// exe 旁边的 config.yaml，"到底读了哪个文件"会比"没找到配置"更难排查。
// 启动日志的 config_file 字段会写明最终用了哪个文件。
func ResolvePath(path string) string {
	if path != "" {
		return path
	}
	if env := os.Getenv(EnvPrefix + "_CONFIG"); env != "" {
		return env
	}
	// 存在才用。是目录、或探测本身失败（如无权访问当前目录）都当"没有"，
	// 不额外制造一个"起不来"的失败模式；但文件存在却读不出来时，
	// 下面 Load 里的 os.ReadFile 会明确报错，不会静默降级成默认值。
	if fi, err := os.Stat(DefaultFile); err == nil && fi.Mode().IsRegular() {
		return DefaultFile
	}
	return ""
}

// Load 读取配置文件、应用环境变量并完成校验。
//
// path 为 -config 的取值，可为空 —— 为空时按 ResolvePath 的优先级定位文件。
func Load(path string) (*Config, error) {
	cfg := Default()

	if resolved := ResolvePath(path); resolved != "" {
		raw, err := os.ReadFile(resolved)
		if err != nil {
			return nil, fmt.Errorf("读取配置文件 %s: %w", resolved, err)
		}
		dec := yaml.NewDecoder(strings.NewReader(string(raw)))
		dec.KnownFields(true)
		if err := dec.Decode(cfg); err != nil && !errors.Is(err, os.ErrClosed) {
			return nil, fmt.Errorf("解析配置文件 %s: %w", resolved, err)
		}
	}

	if err := applyEnv(cfg); err != nil {
		return nil, err
	}
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// applyEnv 覆盖受支持的环境变量。
func applyEnv(cfg *Config) error {
	str := func(key string, dst *string) {
		if v, ok := lookupEnv(key); ok {
			*dst = v
		}
	}
	boolean := func(key string, dst *bool) error {
		v, ok := lookupEnv(key)
		if !ok {
			return nil
		}
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("环境变量 %s 需要布尔值: %w", EnvPrefix+"_"+key, err)
		}
		*dst = b
		return nil
	}
	integer := func(key string, dst *int) error {
		v, ok := lookupEnv(key)
		if !ok {
			return nil
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("环境变量 %s 需要整数: %w", EnvPrefix+"_"+key, err)
		}
		*dst = n
		return nil
	}
	duration := func(key string, dst *Duration) error {
		v, ok := lookupEnv(key)
		if !ok {
			return nil
		}
		if n, err := strconv.Atoi(v); err == nil {
			*dst = Duration(time.Duration(n) * time.Second)
			return nil
		}
		d, err := parseDuration(v)
		if err != nil {
			return fmt.Errorf("环境变量 %s 需要时长: %w", EnvPrefix+"_"+key, err)
		}
		*dst = Duration(d)
		return nil
	}

	str("SERVER_ADDR", &cfg.Server.Addr)
	str("SERVER_BASE_PATH", &cfg.Server.BasePath)
	str("SERVER_USERNAME", &cfg.Server.Username)
	str("SERVER_PASSWORD", &cfg.Server.Password)
	str("SERVER_SESSION_SECRET", &cfg.Server.SessionSecret)

	str("RCLONE_PATH", &cfg.Rclone.Path)
	str("RCLONE_CONFIG_FILE", &cfg.Rclone.ConfigFile)
	str("RCLONE_RC_ADDR", &cfg.Rclone.RCAddr)
	str("RCLONE_RC_USER", &cfg.Rclone.RCUser)
	str("RCLONE_RC_PASS", &cfg.Rclone.RCPass)
	str("RCLONE_LOG_LEVEL", &cfg.Rclone.LogLevel)

	str("STORAGE_DRIVER", &cfg.Storage.Driver)
	str("STORAGE_DSN", &cfg.Storage.DSN)

	str("SCHEDULER_TIMEZONE", &cfg.Scheduler.Timezone)

	str("LOG_LEVEL", &cfg.Log.Level)
	str("LOG_FORMAT", &cfg.Log.Format)
	str("LOG_FILE", &cfg.Log.File)

	for _, fn := range []func() error{
		func() error { return boolean("SERVER_TRUSTED_PROXY", &cfg.Server.TrustedProxy) },
		func() error { return boolean("RCLONE_NO_AUTH", &cfg.Rclone.RCNoAuth) },
		func() error { return boolean("RCLONE_AUTO_START", &cfg.Rclone.AutoStart) },
		func() error { return boolean("RCLONE_AUTO_RESTART", &cfg.Rclone.AutoRestart) },
		func() error { return boolean("SCHEDULER_ENABLED", &cfg.Scheduler.Enabled) },
		func() error { return boolean("SCHEDULER_SECONDS", &cfg.Scheduler.Seconds) },
		func() error { return boolean("SCHEDULER_SKIP_OVERLAP", &cfg.Scheduler.SkipOverlap) },
		func() error { return integer("RCLONE_MAX_RESTARTS", &cfg.Rclone.MaxRestarts) },
		func() error { return integer("RCLONE_JOURNAL_SIZE", &cfg.Rclone.JournalSize) },
		func() error { return integer("STORAGE_HISTORY_LIMIT", &cfg.Storage.HistoryLimit) },
		func() error { return duration("STORAGE_RUN_RETENTION", &cfg.Storage.RunRetention) },
		func() error { return duration("STORAGE_RETENTION_INTERVAL", &cfg.Storage.RetentionInterval) },
		func() error { return integer("SCHEDULER_MAX_CONCURRENT_RUNS", &cfg.Scheduler.MaxConcurrentRuns) },
		func() error { return duration("RCLONE_STARTUP_TIMEOUT", &cfg.Rclone.StartupTimeout) },
		func() error { return duration("RCLONE_SHUTDOWN_TIMEOUT", &cfg.Rclone.ShutdownTimeout) },
		func() error { return duration("SCHEDULER_DEFAULT_TIMEOUT", &cfg.Scheduler.DefaultTimeout) },
	} {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

func lookupEnv(key string) (string, bool) {
	if v, ok := os.LookupEnv(EnvPrefix + "_" + key); ok && v != "" {
		return v, true
	}
	return "", false
}

// normalize 填充派生默认值并生成随机凭据。
func (cfg *Config) normalize() error {
	if cfg.Server.BasePath == "" {
		cfg.Server.BasePath = "/"
	}
	if !strings.HasPrefix(cfg.Server.BasePath, "/") {
		cfg.Server.BasePath = "/" + cfg.Server.BasePath
	}
	if !strings.HasSuffix(cfg.Server.BasePath, "/") {
		cfg.Server.BasePath += "/"
	}
	if cfg.Server.SessionSecret == "" {
		cfg.Server.SessionSecret = randomHex(32)
	}
	if cfg.Rclone.RCPass == "" && !cfg.Rclone.RCNoAuth {
		cfg.Rclone.RCPass = randomHex(16)
	}
	if cfg.Rclone.Path == "" {
		cfg.Rclone.Path = "rclone"
	}
	if cfg.Rclone.RestartDelay <= 0 {
		cfg.Rclone.RestartDelay = Duration(2 * time.Second)
	}
	if cfg.Rclone.JournalSize <= 0 {
		cfg.Rclone.JournalSize = 2000
	}
	if cfg.Rclone.LogLevel == "" {
		cfg.Rclone.LogLevel = "INFO"
	}
	if cfg.Storage.Driver == "" {
		cfg.Storage.Driver = "sqlite"
	}
	if cfg.Storage.DSN == "" {
		cfg.Storage.DSN = "data/cloudsync.db"
	}
	if cfg.Storage.RetentionInterval <= 0 {
		cfg.Storage.RetentionInterval = Duration(time.Hour)
	}
	if cfg.Storage.RetentionInterval.D() < minRetentionInterval {
		cfg.Storage.RetentionInterval = Duration(minRetentionInterval)
	}
	if cfg.Scheduler.MaxConcurrentRuns <= 0 {
		cfg.Scheduler.MaxConcurrentRuns = 1
	}
	if cfg.Scheduler.Timezone != "" {
		if _, err := time.LoadLocation(cfg.Scheduler.Timezone); err != nil {
			// 时区库已编入二进制，走到这里基本只可能是名字写错，
			// 因此补一句可用写法的提示，同时保留底层错误便于排查。
			return fmt.Errorf(
				"无效的时区 %q（请使用 IANA 名称，如 Asia/Shanghai、Europe/London、UTC）: %w",
				cfg.Scheduler.Timezone, err,
			)
		}
	}
	return cfg.Validate()
}

// Validate 校验关键字段。
func (cfg *Config) Validate() error {
	if cfg.Server.Addr == "" {
		return errors.New("server.addr 不能为空")
	}
	if cfg.Server.Username == "" {
		return errors.New("server.username 不能为空")
	}
	if cfg.Server.Password == "" {
		return errors.New("server.password 不能为空（可通过 CLOUDSYNC_SERVER_PASSWORD 设置）")
	}
	if !strings.EqualFold(cfg.Storage.Driver, "sqlite") {
		return fmt.Errorf("不支持的 storage.driver: %s", cfg.Storage.Driver)
	}
	if cfg.Storage.RunRetention < 0 {
		return errors.New("storage.run_retention 不能为负数（<=0 表示不限制）")
	}
	if d := cfg.Storage.RunRetention.D(); d > 0 && d < MinRunRetention {
		return fmt.Errorf("storage.run_retention 最短为 %s（当前 %s）", MinRunRetention, d)
	}
	if d := cfg.Storage.RunRetention.D(); d > MaxRunRetention {
		return fmt.Errorf("storage.run_retention 最长为 %s（当前 %s）", MaxRunRetention, d)
	}
	if cfg.Storage.RetentionInterval < 0 {
		return errors.New("storage.retention_interval 不能为负数")
	}
	if cfg.Rclone.RCAddr == "" {
		return errors.New("rclone.rc_addr 不能为空")
	}
	switch strings.ToLower(cfg.Log.Format) {
	case "json", "text":
	default:
		return fmt.Errorf("无效的 log.format: %s（可选 json/text）", cfg.Log.Format)
	}
	switch strings.ToLower(cfg.Log.Level) {
	case "debug", "info", "warn", "warning", "error":
	default:
		return fmt.Errorf("无效的 log.level: %s", cfg.Log.Level)
	}
	return nil
}

// EnsureDataDir 为 SQLite 的 DSN 创建父目录。
func (cfg *Config) EnsureDataDir() error {
	dsn := cfg.Storage.DSN
	if dsn == "" || dsn == ":memory:" || strings.HasPrefix(dsn, "file:") {
		return nil
	}
	dir := filepath.Dir(dsn)
	if dir == "." || dir == "" {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}

func randomHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand 失败属于不可恢复的环境问题，退化为固定前缀避免 panic。
		return strings.Repeat("0", n*2)
	}
	return hex.EncodeToString(buf)
}
