package config

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestDefaultAndValidate(t *testing.T) {
	cfg := Default()
	cfg.Server.Password = "secret"
	if err := cfg.normalize(); err != nil {
		t.Fatalf("normalize 失败: %v", err)
	}
	if cfg.Storage.Driver != "sqlite" {
		t.Fatalf("默认驱动应为 sqlite，得到 %q", cfg.Storage.Driver)
	}
	if cfg.Rclone.RCPass == "" {
		t.Fatal("未禁用认证时应自动生成 rc 密码")
	}
	if cfg.Server.BasePath != "/" {
		t.Fatalf("默认 base_path 应为 /，得到 %q", cfg.Server.BasePath)
	}
	if !cfg.Rclone.AutoStart {
		t.Fatal("默认应自动拉起 rclone")
	}
}

// TestDefaultTimeoutIsUnlimited 锁住「任务默认不超时」。
//
// 曾经内置 24h。用户明确要求默认不限时，理由是超大目录的首次同步常超过一天，
// 被上限砍断等于白跑一轮。这条用例拦住以后有人"顺手"加回一个看起来更安全的上限。
func TestDefaultTimeoutIsUnlimited(t *testing.T) {
	cfg := Default()
	if got := cfg.Scheduler.DefaultTimeout.D(); got != 0 {
		t.Fatalf("任务默认超时应为不限制（0），得到 %v", got)
	}
	cfg.Server.Password = "p"
	if err := cfg.normalize(); err != nil {
		t.Fatalf("normalize 失败: %v", err)
	}
	if got := cfg.Scheduler.DefaultTimeout.D(); got != 0 {
		t.Fatalf("normalize 不应给 default_timeout 填默认值，得到 %v", got)
	}
	// 显式配了有限值时要原样保留，否则"能配但配了没用"比不能配更坑。
	cfg.Scheduler.DefaultTimeout = Duration(90 * time.Minute)
	if err := cfg.normalize(); err != nil {
		t.Fatalf("normalize 失败: %v", err)
	}
	if got := cfg.Scheduler.DefaultTimeout.D(); got != 90*time.Minute {
		t.Fatalf("显式配置的超时应被保留，得到 %v", got)
	}
}

func TestBasePathNormalization(t *testing.T) {
	cases := map[string]string{
		"":              "/",
		"cloudsync":     "/cloudsync/",
		"/cloudsync":    "/cloudsync/",
		"/cloudsync/":   "/cloudsync/",
		"//nested/path": "//nested/path/",
	}
	for in, want := range cases {
		cfg := Default()
		cfg.Server.Password = "p"
		cfg.Server.BasePath = in
		if err := cfg.normalize(); err != nil {
			t.Fatalf("normalize(%q) 失败: %v", in, err)
		}
		if cfg.Server.BasePath != want {
			t.Errorf("base_path %q → 期望 %q，得到 %q", in, want, cfg.Server.BasePath)
		}
	}
}

func TestLoadYAMLAndEnvOverride(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
server:
  addr: 127.0.0.1:9090
  username: admin
  password: fromfile
  session_ttl: 2h
rclone:
  rc_addr: 127.0.0.1:6000
  path: /opt/rclone
  startup_timeout: 12s
  global_options:
    transfers: 8
storage:
  driver: sqlite
  dsn: ` + filepath.ToSlash(filepath.Join(dir, "db.sqlite")) + `
scheduler:
  enabled: true
  timezone: Asia/Shanghai
  seconds: true
  max_concurrent_runs: 3
  default_timeout: 90m
log:
  level: debug
  format: text
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	// 环境变量优先级高于文件。
	t.Setenv(EnvPrefix+"_SERVER_PASSWORD", "fromenv")
	t.Setenv(EnvPrefix+"_RCLONE_RC_ADDR", "127.0.0.1:7000")
	t.Setenv(EnvPrefix+"_SCHEDULER_MAX_CONCURRENT_RUNS", "7")
	t.Setenv(EnvPrefix+"_RCLONE_AUTO_RESTART", "false")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}

	if cfg.Server.Password != "fromenv" {
		t.Errorf("环境变量未覆盖密码，得到 %q", cfg.Server.Password)
	}
	if cfg.Server.Addr != "127.0.0.1:9090" {
		t.Errorf("addr 未从文件加载，得到 %q", cfg.Server.Addr)
	}
	if cfg.Server.SessionTTL.D() != 2*time.Hour {
		t.Errorf("session_ttl 解析错误: %v", cfg.Server.SessionTTL.D())
	}
	if cfg.Rclone.RCAddr != "127.0.0.1:7000" {
		t.Errorf("rc_addr 环境变量覆盖失败: %q", cfg.Rclone.RCAddr)
	}
	if cfg.Rclone.Path != "/opt/rclone" {
		t.Errorf("rclone.path 未从文件加载: %q", cfg.Rclone.Path)
	}
	if cfg.Rclone.StartupTimeout.D() != 12*time.Second {
		t.Errorf("startup_timeout 解析错误: %v", cfg.Rclone.StartupTimeout.D())
	}
	if cfg.Rclone.AutoRestart {
		t.Error("auto_restart 应被环境变量设为 false")
	}
	if got := cfg.Rclone.GlobalOptions["transfers"]; got != 8 {
		t.Errorf("global_options.transfers 期望 8，得到 %v", got)
	}
	if !cfg.Scheduler.Seconds {
		t.Error("scheduler.seconds 应为 true")
	}
	if cfg.Scheduler.MaxConcurrentRuns != 7 {
		t.Errorf("max_concurrent_runs 期望 7，得到 %d", cfg.Scheduler.MaxConcurrentRuns)
	}
	if cfg.Scheduler.DefaultTimeout.D() != 90*time.Minute {
		t.Errorf("default_timeout 解析错误: %v", cfg.Scheduler.DefaultTimeout.D())
	}
	if cfg.Log.Format != "text" || cfg.Log.Level != "debug" {
		t.Errorf("日志配置错误: %+v", cfg.Log)
	}
}

// TestResolvePathPriority 锁定配置文件定位优先级：
// -config > CLOUDSYNC_CONFIG > 当前目录的 config.yaml > 不读文件。
func TestResolvePathPriority(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, DefaultFile),
		[]byte("server:\n  addr: 127.0.0.1:1111\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	envPath := filepath.Join(dir, "fromenv.yaml")

	t.Run("显式 path 原样返回，压过环境变量与默认文件", func(t *testing.T) {
		t.Setenv(EnvPrefix+"_CONFIG", envPath)
		t.Chdir(dir)
		explicit := filepath.Join(dir, "explicit.yaml")
		if got := ResolvePath(explicit); got != explicit {
			t.Errorf("ResolvePath(%q) = %q，应原样返回", explicit, got)
		}
	})

	t.Run("环境变量压过当前目录的默认文件", func(t *testing.T) {
		t.Setenv(EnvPrefix+"_CONFIG", envPath)
		t.Chdir(dir)
		if got := ResolvePath(""); got != envPath {
			t.Errorf(`ResolvePath("") = %q，应为 %q`, got, envPath)
		}
	})

	t.Run("都没有时用当前目录的 config.yaml", func(t *testing.T) {
		t.Setenv(EnvPrefix+"_CONFIG", "")
		t.Chdir(dir)
		if got := ResolvePath(""); got != DefaultFile {
			t.Errorf(`ResolvePath("") = %q，应为 %q`, got, DefaultFile)
		}
	})

	t.Run("当前目录没有就返回空串（不读文件）", func(t *testing.T) {
		t.Setenv(EnvPrefix+"_CONFIG", "")
		t.Chdir(t.TempDir())
		if got := ResolvePath(""); got != "" {
			t.Errorf(`ResolvePath("") = %q，应为空串`, got)
		}
	})

	t.Run("同名目录不算配置文件", func(t *testing.T) {
		t.Setenv(EnvPrefix+"_CONFIG", "")
		d := t.TempDir()
		if err := os.Mkdir(filepath.Join(d, DefaultFile), 0o755); err != nil {
			t.Fatal(err)
		}
		t.Chdir(d)
		if got := ResolvePath(""); got != "" {
			t.Errorf(`ResolvePath("") = %q，目录不该被当成配置文件`, got)
		}
	})
}

// TestLoadAutoDiscoversCwdConfig 是上面那条规则真正生效的端到端防线：
// 完全不传路径，配置必须来自当前目录的 config.yaml。
func TestLoadAutoDiscoversCwdConfig(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, DefaultFile),
		[]byte("server:\n  addr: 127.0.0.1:12345\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvPrefix+"_CONFIG", "")
	// server.password 是必填项（Validate 会拒），与本次改动无关，这里显式给一个。
	t.Setenv(EnvPrefix+"_SERVER_PASSWORD", "testpw")
	t.Chdir(dir)

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load(\"\") 失败: %v", err)
	}
	if cfg.Server.Addr != "127.0.0.1:12345" {
		t.Errorf("没有自动加载当前目录的 config.yaml，addr = %q", cfg.Server.Addr)
	}
}

// TestLoadWithoutAnyFileUsesDefaults 确保新增的自动探测没有破坏
// "什么都没有就用内置默认值" 这条既有行为。
func TestLoadWithoutAnyFileUsesDefaults(t *testing.T) {
	t.Setenv(EnvPrefix+"_CONFIG", "")
	t.Setenv(EnvPrefix+"_SERVER_PASSWORD", "testpw")
	t.Chdir(t.TempDir()) // 空目录，没有 config.yaml

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load(\"\") 失败: %v", err)
	}
	if want := Default().Server.Addr; cfg.Server.Addr != want {
		t.Errorf("无配置文件时应回落内置默认值，addr = %q，期望 %q", cfg.Server.Addr, want)
	}
}

func TestLoadRejectsUnknownField(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte("server:\n  passwrod: typo\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("拼写错误的字段应当被拒绝")
	}
}

func TestValidateRejectsBadValues(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"空密码", func(c *Config) { c.Server.Password = "" }},
		{"空地址", func(c *Config) { c.Server.Addr = "" }},
		{"未知驱动", func(c *Config) { c.Storage.Driver = "mysql" }},
		{"非法日志格式", func(c *Config) { c.Log.Format = "xml" }},
		{"非法日志级别", func(c *Config) { c.Log.Level = "trace" }},
		{"非法时区", func(c *Config) { c.Scheduler.Timezone = "Mars/Olympus" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Server.Password = "p"
			tt.mutate(cfg)
			if err := cfg.normalize(); err == nil {
				t.Fatalf("%s 应当校验失败", tt.name)
			}
		})
	}
}

// 回归测试：IANA 时区数据库必须编入二进制。
//
// 背景：发布构建用了 -trimpath，它会让 runtime.GOROOT() 返回空字符串，
// 于是 LoadLocation 的 $GOROOT/lib/time/zoneinfo.zip 兜底失效。而 Windows 上
// time.platformZoneSources 为空（不读系统时区库），结果 scheduler.timezone
// 写 "Asia/Shanghai" 就会直接报 "unknown time zone"，进程无法启动。
//
// 复现方式说明：这里必须用「子进程 + 预先设好 GOROOT」来模拟，不能在本进程里
// os.Setenv("GOROOT", ...) —— runtime.GOROOT() 读的是运行时启动时捕获的环境，
// 运行中改 os 包的 environ 对它无效（已实测）。
//
// 平台差异：Windows 无系统时区库，本用例是真正的防线；Linux/macOS 自带
// /usr/share/zoneinfo，会退化为冒烟测试。
func TestTimezoneDatabaseIsEmbedded(t *testing.T) {
	const probeEnv = "CLOUDSYNC_TZ_PROBE"

	if os.Getenv(probeEnv) == "1" {
		// 子进程分支：此时 GOROOT 已指向不存在的路径，只能靠编入的时区库。
		if _, err := time.LoadLocation("Asia/Shanghai"); err != nil {
			fmt.Fprintf(os.Stderr, "时区解析失败: %v\n", err)
			os.Exit(3)
		}
		os.Exit(0)
	}

	bogusGoroot := filepath.Join(t.TempDir(), "nonexistent-goroot")
	cmd := exec.Command(os.Args[0], "-test.run=^TestTimezoneDatabaseIsEmbedded$")
	cmd.Env = append(os.Environ(), probeEnv+"=1", "GOROOT="+bogusGoroot)
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		t.Fatalf("GOROOT 不可用时 IANA 时区无法解析（检查 internal/config 是否仍 import _ \"time/tzdata\"）: %v", err)
	}
	if _, err := time.LoadLocation("Asia/Shanghai"); err != nil {
		t.Fatalf("Asia/Shanghai 应当能解析: %v", err)
	}
}

// 合法的 IANA 时区应当通过校验；这条用例覆盖用户实际使用的配置写法。
func TestNormalizeAcceptsIANATimezone(t *testing.T) {
	cfg := Default()
	cfg.Server.Password = "p"
	cfg.Scheduler.Timezone = "Asia/Shanghai"
	if err := cfg.normalize(); err != nil {
		t.Fatalf("Asia/Shanghai 应当被接受: %v", err)
	}
}

func TestDurationUnmarshal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "d.yaml")
	// 同时验证字符串时长与整数秒。
	content := `
server:
  password: p
rclone:
  startup_timeout: 45s
  shutdown_timeout: 20
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Rclone.StartupTimeout.D() != 45*time.Second {
		t.Errorf("startup_timeout 期望 45s，得到 %v", cfg.Rclone.StartupTimeout.D())
	}
	if cfg.Rclone.ShutdownTimeout.D() != 20*time.Second {
		t.Errorf("整数秒应解析为 20s，得到 %v", cfg.Rclone.ShutdownTimeout.D())
	}
}

// TestDurationSupportsDayAndWeekUnits：保留时长用天/周描述最自然
// （run_retention: "30d"），而 time.ParseDuration 只认 h 及以下单位。
func TestDurationSupportsDayAndWeekUnits(t *testing.T) {
	cases := []struct {
		raw  string
		want time.Duration
	}{
		{"7d", 7 * 24 * time.Hour},
		{"30d", 30 * 24 * time.Hour},
		{"2w", 14 * 24 * time.Hour},
		{"12h", 12 * time.Hour},
		{"90m", 90 * time.Minute},
		{"720", 720 * time.Second},
	}
	for _, c := range cases {
		t.Run(c.raw, func(t *testing.T) {
			var d Duration
			node := &yaml.Node{Kind: yaml.ScalarNode, Value: c.raw}
			if err := d.UnmarshalYAML(node); err != nil {
				t.Fatalf("解析 %q 失败: %v", c.raw, err)
			}
			if d.D() != c.want {
				t.Errorf("解析 %q 期望 %v，得到 %v", c.raw, c.want, d.D())
			}
		})
	}

	for _, bad := range []string{"x d", "", "abc"} {
		if bad == "" {
			continue // 空串合法，语义为 0
		}
		var d Duration
		node := &yaml.Node{Kind: yaml.ScalarNode, Value: bad}
		if err := d.UnmarshalYAML(node); err == nil {
			t.Errorf("%q 应当解析失败", bad)
		}
	}
}

// TestStorageRetentionValidation 覆盖保留策略的边界：负值、过短、过长都应报错，
// 0（不限）与常规值应通过。
func TestStorageRetentionValidation(t *testing.T) {
	ok := []time.Duration{0, time.Hour, 30 * 24 * time.Hour, MaxRunRetention}
	for _, d := range ok {
		cfg := Default()
		cfg.Server.Password = "p"
		cfg.Storage.RunRetention = Duration(d)
		if err := cfg.normalize(); err != nil {
			t.Errorf("保留时长 %v 应当合法: %v", d, err)
		}
	}

	bad := map[string]time.Duration{
		"负数":    -time.Hour,
		"短于1小时": 30 * time.Minute,
		"超过上限":  11 * 365 * 24 * time.Hour,
	}
	for name, d := range bad {
		cfg := Default()
		cfg.Server.Password = "p"
		cfg.Storage.RunRetention = Duration(d)
		if err := cfg.normalize(); err == nil {
			t.Errorf("%s（%v）应当校验失败", name, d)
		}
	}

	// 清理间隔过短时应被抬到下限，而不是报错。
	cfg := Default()
	cfg.Server.Password = "p"
	cfg.Storage.RetentionInterval = Duration(time.Minute)
	if err := cfg.normalize(); err != nil {
		t.Fatalf("清理间隔过短应被修正而非报错: %v", err)
	}
	if cfg.Storage.RetentionInterval.D() != 5*time.Minute {
		t.Errorf("清理间隔应被抬到 5m，得到 %v", cfg.Storage.RetentionInterval.D())
	}
}
