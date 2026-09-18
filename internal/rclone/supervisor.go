package rclone

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"cloudsync/internal/config"
	"cloudsync/internal/logging"
)

// State 是 rcd 子进程的状态。
type State string

// 子进程状态取值。
const (
	StateStopped  State = "stopped"
	StateStarting State = "starting"
	StateRunning  State = "running"
	StateRestart  State = "restarting"
	StateFailed   State = "failed"
)

// ErrExternalRestart 表示当前 rclone 由外部进程托管，程序无权重启它。
var ErrExternalRestart = errors.New("外部托管的 rclone 实例不支持重启")

// Status 是 rcd 托管状态的快照。
type Status struct {
	State         State      `json:"state"`
	Ready         bool       `json:"ready"`
	PID           int        `json:"pid"`
	Version       string     `json:"version"`
	Endpoint      string     `json:"endpoint"`
	Binary        string     `json:"binary"`
	ConfigFile    string     `json:"config_file,omitempty"`
	Restarts      int        `json:"restarts"`
	LastError     string     `json:"last_error,omitempty"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	UptimeSeconds float64    `json:"uptime_seconds"`
	AutoStart     bool       `json:"auto_start"`
	AutoRestart   bool       `json:"auto_restart"`
	JournalLines  int        `json:"journal_lines"`
	External      bool       `json:"external"`
}

// RuntimeInfo 是汇报给管理端的运行时信息。
type RuntimeInfo struct {
	Status
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	GoVersion string `json:"go_version"`
}

// Supervisor 负责启动、监控并回收 rclone rcd 子进程。
//
// 它同时充当 RC 客户端工厂：Client() 返回的客户端在子进程重启后依然可用
// （因为监听地址不变）。
type Supervisor struct {
	cfg     config.RcloneConfig
	logger  *slog.Logger
	client  *Client
	journal *logging.RingBuffer

	mu          sync.RWMutex
	state       State
	ready       bool
	cmd         *exec.Cmd
	exited      chan struct{}
	pid         int
	version     string
	startedAt   time.Time
	restarts    int
	lastErr     string
	external    bool
	supervising bool

	stopOnce   sync.Once
	stopped    chan struct{}
	superviseW sync.WaitGroup
	restartCh  chan struct{}
}

// NewSupervisor 创建托管器。
func NewSupervisor(cfg config.RcloneConfig, logger *slog.Logger) *Supervisor {
	if logger == nil {
		logger = slog.Default()
	}
	return &Supervisor{
		cfg:       cfg,
		logger:    logger.With("component", "rclone.supervisor"),
		client:    NewClient(cfg, logger),
		journal:   logging.NewRingBuffer(cfg.JournalSize),
		state:     StateStopped,
		stopped:   make(chan struct{}),
		restartCh: make(chan struct{}, 1),
	}
}

// Client 返回 RC 客户端。
func (s *Supervisor) Client() *Client { return s.client }

// Journal 返回 rclone 输出缓冲。
func (s *Supervisor) Journal() *logging.RingBuffer { return s.journal }

// Start 拉起 rcd 并等待其就绪。
//
// 当 cfg.AutoStart 为 false 时，仅探测已存在的 rcd 实例：探测成功则标记为
// external（不托管，退出时也不会关闭它），失败则返回错误。
func (s *Supervisor) Start(ctx context.Context) error {
	if !s.cfg.AutoStart {
		if err := s.WaitReady(ctx, s.cfg.StartupTimeout.D()); err != nil {
			return fmt.Errorf("未托管 rclone（rclone.auto_start=false），且无法连接已运行的 rcd %s: %w",
				s.client.Endpoint(), err)
		}
		s.mu.Lock()
		s.external = true
		s.mu.Unlock()
		s.setState(StateRunning, nil)
		s.logger.Info("已连接到外部 rclone rcd 实例", "endpoint", s.client.Endpoint(), "version", s.versionLocked())
		return nil
	}

	if err := s.spawn(); err != nil {
		return err
	}
	if err := s.WaitReady(ctx, s.cfg.StartupTimeout.D()); err != nil {
		s.killProcess()
		s.setState(StateFailed, err)
		return err
	}

	if len(s.cfg.GlobalOptions) > 0 {
		if err := s.client.SetOptions(ctx, "main", s.cfg.GlobalOptions); err != nil {
			// 全局选项下发失败不阻断启动，但要让用户可见。
			s.logger.Warn("下发 rclone 全局选项失败", logging.Err(err))
		} else {
			s.logger.Info("已下发 rclone 全局选项", "options", s.cfg.GlobalOptions)
		}
	}

	s.ensureSupervise()
	return nil
}

// buildArgs 组装 rclone rcd 命令行参数。
func (s *Supervisor) buildArgs() []string {
	args := []string{
		"rcd",
		"--rc-addr", s.cfg.RCAddr,
		"--log-level", s.cfg.LogLevel,
	}
	if s.cfg.ConfigFile != "" {
		args = append(args, "--config", s.cfg.ConfigFile)
	}
	if s.cfg.RCNoAuth {
		args = append(args, "--rc-no-auth")
	} else {
		args = append(args, "--rc-user", s.cfg.RCUser, "--rc-pass", s.cfg.RCPass)
	}
	// 让已完成的 job 保留 1 小时，避免长任务结束后状态立即被回收。
	args = append(args, "--rc-job-expire-duration", "1h", "--rc-job-expire-interval", "5m")
	args = append(args, s.cfg.ExtraArgs...)
	return args
}

// spawn 启动子进程。
func (s *Supervisor) spawn() error {
	args := s.buildArgs()
	binary := resolveBinary(s.cfg)

	s.setState(StateStarting, nil)

	cmd := exec.Command(binary, args...)
	cmd.Env = os.Environ()
	// 子进程输出统一进入 journal，同时以 debug 级别透传到宿主日志。
	cmd.Stdout = io.MultiWriter(s.journal, &logBridge{logger: s.logger})
	cmd.Stderr = cmd.Stdout
	cmd.Stdin = nil

	prepareCommand(cmd)

	if err := cmd.Start(); err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return fmt.Errorf("找不到 rclone 可执行文件 %q，请设置 rclone.path 或将其加入 PATH: %w", binary, err)
		}
		return fmt.Errorf("启动 rclone rcd 失败 (%s %s): %w", binary, strings.Join(args, " "), err)
	}

	exited := make(chan struct{})
	s.mu.Lock()
	s.cmd = cmd
	s.exited = exited
	s.pid = cmd.Process.Pid
	s.startedAt = time.Now().UTC()
	s.mu.Unlock()

	if err := afterStart(cmd); err != nil {
		s.logger.Warn("进程回收策略降级", logging.Err(err))
	}

	s.logger.Info("rclone rcd 已启动",
		"pid", cmd.Process.Pid,
		"binary", binary,
		"rc_addr", s.cfg.RCAddr,
		"args", args,
	)

	go s.reap(cmd, exited)
	return nil
}

// reap 等待子进程退出，并在需要时触发重启。
func (s *Supervisor) reap(cmd *exec.Cmd, exited chan struct{}) {
	waitErr := cmd.Wait()
	releaseCommand(cmd)

	s.mu.Lock()
	if s.cmd != cmd {
		// 已被更新一轮的进程取代，忽略本次退出。
		s.mu.Unlock()
		return
	}
	close(exited)
	uptime := time.Since(s.startedAt)
	s.cmd = nil
	s.exited = nil
	s.pid = 0
	s.ready = false
	external := s.external
	s.mu.Unlock()

	if external {
		return
	}

	select {
	case <-s.stopped:
		s.logger.Info("rclone rcd 已退出", "uptime", uptime.Round(time.Millisecond))
		s.setState(StateStopped, nil)
		return
	default:
	}

	if waitErr != nil {
		s.logger.Error("rclone rcd 异常退出", "uptime", uptime.Round(time.Millisecond), logging.Err(waitErr))
		s.setState(StateFailed, waitErr)
	} else {
		s.logger.Warn("rclone rcd 意外退出", "uptime", uptime.Round(time.Millisecond))
		s.setState(StateFailed, nil)
	}

	if !s.cfg.AutoRestart {
		return
	}
	select {
	case s.restartCh <- struct{}{}:
	default:
	}
}

// ensureSupervise 保证监管 goroutine 在运行。
func (s *Supervisor) ensureSupervise() {
	s.mu.Lock()
	if s.supervising {
		s.mu.Unlock()
		return
	}
	s.supervising = true
	s.superviseW.Add(1)
	s.mu.Unlock()
	go s.supervise()
}

// supervise 在子进程退出后按退避策略重启，直到调用方要求停止。
func (s *Supervisor) supervise() {
	defer func() {
		s.mu.Lock()
		s.supervising = false
		s.mu.Unlock()
		s.superviseW.Done()
	}()

	for {
		select {
		case <-s.stopped:
			return
		case <-s.restartCh:
		}

		// 稳定运行超过 2 分钟则重置重启计数。
		s.mu.RLock()
		stable := !s.startedAt.IsZero() && time.Since(s.startedAt) > 2*time.Minute
		s.mu.RUnlock()
		if stable {
			s.mu.Lock()
			s.restarts = 0
			s.mu.Unlock()
		}

		s.mu.Lock()
		if s.restarts >= s.cfg.MaxRestarts {
			msg := fmt.Sprintf("连续重启 %d 次后放弃，请检查 rclone 可执行文件与配置", s.restarts)
			s.lastErr = msg
			s.mu.Unlock()
			s.setState(StateFailed, errors.New(msg))
			s.logger.Error("rclone rcd 重启次数达到上限，停止自动重启", "max_restarts", s.cfg.MaxRestarts)
			return
		}
		s.restarts++
		attempt := s.restarts
		s.mu.Unlock()

		delay := s.cfg.RestartDelay.D() * time.Duration(attempt)
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}
		s.setState(StateRestart, nil)
		s.logger.Warn("准备重启 rclone rcd", "attempt", attempt, "delay", delay.String())

		timer := time.NewTimer(delay)
		select {
		case <-s.stopped:
			timer.Stop()
			return
		case <-timer.C:
		}

		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.StartupTimeout.D())
		err := s.spawn()
		if err == nil {
			err = s.WaitReady(ctx, s.cfg.StartupTimeout.D())
		}
		cancel()
		if err != nil {
			s.logger.Error("重启 rclone rcd 失败", "attempt", attempt, logging.Err(err))
			s.setState(StateFailed, err)
			select {
			case s.restartCh <- struct{}{}:
			default:
			}
			continue
		}
		s.logger.Info("rclone rcd 重启成功", "attempt", attempt)
		s.setState(StateRunning, nil)
	}
}

// WaitReady 轮询 rc/noop 直到 rcd 就绪或超时。
func (s *Supervisor) WaitReady(ctx context.Context, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	deadline := time.Now().Add(timeout)
	backoff := 50 * time.Millisecond
	var lastErr error

	for {
		probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		lastErr = s.client.Ping(probeCtx)
		cancel()
		if lastErr == nil {
			version := ""
			vctx, vcancel := context.WithTimeout(ctx, 5*time.Second)
			if v, err := s.client.Version(vctx); err == nil {
				version = v.Version
			}
			vcancel()

			s.mu.Lock()
			s.version = version
			s.lastErr = ""
			s.ready = true
			s.mu.Unlock()
			s.logger.Info("rclone rcd 就绪", "endpoint", s.client.Endpoint(), "version", version)
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("等待 rclone rcd 就绪超时 (%s): %w", timeout, lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < time.Second {
			backoff *= 2
		}
	}
}

func (s *Supervisor) versionLocked() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.version
}

func (s *Supervisor) setState(st State, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = st
	switch st {
	case StateRunning:
		s.ready = true
	case StateStarting, StateFailed, StateStopped:
		s.ready = false
	}
	if err != nil {
		s.lastErr = err.Error()
	}
}

// Stop 优雅关闭 rcd 子进程。
//
// 顺序：core/quit -> 等待自行退出 -> SIGTERM/强杀兜底 -> 等待监管协程收尾。
func (s *Supervisor) Stop(ctx context.Context) error {
	s.stopOnce.Do(func() { close(s.stopped) })

	s.mu.RLock()
	external := s.external
	pid := s.pid
	s.mu.RUnlock()

	if external {
		s.logger.Info("外部 rclone rcd 不受本程序托管，跳过关闭")
		return nil
	}
	if pid == 0 {
		return nil
	}

	timeout := s.cfg.ShutdownTimeout.D()
	if timeout <= 0 {
		timeout = 15 * time.Second
	}

	s.logger.Info("正在关闭 rclone rcd", "pid", pid, "timeout", timeout.String())

	quitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err := s.client.Quit(quitCtx)
	cancel()
	if err != nil {
		s.logger.Debug("core/quit 调用失败，将直接结束进程", logging.Err(err))
	}

	if s.waitExit(timeout) {
		s.logger.Info("rclone rcd 已优雅退出", "pid", pid)
		s.setState(StateStopped, nil)
		return nil
	}

	// core/quit 无效时先尝试 SIGTERM，再强杀。
	s.mu.RLock()
	cmd := s.cmd
	s.mu.RUnlock()
	if graceful(cmd, minDuration(timeout, 5*time.Second)) {
		s.logger.Info("rclone rcd 已通过信号退出", "pid", pid)
		s.setState(StateStopped, nil)
		return nil
	}

	s.logger.Warn("rclone rcd 未在超时内退出，强制结束", "pid", pid)
	s.killProcess()
	if !s.waitExit(5 * time.Second) {
		s.logger.Error("rclone rcd 强制结束后仍未回收，可能有残留进程", "pid", pid)
	}
	s.setState(StateStopped, nil)
	return nil
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// waitExit 等待当前子进程退出。
func (s *Supervisor) waitExit(timeout time.Duration) bool {
	s.mu.RLock()
	ch := s.exited
	s.mu.RUnlock()
	if ch == nil {
		return true
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ch:
		return true
	case <-timer.C:
		return false
	}
}

// killProcess 强制结束子进程。
func (s *Supervisor) killProcess() {
	s.mu.RLock()
	cmd := s.cmd
	s.mu.RUnlock()
	if cmd == nil || cmd.Process == nil {
		return
	}
	if err := forceKill(cmd); err != nil {
		s.logger.Warn("强制结束 rclone 进程失败", "pid", cmd.Process.Pid, logging.Err(err))
	}
}

// Restart 主动重启 rcd。
func (s *Supervisor) Restart(ctx context.Context) error {
	s.mu.RLock()
	external := s.external
	s.mu.RUnlock()
	if external {
		return ErrExternalRestart
	}

	s.logger.Info("收到手动重启 rclone 请求")
	s.mu.Lock()
	s.restarts = 0
	s.mu.Unlock()

	s.killProcess()
	s.waitExit(10 * time.Second)

	// 若监管协程此前已因达到重启上限而退出，这里重新拉起。
	s.ensureSupervise()

	spawnCtx, cancel := context.WithTimeout(ctx, s.cfg.StartupTimeout.D())
	defer cancel()
	if err := s.spawn(); err != nil {
		return err
	}
	if err := s.WaitReady(spawnCtx, s.cfg.StartupTimeout.D()); err != nil {
		return err
	}
	s.setState(StateRunning, nil)
	return nil
}

// Status 返回当前托管状态快照。
func (s *Supervisor) Status() Status {
	s.mu.RLock()
	defer s.mu.RUnlock()

	st := Status{
		State:        s.state,
		Ready:        s.ready,
		PID:          s.pid,
		Version:      s.version,
		Endpoint:     s.client.Endpoint(),
		Binary:       s.cfg.Path,
		ConfigFile:   s.cfg.ConfigFile,
		Restarts:     s.restarts,
		LastError:    s.lastErr,
		AutoStart:    s.cfg.AutoStart,
		AutoRestart:  s.cfg.AutoRestart,
		JournalLines: s.journal.Len(),
		External:     s.external,
	}
	if !s.startedAt.IsZero() {
		t := s.startedAt
		st.StartedAt = &t
		if s.pid != 0 {
			st.UptimeSeconds = time.Since(t).Seconds()
		}
	}
	if st.State == "" {
		st.State = StateStopped
	}
	return st
}

// Info 返回含运行环境的状态信息。
func (s *Supervisor) Info() RuntimeInfo {
	return RuntimeInfo{
		Status:    s.Status(),
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
		GoVersion: runtime.Version(),
	}
}

// logBridge 把子进程输出以 debug 级别写入宿主结构化日志。
type logBridge struct {
	logger *slog.Logger
	buf    []byte
}

func (b *logBridge) Write(p []byte) (int, error) {
	b.buf = append(b.buf, p...)
	for {
		idx := bytes.IndexByte(b.buf, '\n')
		if idx < 0 {
			break
		}
		line := strings.TrimRight(string(b.buf[:idx]), "\r")
		b.buf = b.buf[idx+1:]
		if strings.TrimSpace(line) != "" {
			b.logger.Debug("rclone: " + line)
		}
	}
	if len(b.buf) > 1<<20 {
		b.buf = nil
	}
	return len(p), nil
}
