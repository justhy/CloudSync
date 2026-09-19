package rclone

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
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
	// Adopted 为 true 表示 RC 地址上本来就有 rclone rcd（不是本进程拉起的），
	// 本程序接管了它：任务照常执行，但重启/退出只能通过 core/quit 请它自己退出。
	Adopted bool `json:"adopted"`
	// Notice 是给用户看的一句话说明（如"接管了既有的 rcd"），没有特殊情况时为空。
	Notice string `json:"notice,omitempty"`
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

	mu        sync.RWMutex
	state     State
	ready     bool
	cmd       *exec.Cmd
	exited    chan struct{}
	pid       int
	version   string
	startedAt time.Time
	restarts  int
	lastErr   string
	external  bool
	adopted   bool
	notice    string
	// blocked 非空表示自动重启已被判定为无意义（如 RC 端口被别的进程占着），
	// 内容就是给用户看的原因。手动 Restart 会清空它。
	blocked     string
	supervising bool

	// opMu 串行化"停止 / 拉起实例"这类会改变 RC 地址占用者的操作。
	//
	// 同一个 RC 地址上不可能同时跑两个 rclone：后拉起的那个必然 bind 失败
	// （bind: Only one usage of each socket address），而它一退出又会被本进程记成
	// 一次"启动失败"——用户看到的就是「点了重启，还是失败」。手动重启与自动重启
	// 是两条独立的路，谁都不该在对方刚拉起实例之后再拉一个，所以必须互斥。
	opMu sync.Mutex

	stopOnce sync.Once
	stopped  chan struct{}

	// stopIntent 置位表示"接下来这次子进程退出是我们自己造成的"（Stop / Restart
	// 正在结束实例），reap 据此**不再排自动重启**——那次拉起本来就要由我们自己完成。
	//
	// 不做这个标记的话：Restart 亲手杀掉子进程 → reap 排一次自动重启 → 监管协程
	// 把它记成一次自动重启，于是用户刚点完重启、界面显示成功，重启计数却从 0 变成 1，
	// max_restarts 的额度凭空少一次（这就是"点了重启计数还是不为 0"的原因）。
	// spawn 会把它复位：新拉起的子进程之后若再退出，就是真的故障，必须照常自动重启。
	stopIntent atomic.Bool

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
//
// 当 cfg.AutoStart 为 true 时，**先看 RC 地址上是不是已经有一个 rcd**：
// 若有，则直接拉起自己的实例必然 bind 失败，而紧随其后的 WaitReady 又会被
// 对方的应答骗过（它是按地址探测的），于是出现"状态显示就绪、pid 却早已退出、
// 重启按钮永远失败"的假成功。因此这里把"接管"做成显式路径。
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

	probeCtx, cancelProbe := context.WithTimeout(ctx, 5*time.Second)
	verdict, occ, occupiedBy := s.probeAddress(probeCtx)
	cancelProbe()

	switch verdict {
	case addrOccupied:
		s.adopt(occ)
		s.applyGlobalOptions(ctx)
		return nil
	case addrForeign:
		return fmt.Errorf("RC 地址 %s 已被占用：%s。请结束占用该端口的程序，或改用其它 rclone.rc_addr",
			s.cfg.RCAddr, occupiedBy)
	case addrReserved:
		// 地址被占着但无人应答：拉起子进程必然 bind 失败，然后被日志刷成
		// "重启永远失败"。等它释放；等不到也**不**用启动失败把整个程序按死——
		// 界面照常能开、能看日志，后台监管协程会接着等，地址一空就自动拉起 rclone。
		if err := s.waitAddressFree(ctx, s.addressWaitBudget()); err != nil {
			msg := fmt.Sprintf("RC 地址 %s 暂时无法 bind：%v", s.cfg.RCAddr, err)
			s.logger.Error("rclone 尚未启动：RC 地址暂不可用，转入后台等待",
				"rc_addr", s.cfg.RCAddr, logging.Err(err))
			s.note("%s。rclone 会在地址释放后自动拉起，无需重启本程序；若长时间如此，"+
				"请检查是否有别的程序占用该端口，或改用其它 rclone.rc_addr。", msg)
			s.setState(StateFailed, errors.New(msg))
			// 让监管协程接手：它会在每一轮真正拉起之前先等地址可用。
			s.ensureSupervise()
			select {
			case s.restartCh <- struct{}{}:
			default:
			}
			return nil
		}
	}

	s.opMu.Lock()
	startErr := s.spawn()
	if startErr == nil {
		startErr = s.waitReady(ctx, s.cfg.StartupTimeout.D(), true)
	}
	s.opMu.Unlock()

	if startErr != nil {
		s.client.CloseIdleConnections()
		s.killProcess()
		s.setState(StateFailed, startErr)
		return startErr
	}

	// 拉起自己的子进程这条路上必须显式置为 running：waitReady 只把 ready 置真，
	// 不碰 state。少了这一句，auto_start=true 的常规启动会永远停在"启动中"
	// （状态显示 starting、ready 却是 true），直到用户点一次重启才纠正。
	s.setState(StateRunning, nil)
	s.applyGlobalOptions(ctx)
	s.ensureSupervise()
	return nil
}

// adopt 接管 RC 地址上已有的 rclone rcd 实例。
//
// 语义：这个实例不是本进程拉起的，因此没有进程句柄；本程序仍然"接管"它 ——
// 用它执行任务，并在重启/退出时通过 core/quit 请它退出。这样接管之后
// 「重启 rclone」按钮依然可用，用户也不需要自己去找到那个残留进程。
func (s *Supervisor) adopt(occ *occupant) {
	s.mu.Lock()
	s.pid = occ.pid
	s.version = occ.version
	s.adopted = true
	s.startedAt = time.Now().UTC()
	s.lastErr = ""
	s.notice = fmt.Sprintf("RC 地址 %s 上已有 rclone rcd（pid %d），本程序已接管它：任务照常执行，"+
		"重启或退出时会通过 core/quit 结束该实例。若它是你自己长期运行的 rclone，请把 rclone.auto_start 设为 false。",
		s.cfg.RCAddr, occ.pid)
	s.mu.Unlock()

	s.setState(StateRunning, nil)
	s.logger.Warn("RC 地址上已存在 rclone rcd，转为接管模式",
		"endpoint", s.client.Endpoint(), "pid", occ.pid, "version", occ.version)
	s.note("检测到 RC 地址 %s 上已有 rclone rcd（pid %d，版本 %s）——多半是上一次没有正常退出的残留实例。"+
		"已接管：任务照常执行，点「重启 rclone」会让它退出后按当前配置重新拉起。",
		s.cfg.RCAddr, occ.pid, orUnknown(occ.version))
}

// applyGlobalOptions 把配置里的全局选项下发给 rcd；失败不阻断启动，但要让用户可见。
func (s *Supervisor) applyGlobalOptions(ctx context.Context) {
	if len(s.cfg.GlobalOptions) == 0 {
		return
	}
	if err := s.client.SetOptions(ctx, "main", s.cfg.GlobalOptions); err != nil {
		s.logger.Warn("下发 rclone 全局选项失败", logging.Err(err))
		return
	}
	s.logger.Info("已下发 rclone 全局选项", "options", s.cfg.GlobalOptions)
}

// occupant 描述 RC 地址上已存在的 rcd 实例。
type occupant struct {
	pid     int
	version string
}

// addrVerdict 是 RC 地址的探测结论。
type addrVerdict int

const (
	// addrVacant 表示地址现在能被 bind，可以拉起自己的实例。
	addrVacant addrVerdict = iota
	// addrOccupied 表示地址上有一个能通过本程序凭据认证的 rcd，可以接管。
	addrOccupied
	// addrForeign 表示地址被别的进程占着，既接管不了也拉不起来。
	addrForeign
	// addrReserved 表示没有进程在监听，但 bind 仍然失败（地址被系统扣着）。
	// 实测：Windows 上的 TIME_WAIT **不**阻止 bind，所以这种情况很罕见，但既然
	// bind 不了就不能当成"空闲"——那样只会拉起一个必然 bind 失败的子进程。
	addrReserved
)

// probeAddress 判断 RC 地址当前的状态。
func (s *Supervisor) probeAddress(ctx context.Context) (addrVerdict, *occupant, string) {
	if free, _ := addrFree(s.cfg.RCAddr); free {
		return addrVacant, nil, ""
	}
	if !addrIsListening(s.cfg.RCAddr) {
		// 既 bind 不了、又连不上：地址上没有能应答的进程，但仍被系统占着
		// （例如刚被绑定还没开始 listen）。**绝不能**当成"空闲"——那样会拉起一个
		// 必然 bind 失败的子进程。
		return addrReserved, nil, ""
	}
	if err := s.client.Ping(ctx); err != nil {
		return addrForeign, nil, fmt.Sprintf("TCP 端口可连接但 RC 探测失败（%v）", err)
	}
	var occ occupant
	if pid, err := s.client.PID(ctx); err == nil {
		occ.pid = pid
	}
	if v, err := s.client.Version(ctx); err == nil {
		occ.version = v.Version
	}
	return addrOccupied, &occ, ""
}

// addrFree 判断地址现在能不能被 bind。
//
// 这是"地址到底被没被占着"里最硬的一个判据：bind 成功才是真的没人占。TCP connect
// 只能看出"有没有人在监听"，看不出"地址已被绑定但还没 listen"这类中间状态，而那种
// 状态下 connect 会直接拿到 RST，于是把已被占用的地址误判成空闲。
//
// 注意：Windows 上 TIME_WAIT **不会**阻止 bind（已实测：连接先关的一方留下 TIME_WAIT
// 后，同一地址仍能立刻 bind 成功），所以 bind 失败的原因只可能是"此刻真有一个活着的
// socket 绑在上面"——最常见的就是本进程自己上一轮拉起的那个 rclone。
func addrFree(addr string) (bool, error) {
	ln, err := net.Listen("tcp", addrHostPort(addr))
	if err != nil {
		return false, err
	}
	// 探测用的监听套接字从未 accept 过连接，关掉即销毁，不会自己留下 TIME_WAIT。
	_ = ln.Close()
	return true, nil
}

// addrIsListening 判断地址上是否已经有人监听。仅用于探测，不区分对方是谁。
func addrIsListening(addr string) bool {
	conn, err := net.DialTimeout("tcp", addrHostPort(addr), 2*time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// addrHostPort 把配置里的 RC 地址归一成 net.Dial/net.Listen 能用的 host:port。
func addrHostPort(addr string) string {
	addr = strings.TrimSpace(addr)
	if strings.Contains(addr, "://") {
		if u, err := url.Parse(addr); err == nil && u.Host != "" {
			return u.Host
		}
	}
	return strings.TrimSuffix(addr, "/")
}

// addressWaitBudget 是"等 RC 地址变回可 bind"的单次等待预算。
//
// 下限 30s 是为了盖过最常见的以秒计的情形（进程刚退出、端口还没完全放开）；
// 上限 3min 是为了不把启动流程和重启接口拖到不可接受——真等不到，后台监管协程
// 会接着再等一轮，用户不必自己再点一次重启。
func (s *Supervisor) addressWaitBudget() time.Duration {
	d := s.cfg.StartupTimeout.D()
	if d < 30*time.Second {
		d = 30 * time.Second
	}
	if d > 3*time.Minute {
		d = 3 * time.Minute
	}
	return d
}

// waitAddressFree 轮询直到 RC 地址可以再次被 bind。
//
// 判据必须是"能 bind"而不是"对方不响应"：Windows 上 TerminateProcess 返回成功
// 并不代表监听套接字已经关闭，立刻重新 bind 同一地址会拿到
// "address already in use"，这正是"点重启必失败"的直接原因。
func (s *Supervisor) waitAddressFree(ctx context.Context, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	addr := addrHostPort(s.cfg.RCAddr)
	start := time.Now()
	deadline := start.Add(timeout)
	noted := false
	var lastErr error

	for {
		if free, err := addrFree(addr); free {
			if noted {
				s.note("RC 地址 %s 已释放（等待 %s），继续。", s.cfg.RCAddr, time.Since(start).Round(time.Millisecond))
			}
			return nil
		} else {
			lastErr = err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("等待 %s 释放超时（%s）：%s（bind 报错：%v）",
				s.cfg.RCAddr, timeout, s.occupantReason(ctx), lastErr)
		}
		// 这类等待可能长达两分钟。不解释清楚的话，用户只看到界面卡住、日志沉默，
		// 然后反复点重启。
		if !noted {
			noted = true
			s.noteAddressWait()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// noteAddressWait 把"地址暂时 bind 不了"写进用户可见的日志，并说明它在自愈。
func (s *Supervisor) noteAddressWait() {
	if addrIsListening(s.cfg.RCAddr) {
		s.note("RC 地址 %s 仍被占用，正在等待占用它的进程退出…", s.cfg.RCAddr)
		return
	}
	s.note("RC 地址 %s 已被占用但无人应答（多半是另一个进程刚绑上这个端口、或是本程序"+
		"上一轮拉起的 rclone 还没被回收），正在等待它被释放…", s.cfg.RCAddr)
}

// occupantReason 尽力说清"现在是谁占着这个地址"，用于报错文案。
func (s *Supervisor) occupantReason(ctx context.Context) string {
	if free, _ := addrFree(s.cfg.RCAddr); free {
		return "地址已可 bind"
	}
	if !addrIsListening(s.cfg.RCAddr) {
		return "地址仍被占用但无人应答：多半是本程序上一轮拉起的 rclone 尚未被回收，或另一个进程刚绑上这个端口"
	}
	if err := s.client.Ping(ctx); err != nil {
		return fmt.Sprintf("端口仍被占用且无法通过 RC 认证（%v）", err)
	}
	occ := ""
	if pid, err := s.client.PID(ctx); err == nil {
		occ = fmt.Sprintf("（pid %d）", pid)
	}
	return "端口仍被另一个 rclone rcd 占用" + occ
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
//
// 这里是整个包唯一真正创建 rclone 进程的地方，所以"该不该拉"的判断也放在这里：
// 只要本进程已经有一个活着的实例（自己拉起的子进程，或接管来的既有实例），就
// 不再拉第二个。Start / Restart / 监管协程各自还会提前判一次同样的条件，但那只是
// 为了少做无用功——**正确性靠这一道闸**。
func (s *Supervisor) spawn() error {
	if s.hasLiveInstance() {
		s.mu.RLock()
		pid, adopted := s.pid, s.adopted
		s.mu.RUnlock()
		s.logger.Info("已有 rclone 实例在运行，跳过本次启动——同一个 RC 地址上不能同时跑两个 rclone",
			"pid", pid, "adopted", adopted, "rc_addr", s.cfg.RCAddr)
		return nil
	}

	args := s.buildArgs()
	binary := resolveBinary(s.cfg)

	// 从这里开始，这个子进程的任何退出都算真的故障，reap 必须照常排自动重启
	// （stopCurrent 曾把它置位，那是为了吃掉"我们自己杀掉旧实例"那一次）。
	s.stopIntent.Store(false)

	s.setState(StateStarting, nil)
	s.note("正在启动 rclone rcd：%s %s", binary, strings.Join(args, " "))

	cmd := exec.Command(binary, args...)
	cmd.Env = os.Environ()
	// 子进程输出统一进入 journal，同时以 debug 级别透传到宿主日志。
	cmd.Stdout = io.MultiWriter(s.journal, &logBridge{logger: s.logger})
	cmd.Stderr = cmd.Stdout
	cmd.Stdin = nil

	prepareCommand(cmd)

	// 记下游标：子进程退出时用它取出"这一轮启动"的输出，从中识别 bind 失败之类的
	// 确定性故障。放在 Start 之前取，保证窗口里只有子进程自己的输出。
	mark := s.journal.Mark()

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
	// 拉起自己的子进程意味着上一轮的接管状态结束。
	s.adopted = false
	s.notice = ""
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

	go s.reap(cmd, exited, mark)
	return nil
}

// reap 等待子进程退出，并在需要时触发重启。
func (s *Supervisor) reap(cmd *exec.Cmd, exited chan struct{}, mark int64) {
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

	// 端口占用这类故障重试多少次结果都一样：不但救不回来，还会把 rclone 输出日志
	// 刷满一模一样的 CRITICAL 行，让人以为是"随机失败"。这里直接终止重试。
	if conflict := s.startupConflict(mark); conflict != "" {
		// 这条报错（bind: Only one usage of each socket address）的含义是确定的：
		// 那一刻地址上**已经有一个活着的 socket**——绝大多数情况就是本程序自己
		// 上一轮拉起的 rclone（spawn 里"已有实例就不再拉"那道闸就是为它设的）。
		// 所以文案要往"是不是已经有一个实例在跑"上引导，而不是含糊地说"端口被占用"。
		msg := fmt.Sprintf("rclone 无法监听 RC 地址 %s：%s。已停止自动重启——地址不腾出来，"+
			"重试多少次都一样。这通常意味着该地址上已经有一个 rclone 在跑（本程序上一轮拉起的，"+
			"或你自己启动的）：请确认并结束多余的那个进程，或把 rclone.rc_addr 改成空闲地址，"+
			"然后点「重启 rclone」。", s.cfg.RCAddr, conflict)
		s.mu.Lock()
		s.blocked = msg
		s.mu.Unlock()
		s.setState(StateFailed, errors.New(msg))
		s.logger.Error("rclone 启动失败：RC 地址被占用，已放弃自动重启", "rc_addr", s.cfg.RCAddr, "reason", conflict)
		s.note("%s", msg)
		return
	}

	// 这次退出是 Stop / Restart 主动结束实例造成的：拉起新实例是它们自己的事，
	// 不该再被当成一次故障去排自动重启（那样会让重启计数平白 +1）。
	if s.stopIntent.Load() {
		s.logger.Info("rclone rcd 已按请求结束，不触发自动重启", "uptime", uptime.Round(time.Millisecond))
		return
	}

	if !s.cfg.AutoRestart {
		return
	}
	select {
	case s.restartCh <- struct{}{}:
	default:
	}
}

// startupConflict 从本轮子进程的输出里找出"地址被占用"这类确定性故障。
//
// 返回非空即表示继续重试没有意义。
func (s *Supervisor) startupConflict(mark int64) string {
	lines, _ := s.journal.Since(mark)
	for _, line := range lines {
		low := strings.ToLower(line)
		for _, pat := range startupConflictPatterns {
			if strings.Contains(low, pat) {
				return strings.TrimSpace(line)
			}
		}
	}
	return ""
}

// startupConflictPatterns 是"重试无意义"的故障特征串（全部小写匹配）。
var startupConflictPatterns = []string{
	"address already in use",
	"only one usage of each socket address", // Windows 英文系统提示
	"通常每个套接字地址",                             // Windows 中文系统提示
	"bind: ",
	"failed to init server",
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

	addrWaits := 0
	for {
		select {
		case <-s.stopped:
			return
		case <-s.restartCh:
		}

		// 已被判定为"重试无意义"（如端口被占）时不再空转：刷日志的同时还会让
		// 用户以为程序在自救。手动重启会清掉这个标记。
		if reason := s.blockedReason(); reason != "" {
			s.logger.Warn("rclone 处于不可自动恢复状态，停止自动重启", "reason", reason)
			return
		}

		// 已经有活着的实例就不该再重启。手动重启刚把实例拉起来时，上一轮遗留下来的
		// 重试令牌会走到这里——不挡住它，就会再拉一个 rclone 去抢同一个地址，后到的
		// 那个必然 bind 失败，还会把一次成功的重启写成"启动失败"。
		if s.hasLiveInstance() {
			s.logger.Info("已有 rclone 实例在运行，跳过本次自动重启", "rc_addr", s.cfg.RCAddr)
			continue
		}

		// 真正拉起之前先等 RC 地址可用（地址不可用时 bind 必失败，spawn 只会
		// 换来一行 CRITICAL）。这一步必须在重启计数之前：端口等待是"还没轮到
		// 我们 bind"，不是"这一轮重启失败了"，不该吃掉 max_restarts 的额度。
		switch s.awaitAddress(&addrWaits) {
		case addressReady:
			addrWaits = 0
		case addressRetry:
			select {
			case <-s.stopped:
				return
			case <-time.After(s.cfg.RestartDelay.D()):
			}
			select {
			case s.restartCh <- struct{}{}:
			default:
			}
			continue
		case addressGiveUp:
			return
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

		// 等地址、退避这段时间里手动重启随时可能已经把实例拉起来了：那次"子进程退出"
		// 已经被手动重启处理过，不能在这里再算作一次自动重启——否则用户刚点完重启，
		// 计数就从 0 变成 1，额度凭空少一次。
		if s.hasLiveInstance() {
			s.logger.Info("等待期间实例已被拉起，放弃本次自动重启", "rc_addr", s.cfg.RCAddr)
			continue
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

		// "检查 + 拉起"必须原子：手动重启与自动重启是两条独立的路，谁都不该在
		// 对方刚拉起实例之后再拉一个。这里再确认一次（上面那次只是省无用功），
		// 因为等待地址、退避这段时间里手动重启随时可能接手。
		s.opMu.Lock()
		if s.hasLiveInstance() {
			s.opMu.Unlock()
			s.logger.Info("重启期间已有实例被拉起，放弃本次自动重启", "rc_addr", s.cfg.RCAddr)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.StartupTimeout.D())
		err := s.spawn()
		if err == nil {
			err = s.waitReady(ctx, s.cfg.StartupTimeout.D(), true)
		}
		cancel()
		s.opMu.Unlock()

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

// maxAddressWaits 是"地址迟迟不释放"时允许的连续等待轮数。
// 单轮预算至少 30s；5 轮是一个"再等下去就该让用户介入了"的长度。
const maxAddressWaits = 5

// addressGate 是"能不能拉起子进程"这一步的结论。
type addressGate int

const (
	addressReady  addressGate = iota // 地址可 bind，可以 spawn
	addressRetry                     // 还没释放，稍后再试一轮
	addressGiveUp                    // 等不到了，原因已写给用户
)

// awaitAddress 在真正拉起子进程之前等 RC 地址变回可 bind。
//
// waits 由调用方持有，用来累计连续等待轮数。
func (s *Supervisor) awaitAddress(waits *int) addressGate {
	if free, _ := addrFree(s.cfg.RCAddr); free {
		*waits = 0
		return addressReady
	}

	budget := s.addressWaitBudget()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	err := s.waitAddressFree(ctx, budget)
	cancel()

	if err == nil {
		*waits = 0
		return addressReady
	}
	if s.stopping() {
		return addressGiveUp
	}

	*waits++
	if *waits > maxAddressWaits {
		msg := fmt.Sprintf("RC 地址 %s 连续等待 %d 轮后仍不可用：%v。已停止自动重启——请结束占用"+
			"该端口的进程，或把 rclone.rc_addr 改成空闲地址，然后点「重启 rclone」。",
			s.cfg.RCAddr, *waits-1, err)
		s.mu.Lock()
		s.blocked = msg
		s.mu.Unlock()
		s.setState(StateFailed, errors.New(msg))
		s.logger.Error("RC 地址长时间不可用，放弃自动重启", "rc_addr", s.cfg.RCAddr, logging.Err(err))
		s.note("%s", msg)
		return addressGiveUp
	}
	return addressRetry
}

// stopping 报告调用方是否已要求停止（Stop 已关闭 stopped 通道）。
func (s *Supervisor) stopping() bool {
	select {
	case <-s.stopped:
		return true
	default:
		return false
	}
}

// WaitReady 轮询 rc/noop 直到 rcd 就绪或超时。
//
// 探活是按地址做的，因此它只能回答"这个地址上有一个能用的 rcd"，
// 不能回答"那是不是我拉起来的那个"。需要后者时用 waitReady。
func (s *Supervisor) WaitReady(ctx context.Context, timeout time.Duration) error {
	return s.waitReady(ctx, timeout, false)
}

// waitReady 轮询 rc/noop 直到 rcd 就绪或超时。
//
// expectChild 为 true 时额外要求"本进程刚拉起的子进程还活着"：否则一个占着
// 同一地址的外部 rcd 会让这里误判成功，而实际上没有任何进程受本程序托管
// （表现为状态显示就绪、PID 早已消失、重启按钮永远失败）。
func (s *Supervisor) waitReady(ctx context.Context, timeout time.Duration, expectChild bool) error {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	deadline := time.Now().Add(timeout)
	backoff := 50 * time.Millisecond
	var lastErr error

	for {
		if expectChild {
			if reason := s.ownChildGone(); reason != "" {
				return errors.New(reason)
			}
		}

		probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		lastErr = s.client.Ping(probeCtx)
		cancel()
		if lastErr == nil {
			// 应答可能来自别人的 rcd（本进程的子进程已经死了，而上一轮的残留实例
			// 还占着这个地址）。宣告就绪之前必须再确认一次，否则会出现"状态就绪、
			// PID 早已消失、重启永远失败"的假成功。
			if expectChild {
				if reason := s.ownChildGone(); reason != "" {
					return errors.New(reason)
				}
			}

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

// hasLiveInstance 报告本进程当前是否已经有一个"活着的、占着 RC 地址的"实例。
//
// 它只看"是否已有实例在跑"，不区分是自己拉起的子进程还是接管来的既有实例——
// 因为对 bind 来说两者没有区别，地址只有一个。
func (s *Supervisor) hasLiveInstance() bool {
	s.mu.RLock()
	pid, adopted, cmd := s.pid, s.adopted, s.cmd
	s.mu.RUnlock()
	if pid == 0 {
		return false
	}
	// 接管的实例没有进程句柄，只能按 pid 判断存活。
	if adopted {
		return processAlive(pid)
	}
	// 自己拉起的子进程：进程句柄还在，且进程确实活着。
	return cmd != nil && processAlive(pid)
}

// waitInstanceGone 等到本进程不再持有活着的实例。
//
// 判据是 hasLiveInstance（子进程句柄已被 reap 清掉，或接管的实例已消失），而不是
// 只看进程是否退出：Windows 上进程退出后、父进程 Wait 之前 OpenProcess 依然能成功，
// 只按 pid 判断会把一个已经死掉的子进程当成还活着，反而把紧接着的重启挡回去。
func (s *Supervisor) waitInstanceGone(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !s.hasLiveInstance() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// ownChildGone 判断"本次等待本该由本进程拉起的子进程来应答"时，它是否已经不在了。
//
// 返回空串表示子进程仍在（或本次等待本来就不依赖子进程）。
func (s *Supervisor) ownChildGone() string {
	s.mu.RLock()
	pid := s.pid
	adopted := s.adopted
	external := s.external
	s.mu.RUnlock()
	if external || adopted {
		return ""
	}
	if pid != 0 && processAlive(pid) {
		return ""
	}

	// 子进程已经退出。给 reap 一点时间把退出原因记下来（journal 里的 bind 失败等），
	// 否则用户只会看到一句"启动后立即退出"，真正的原因躺在日志里没人看。
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && s.blockedReason() == "" {
		time.Sleep(25 * time.Millisecond)
	}
	if reason := s.blockedReason(); reason != "" {
		return reason
	}
	return "rclone rcd 进程启动后立即退出，具体原因见「rclone 输出日志」"
}

// blockedReason 返回自动重启被放弃的原因（空串表示未放弃）。
func (s *Supervisor) blockedReason() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.blocked
}

// note 把一条面向用户的说明同时写进 rclone 输出缓冲与宿主日志。
//
// 托管的很多决策（接管既有实例、放弃自动重启……）发生在 CloudSync 侧，
// 只写进宿主日志的话，用户在「rclone 输出日志」里只能看到一串一模一样的
// CRITICAL 行，完全不知道程序做了什么、该做什么。
func (s *Supervisor) note(format string, args ...any) {
	text := fmt.Sprintf(format, args...)
	_, _ = fmt.Fprintf(s.journal, "[cloudsync] %s\n", text)
	s.logger.Info("rclone 托管：" + text)
}

func orUnknown(v string) string {
	if strings.TrimSpace(v) == "" {
		return "未知"
	}
	return v
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

	// 与监管协程、手动重启互斥：否则关停过程中对方刚好拉起一个新子进程，
	// 就会留下一个没人管的 rclone 继续占着 RC 地址。
	s.opMu.Lock()
	defer s.opMu.Unlock()

	s.mu.RLock()
	external := s.external
	adopted := s.adopted
	pid := s.pid
	s.mu.RUnlock()

	if external {
		s.logger.Info("外部 rclone rcd 不受本程序托管，跳过关闭")
		return nil
	}
	if pid == 0 {
		return nil
	}

	// 接管来的实例没有进程句柄，只能 core/quit，并用"地址是否释放"作为判据。
	if adopted {
		timeout := s.cfg.ShutdownTimeout.D()
		if timeout <= 0 {
			timeout = 15 * time.Second
		}
		s.logger.Info("正在关闭接管的 rclone rcd", "pid", pid, "timeout", timeout.String())
		if err := s.quitInstance(ctx, 5*time.Second); err != nil {
			s.logger.Debug("core/quit 调用失败", logging.Err(err))
		}
		if err := s.waitAddressFree(ctx, timeout); err != nil {
			// 退出流程不阻断：报出来让用户知道 5572 上还剩一个 rclone。
			s.logger.Warn("接管的 rclone rcd 未退出，RC 地址仍被占用", logging.Err(err))
		} else {
			s.logger.Info("接管的 rclone rcd 已退出", "pid", pid)
		}
		s.setState(StateStopped, nil)
		return nil
	}

	timeout := s.cfg.ShutdownTimeout.D()
	if timeout <= 0 {
		timeout = 15 * time.Second
	}

	s.logger.Info("正在关闭 rclone rcd", "pid", pid, "timeout", timeout.String())

	if err := s.quitInstance(ctx, 5*time.Second); err != nil {
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
	// 强杀会连它的监听套接字与连接一起带走。先断开我们这一侧的空闲连接，
	// 免得本进程这边还挂着指向已死实例的连接（探测时会被它们误导）。
	s.client.CloseIdleConnections()
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
//
// 手动重启的含义是"这个地址上应该跑着按当前配置拉起的 rclone"，因此这里会：
//  1. 结束当前实例（自己拉起的子进程先 core/quit 再强杀；接管来的只能请它退出）；
//  2. **等到 RC 地址真的能被重新 bind** —— 直接杀进程后立刻 spawn 会撞上
//     "address already in use"，这正是"点重启必失败"的直接原因；
//  3. 清掉上一轮的"放弃自动重启"标记与接管状态，重新拉起并按子进程存活校验就绪。
//
// 整个过程持有 opMu：监管协程的自动重启不能插进来——它一旦在这中间也拉一个
// rclone，两个进程就会抢同一个 RC 地址，后到的那个必然 bind 失败。
func (s *Supervisor) Restart(ctx context.Context) error {
	s.mu.RLock()
	external := s.external
	s.mu.RUnlock()
	if external {
		return ErrExternalRestart
	}

	s.opMu.Lock()
	defer s.opMu.Unlock()

	s.logger.Info("收到手动重启 rclone 请求")
	// 用户点了「重启」就代表"重新开始"：自动重启的累计次数、放弃标记、以及那句
	// "连续重启 N 次后放弃"都要一并清零。否则上一轮把额度耗光之后，即使这次手动
	// 重启成功，之后一次普通崩溃也会立刻撞上 max_restarts——用户会觉得"重启没用"。
	s.mu.Lock()
	s.restarts = 0
	s.blocked = ""
	s.notice = ""
	s.lastErr = ""
	s.mu.Unlock()

	if err := s.stopCurrent(ctx); err != nil {
		return err
	}
	// 等旧的实例被彻底收尾（子进程句柄已被 reap 清掉）：只判断端口空不空还不够，
	// 否则下面 spawn 的"已有实例在跑"那道闸会把这次重启当成重复启动而跳过。
	s.waitInstanceGone(5 * time.Second)
	s.mu.Lock()
	s.adopted = false
	s.pid = 0
	s.startedAt = time.Time{}
	s.mu.Unlock()

	// 手动重启**取代**待处理的自动重启：上面 stopCurrent 是我们自己结束的子进程，
	// 它的退出会让 reap 顺手排一次自动重启。留着那个令牌，监管协程就会把这次
	// 用户点的、马上要由本函数完成的重启算成一次自动重启（计数平白变成 1），
	// 于是"点完重启计数还是不为 0"。
	select {
	case <-s.restartCh:
	default:
	}

	// 若监管协程此前已因达到重启上限（或被判定不可恢复）而退出，这里重新拉起。
	s.ensureSupervise()

	// 最后一道闸：真要 bind 了。stopCurrent 已经等过一轮，但它的 pid==0 支线可能
	// 什么都没做（例如地址上还留着一个既有的实例），所以这里必须再确认一次——
	// 不确认就 spawn，结果只会是子进程 bind 失败、日志里再刷一行 CRITICAL。
	if err := s.waitAddressFree(ctx, s.addressWaitBudget()); err != nil {
		// 端口还没释放：交给后台监管协程接着等（它在每一轮 spawn 之前都会先确认
		// 地址可用），端口一空就自动拉起，用户不必盯着再点一次。
		s.handOffToSupervise()
		return fmt.Errorf("RC 地址 %s 尚未释放，无法立即重启：%w。后台会自动重试，端口释放后无需再点一次",
			s.cfg.RCAddr, err)
	}

	spawnCtx, cancel := context.WithTimeout(ctx, s.cfg.StartupTimeout.D())
	defer cancel()
	// 上面 drain 掉的那次令牌是"我们自己杀掉旧实例"排的，所以失败路径必须把重试
	// 机会补回去：否则可执行文件临时不可用、或新实例没能在超时内就绪时，手动重启
	// 一失败就再没人在后台接管了（这与 drain 之前的行为一致）。
	if err := s.spawn(); err != nil {
		s.setState(StateFailed, err)
		s.handOffToSupervise()
		return err
	}
	if err := s.waitReady(spawnCtx, s.cfg.StartupTimeout.D(), true); err != nil {
		s.client.CloseIdleConnections()
		s.killProcess()
		s.setState(StateFailed, err)
		s.handOffToSupervise()
		return err
	}
	s.setState(StateRunning, nil)
	return nil
}

// handOffToSupervise 把"接下来该怎么办"交回后台监管协程：它在每一轮真正拉起之前
// 都会先确认地址可用，并按退避与上限重试，用户不必自己再点一次。
func (s *Supervisor) handOffToSupervise() {
	s.ensureSupervise()
	select {
	case s.restartCh <- struct{}{}:
	default:
	}
}

// stopCurrent 结束当前的 rcd 实例，并等到 RC 地址可以被重新 bind。
//
// 地址能否 bind 是唯一的成功判据：进程"已结束"不等于端口已释放。
func (s *Supervisor) stopCurrent(ctx context.Context) error {
	s.mu.RLock()
	pid := s.pid
	adopted := s.adopted
	s.mu.RUnlock()

	timeout := s.cfg.ShutdownTimeout.D()
	if timeout <= 0 {
		timeout = 15 * time.Second
	}

	if pid == 0 {
		// 本程序不认为自己在托管任何实例，但地址未必就空闲——上一轮崩溃/异常退出
		// 留下的 rclone 可能还占着它。所以判据必须是"能不能 bind"，而不是
		// "连不连得上"：把"连不上"当成"空闲"会直接放行去 spawn，子进程 bind 失败
		// 又被判为不可恢复，于是"点几次重启就失败几次"。
		verdict, occ, reason := s.probeAddress(ctx)
		switch verdict {
		case addrVacant:
			return nil
		case addrReserved:
			// 没人监听但 bind 不了：等它释放，等不到就把话说清楚，
			// 而不是放行去 spawn（那只会换来子进程 bind 失败）。
			if err := s.waitAddressFree(ctx, s.addressWaitBudget()); err != nil {
				return fmt.Errorf("RC 地址 %s 尚未释放，无法重启：%w", s.cfg.RCAddr, err)
			}
			return nil
		case addrForeign:
			return fmt.Errorf("RC 地址 %s 已被占用，且不是可用的 rclone rcd：%s", s.cfg.RCAddr, reason)
		}
		// 剩下 addrOccupied：多半是别人的 rclone（例如上一轮判定"不可恢复"之后，
		// 用户自己又把 rclone 起了起来）。用户点的是「重启 rclone」，意图就是
		// "这个地址该跑我配置里的实例"，所以这里请它退出；不是可用的 rcd 就明确报错，不猜。
		s.note("RC 地址 %s 上有一个既有的 rclone rcd（pid %d），按重启请求先结束它。", s.cfg.RCAddr, occ.pid)
		if err := s.quitInstance(ctx, 5*time.Second); err != nil {
			s.logger.Debug("core/quit 调用失败", logging.Err(err))
		}
		if err := s.waitAddressFree(ctx, timeout); err != nil {
			return fmt.Errorf("无法让 RC 地址 %s 上的既有 rclone（pid %d）退出：%w", s.cfg.RCAddr, occ.pid, err)
		}
		return nil
	}

	// 从这一刻起直到下一次 spawn 成功，子进程的退出都是我们主动造成的，
	// reap 不该把它当成故障去排自动重启（详见 stopIntent 的说明）。
	s.stopIntent.Store(true)

	// 先请它自己退出：对 rclone 来说 core/quit 是干净的关闭路径，比强杀少一堆
	// "被强制结束"的噪音日志；接管来的实例也只可能用这个办法。
	if err := s.quitInstance(ctx, 5*time.Second); err != nil {
		s.logger.Debug("core/quit 调用失败，将直接结束进程", logging.Err(err))
	}

	if err := s.waitAddressFree(ctx, timeout); err == nil {
		return nil
	} else if adopted {
		return fmt.Errorf("接管的外部 rclone 实例（pid %d）未在 %s 内释放 %s，无法重启：%w",
			pid, timeout, s.cfg.RCAddr, err)
	}

	s.logger.Warn("rclone rcd 未在超时内退出，强制结束", "pid", pid)
	// 强杀同样会把它的连接一起带走，所以先断开我们这一侧：留着这些死连接，
	// 之后的探测可能把"已退出"误判成"还在应答"。
	s.client.CloseIdleConnections()
	s.killProcess()
	s.waitExit(5 * time.Second)
	if err := s.waitAddressFree(ctx, 10*time.Second); err != nil {
		return fmt.Errorf("RC 地址 %s 仍被占用，无法重启：%w", s.cfg.RCAddr, err)
	}
	return nil
}

// quitInstance 请 rclone 自己退出，并在前后各断开一次我们持有的空闲连接。
//
// core/quit 是 rclone 唯一的干净关闭路径（比强杀少一堆"被强制结束"的噪音日志），
// 接管来的实例也只可能用这个办法。前后各清一次空闲连接，是为了让本进程手上不再
// 留有指向已退出实例的连接——留着会让之后的 Ping/探测打到死连接上，把"已经退出"
// 误判成"还在应答"。
//
// 注意：这里**不是**为了躲 TIME_WAIT。实测 Windows 上 TIME_WAIT 不阻止同一地址
// 重新 bind（详见 quitInstance 下方的地址探测说明与 supervisor_test.go 的实测注释），
// "点重启必失败"的根因是本程序拉起了第二个实例，不是端口被内核扣住。
func (s *Supervisor) quitInstance(ctx context.Context, timeout time.Duration) error {
	s.client.CloseIdleConnections()
	quitCtx, cancel := context.WithTimeout(ctx, timeout)
	err := s.client.Quit(quitCtx)
	cancel()
	// 承载 core/quit 的那条连接此刻已回到空闲池，趁 rclone 的关闭流程还没碰到它，先由我们关掉。
	s.client.CloseIdleConnections()
	return err
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
		Adopted:      s.adopted,
		Notice:       s.notice,
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
