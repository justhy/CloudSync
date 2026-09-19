package rclone

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"cloudsync/internal/config"
	"cloudsync/internal/logging"
)

// runFakeRCDChild 让本测试二进制扮演一个 rcd：在 supervisor 指定的地址上提供最小的
// RC 接口，并在收到 core/quit 后像真实 rclone 那样整个进程退出。
func runFakeRCDChild() {
	addr := ""
	for i, a := range os.Args {
		if a == "--rc-addr" && i+1 < len(os.Args) {
			addr = os.Args[i+1]
		}
	}
	if addr == "" {
		fmt.Fprintln(os.Stderr, "helper rcd: 命令行里没有 --rc-addr")
		os.Exit(2)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "helper rcd: %v\n", err)
		os.Exit(3)
	}

	mux := http.NewServeMux()
	write := func(w http.ResponseWriter, body string) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}
	mux.HandleFunc("/rc/noop", func(w http.ResponseWriter, r *http.Request) { write(w, `{}`) })
	mux.HandleFunc("/core/pid", func(w http.ResponseWriter, r *http.Request) {
		write(w, fmt.Sprintf(`{"pid":%d}`, os.Getpid()))
	})
	mux.HandleFunc("/core/version", func(w http.ResponseWriter, r *http.Request) {
		write(w, `{"version":"v1.75.1","os":"windows","arch":"amd64"}`)
	})
	mux.HandleFunc("/core/quit", func(w http.ResponseWriter, r *http.Request) {
		write(w, `{}`)
		// 真实 rclone 收到 core/quit 后是整个进程退出：监听套接字与所有连接一起消失。
		go func() {
			time.Sleep(50 * time.Millisecond)
			os.Exit(0)
		}()
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { write(w, `{}`) })
	_ = (&http.Server{Handler: mux}).Serve(ln)
}

// ---------------------------------------------------------------------------
// 子进程替身
// ---------------------------------------------------------------------------

// helperChildEnv 置位时，本测试二进制自己扮演"启动即失败的 rclone"。
//
// 必须在 TestMain 里、flag.Parse 之前处理：supervisor 传过来的命令行是
// `rcd --rc-addr ... --log-level ...`，交给 flag 包会以
// "flag provided but not defined" 退出，我们想验证的 bind 报错就看不到了。
const helperChildEnv = "CLOUDSYNC_TEST_RCLONE_CHILD"

// helperRCDChildEnv 置位时，本测试二进制扮演一个**真的监听、真的能被 core/quit 关掉**的
// 假 rclone rcd（地址从命令行里取：supervisor 传的是 `rcd --rc-addr 127.0.0.1:PORT ...`）。
//
// 用真实子进程而不是进程内 httptest，是因为"重启计数平白 +1"这件事就发生在
// 「子进程真的被拉起 → 真的被我们杀掉 → reap 真的往 restartCh 排了令牌」这条链路上，
// 进程内假服务碰不到它。
const helperRCDChildEnv = "CLOUDSYNC_TEST_RCLONE_RCD"

// helperCrashEnv 置位时，本测试二进制扮演"拉起后立刻崩溃"的 rclone。
//
// 不能用"传一套 rclone 的命令行让它解析失败"来代替：Go 的 flag 包遇到第一个位置
// 参数（supervisor 传的 `rcd`）就停止解析，于是子进程会把整个测试套件再跑一遍。
// 而且它打印的内容必须**不含** bind 冲突那几个特征串——否则走的就不是"崩溃→自动重启"，
// 而是"确定性故障→放弃重启"那条路了。
const helperCrashEnv = "CLOUDSYNC_TEST_RCLONE_CRASH"

func TestMain(m *testing.M) {
	if os.Getenv(helperCrashEnv) == "1" {
		fmt.Fprintln(os.Stderr, "helper: 子进程按用例要求立刻崩溃")
		os.Exit(2)
	}
	if os.Getenv(helperRCDChildEnv) == "1" {
		runFakeRCDChild()
		return
	}
	if os.Getenv(helperChildEnv) == "1" {
		fmt.Fprintln(os.Stderr,
			"2026/09/19 22:30:01 CRITICAL: Failed to start remote control: failed to init server: "+
				"listen tcp 127.0.0.1:5572: bind: address already in use")
		os.Exit(2)
	}
	os.Exit(m.Run())
}

// fakeRCD 是一个真的占着端口、并且能被 core/quit 关掉的假 rclone rcd。
//
// 用它而不是 httptest.NewServer，是因为本轮修复的核心判据就是"地址能不能被
// 重新 bind"——假实例必须真的监听、也真的能停掉，否则测不出东西。
type fakeRCD struct {
	addr string
	pid  int

	mu    sync.Mutex
	ln    net.Listener
	srv   *http.Server
	quits int
}

func newFakeRCD(t *testing.T, pid int) *fakeRCD {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听端口失败: %v", err)
	}
	f := &fakeRCD{addr: ln.Addr().String(), pid: pid, ln: ln}

	mux := http.NewServeMux()
	write := func(w http.ResponseWriter, body string) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}
	mux.HandleFunc("/rc/noop", func(w http.ResponseWriter, r *http.Request) { write(w, `{}`) })
	mux.HandleFunc("/core/pid", func(w http.ResponseWriter, r *http.Request) {
		write(w, fmt.Sprintf(`{"pid":%d}`, f.pid))
	})
	mux.HandleFunc("/core/version", func(w http.ResponseWriter, r *http.Request) {
		write(w, `{"version":"v1.75.1","os":"windows","arch":"amd64"}`)
	})
	mux.HandleFunc("/core/quit", func(w http.ResponseWriter, r *http.Request) {
		write(w, `{}`)
		f.mu.Lock()
		f.quits++
		f.mu.Unlock()
		// 真实 rclone 收到 core/quit 后是整个进程退出：监听套接字与**所有已建立的
		// 连接**一起消失。这里必须连同连接一起关，才能忠实模拟"实例真的没了"
		// ——否则本进程手上还留着半开连接，探测会被它们骗过。
		go func() {
			time.Sleep(50 * time.Millisecond)
			_ = f.srv.Close()
		}()
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { write(w, `{}`) })

	f.srv = &http.Server{Handler: mux}
	go func() { _ = f.srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = f.srv.Close()
		f.mu.Lock()
		if f.ln != nil {
			_ = f.ln.Close()
		}
		f.mu.Unlock()
	})
	return f
}

func (f *fakeRCD) quitCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.quits
}

// freeAddr 返回一个当前空闲的地址（占住再放掉，避免撞上别的进程）。
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("探测空闲端口失败: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// deadPID 返回一个确定已经退出的进程号，用于构造"子进程已消失"的状态。
func deadPID(t *testing.T) int {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestNonexistentMarkerNeverMatches")
	cmd.Env = append(os.Environ(), helperChildEnv+"=1")
	_ = cmd.Run()
	if cmd.Process == nil {
		t.Fatal("无法取得子进程 pid")
	}
	return cmd.Process.Pid
}

func testRcloneConfig(addr string) config.RcloneConfig {
	cfg := config.Default().Rclone
	cfg.Path = "rclone"
	cfg.RCAddr = addr
	cfg.AutoStart = true
	cfg.AutoRestart = true
	cfg.StartupTimeout = config.Duration(3 * time.Second)
	cfg.ShutdownTimeout = config.Duration(2 * time.Second)
	cfg.RestartDelay = config.Duration(10 * time.Millisecond)
	cfg.MaxRestarts = 5
	cfg.RCUser = "tester"
	cfg.RCPass = "secret"
	return cfg
}

// ---------------------------------------------------------------------------
// 用例
// ---------------------------------------------------------------------------

// rclone.auto_start=true 时，如果 RC 地址上已经有 rclone rcd，本程序不能
// 再拉一个（必然 bind 失败），而应接管它。
//
// 反例（修复前）：照样 spawn → 子进程立刻死于 "address already in use"，
// 但 WaitReady 被对方的应答骗过，于是状态显示就绪、PID 却早已消失，
// 「重启 rclone」从此永远失败。
func TestStartAdoptsExistingRCDInsteadOfSpawning(t *testing.T) {
	f := newFakeRCD(t, 4242)
	cfg := testRcloneConfig(f.addr)
	// 指向一个不存在的可执行文件：只要 Start 试图拉起子进程，就必须报错失败。
	cfg.Path = "definitely-not-a-real-rclone-binary"

	sv := NewSupervisor(cfg, logging.Discard())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := sv.Start(ctx); err != nil {
		t.Fatalf("已有 rcd 时应转为接管而不是报错: %v", err)
	}
	st := sv.Status()
	if !st.Adopted {
		t.Fatalf("应标记为接管（adopted=true），实际 %+v", st)
	}
	if st.External {
		t.Fatalf("接管不等于「外部托管（不管理）」，external 应为 false: %+v", st)
	}
	if st.PID != 4242 {
		t.Fatalf("PID 应取自 core/pid（4242），实际 %d", st.PID)
	}
	if !st.Ready || st.State != StateRunning {
		t.Fatalf("接管后应处于 running/ready，实际 state=%s ready=%v", st.State, st.Ready)
	}
	if !strings.Contains(st.Notice, "已接管") {
		t.Fatalf("应给出接管说明，实际 notice=%q", st.Notice)
	}
	if st.Version != "v1.75.1" {
		t.Fatalf("版本应取自既有实例，实际 %q", st.Version)
	}
	if !strings.Contains(strings.Join(sv.Journal().Lines(), "\n"), "[cloudsync]") {
		t.Fatal("接管这件事必须写进 rclone 输出日志，否则用户在日志里只看到别人的输出")
	}

	// 退出时应当通过 core/quit 关掉接管的实例，并真正释放端口。
	if err := sv.Stop(ctx); err != nil {
		t.Fatalf("Stop 失败: %v", err)
	}
	if f.quitCount() == 0 {
		t.Fatal("关闭接管的实例应调用 core/quit")
	}
	if err := sv.waitAddressFree(ctx, 3*time.Second); err != nil {
		t.Fatalf("接管实例退出后端口应被释放: %v", err)
	}
}

// 端口被非 rclone 的东西占着时，必须立刻给出明确原因，而不是拉起子进程去撞墙。
func TestStartRejectsAddressOccupiedByNonRclone(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}
	defer ln.Close()
	// 只说 TCP、不响应 RC：模拟"被别的程序占了 5572"。
	go func() {
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			_ = conn.Close()
		}
	}()

	cfg := testRcloneConfig(ln.Addr().String())
	cfg.Path = "definitely-not-a-real-rclone-binary"
	sv := NewSupervisor(cfg, logging.Discard())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err = sv.Start(ctx)
	if err == nil {
		t.Fatal("端口被非 rclone 占用时必须报错")
	}
	if !strings.Contains(err.Error(), "已被占用") {
		t.Fatalf("错误信息应点明「地址已被占用」并给出原因，实际: %v", err)
	}
}

// 子进程因地址被占用而启动失败时：
//  1. 必须给出可操作的错误（含 rclone 的原始报错），而不是等到 30s 超时；
//  2. 必须终止自动重启——端口不释放，重试多少次都一样。
func TestChildBindConflictStopsAutoRestart(t *testing.T) {
	t.Setenv(helperChildEnv, "1")
	cfg := testRcloneConfig(freeAddr(t))
	cfg.Path = os.Args[0] // 测试二进制自己扮演"启动即失败的 rclone"

	sv := NewSupervisor(cfg, logging.Discard())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	err := sv.Start(ctx)
	if err == nil {
		t.Fatal("子进程 bind 失败时 Start 必须返回错误")
	}
	msg := err.Error()
	if !strings.Contains(msg, "address already in use") && !strings.Contains(msg, "被占用") {
		t.Fatalf("错误信息应包含 rclone 的原始报错或端口占用说明，实际: %v", err)
	}

	st := sv.Status()
	if st.State != StateFailed {
		t.Fatalf("状态应为 failed，实际 %s", st.State)
	}
	if reason := sv.blockedReason(); reason == "" {
		t.Fatal("应把「重试无意义」标记下来，否则监管协程会一直空转刷日志")
	}

	// 监管协程必须已经收工：再等一会儿也不该出现新的尝试。
	before := sv.Journal().Len()
	time.Sleep(300 * time.Millisecond)
	if after := sv.Journal().Len(); after != before {
		t.Fatalf("放弃自动重启后不应再产生输出：%d -> %d", before, after)
	}
}

// 匹配规则本身：认得 rclone / Windows 两种 bind 报错，且不误伤正常输出。
func TestStartupConflictPatterns(t *testing.T) {
	sv := NewSupervisor(testRcloneConfig("127.0.0.1:1"), nil)
	sv.journal.Reset()
	mark := sv.journal.Mark()
	_, _ = sv.journal.Write([]byte("2026/09/19 22:30:01 INFO  : Using --user godcic --pass XXXX as authenticated user\n"))
	if got := sv.startupConflict(mark); got != "" {
		t.Fatalf("正常输出不应被判为确定性故障，得到 %q", got)
	}

	cases := []string{
		`CRITICAL: Failed to start remote control: failed to init server: listen tcp 127.0.0.1:5572: bind: address already in use`,
		`CRITICAL: Failed to start remote control: failed to init server: listen tcp 127.0.0.1:5572: bind: Only one usage of each socket address (protocol/network address/port) is normally permitted.`,
		`CRITICAL: Failed to start remote control: failed to init server: 通常每个套接字地址(协议/网络地址/端口)只允许使用一次。`,
	}
	for _, line := range cases {
		s := NewSupervisor(testRcloneConfig("127.0.0.1:1"), nil)
		s.journal.Reset()
		m := s.journal.Mark()
		_, _ = s.journal.Write([]byte(line + "\n"))
		if got := s.startupConflict(m); got == "" {
			t.Fatalf("应识别为确定性故障: %s", line)
		}
	}
}

// 就绪探测不能被骗：地址上有别人的 rcd 在应答，但本进程拉起的子进程已经死了，
// 这时必须失败而不是报告"就绪"。
//
// 两臂对照：同一个地址，不带子进程存活要求时探测得到成功（证明地址确实可用），
// 带上要求后必须失败。
func TestWaitReadyIsNotFooledByStrayRCD(t *testing.T) {
	f := newFakeRCD(t, 9999)
	cfg := testRcloneConfig(f.addr)
	sv := NewSupervisor(cfg, logging.Discard())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 对照臂：只要"地址上有能应答的 rcd"，探测是成功的。
	if err := sv.waitReady(ctx, 2*time.Second, false); err != nil {
		t.Fatalf("对照臂应探测成功（地址上确实有 rcd）: %v", err)
	}

	// 实验臂：本进程认领的子进程已经退出，就不允许把别人的应答当成就绪。
	sv2 := NewSupervisor(cfg, logging.Discard())
	sv2.mu.Lock()
	sv2.pid = deadPID(t)
	sv2.mu.Unlock()
	err := sv2.waitReady(ctx, 2*time.Second, true)
	if err == nil {
		t.Fatal("子进程已死时不允许报告就绪——哪怕地址上有别人的 rcd 能应答")
	}
	if st := sv2.Status(); st.Ready {
		t.Fatalf("不应处于就绪状态: %+v", st)
	}
}

// 「重启 rclone」按钮要能work：地址被一个既有 rclone 占着时，先请它退出、
// 等端口真正释放，再拉起自己的实例。
func TestRestartClearsOccupiedAddress(t *testing.T) {
	f := newFakeRCD(t, 7777)
	cfg := testRcloneConfig(f.addr)
	cfg.Path = "definitely-not-a-real-rclone-binary" // 拉起必然失败，但至少要走到那一步

	sv := NewSupervisor(cfg, logging.Discard())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// 构造"本程序不认为自己托管任何实例，但地址被一个 rclone 占着"的状态：
	// 这正是"上一轮判定不可恢复"或"上次异常退出留下残留实例"之后的局面。
	if err := sv.Restart(ctx); err == nil {
		t.Fatal("拉起自己的实例会失败（可执行文件不存在），Restart 不应报成功")
	}
	if f.quitCount() == 0 {
		t.Fatal("地址被既有 rclone 占用时，重启应先请它退出（core/quit）")
	}
	if err := sv.waitAddressFree(ctx, 3*time.Second); err != nil {
		t.Fatalf("既有实例退出后端口应被释放: %v", err)
	}
	if got := sv.blockedReason(); got != "" {
		t.Fatalf("手动重启应清掉上一轮的终止标记，实际 %q", got)
	}
}

// 手动点「重启 rclone」必须把自动重启的累计次数归零。
//
// 场景：rclone 连续崩溃，自动重启把 max_restarts 的额度耗光并放弃；用户点「重启」
// 手动拉起之后，下一次普通崩溃必须**从第 1 次重新计数**。不归零的话，一次普通
// 崩溃就会立刻撞上"连续重启 N 次后放弃"，用户看到的就是"重启了也没用"。
func TestManualRestartResetsAutoRestartCounter(t *testing.T) {
	f := newFakeRCD(t, 4242)
	cfg := testRcloneConfig(f.addr)
	// 拉起必然失败（可执行文件不存在），但"归零"发生在尝试拉起**之前**，
	// 所以这条用例照样能验到要验的东西。
	cfg.Path = "definitely-not-a-real-rclone-binary"

	sv := NewSupervisor(cfg, logging.Discard())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// 构造"额度已耗光并已放弃自动重启"的状态。
	sv.mu.Lock()
	sv.restarts = cfg.MaxRestarts
	sv.blocked = "RC 地址 127.0.0.1:5572 已被占用"
	sv.lastErr = fmt.Sprintf("连续重启 %d 次后放弃，请检查 rclone 可执行文件与配置", cfg.MaxRestarts)
	sv.mu.Unlock()
	if got := sv.Status().Restarts; got != cfg.MaxRestarts {
		t.Fatalf("前提不成立：重启计数应为 %d，实际 %d", cfg.MaxRestarts, got)
	}

	_ = sv.Restart(ctx)

	st := sv.Status()
	if st.Restarts != 0 {
		t.Fatalf("手动重启后自动重启计数应归零，实际 %d", st.Restarts)
	}
	if got := sv.blockedReason(); got != "" {
		t.Fatalf("手动重启后应清掉「放弃自动重启」标记，实际 %q", got)
	}
	if strings.Contains(st.LastError, "连续重启") {
		t.Fatalf("手动重启后不应再挂着上一轮的「连续重启 N 次后放弃」，实际 %q", st.LastError)
	}
}

// 手动重启不能被算成一次自动重启。
//
// 链路：Restart 先 stopCurrent（亲手杀掉自己拉起的子进程）→ reap 检测到"意外退出"→
// 往 restartCh 排一次自动重启。这枚令牌是**我们自己造成的**，若不去管它，监管协程会
// 把它当成一次真实的崩溃来计数：用户刚点完重启、界面显示成功，重启计数却从 0 变成 1，
// max_restarts 的额度凭空少一次。
func TestManualRestartDoesNotCountAsAutoRestart(t *testing.T) {
	if os.Getenv(helperRCDChildEnv) == "1" {
		t.Skip("子进程模式下不跑这条用例")
	}
	// 让 supervisor 拉起的子进程变成上面那个假 rcd（它会继承本进程的环境变量）。
	t.Setenv(helperRCDChildEnv, "1")

	cfg := testRcloneConfig(freeAddr(t))
	cfg.Path = os.Args[0]
	sv := NewSupervisor(cfg, logging.Discard())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	t.Cleanup(func() { _ = sv.Stop(context.Background()) })

	if err := sv.Start(ctx); err != nil {
		t.Fatalf("启动假 rcd 失败: %v", err)
	}
	firstPID := sv.Status().PID
	if firstPID == 0 {
		t.Fatal("前提不成立：应已拉起子进程")
	}

	if err := sv.Restart(ctx); err != nil {
		t.Fatalf("手动重启失败: %v", err)
	}

	if got := sv.Status().Restarts; got != 0 {
		t.Fatalf("手动重启后自动重启计数应仍为 0（自己杀进程那次不该计入），实际 %d", got)
	}
	if got := len(sv.restartCh); got != 0 {
		t.Fatalf("不应残留待处理的自动重启令牌，实际 %d 个", got)
	}
	st := sv.Status()
	if !st.Ready || st.State != StateRunning || st.PID == 0 || st.PID == firstPID {
		t.Fatalf("手动重启应重新拉起一个新实例，实际 state=%s ready=%v pid=%d（旧 pid=%d）",
			st.State, st.Ready, st.PID, firstPID)
	}
}

// 反过来：实例**自己崩了**（不是我们杀的）必须照常排自动重启，
// 否则上面的 stopIntent 会把真正的故障也一起吞掉。
func TestGenuineCrashStillSchedulesAutoRestart(t *testing.T) {
	cfg := testRcloneConfig(freeAddr(t))
	cfg.Path = os.Args[0]
	sv := NewSupervisor(cfg, logging.Discard())
	t.Setenv(helperCrashEnv, "1")

	// 直接 spawn 而不走 Start：监管协程不在跑，排下的令牌会留在 restartCh 里可查
	// （走 Start 的话令牌会被监管协程立刻取走，断言就成了碰运气）。
	if err := sv.spawn(); err != nil {
		t.Fatalf("spawn 不应在这里失败: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(sv.restartCh) == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if len(sv.restartCh) == 0 {
		t.Fatalf("实例自己崩溃时必须照常排自动重启；blocked=%q journal=%v",
			sv.blockedReason(), sv.Journal().Lines())
	}
}

// auto_start=false 时 RESTART 仍然必须是「外部托管不可重启」，
// 不能因为新增的接管逻辑而变成可管理。
func TestRestartStillRejectsExternalInstance(t *testing.T) {
	f := newFakeRCD(t, 31)
	cfg := testRcloneConfig(f.addr)
	cfg.AutoStart = false
	cfg.Path = "definitely-not-a-real-rclone-binary"

	sv := NewSupervisor(cfg, logging.Discard())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := sv.Start(ctx); err != nil {
		t.Fatalf("连接既有 rcd 失败: %v", err)
	}
	if st := sv.Status(); !st.External || st.Adopted {
		t.Fatalf("auto_start=false 应标记为 external 而非 adopted: %+v", st)
	}
	if err := sv.Restart(ctx); err != ErrExternalRestart {
		t.Fatalf("期望 ErrExternalRestart，实际 %v", err)
	}
	if f.quitCount() != 0 {
		t.Fatal("外部托管实例绝不能被本程序关掉")
	}
}

// 清空日志（管理端按钮）之后，旧游标不能因为缓冲重置而错乱：
// 清空前标记的行会丢（这是"清空"的本意，且必须如实上报 overflow），
// 清空后写入的行必须照常被同一个游标取到——否则正在运行的任务会丢掉后续日志。
func TestJournalResetKeepsCursorMonotonic(t *testing.T) {
	buf := logging.NewRingBuffer(16)
	_, _ = buf.Write([]byte("第一行\n"))
	mark := buf.Mark()
	_, _ = buf.Write([]byte("第二行\n"))
	if lines, overflow := buf.Since(mark); len(lines) != 1 || lines[0] != "第二行" || overflow {
		t.Fatalf("期望只取到 mark 之后的输出且未滚动，实际 %v overflow=%v", lines, overflow)
	}

	buf.Reset()
	if n := buf.Len(); n != 0 {
		t.Fatalf("清空后行数应为 0，实际 %d", n)
	}
	_, _ = buf.Write([]byte("第三行\n"))
	lines, overflow := buf.Since(mark)
	if len(lines) != 1 || lines[0] != "第三行" {
		t.Fatalf("清空后新行仍应能被旧游标取到，实际 %v", lines)
	}
	if !overflow {
		t.Fatal("清空丢掉了游标之前的行，必须如实上报 overflow（调用方据此提示日志片段不完整）")
	}
}

// ---------------------------------------------------------------------------
// 不该出现第二个 rclone —— "点重启必失败"的真正原因
// ---------------------------------------------------------------------------
//
// 实测结论（用真实 rclone 在 Windows 上验证过）：
//   - 连接先关的一方会在监听端口上留下 TIME_WAIT，但**这不影响 bind**，
//     同一地址可以立刻被重新 bind，新起的 rclone 也活得好好的；
//   - "bind: Only one usage of each socket address" 只会在**此刻真的另有一个活着的
//     socket 绑在该地址上**时出现。
//
// 也就是说，这条报错几乎总是本程序自己造成的：手动重启与自动重启各拉了一个 rclone，
// 后到的那个去抢同一个 RC 地址。下面两个用例分别锁住"不该拉第二个"和"该拉时还是要拉"。

// 已经有一个活着的实例时，spawn 必须拒绝再拉一个。
//
// 这是本次修复的核心：旧实现没有任何"是否已有实例"的判断，于是
// 手动重启刚落起实例、队列里又压着一次失败重试时，2s 后会再拉一个 rclone，
// 后者 bind 失败 → 被记成一次启动失败 → 用户看到「点了重启，还是失败」。
func TestSpawnRefusesSecondInstanceWhileOneIsLive(t *testing.T) {
	cfg := testRcloneConfig(freeAddr(t))
	// 指向不存在的可执行文件：只要真的去拉起子进程，就一定会报错。
	cfg.Path = "definitely-not-a-real-rclone-binary"

	sv := NewSupervisor(cfg, logging.Discard())
	// 构造"已有一个活着的实例"：用本测试进程自己的 pid，它当然活着。
	sv.mu.Lock()
	sv.adopted = true
	sv.pid = os.Getpid()
	sv.mu.Unlock()

	if !sv.hasLiveInstance() {
		t.Fatal("前提不成立：应当被判定为已有活着的实例")
	}

	sv.journal.Reset()
	if err := sv.spawn(); err != nil {
		t.Fatalf("已有实例在跑时不该再去拉起子进程（那会撞 bind 失败）：%v", err)
	}
	if lines := strings.Join(sv.Journal().Lines(), "\n"); strings.Contains(lines, "正在启动 rclone rcd") {
		t.Fatalf("不该走到真正的启动流程:\n%s", lines)
	}
}

// 对照臂：没有实例在跑时，spawn 必须照常尝试启动——守卫不能把正常重启也挡掉。
func TestSpawnStillStartsWhenNoInstanceLive(t *testing.T) {
	cfg := testRcloneConfig(freeAddr(t))
	cfg.Path = "definitely-not-a-real-rclone-binary"

	sv := NewSupervisor(cfg, logging.Discard())
	if sv.hasLiveInstance() {
		t.Fatal("前提不成立：全新托管器不该被认为已有实例")
	}

	sv.journal.Reset()
	err := sv.spawn()
	if err == nil {
		t.Fatal("没有实例在跑时必须真的去启动（可执行文件不存在，应当报错）")
	}
	if !strings.Contains(err.Error(), "找不到 rclone 可执行文件") {
		t.Fatalf("错误应来自「找不到可执行文件」，说明确实走了启动流程，实际: %v", err)
	}
	if lines := strings.Join(sv.Journal().Lines(), "\n"); !strings.Contains(lines, "正在启动 rclone rcd") {
		t.Fatalf("应当留下启动记录:\n%s", lines)
	}
}

// 监管协程拿到一次"上一轮遗留的重试令牌"时，如果实例已经活着，绝不能再去拉一个。
//
// 这是用户那次故障的直接还原：重启成功拉起实例之后，队列里还压着上一次失败留下的
// 令牌，监管协程 2s 后醒来又 spawn 一个 → 抢同一个地址 → bind 失败 +
// "已停止自动重启" 告警，把一次成功的重启写成了失败。
func TestStaleRestartTokenDoesNotSpawnSecondInstance(t *testing.T) {
	cfg := testRcloneConfig(freeAddr(t))
	cfg.Path = "definitely-not-a-real-rclone-binary"

	sv := NewSupervisor(cfg, logging.Discard())
	sv.mu.Lock()
	sv.adopted = true
	sv.pid = os.Getpid()
	sv.mu.Unlock()

	sv.journal.Reset()
	select {
	case sv.restartCh <- struct{}{}:
	default:
		t.Fatal("前提不成立：应当能放入一个待处理的重启请求")
	}

	sv.superviseW.Add(1)
	done := make(chan struct{})
	go func() {
		sv.supervise()
		close(done)
	}()

	// 留足时间越过退避（配置里是 10ms）与地址探测，看它会不会偷偷拉起第二个。
	time.Sleep(600 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("监管协程不该退出")
	default:
	}

	if lines := strings.Join(sv.Journal().Lines(), "\n"); strings.Contains(lines, "正在启动 rclone rcd") {
		t.Fatalf("实例已活着时不该再拉起第二个 rclone:\n%s", lines)
	}
	if got := sv.blockedReason(); got != "" {
		t.Fatalf("这不该被判定为不可恢复（旧实现会在这里刷出「已停止自动重启」）: %q", got)
	}

	close(sv.stopped)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("监管协程未在 stopped 关闭后退出")
	}
}

// 「重启 rclone」遇到"地址被占着但无人应答"时要如实报"地址尚未释放"，
// 而不是硬去 bind（只会换来子进程启动失败）。
func TestRestartReportsAddressNotReleasedInsteadOfSpawning(t *testing.T) {
	// 占住地址但不做任何应答：既 bind 不了，也连不上（accept 后立刻关）。
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			_ = conn.Close()
		}
	}()

	cfg := testRcloneConfig(ln.Addr().String())
	cfg.Path = "definitely-not-a-real-rclone-binary"
	sv := NewSupervisor(cfg, logging.Discard())
	// 短 ctx：这段等待最长可达 addressWaitBudget，用例不必真等。
	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()

	err = sv.Restart(ctx)
	if err == nil {
		t.Fatal("地址被占用时 Restart 必须报错")
	}
	// 这个地址其实是"能连上但不是 rclone"，应当点明这一点，而不是含糊的超时。
	if !strings.Contains(err.Error(), "已被占用") {
		t.Fatalf("错误应说明地址被占用、且不是可用的 rcd，实际: %v", err)
	}
}

// 关闭 rclone 之后，RC 地址必须立刻可以重新 bind。
//
// 判据用 addrFree 而不是"端口连不上"：只有 bind 得成功，下一次启动才拉得起来。
// fakeRCD 在收到 core/quit 时会像真实 rclone 那样把连接一起关掉，因此这里也顺带
// 覆盖了"关闭前先断开我们自己这一侧"，不留半开的连接给下一个人。
func TestStopReleasesAddressPromptly(t *testing.T) {
	f := newFakeRCD(t, 8080)
	cfg := testRcloneConfig(f.addr)
	cfg.Path = "definitely-not-a-real-rclone-binary"

	sv := NewSupervisor(cfg, logging.Discard())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := sv.Start(ctx); err != nil {
		t.Fatalf("接管既有实例失败: %v", err)
	}
	// 多打几次 RC，确保连接池里真的躺着空闲连接：这样 Stop 才是在"有活跃连接"
	// 的真实条件下验证地址能否立刻被重新 bind，而不是碰巧因为没连接而通过。
	for i := 0; i < 3; i++ {
		_, _ = sv.Client().Version(ctx)
		_, _ = sv.Client().PID(ctx)
	}

	if err := sv.Stop(ctx); err != nil {
		t.Fatalf("Stop 失败: %v", err)
	}
	if free, err := addrFree(f.addr); !free {
		t.Fatalf("关闭 rclone 之后 RC 地址应当立即可以 bind（说明关闭前已断开我们这一侧），实际不能: %v", err)
	}
}
