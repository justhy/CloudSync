// Command rclone-mock 是一个最小化的 rclone rcd 模拟器。
//
// 用途：在未安装 rclone 的环境下验证宿主程序的完整链路
// （子进程拉起、RC API 调用、异步 job 与进度轮询、取消、优雅退出）。
//
// 用法（与真实 rclone 的 rcd 参数兼容）:
//
//	rclone-mock rcd --rc-addr 127.0.0.1:5572 --rc-user u --rc-pass p [--rc-no-auth]
//
// 本程序只识别下面声明的少数几个 flag，其余参数一律忽略。这样当宿主程序
// （internal/rclone）追加真实 rclone 才有的参数（如 --rc-job-expire-duration）
// 时，mock 不会因“flag provided but not defined”直接退出。行为由 stripUnknownFlags
// 实现，回归测试见 main_test.go。
//
// 可通过环境变量控制行为：
//
//	MOCK_DURATION_MS  单次任务模拟耗时（默认 6000）
//	MOCK_TOTAL_BYTES  模拟总字节数（默认 100MiB）
//	MOCK_FAIL         非空时，所有传输任务最终失败，其内容作为错误信息
//	MOCK_DELAY_MS     就绪前延迟毫秒，用于测试启动健康检查重试（默认 0）
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func envInt(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

// ---------------------------------------------------------------------------
// 状态模型
// ---------------------------------------------------------------------------

type groupStats struct {
	Bytes            int64    `json:"bytes"`
	TotalBytes       int64    `json:"totalBytes"`
	Checks           int64    `json:"checks"`
	TotalChecks      int64    `json:"totalChecks"`
	Transfers        int64    `json:"transfers"`
	TotalTransfers   int64    `json:"totalTransfers"`
	Errors           int64    `json:"errors"`
	Renames          int64    `json:"renames"`
	Deletes          int64    `json:"deletes"`
	ElapsedTime      float64  `json:"elapsedTime"`
	Speed            float64  `json:"speed"`
	ETA              *float64 `json:"eta"`
	FatalError       bool     `json:"fatalError"`
	ServerSideCopies int64    `json:"serverSideCopies"`
	ServerSideMoves  int64    `json:"serverSideMoves"`
	TransferTime     float64  `json:"transferTime"`
	Listed           int64    `json:"listed"`
	LastError        *string  `json:"lastError"`
	Group            string   `json:"group"`
	started          time.Time
	done             bool
}

type jobState struct {
	ID       int64          `json:"id"`
	JobID    int64          `json:"jobid"`
	Group    string         `json:"group"`
	Finished bool           `json:"finished"`
	Success  bool           `json:"success"`
	Error    string         `json:"error"`
	Duration float64        `json:"duration"`
	StartAt  time.Time      `json:"startTime"`
	EndAt    time.Time      `json:"endTime"`
	Output   map[string]any `json:"output"`
	Progress []any          `json:"progress"`

	mu         sync.Mutex
	cancel     chan struct{}
	cancelOnce sync.Once
	cancelled  bool
}

// stop 幂等地关闭取消通道。
func (j *jobState) stop() {
	j.mu.Lock()
	j.cancelled = true
	j.mu.Unlock()
	j.cancelOnce.Do(func() { close(j.cancel) })
}

type server struct {
	mu        sync.Mutex
	nextJobID int64
	jobs      map[int64]*jobState
	groups    map[string]*groupStats
	user      string
	pass      string
	noAuth    bool
	quit      chan struct{}
	quitOnce  sync.Once
}

func (s *server) createJob(group string, cancel chan struct{}) *jobState {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextJobID++
	j := &jobState{
		ID:       s.nextJobID,
		JobID:    s.nextJobID,
		Group:    group,
		StartAt:  time.Now().UTC(),
		Output:   map[string]any{},
		Progress: []any{},
		cancel:   cancel,
	}
	s.jobs[j.ID] = j
	if group != "" {
		if _, ok := s.groups[group]; !ok {
			s.groups[group] = &groupStats{Group: group, started: time.Now()}
		}
	}
	return j
}

func (s *server) group(group string) *groupStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	if group == "" {
		return nil
	}
	g, ok := s.groups[group]
	if !ok {
		g = &groupStats{Group: group, started: time.Now()}
		s.groups[group] = g
	}
	return g
}

// ---------------------------------------------------------------------------
// HTTP 入口
// ---------------------------------------------------------------------------

// rpcError 模拟 rclone 的错误响应体。
type rpcError struct {
	Error  string         `json:"error"`
	Input  map[string]any `json:"input"`
	Path   string         `json:"path"`
	Status int            `json:"status"`
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !s.authorized(r) {
		w.Header().Set("WWW-Authenticate", `Basic realm="rclone"`)
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"Unauthorized"}`))
		return
	}

	method := strings.TrimPrefix(r.URL.Path, "/")
	raw, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	params := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &params); err != nil {
			writeRPCError(w, method, params, fmt.Sprintf("cannot unmarshal params: %v", err))
			return
		}
	}
	// 允许通过 query string 传参（rclone 同时支持两种方式）。
	for k, vs := range r.URL.Query() {
		if len(vs) > 0 && k != "" {
			params[k] = vs[0]
		}
	}

	if b, ok := params["_async"].(string); ok {
		params["_async"] = b == "true" || b == "1"
	}

	out, err := s.dispatch(method, params)
	if err != nil {
		writeRPCError(w, method, params, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *server) authorized(r *http.Request) bool {
	if s.noAuth {
		return true
	}
	if s.user == "" && s.pass == "" {
		return true
	}
	u, p, ok := r.BasicAuth()
	if !ok {
		return false
	}
	return u == s.user && p == s.pass
}

func writeRPCError(w http.ResponseWriter, method string, params map[string]any, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	_ = json.NewEncoder(w).Encode(rpcError{Error: msg, Input: params, Path: method, Status: 500})
}

func strParam(p map[string]any, key string) string {
	v, ok := p[key]
	if !ok || v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

func (s *server) dispatch(method string, params map[string]any) (map[string]any, error) {
	switch method {
	case "rc/noop":
		return map[string]any{}, nil
	case "rc/error":
		return nil, fmt.Errorf("this is an error")
	case "core/version":
		return map[string]any{
			"version":    "v1.99.0-mock",
			"decomposed": []int{1, 99, 0},
			"goVersion":  "go1.23",
			"os":         "windows",
			"arch":       "amd64",
			"isBeta":     false,
			"isGit":      false,
			"metadata":   map[string]any{},
		}, nil
	case "core/pid":
		return map[string]any{"pid": os.Getpid()}, nil
	case "core/memstats":
		return map[string]any{"Alloc": 6_400_000, "TotalAlloc": 12_000_000, "Sys": 9_000_000, "NumGC": 3}, nil
	case "core/quit":
		go func() {
			time.Sleep(120 * time.Millisecond)
			s.quitOnce.Do(func() { close(s.quit) })
		}()
		return map[string]any{}, nil
	case "core/stats":
		return s.statsFor(strParam(params, "group")), nil
	case "core/stats-reset":
		s.mu.Lock()
		if g := strParam(params, "group"); g != "" {
			delete(s.groups, g)
		} else {
			s.groups = map[string]*groupStats{}
		}
		s.mu.Unlock()
		return map[string]any{}, nil
	case "core/group-list":
		s.mu.Lock()
		names := make([]string, 0, len(s.groups))
		for name := range s.groups {
			names = append(names, name)
		}
		s.mu.Unlock()
		return map[string]any{"groups": names}, nil
	case "job/list":
		s.mu.Lock()
		ids := make([]int64, 0, len(s.jobs))
		for id := range s.jobs {
			ids = append(ids, id)
		}
		s.mu.Unlock()
		return map[string]any{"jobids": ids}, nil
	case "job/status":
		return s.jobStatus(params)
	case "job/stop":
		return s.stopJob(params)
	case "job/stopgroup":
		return s.stopGroup(strParam(params, "group"))
	case "config/listremotes":
		return map[string]any{"remotes": []string{"mockremote", "gdrive", "s3backup"}}, nil
	case "options/get":
		return map[string]any{"main": map[string]any{
			"transfers": 4, "checkers": 8, "retries": 3, "bwlimit": "",
			"dry-run": false, "log-level": "INFO", "checksum": false,
		}}, nil
	case "options/set":
		return map[string]any{}, nil
	case "operations/list":
		fsName := strParam(params, "fs")
		if fsName == "" {
			return nil, fmt.Errorf("failed to create file system for \"\": didn't find section in config file")
		}
		if strings.HasPrefix(fsName, "missing") {
			return nil, fmt.Errorf("failed to create file system for %q: didn't find section in config file", fsName)
		}
		return map[string]any{"list": []map[string]any{
			{"Path": "a.txt", "Name": "a.txt", "Size": 1024, "IsDir": false, "ModTime": time.Now().Format(time.RFC3339)},
			{"Path": "sub", "Name": "sub", "Size": -1, "IsDir": true, "ModTime": time.Now().Format(time.RFC3339)},
		}}, nil
	case "operations/about":
		return map[string]any{"total": 100 << 30, "used": 42 << 30, "free": 58 << 30, "trashed": 0, "other": 0}, nil
	}

	if fn, ok := transferMethods[method]; ok {
		return s.startTransfer(method, fn, params)
	}
	return nil, fmt.Errorf("couldn't find method %q", method)
}

// transferMethods 定义传输类方法的必需参数与模拟特征。
var transferMethods = map[string]struct {
	required []string
	// kind 决定统计的字段组合。
	kind string
}{
	"sync/sync":         {required: []string{"srcFs", "dstFs"}, kind: "transfer"},
	"sync/copy":         {required: []string{"srcFs", "dstFs"}, kind: "transfer"},
	"sync/move":         {required: []string{"srcFs", "dstFs"}, kind: "transfer"},
	"sync/check":        {required: []string{"srcFs", "dstFs"}, kind: "check"},
	"sync/bisync":       {required: []string{"path1", "path2"}, kind: "transfer"},
	"operations/delete": {required: []string{"fs"}, kind: "delete"},
	"operations/purge":  {required: []string{"fs"}, kind: "delete"},
	"operations/mkdir":  {required: []string{"fs"}, kind: "mkdir"},
}

func (s *server) startTransfer(method string, spec struct {
	required []string
	kind     string
}, params map[string]any) (map[string]any, error) {
	for _, key := range spec.required {
		if v := strParam(params, key); strings.TrimSpace(v) == "" {
			return nil, fmt.Errorf("missing parameter %q", key)
		}
	}
	for _, key := range []string{"srcFs", "dstFs", "path1", "path2", "fs"} {
		if v := strParam(params, key); strings.Contains(v, "nonexistent") {
			return nil, fmt.Errorf("failed to create file system for %q: didn't find section in config file", v)
		}
	}

	group := strParam(params, "_group")
	async := false
	if b, ok := params["_async"].(bool); ok {
		async = b
	}

	cancel := make(chan struct{})
	j := s.createJob(group, cancel)

	run := func() {
		s.runTransfer(j, spec.kind, method, params)
	}

	if !async {
		run()
		j.mu.Lock()
		okNow, errMsg := j.Finished && j.Success, j.Error
		j.mu.Unlock()
		if !okNow {
			return nil, fmt.Errorf("%s", orElse(errMsg, "sync failed"))
		}
		return map[string]any{}, nil
	}

	go run()
	return map[string]any{"jobid": j.ID}, nil
}

func orElse(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// runTransfer 模拟一次传输，按固定节拍推进进度。
func (s *server) runTransfer(j *jobState, kind, method string, params map[string]any) {
	duration := envInt("MOCK_DURATION_MS", 6000)
	totalBytes := envInt("MOCK_TOTAL_BYTES", 100<<20)
	failMsg := os.Getenv("MOCK_FAIL")
	dryRun := false
	if v, ok := params["dry_run"].(bool); ok {
		dryRun = v
	}
	if dryRun {
		failMsg = "" // dry-run 不会真正失败
	}

	steps := 12
	stepDelay := time.Duration(duration) * time.Millisecond / time.Duration(steps)
	g := s.group(j.Group)

	for i := 1; i <= steps; i++ {
		select {
		case <-j.cancel:
			j.mu.Lock()
			j.cancelled = true
			j.mu.Unlock()
			s.finishJob(j, false, "context canceled")
			return
		case <-s.quit:
			s.finishJob(j, false, "program terminated")
			return
		case <-time.After(stepDelay):
		}

		if g != nil {
			s.mu.Lock()
			switch kind {
			case "check":
				g.Checks = int64(i * 10)
				g.TotalChecks = int64(steps * 10)
			case "delete":
				g.Deletes = int64(i)
				g.TotalTransfers = int64(steps)
				g.Transfers = int64(i)
			case "mkdir":
				g.Listed = 1
			default:
				g.Bytes = totalBytes * int64(i) / int64(steps)
				g.TotalBytes = totalBytes
				g.Transfers = int64(i) * 3
				g.TotalTransfers = int64(steps) * 3
				g.Checks = int64(i) * 5
				g.Renames = int64(i) / 4
				g.Deletes = int64(i) / 6
				g.ServerSideCopies = int64(i) / 5
			}
			elapsed := time.Since(g.started).Seconds()
			g.ElapsedTime = elapsed
			if elapsed > 0 {
				g.Speed = float64(g.Bytes) / elapsed
			}
			if g.Speed > 0 && g.TotalBytes > g.Bytes {
				eta := float64(g.TotalBytes-g.Bytes) / g.Speed
				g.ETA = &eta
			}
			s.mu.Unlock()
		}
		fmt.Printf("%s: transferred %d / %d bytes\n", method, totalBytes*int64(i)/int64(steps), totalBytes)
	}

	if failMsg != "" {
		if g != nil {
			s.mu.Lock()
			g.Errors = 3
			g.FatalError = true
			g.LastError = &failMsg
			s.mu.Unlock()
		}
		s.finishJob(j, false, failMsg)
		return
	}
	s.finishJob(j, true, "")
}

// finishJob 收敛 job 终态。
func (s *server) finishJob(j *jobState, success bool, errMsg string) {
	j.mu.Lock()
	j.Finished = true
	j.Success = success
	j.Error = errMsg
	j.EndAt = time.Now().UTC()
	j.Duration = j.EndAt.Sub(j.StartAt).Seconds()
	if !success && errMsg != "" {
		j.Output["error"] = errMsg
	}
	group := j.Group
	j.mu.Unlock()

	if g := s.group(group); g != nil {
		s.mu.Lock()
		g.done = true
		s.mu.Unlock()
	}
}

// snapshot 返回可在锁外序列化的副本。
func (j *jobState) snapshot() map[string]any {
	j.mu.Lock()
	defer j.mu.Unlock()
	return map[string]any{
		"id":        j.ID,
		"jobid":     j.ID,
		"group":     j.Group,
		"finished":  j.Finished,
		"success":   j.Success,
		"error":     j.Error,
		"duration":  j.Duration,
		"startTime": j.StartAt,
		"endTime":   j.EndAt,
		"output":    j.Output,
		"progress":  j.Progress,
	}
}

func (s *server) jobStatus(params map[string]any) (map[string]any, error) {
	raw := strParam(params, "jobid")
	if raw == "" {
		return nil, fmt.Errorf("missing parameter \"jobid\"")
	}
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("couldn't parse jobid: %v", err)
	}
	s.mu.Lock()
	j, ok := s.jobs[id]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("job not found: %d", id)
	}
	return j.snapshot(), nil
}

func (s *server) stopJob(params map[string]any) (map[string]any, error) {
	raw := strParam(params, "jobid")
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("missing or invalid jobid")
	}
	s.mu.Lock()
	j, ok := s.jobs[id]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("job not found: %d", id)
	}
	j.stop()
	return map[string]any{}, nil
}

func (s *server) stopGroup(group string) (map[string]any, error) {
	s.mu.Lock()
	targets := make([]*jobState, 0)
	for _, j := range s.jobs {
		if j.Group == group {
			targets = append(targets, j)
		}
	}
	s.mu.Unlock()
	for _, j := range targets {
		j.stop()
	}
	return map[string]any{}, nil
}

func (s *server) statsFor(group string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()

	if group != "" {
		g, ok := s.groups[group]
		if !ok {
			return emptyStats(group)
		}
		return statsMap(g)
	}

	// 无 group 时聚合全部。
	agg := &groupStats{started: time.Now()}
	for _, g := range s.groups {
		agg.Bytes += g.Bytes
		agg.TotalBytes += g.TotalBytes
		agg.Checks += g.Checks
		agg.TotalChecks += g.TotalChecks
		agg.Transfers += g.Transfers
		agg.TotalTransfers += g.TotalTransfers
		agg.Errors += g.Errors
		agg.Renames += g.Renames
		agg.Deletes += g.Deletes
		agg.ServerSideCopies += g.ServerSideCopies
		agg.ServerSideMoves += g.ServerSideMoves
		if g.ElapsedTime > agg.ElapsedTime {
			agg.ElapsedTime = g.ElapsedTime
		}
		agg.Speed += g.Speed
	}
	return statsMap(agg)
}

func statsMap(g *groupStats) map[string]any {
	return map[string]any{
		"bytes":            g.Bytes,
		"totalBytes":       g.TotalBytes,
		"checks":           g.Checks,
		"totalChecks":      g.TotalChecks,
		"transfers":        g.Transfers,
		"totalTransfers":   g.TotalTransfers,
		"errors":           g.Errors,
		"renames":          g.Renames,
		"deletes":          g.Deletes,
		"elapsedTime":      g.ElapsedTime,
		"speed":            g.Speed,
		"eta":              g.ETA,
		"fatalError":       g.FatalError,
		"serverSideCopies": g.ServerSideCopies,
		"serverSideMoves":  g.ServerSideMoves,
		"transferTime":     g.ElapsedTime,
		"listed":           g.Listed,
		"lastError":        g.LastError,
		"group":            g.Group,
		"transferring":     []any{},
	}
}

func emptyStats(group string) map[string]any {
	return statsMap(&groupStats{Group: group, started: time.Now()})
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

// options 保存 rclone-mock 自身关心的命令行参数。
type options struct {
	rcAddr   string
	rcUser   string
	rcPass   string
	noAuth   bool
	logLevel string
}

// newFlagSet 在本程序实现的范围内声明与真实 rclone 同名的 flag。
//
// 单独抽出来是为了让 stripUnknownFlags 的测试能针对真实 flag 集合运行，
// 避免测试里另抄一份声明导致两边漂移。
func newFlagSet(o *options) *flag.FlagSet {
	fs := flag.NewFlagSet("rclone-mock", flag.ContinueOnError)
	fs.StringVar(&o.rcAddr, "rc-addr", "127.0.0.1:5572", "RC 监听地址")
	fs.StringVar(&o.rcUser, "rc-user", "", "RC 用户名")
	fs.StringVar(&o.rcPass, "rc-pass", "", "RC 密码")
	fs.BoolVar(&o.noAuth, "rc-no-auth", false, "禁用 RC 认证")
	fs.StringVar(&o.logLevel, "log-level", "INFO", "日志级别（占位）")
	return fs
}

// boolFlagger 由 flag 包内建的布尔标志实现（IsBoolFlag 返回 true）。
type boolFlagger interface{ IsBoolFlag() bool }

// takesValue 判断某个 flag 是否需要紧跟一个取值。
//
// flag 包没有导出这个信息，但标准库自身就是靠 IsBoolFlag 区分布尔标志的，
// 这里复用同一约定，避免手工维护一份 flag 名单。
func takesValue(f *flag.Flag) bool {
	if bf, ok := f.Value.(boolFlagger); ok {
		return !bf.IsBoolFlag()
	}
	return true
}

// stripUnknownFlags 丢弃本程序不认识的 flag，保留位置参数与已知 flag（含其取值）。
//
// 动机：宿主程序传给 `rclone rcd` 的合法参数会随 rclone 演进增加，而 mock 只实现
// 其中几个 RC 端点。若把整条命令行原样交给 flag.Parse，任何未知 flag 都会导致
// 退出码 2、进程秒退，宿主便只能看到 "context deadline exceeded" —— 排查成本很高。
//
// 处理规则：
//   - 非 `-` 开头：视为位置参数（含 rcd 子命令），原样保留。
//   - 已知 flag：保留；需要取值的，连带下一个 token 一起保留。
//   - 未知 flag：丢弃；若形如 `--flag value`（下一个 token 不以 `-` 开头），
//     把取值也一并丢弃，避免它变成位置参数后使 flag 包提前停止解析。
func stripUnknownFlags(fs *flag.FlagSet, args []string) []string {
	known := make(map[string]bool)
	fs.VisitAll(func(f *flag.Flag) { known[f.Name] = takesValue(f) })

	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "-") {
			out = append(out, a)
			continue
		}
		name := strings.TrimLeft(a, "-")
		hasInlineValue := false
		if idx := strings.IndexByte(name, '='); idx >= 0 {
			name, hasInlineValue = name[:idx], true
		}
		wantValue, ok := known[name]
		if !ok {
			if !hasInlineValue && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				i++
			}
			continue
		}
		out = append(out, a)
		if wantValue && !hasInlineValue && i+1 < len(args) {
			i++
			out = append(out, args[i])
		}
	}
	return out
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("mock-rclone: ")

	// 兼容真实 rclone 的命令行：第一个参数是子命令 rcd。
	args := os.Args[1:]
	var opts options
	fs := newFlagSet(&opts)

	var command string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		command = args[0]
		args = args[1:]
	}
	fs.SetOutput(io.Discard)
	args = stripUnknownFlags(fs, args)
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "参数解析失败: %v\n", err)
		os.Exit(2)
	}
	// 忽略未知位置参数，保证与真实 rclone 的参数演进兼容。
	_ = fs.NArg()

	if command != "rcd" {
		fmt.Fprintf(os.Stderr, "rclone-mock 只模拟 rcd 子命令，收到 %q\n", command)
		os.Exit(2)
	}

	if d := envInt("MOCK_DELAY_MS", 0); d > 0 {
		log.Printf("模拟启动延迟 %dms", d)
		time.Sleep(time.Duration(d) * time.Millisecond)
	}

	srv := &server{
		jobs:   map[int64]*jobState{},
		groups: map[string]*groupStats{},
		user:   opts.rcUser,
		pass:   opts.rcPass,
		noAuth: opts.noAuth,
		quit:   make(chan struct{}),
	}

	log.Printf("mock rclone rcd 启动，监听 %s（user=%q noauth=%v level=%s）",
		opts.rcAddr, opts.rcUser, opts.noAuth, opts.logLevel)
	log.Printf("使用 %d 字节 / %dms 模拟单次传输", envInt("MOCK_TOTAL_BYTES", 100<<20), envInt("MOCK_DURATION_MS", 6000))

	httpSrv := &http.Server{
		Addr:              opts.rcAddr,
		Handler:           srv,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errc := make(chan error, 1)
	go func() { errc <- httpSrv.ListenAndServe() }()

	select {
	case err := <-errc:
		if err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "监听失败: %v\n", err)
			os.Exit(1)
		}
	case <-srv.quit:
		log.Printf("收到 core/quit，正在退出")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)
	log.Printf("mock rclone 已退出")
}
