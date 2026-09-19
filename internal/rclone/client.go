// Package rclone 封装 rclone RC (Remote Control) HTTP API，
// 并提供 rclone rcd 子进程的生命周期托管。
package rclone

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"cloudsync/internal/config"
)

// APIError 表示 RC API 返回的业务错误（HTTP 5xx 或 {"error": ...}）。
type APIError struct {
	Method string
	Path   string
	Status int
	Input  map[string]any
	Msg    string
}

func (e *APIError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "rclone rc %s 失败", e.Method)
	if e.Status != 0 {
		fmt.Fprintf(&b, " (HTTP %d)", e.Status)
	}
	if e.Msg != "" {
		b.WriteString(": ")
		b.WriteString(e.Msg)
	}
	if hint := explain(e.Msg, e.Input); hint != "" {
		b.WriteString(" → ")
		b.WriteString(hint)
	}
	return b.String()
}

// explain 把 rclone 的英文报错翻译为可操作的提示。
func explain(msg string, input map[string]any) string {
	low := strings.ToLower(msg)
	path, _ := input["fs"].(string)
	switch {
	case strings.Contains(low, "section in config file"),
		strings.Contains(low, "couldn't find remote"),
		strings.Contains(low, "didn't find section"):
		return "远程配置不存在，请检查任务中的 remote 名称是否已在 rclone.conf 中定义"
	case strings.Contains(low, "no such file or directory"):
		return "路径不存在，请检查源/目标路径"
	case strings.Contains(low, "permission denied"):
		return "权限不足，请检查 rclone 运行账户的读写权限"
	case strings.Contains(low, "error reading source root directory"):
		return "读取源目录失败，可能是凭据失效或网络异常"
	case strings.Contains(low, "not found"):
		return "远端资源未找到"
	case strings.Contains(low, "can't sync to itself"), strings.Contains(low, "same file"):
		return "源和目标不能相同"
	case strings.Contains(low, "failed to create file system"):
		return fmt.Sprintf("无法初始化文件系统 %q，请检查 remote 名称、凭据与网络", path)
	case strings.Contains(low, "context canceled"):
		return "操作已被取消"
	case strings.Contains(low, "authentication"), strings.Contains(low, "unauthorized"):
		return "RC API 认证失败，请检查 rclone.rc_user / rc_pass"
	default:
		return ""
	}
}

// Client 是 RC API 客户端。所有方法都是并发安全的。
type Client struct {
	base   *url.URL
	user   string
	pass   string
	hc     *http.Client
	logger *slog.Logger
}

// NewClient 根据配置创建客户端。
func NewClient(cfg config.RcloneConfig, logger *slog.Logger) *Client {
	if logger == nil {
		logger = slog.Default()
	}
	addr := cfg.RCAddr
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	u, err := url.Parse(addr)
	if err != nil {
		// normalize 阶段已确保 addr 非空；这里退化为字面量拼接。
		u = &url.URL{Scheme: "http", Host: cfg.RCAddr}
	}
	if u.Scheme == "" {
		u.Scheme = "http"
	}
	u.Path = ""
	u.RawQuery = ""

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          32,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	return &Client{
		base:   u,
		user:   cfg.RCUser,
		pass:   cfg.RCPass,
		logger: logger,
		hc: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		},
	}
}

// Endpoint 返回 RC API 的基地址，便于日志展示。
func (c *Client) Endpoint() string { return c.base.String() }

// CloseIdleConnections 关闭本客户端持有的空闲 keep-alive 连接。
//
// 用途是"让本进程手上不再留有指向已退出实例的连接"：留着的话，之后的 Ping/探测
// 会打到死连接上，把"实例已退出"误判成"还在应答"。
//
// 注意：这里**不是**为了躲 TIME_WAIT。实测（Windows，真实 rclone）TIME_WAIT
// **不会**阻止同一地址重新 bind——`netstat` 里确实会出现 pid=0 的 TIME_WAIT 条目，
// 但紧接着 `net.Listen` 同一地址照样成功。真正让 bind 失败的只有一个活着占着
// 地址的 socket，所以FIN 的先后顺序不必在这里纠结。
func (c *Client) CloseIdleConnections() {
	if c.hc != nil {
		c.hc.CloseIdleConnections()
	}
}

// Call 调用一个 RC 方法，params 会被编码为 JSON 请求体，响应解析到 out。
// out 为 nil 时忽略响应体。params 为 nil 时发送空对象。
func (c *Client) Call(ctx context.Context, method string, params map[string]any, out any) error {
	if params == nil {
		params = map[string]any{}
	}
	body, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("序列化 %s 参数: %w", method, err)
	}

	endpoint := c.base.JoinPath(method).String()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("构造 %s 请求: %w", method, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if c.user != "" || c.pass != "" {
		req.SetBasicAuth(c.user, c.pass)
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return fmt.Errorf("请求 rclone rc %s (%s): %w", method, endpoint, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		_ = resp.Body.Close()
	}()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return fmt.Errorf("读取 %s 响应: %w", method, err)
	}

	if resp.StatusCode != http.StatusOK {
		apiErr := &APIError{Method: method, Path: method, Status: resp.StatusCode}
		// rclone 的错误响应形如 {"error":"...","input":{...},"path":"...","status":500}
		var errBody struct {
			Error string         `json:"error"`
			Input map[string]any `json:"input"`
			Path  string         `json:"path"`
		}
		if json.Unmarshal(raw, &errBody) == nil {
			apiErr.Msg = errBody.Error
			apiErr.Input = errBody.Input
			if errBody.Path != "" {
				apiErr.Path = errBody.Path
			}
		}
		if apiErr.Msg == "" {
			apiErr.Msg = strings.TrimSpace(string(raw))
		}
		return apiErr
	}

	if out == nil {
		return nil
	}
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("解析 %s 响应: %w (原始内容: %s)", method, err, truncate(string(raw), 512))
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// ---------------------------------------------------------------------------
// 基础探测
// ---------------------------------------------------------------------------

// Version 是 core/version 的响应。
type Version struct {
	Version    string         `json:"version"`
	Decomposed []int          `json:"decomposed"`
	IsBeta     bool           `json:"isBeta"`
	IsGit      bool           `json:"isGit"`
	GoVersion  string         `json:"goVersion"`
	OS         string         `json:"os"`
	Arch       string         `json:"arch"`
	Metadata   map[string]any `json:"metadata"`
}

// Version 调用 core/version。
func (c *Client) Version(ctx context.Context) (*Version, error) {
	var v Version
	if err := c.Call(ctx, "core/version", nil, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// Ping 调用 rc/noop，用于健康检查。
func (c *Client) Ping(ctx context.Context) error {
	return c.Call(ctx, "rc/noop", nil, nil)
}

// PID 返回 rcd 进程号（core/pid）。
func (c *Client) PID(ctx context.Context) (int, error) {
	var out struct {
		PID int `json:"pid"`
	}
	if err := c.Call(ctx, "core/pid", nil, &out); err != nil {
		return 0, err
	}
	return out.PID, nil
}

// Quit 请求 rcd 主动退出（core/quit）。
func (c *Client) Quit(ctx context.Context) error {
	return c.Call(ctx, "core/quit", nil, nil)
}

// ---------------------------------------------------------------------------
// 统计
// ---------------------------------------------------------------------------

// Stats 是 core/stats 的响应，字段与 rclone 保持一致。
type Stats struct {
	Bytes            int64      `json:"bytes"`
	Checks           int64      `json:"checks"`
	Deletes          int64      `json:"deletes"`
	ElapsedTime      float64    `json:"elapsedTime"`
	Errors           int64      `json:"errors"`
	ETA              *float64   `json:"eta"`
	FatalError       bool       `json:"fatalError"`
	LastError        *string    `json:"lastError"`
	Listed           int64      `json:"listed"`
	Renames          int64      `json:"renames"`
	RetryError       bool       `json:"retryError"`
	ServerSideCopies int64      `json:"serverSideCopies"`
	ServerSideMoves  int64      `json:"serverSideMoves"`
	Speed            float64    `json:"speed"`
	TotalBytes       int64      `json:"totalBytes"`
	TotalChecks      int64      `json:"totalChecks"`
	TotalTransfers   int64      `json:"totalTransfers"`
	TransferTime     float64    `json:"transferTime"`
	Transfers        int64      `json:"transfers"`
	Group            string     `json:"group"`
	Transferring     []Transfer `json:"transferring"`
}

// Transfer 是正在传输的单个文件信息。
type Transfer struct {
	Name           string   `json:"name"`
	Size           int64    `json:"size"`
	Bytes          int64    `json:"bytes"`
	Speed          float64  `json:"speed"`
	SpeedAvg       float64  `json:"speedAvg"`
	ETA            *float64 `json:"eta"`
	Percentage     int      `json:"percentage"`
	Group          string   `json:"group"`
	ServerSideCopy bool     `json:"serverSideCopy"`
	ErrorMessage   string   `json:"error"`
}

// Stats 查询统计信息。group 非空时只查询该组的统计。
func (c *Client) Stats(ctx context.Context, group string) (*Stats, error) {
	params := map[string]any{}
	if group != "" {
		params["group"] = group
	}
	var s Stats
	if err := c.Call(ctx, "core/stats", params, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// StatsReset 重置统计。group 非空时只重置该组。
func (c *Client) StatsReset(ctx context.Context, group string) error {
	params := map[string]any{}
	if group != "" {
		params["group"] = group
	}
	return c.Call(ctx, "core/stats-reset", params, nil)
}

// GroupList 返回当前存在的统计组名（core/group-list）。
func (c *Client) GroupList(ctx context.Context) ([]string, error) {
	var out struct {
		Groups []string `json:"groups"`
	}
	if err := c.Call(ctx, "core/group-list", nil, &out); err != nil {
		return nil, err
	}
	return out.Groups, nil
}

// ---------------------------------------------------------------------------
// 异步任务（job）
// ---------------------------------------------------------------------------

// JobStatus 是 job/status 的响应。
type JobStatus struct {
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
	Progress []Transfer     `json:"progress"`
}

// IDOrJobID 返回可用的 job id。
func (j *JobStatus) IDOrJobID() int64 {
	if j.ID != 0 {
		return j.ID
	}
	return j.JobID
}

// StartAsync 以 _async 方式启动一个 RC 方法，返回 rclone 侧 job id。
// group 非空时同时下发 _group，便于后续按组查询统计。
func (c *Client) StartAsync(ctx context.Context, method string, params map[string]any, group string) (int64, error) {
	if params == nil {
		params = map[string]any{}
	}
	params["_async"] = true
	if group != "" {
		params["_group"] = group
	}
	var out struct {
		JobID int64 `json:"jobid"`
		ID    int64 `json:"id"`
	}
	if err := c.Call(ctx, method, params, &out); err != nil {
		return 0, err
	}
	if out.JobID != 0 {
		return out.JobID, nil
	}
	if out.ID != 0 {
		return out.ID, nil
	}
	return 0, fmt.Errorf("rclone rc %s 未返回 jobid", method)
}

// JobStatus 查询 job 状态。
func (c *Client) JobStatus(ctx context.Context, jobID int64) (*JobStatus, error) {
	var st JobStatus
	if err := c.Call(ctx, "job/status", map[string]any{"jobid": jobID}, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

// JobList 返回全部未过期的 job id。
func (c *Client) JobList(ctx context.Context) ([]int64, error) {
	var out struct {
		JobIDs []int64 `json:"jobids"`
	}
	if err := c.Call(ctx, "job/list", nil, &out); err != nil {
		return nil, err
	}
	return out.JobIDs, nil
}

// StopJob 请求停止指定 job（job/stop），其内部会取消对应的 context。
func (c *Client) StopJob(ctx context.Context, jobID int64) error {
	return c.Call(ctx, "job/stop", map[string]any{"jobid": jobID}, nil)
}

// StopGroup 停止一个统计组下的全部 job（job/stopgroup）。
func (c *Client) StopGroup(ctx context.Context, group string) error {
	return c.Call(ctx, "job/stopgroup", map[string]any{"group": group}, nil)
}

// ---------------------------------------------------------------------------
// 配置与文件系统
// ---------------------------------------------------------------------------

// ListRemotes 返回 rclone 配置中的 remote 名称（config/listremotes）。
func (c *Client) ListRemotes(ctx context.Context) ([]string, error) {
	var out struct {
		Remotes []string `json:"remotes"`
	}
	if err := c.Call(ctx, "config/listremotes", nil, &out); err != nil {
		return nil, err
	}
	return out.Remotes, nil
}

// ListItem 是 operations/list 返回的条目。
type ListItem struct {
	Path     string `json:"Path"`
	Name     string `json:"Name"`
	Size     int64  `json:"Size"`
	MimeType string `json:"MimeType"`
	ModTime  string `json:"ModTime"`
	IsDir    bool   `json:"IsDir"`
	ID       string `json:"ID"`
}

// List 列出目录内容（operations/list）。
func (c *Client) List(ctx context.Context, fsName, remote string, recurse bool) ([]ListItem, error) {
	params := map[string]any{"fs": fsName}
	if remote != "" {
		params["remote"] = remote
	}
	if recurse {
		params["recurse"] = true
	}
	var out struct {
		List []ListItem `json:"list"`
	}
	if err := c.Call(ctx, "operations/list", params, &out); err != nil {
		return nil, err
	}
	return out.List, nil
}

// About 返回远端容量信息（operations/about）。
type About struct {
	Total   int64 `json:"total"`
	Used    int64 `json:"used"`
	Free    int64 `json:"free"`
	Trashed int64 `json:"trashed"`
	Other   int64 `json:"other"`
}

// About 查询 fs 的容量信息。
func (c *Client) About(ctx context.Context, fsName string) (*About, error) {
	var out About
	if err := c.Call(ctx, "operations/about", map[string]any{"fs": fsName}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// SetOptions 下发全局选项（options/set）。section 通常为 "main"。
func (c *Client) SetOptions(ctx context.Context, section string, opts map[string]any) error {
	if len(opts) == 0 {
		return nil
	}
	if section == "" {
		section = "main"
	}
	return c.Call(ctx, "options/set", map[string]any{section: opts}, nil)
}

// GetOptions 读取选项（options/get），section 为空时返回全部。
func (c *Client) GetOptions(ctx context.Context, section string) (map[string]any, error) {
	var out map[string]any
	if err := c.Call(ctx, "options/get", nil, &out); err != nil {
		return nil, err
	}
	if section == "" {
		return out, nil
	}
	if v, ok := out[section].(map[string]any); ok {
		return v, nil
	}
	return nil, nil
}

// MemStats 返回 rclone 进程内存占用（core/memstats）。
type MemStats struct {
	Alloc      int64 `json:"Alloc"`
	TotalAlloc int64 `json:"TotalAlloc"`
	Sys        int64 `json:"Sys"`
	NumGC      int64 `json:"NumGC"`
}

// MemStats 调用 core/memstats。
func (c *Client) MemStats(ctx context.Context) (*MemStats, error) {
	var m MemStats
	if err := c.Call(ctx, "core/memstats", nil, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// MethodForKind 将任务类型映射为 RC 方法名。
func MethodForKind(kind string) (string, error) {
	switch strings.ToLower(kind) {
	case "sync":
		return "sync/sync", nil
	case "copy":
		return "sync/copy", nil
	case "move":
		return "sync/move", nil
	case "bisync":
		return "sync/bisync", nil
	case "check":
		return "sync/check", nil
	case "delete":
		return "operations/delete", nil
	case "purge":
		return "operations/purge", nil
	case "mkdir":
		return "operations/mkdir", nil
	default:
		return "", fmt.Errorf("不支持的任务类型 %q", kind)
	}
}

// SplitRemote 把 rclone 路径拆成「远端名」与「远端内路径」。
//
// RC 的部分方法（如 operations/list、operations/deletefile）要求把两者分开传：
// fs 只接受形如 "115Drive:" 的远端名，目录与文件名要放进 remote。
// 而任务里的 Dest 是合在一起的 "115Drive:音乐/自收集"。
//
// 只在**第一个**冒号处切分：Windows 绝对路径含盘符（C:\...），
// rclone 的本地路径写法是 "C:/..." 或 "/..."，冒号出现在盘符之后时不能按远端解析。
func SplitRemote(remote string) (fs, path string, err error) {
	remote = strings.TrimSpace(remote)
	if remote == "" {
		return "", "", fmt.Errorf("路径为空")
	}
	i := strings.Index(remote, ":")
	if i < 0 {
		return "", "", fmt.Errorf("路径 %q 缺少远端分隔符 \":\"", remote)
	}
	// 盘符场景："C:/xxx" 的冒号在下标 1，且其后紧跟路径分隔符，按本地路径处理。
	if i == 1 && strings.ContainsAny(remote[0:1], `ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz`) {
		return "", "", fmt.Errorf("路径 %q 是本地路径，没有远端名", remote)
	}
	fs = remote[:i+1]
	path = strings.TrimPrefix(remote[i+1:], "/")
	return fs, path, nil
}

// ParamNameForKind 返回任务类型对应的源参数名。
// 注意 rclone 各方法参数名不一致：sync/bisync 用 path1/path2，
// 其余用 srcFs/dstFs，operations/* 用 fs。
func ParamNamesForKind(kind string) (srcParam, dstParam string) {
	switch strings.ToLower(kind) {
	case "sync", "copy", "move", "check":
		return "srcFs", "dstFs"
	case "bisync":
		return "path1", "path2"
	default:
		return "fs", "fs"
	}
}

// ParseFlags 保留占位：便于未来支持从字符串解析 extra_flags。
func ParseFlags(s string) (map[string]any, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	out := map[string]any{}
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, fmt.Errorf("解析参数字符串: %w", err)
	}
	return out, nil
}

// Int64Ptr 是便捷构造。
func Int64Ptr(v int64) *int64 { return &v }

// FormatBytes 将字节数格式化为人类可读字符串。
func FormatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return strconv.FormatInt(n, 10) + " B"
	}
	units := []string{"KiB", "MiB", "GiB", "TiB", "PiB"}
	value := float64(n)
	idx := -1
	for value >= unit && idx < len(units)-1 {
		value /= unit
		idx++
	}
	return strconv.FormatFloat(value, 'f', 1, 64) + " " + units[idx]
}
