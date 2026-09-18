package rclone

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cloudsync/internal/config"
	"cloudsync/internal/logging"
)

// fakeRC 是一个可编程的 RC 服务端，用于验证客户端的请求构造与错误处理。
type fakeRC struct {
	t           *testing.T
	gotMethod   string
	gotParams   map[string]any
	gotAuthUser string
	gotAuthPass string
	gotAuthOK   bool

	respond func(w http.ResponseWriter, r *http.Request, method string, params map[string]any)
}

func (f *fakeRC) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	method := strings.TrimPrefix(r.URL.Path, "/")
	f.gotMethod = method

	body := map[string]any{}
	_ = json.NewDecoder(r.Body).Decode(&body)
	for k, vs := range r.URL.Query() {
		if len(vs) > 0 {
			body[k] = vs[0]
		}
	}
	f.gotParams = body
	f.gotAuthUser, f.gotAuthPass, f.gotAuthOK = r.BasicAuth()

	if f.respond != nil {
		f.respond(w, r, method, body)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{}`))
}

func newTestClient(t *testing.T, rc *fakeRC) (*Client, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(rc)
	t.Cleanup(srv.Close)

	cfg := config.Default().Rclone
	cfg.RCAddr = strings.TrimPrefix(srv.URL, "http://")
	cfg.RCUser = "u1"
	cfg.RCPass = "p1"
	return NewClient(cfg, logging.Discard()), srv
}

func writeJSONResp(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

func TestVersionAndAuth(t *testing.T) {
	rc := &fakeRC{t: t, respond: func(w http.ResponseWriter, r *http.Request, method string, params map[string]any) {
		writeJSONResp(w, 200, `{"version":"v1.65.0","isBeta":false,"goVersion":"go1.22","os":"linux","arch":"arm64"}`)
	}}
	c, _ := newTestClient(t, rc)

	v, err := c.Version(context.Background())
	if err != nil {
		t.Fatalf("Version 失败: %v", err)
	}
	if v.Version != "v1.65.0" || v.OS != "linux" {
		t.Errorf("版本解析错误: %+v", v)
	}
	if rc.gotMethod != "core/version" {
		t.Errorf("方法名错误: %s", rc.gotMethod)
	}
	if !rc.gotAuthOK || rc.gotAuthUser != "u1" || rc.gotAuthPass != "p1" {
		t.Errorf("应携带 Basic 认证，得到 ok=%v user=%q pass=%q", rc.gotAuthOK, rc.gotAuthUser, rc.gotAuthPass)
	}
}

func TestPingAndQuit(t *testing.T) {
	rc := &fakeRC{t: t}
	c, _ := newTestClient(t, rc)

	if err := c.Ping(context.Background()); err != nil {
		t.Fatalf("Ping 失败: %v", err)
	}
	if rc.gotMethod != "rc/noop" {
		t.Errorf("Ping 应调用 rc/noop，得到 %s", rc.gotMethod)
	}
	if err := c.Quit(context.Background()); err != nil {
		t.Fatalf("Quit 失败: %v", err)
	}
	if rc.gotMethod != "core/quit" {
		t.Errorf("Quit 应调用 core/quit，得到 %s", rc.gotMethod)
	}
}

func TestAPIErrorParsingAndHint(t *testing.T) {
	rc := &fakeRC{t: t, respond: func(w http.ResponseWriter, r *http.Request, method string, params map[string]any) {
		writeJSONResp(w, 500, `{"error":"failed to create file system for \"bad:\": didn't find section in config file","input":{"fs":"bad:"},"path":"sync/sync","status":500}`)
	}}
	c, _ := newTestClient(t, rc)

	_, err := c.Version(context.Background())
	if err == nil {
		t.Fatal("应返回错误")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("错误类型应为 *APIError，得到 %T", err)
	}
	if apiErr.Status != 500 {
		t.Errorf("状态码错误: %d", apiErr.Status)
	}
	if !strings.Contains(err.Error(), "rclone rc core/version 失败") {
		t.Errorf("错误信息缺少方法名: %q", err.Error())
	}
	// 应附带可操作的中文提示
	if !strings.Contains(err.Error(), "远程配置不存在") {
		t.Errorf("错误信息缺少提示: %q", err.Error())
	}
}

func TestAPIErrorWithoutJSON(t *testing.T) {
	rc := &fakeRC{t: t, respond: func(w http.ResponseWriter, r *http.Request, method string, params map[string]any) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("<html>proxy error</html>"))
	}}
	c, _ := newTestClient(t, rc)

	err := c.Ping(context.Background())
	if err == nil {
		t.Fatal("应返回错误")
	}
	if !strings.Contains(err.Error(), "proxy error") {
		t.Errorf("错误信息应包含原始响应: %q", err.Error())
	}
}

func TestStartAsyncInjectsAsyncAndGroup(t *testing.T) {
	rc := &fakeRC{t: t, respond: func(w http.ResponseWriter, r *http.Request, method string, params map[string]any) {
		writeJSONResp(w, 200, `{"jobid":17}`)
	}}
	c, _ := newTestClient(t, rc)

	jobID, err := c.StartAsync(context.Background(), "sync/copy",
		map[string]any{"srcFs": "a:", "dstFs": "b:"}, "run-3")
	if err != nil {
		t.Fatalf("StartAsync 失败: %v", err)
	}
	if jobID != 17 {
		t.Errorf("jobid 期望 17，得到 %d", jobID)
	}
	if rc.gotParams["_async"] != true {
		t.Errorf("应注入 _async=true，得到 %v", rc.gotParams["_async"])
	}
	if rc.gotParams["_group"] != "run-3" {
		t.Errorf("应注入 _group=run-3，得到 %v", rc.gotParams["_group"])
	}
	if rc.gotParams["srcFs"] != "a:" || rc.gotParams["dstFs"] != "b:" {
		t.Errorf("业务参数被丢弃: %+v", rc.gotParams)
	}
}

func TestStartAsyncMissingJobID(t *testing.T) {
	rc := &fakeRC{t: t, respond: func(w http.ResponseWriter, r *http.Request, method string, params map[string]any) {
		writeJSONResp(w, 200, `{"other":1}`)
	}}
	c, _ := newTestClient(t, rc)
	if _, err := c.StartAsync(context.Background(), "sync/copy", nil, "g"); err == nil {
		t.Fatal("缺少 jobid 时应报错")
	}
}

func TestJobStatus(t *testing.T) {
	rc := &fakeRC{t: t, respond: func(w http.ResponseWriter, r *http.Request, method string, params map[string]any) {
		writeJSONResp(w, 200, `{"jobid":5,"finished":true,"success":true,"duration":1.5,
			"startTime":"2026-01-02T03:04:05Z","endTime":"2026-01-02T03:04:06.5Z",
			"error":"","output":{},"group":"run-1"}`)
	}}
	c, _ := newTestClient(t, rc)

	st, err := c.JobStatus(context.Background(), 5)
	if err != nil {
		t.Fatalf("JobStatus 失败: %v", err)
	}
	if !st.Finished || !st.Success {
		t.Errorf("状态解析错误: %+v", st)
	}
	if st.IDOrJobID() != 5 {
		t.Errorf("ID 解析错误: %+v", st)
	}
	if st.Duration != 1.5 {
		t.Errorf("duration 解析错误: %v", st.Duration)
	}
}

func TestJobStatusOnlyID(t *testing.T) {
	// 兼容只返回 id 的旧版本。
	rc := &fakeRC{t: t, respond: func(w http.ResponseWriter, r *http.Request, method string, params map[string]any) {
		writeJSONResp(w, 200, `{"id":9,"finished":false,"success":false}`)
	}}
	c, _ := newTestClient(t, rc)
	st, err := c.JobStatus(context.Background(), 9)
	if err != nil {
		t.Fatal(err)
	}
	if st.IDOrJobID() != 9 {
		t.Errorf("期望 9，得到 %d", st.IDOrJobID())
	}
}

func TestStatsWithGroupAndNilETA(t *testing.T) {
	rc := &fakeRC{t: t, respond: func(w http.ResponseWriter, r *http.Request, method string, params map[string]any) {
		writeJSONResp(w, 200, `{"bytes":1024,"totalBytes":4096,"transfers":2,"totalTransfers":8,
			"errors":0,"speed":512.5,"eta":null,"fatalError":false,"elapsedTime":2.0,"group":"run-1"}`)
	}}
	c, _ := newTestClient(t, rc)

	s, err := c.Stats(context.Background(), "run-1")
	if err != nil {
		t.Fatalf("Stats 失败: %v", err)
	}
	if rc.gotParams["group"] != "run-1" {
		t.Errorf("应传递 group 参数: %+v", rc.gotParams)
	}
	if s.Bytes != 1024 || s.TotalBytes != 4096 || s.Transfers != 2 || s.TotalTransfers != 8 {
		t.Errorf("统计解析错误: %+v", s)
	}
	if s.ETA != nil {
		t.Errorf("eta=null 时应为 nil，得到 %v", *s.ETA)
	}
	if s.Speed != 512.5 {
		t.Errorf("speed 解析错误: %v", s.Speed)
	}
}

func TestStatsWithoutGroupOmitsParam(t *testing.T) {
	rc := &fakeRC{t: t, respond: func(w http.ResponseWriter, r *http.Request, method string, params map[string]any) {
		writeJSONResp(w, 200, `{"bytes":0,"totalBytes":0}`)
	}}
	c, _ := newTestClient(t, rc)
	if _, err := c.Stats(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if _, ok := rc.gotParams["group"]; ok {
		t.Errorf("空 group 不应传递参数: %+v", rc.gotParams)
	}
}

func TestStatsWithETA(t *testing.T) {
	rc := &fakeRC{t: t, respond: func(w http.ResponseWriter, r *http.Request, method string, params map[string]any) {
		writeJSONResp(w, 200, `{"bytes":1,"totalBytes":2,"eta":33.5,"lastError":"boom"}`)
	}}
	c, _ := newTestClient(t, rc)
	s, err := c.Stats(context.Background(), "g")
	if err != nil {
		t.Fatal(err)
	}
	if s.ETA == nil || *s.ETA != 33.5 {
		t.Errorf("eta 解析错误: %v", s.ETA)
	}
	if s.LastError == nil || *s.LastError != "boom" {
		t.Errorf("lastError 解析错误: %v", s.LastError)
	}
}

func TestStopJobAndGroup(t *testing.T) {
	rc := &fakeRC{t: t}
	c, _ := newTestClient(t, rc)

	if err := c.StopJob(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	if rc.gotMethod != "job/stop" || rc.gotParams["jobid"] != float64(3) {
		t.Errorf("job/stop 参数错误: %s %+v", rc.gotMethod, rc.gotParams)
	}
	if err := c.StopGroup(context.Background(), "run-1"); err != nil {
		t.Fatal(err)
	}
	if rc.gotMethod != "job/stopgroup" || rc.gotParams["group"] != "run-1" {
		t.Errorf("job/stopgroup 参数错误: %s %+v", rc.gotMethod, rc.gotParams)
	}
}

func TestRemotesAndList(t *testing.T) {
	rc := &fakeRC{t: t, respond: func(w http.ResponseWriter, r *http.Request, method string, params map[string]any) {
		switch method {
		case "config/listremotes":
			writeJSONResp(w, 200, `{"remotes":["gdrive","s3backup"]}`)
		case "operations/list":
			writeJSONResp(w, 200, `{"list":[{"Path":"a.txt","Name":"a.txt","Size":10,"IsDir":false}]}`)
		default:
			writeJSONResp(w, 200, `{}`)
		}
	}}
	c, _ := newTestClient(t, rc)

	remotes, err := c.ListRemotes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(remotes) != 2 || remotes[0] != "gdrive" {
		t.Errorf("remote 列表错误: %+v", remotes)
	}

	items, err := c.List(context.Background(), "gdrive:photos", "2024", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Name != "a.txt" {
		t.Errorf("列表解析错误: %+v", items)
	}
	if rc.gotParams["fs"] != "gdrive:photos" || rc.gotParams["remote"] != "2024" {
		t.Errorf("列表参数错误: %+v", rc.gotParams)
	}
}

func TestSetOptions(t *testing.T) {
	rc := &fakeRC{t: t}
	c, _ := newTestClient(t, rc)

	if err := c.SetOptions(context.Background(), "", map[string]any{"transfers": 8}); err != nil {
		t.Fatal(err)
	}
	main, ok := rc.gotParams["main"].(map[string]any)
	if !ok {
		t.Fatalf("options/set 应以 main 为键: %+v", rc.gotParams)
	}
	if main["transfers"] != float64(8) {
		t.Errorf("选项内容错误: %+v", main)
	}

	// 空选项不发起请求
	rc.gotMethod = ""
	if err := c.SetOptions(context.Background(), "main", nil); err != nil {
		t.Fatal(err)
	}
	if rc.gotMethod != "" {
		t.Errorf("空选项不应发起请求，却调用了 %s", rc.gotMethod)
	}
}

func TestContextCancellation(t *testing.T) {
	rc := &fakeRC{t: t, respond: func(w http.ResponseWriter, r *http.Request, method string, params map[string]any) {
		time.Sleep(2 * time.Second)
		writeJSONResp(w, 200, `{}`)
	}}
	c, _ := newTestClient(t, rc)

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	err := c.Ping(ctx)
	if err == nil {
		t.Fatal("应因 context 超时返回错误")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("错误应包装 context.DeadlineExceeded，得到 %v", err)
	}
}

func TestMethodForKind(t *testing.T) {
	cases := map[string]string{
		"sync":   "sync/sync",
		"copy":   "sync/copy",
		"move":   "sync/move",
		"bisync": "sync/bisync",
		"check":  "sync/check",
		"delete": "operations/delete",
		"purge":  "operations/purge",
		"mkdir":  "operations/mkdir",
		"SYNC":   "sync/sync",
	}
	for kind, want := range cases {
		got, err := MethodForKind(kind)
		if err != nil {
			t.Errorf("MethodForKind(%q) 报错: %v", kind, err)
			continue
		}
		if got != want {
			t.Errorf("MethodForKind(%q) 期望 %q，得到 %q", kind, want, got)
		}
	}
	if _, err := MethodForKind("rsync"); err == nil {
		t.Error("未知类型应报错")
	}
}

func TestParamNamesForKind(t *testing.T) {
	src, dst := ParamNamesForKind("copy")
	if src != "srcFs" || dst != "dstFs" {
		t.Errorf("copy 参数名错误: %s/%s", src, dst)
	}
	src, dst = ParamNamesForKind("bisync")
	if src != "path1" || dst != "path2" {
		t.Errorf("bisync 参数名错误: %s/%s", src, dst)
	}
	src, dst = ParamNamesForKind("purge")
	if src != "fs" || dst != "fs" {
		t.Errorf("purge 参数名错误: %s/%s", src, dst)
	}
}

func TestFormatBytes(t *testing.T) {
	cases := map[int64]string{
		0:          "0 B",
		512:        "512 B",
		1024:       "1.0 KiB",
		1536:       "1.5 KiB",
		1048576:    "1.0 MiB",
		1073741824: "1.0 GiB",
	}
	for in, want := range cases {
		if got := FormatBytes(in); got != want {
			t.Errorf("FormatBytes(%d) 期望 %q，得到 %q", in, want, got)
		}
	}
}

func TestBuildArgsIncludesAuthAndOptions(t *testing.T) {
	cfg := config.Default().Rclone
	cfg.RCAddr = "127.0.0.1:5999"
	cfg.RCUser = "rcuser"
	cfg.RCPass = "rcpass"
	cfg.ConfigFile = "/etc/rclone.conf"
	cfg.LogLevel = "DEBUG"
	cfg.ExtraArgs = []string{"--rc-serve"}

	sup := NewSupervisor(cfg, logging.Discard())
	args := sup.buildArgs()

	joined := strings.Join(args, " ")
	for _, want := range []string{
		"rcd",
		"--rc-addr 127.0.0.1:5999",
		"--rc-user rcuser",
		"--rc-pass rcpass",
		"--config /etc/rclone.conf",
		"--log-level DEBUG",
		"--rc-serve",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("参数缺少 %q，实际: %s", want, joined)
		}
	}
}

func TestBuildArgsNoAuth(t *testing.T) {
	cfg := config.Default().Rclone
	cfg.RCNoAuth = true
	cfg.RCPass = "should-not-appear"

	sup := NewSupervisor(cfg, logging.Discard())
	joined := strings.Join(sup.buildArgs(), " ")
	if !strings.Contains(joined, "--rc-no-auth") {
		t.Errorf("应包含 --rc-no-auth: %s", joined)
	}
	if strings.Contains(joined, "--rc-pass") {
		t.Errorf("禁用认证时不应传密码: %s", joined)
	}
}

func TestClientEndpointNormalization(t *testing.T) {
	cases := map[string]string{
		"127.0.0.1:5572":      "http://127.0.0.1:5572",
		"http://host:1234":    "http://host:1234",
		"http://host:1234/rc": "http://host:1234",
	}
	for in, want := range cases {
		cfg := config.Default().Rclone
		cfg.RCAddr = in
		c := NewClient(cfg, logging.Discard())
		if got := c.Endpoint(); got != want {
			t.Errorf("Endpoint(%q) 期望 %q，得到 %q", in, want, got)
		}
	}
}

func TestRingBufferSinceExcludesEarlierLines(t *testing.T) {
	buf := logging.NewRingBuffer(100)
	_, _ = buf.Write([]byte("old-1\nold-2\n"))
	mark := buf.Mark()
	_, _ = buf.Write([]byte("new-1\nnew-2\n"))

	lines, overflow := buf.Since(mark)
	if overflow {
		t.Error("未滚动时不应报溢出")
	}
	if len(lines) != 2 || lines[0] != "new-1" || lines[1] != "new-2" {
		t.Errorf("Since 结果错误: %+v", lines)
	}
}

func TestRingBufferSinceReportsOverflow(t *testing.T) {
	buf := logging.NewRingBuffer(2)
	_, _ = buf.Write([]byte("l1\n"))
	mark := buf.Mark()
	_, _ = buf.Write([]byte("l2\nl3\nl4\n"))

	_, overflow := buf.Since(mark)
	if !overflow {
		t.Error("缓冲区滚动后应报告 overflow")
	}
}
