package web

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"cloudsync/internal/config"
	"cloudsync/internal/logging"
	"cloudsync/internal/manager"
	"cloudsync/internal/rclone"
	"cloudsync/internal/retention"
	"cloudsync/internal/scheduler"
	"cloudsync/internal/store"
)

// ---------------------------------------------------------------------------
// 测试替身
// ---------------------------------------------------------------------------

// stubRC 让任务立即成功完成，或在 neverFinish 模式下永不结束。
type stubRC struct {
	mu          sync.Mutex
	started     []map[string]any
	neverFinish bool
}

func (s *stubRC) StartAsync(_ context.Context, method string, params map[string]any, group string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.started = append(s.started, params)
	return 1, nil
}

func (s *stubRC) Stats(_ context.Context, group string) (*rclone.Stats, error) {
	return &rclone.Stats{Group: group, Bytes: 1024, TotalBytes: 1024, Transfers: 2, TotalTransfers: 2}, nil
}

func (s *stubRC) JobStatus(_ context.Context, jobID int64) (*rclone.JobStatus, error) {
	s.mu.Lock()
	never := s.neverFinish
	s.mu.Unlock()
	if never {
		return &rclone.JobStatus{ID: jobID, Finished: false}, nil
	}
	return &rclone.JobStatus{ID: jobID, Finished: true, Success: true}, nil
}

func (s *stubRC) StopJob(_ context.Context, jobID int64) error    { return nil }
func (s *stubRC) StopGroup(_ context.Context, group string) error { return nil }

// fakeRCServer 模拟 rclone RC HTTP 服务，供 supervisor 的客户端访问。
func fakeRCServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	write := func(w http.ResponseWriter, body string) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}
	mux.HandleFunc("/rc/noop", func(w http.ResponseWriter, r *http.Request) { write(w, `{}`) })
	mux.HandleFunc("/core/version", func(w http.ResponseWriter, r *http.Request) {
		write(w, `{"version":"v1.99.0","goVersion":"go1.23","os":"linux","arch":"amd64"}`)
	})
	mux.HandleFunc("/core/memstats", func(w http.ResponseWriter, r *http.Request) {
		write(w, `{"Alloc":1024,"NumGC":1}`)
	})
	mux.HandleFunc("/core/group-list", func(w http.ResponseWriter, r *http.Request) {
		write(w, `{"groups":["run-1"]}`)
	})
	mux.HandleFunc("/core/stats", func(w http.ResponseWriter, r *http.Request) {
		write(w, `{"bytes":10,"totalBytes":20,"speed":5,"errors":0}`)
	})
	mux.HandleFunc("/config/listremotes", func(w http.ResponseWriter, r *http.Request) {
		write(w, `{"remotes":["gdrive","s3"]}`)
	})
	mux.HandleFunc("/operations/list", func(w http.ResponseWriter, r *http.Request) {
		write(w, `{"list":[{"Path":"x.txt","Name":"x.txt","Size":3,"IsDir":false}]}`)
	})
	mux.HandleFunc("/operations/about", func(w http.ResponseWriter, r *http.Request) {
		write(w, `{"total":100,"used":40,"free":60}`)
	})
	mux.HandleFunc("/options/get", func(w http.ResponseWriter, r *http.Request) {
		write(w, `{"main":{"transfers":4,"checkers":8,"bwlimit":""}}`)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { write(w, `{}`) })

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// testEnv 聚合被测组件。
type testEnv struct {
	t     *testing.T
	srv   *httptest.Server
	http  *http.Client
	store *store.SQLiteStore
	mgr   *manager.Manager
	sch   *scheduler.Scheduler
	sup   *rclone.Supervisor
	ret   *retention.Service
	rc    *stubRC
}

func newTestEnv(t *testing.T, mutate func(*config.Config)) *testEnv {
	return newTestEnvWith(t, mutate, nil)
}

// newTestEnvWith 额外允许调整 RC 替身的行为。
func newTestEnvWith(t *testing.T, mutate func(*config.Config), mutateRC func(*stubRC)) *testEnv {
	t.Helper()
	rcloneSrv := fakeRCServer(t)

	cfg := config.Default()
	cfg.Server.Password = "test-pass"
	cfg.Server.Username = "tester"
	cfg.Server.SessionSecret = "unit-test-secret"
	cfg.Scheduler.Seconds = true
	cfg.Rclone.AutoStart = false
	cfg.Rclone.RCAddr = strings.TrimPrefix(rcloneSrv.URL, "http://")
	cfg.Rclone.RCPass = ""
	cfg.Storage.DSN = filepath.Join(t.TempDir(), "web.db")
	if mutate != nil {
		mutate(cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("配置无效: %v", err)
	}

	st, err := store.Open(cfg.Storage.DSN, logging.Discard())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}

	sup := rclone.NewSupervisor(cfg.Rclone, logging.Discard())
	// 连接假 rclone RC 服务：AutoStart=false 时 Start 会探测到它并标记为 external，
	// 这也是 /api/rclone 各接口与「外部托管不可重启」语义的测试前提。
	if err := sup.Start(context.Background()); err != nil {
		t.Fatalf("连接假 rclone 失败: %v", err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background()) })
	rc := &stubRC{}
	if mutateRC != nil {
		mutateRC(rc)
	}
	mgr := manager.New(st, rc, rclone.NewRunner(cfg.Rclone), sup.Journal(), manager.Options{
		MaxConcurrent: 2,
		PollInterval:  15 * time.Millisecond,
		SkipOverlap:   true,
	}, logging.Discard())
	mgr.SetBaseContext(context.Background())

	sch, err := scheduler.New(cfg.Scheduler, st, mgr, logging.Discard())
	if err != nil {
		t.Fatal(err)
	}
	if err := sch.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sch.Stop)

	// 保留策略服务：默认不限，需要验证清理的用例自行改写设置。
	ret := retention.New(st, cfg.Storage.RunRetention.D(), cfg.Storage.RetentionInterval.D(), logging.Discard())

	s := New(cfg, st, mgr, sch, sup, ret, logging.Discard())
	httpSrv := httptest.NewServer(s.Handler())
	t.Cleanup(httpSrv.Close)

	jar, _ := cookiejar.New(nil)
	return &testEnv{
		t: t, srv: httpSrv, store: st, mgr: mgr, sch: sch, sup: sup, ret: ret, rc: rc,
		http: &http.Client{Jar: jar, Timeout: 15 * time.Second},
	}
}

func (e *testEnv) do(method, path string, body any) (*http.Response, []byte) {
	e.t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			e.t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, e.srv.URL+path, reader)
	if err != nil {
		e.t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := e.http.Do(req)
	if err != nil {
		e.t.Fatalf("%s %s 请求失败: %v", method, path, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp, data
}

func (e *testEnv) doJSON(method, path string, body any, out any) int {
	e.t.Helper()
	resp, data := e.do(method, path, body)
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			e.t.Fatalf("解析 %s %s 响应失败: %v（原始: %s）", method, path, err, data)
		}
	}
	return resp.StatusCode
}

func (e *testEnv) login() {
	e.t.Helper()
	var out sessionInfo
	code := e.doJSON("POST", "/api/login", map[string]string{
		"username": "tester", "password": "test-pass",
	}, &out)
	if code != http.StatusOK || !out.Authenticated {
		e.t.Fatalf("登录失败: code=%d out=%+v", code, out)
	}
}

func samplePayload(name string) map[string]any {
	return map[string]any{
		"name":        name,
		"kind":        "sync",
		"source":      "gdrive:photos",
		"dest":        "/backup/photos",
		"cron_expr":   "0 0 3 * * *",
		"enabled":     true,
		"extra_flags": map[string]any{"transfers": 4},
	}
}

// ---------------------------------------------------------------------------
// 认证
// ---------------------------------------------------------------------------

func TestHealthNeedsNoAuth(t *testing.T) {
	e := newTestEnv(t, nil)
	var out map[string]any
	if code := e.doJSON("GET", "/api/health", nil, &out); code != http.StatusOK {
		t.Fatalf("健康检查应无需认证，得到 %d", code)
	}
	if out["status"] != "ok" {
		t.Errorf("健康检查响应错误: %+v", out)
	}
}

func TestProtectedEndpointsRequireAuth(t *testing.T) {
	e := newTestEnv(t, nil)
	for _, path := range []string{"/api/overview", "/api/tasks", "/api/runs", "/api/rclone"} {
		resp, _ := e.do("GET", path, nil)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s 未认证时应返回 401，得到 %d", path, resp.StatusCode)
		}
	}
}

func TestLoginWithWrongCredentials(t *testing.T) {
	e := newTestEnv(t, nil)
	resp, _ := e.do("POST", "/api/login", map[string]string{"username": "tester", "password": "nope"})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("错误密码应返回 401，得到 %d", resp.StatusCode)
	}
}

func TestLoginRateLimit(t *testing.T) {
	e := newTestEnv(t, nil)
	var lastCode int
	for i := 0; i < 8; i++ {
		resp, _ := e.do("POST", "/api/login", map[string]string{"username": "tester", "password": "bad"})
		lastCode = resp.StatusCode
	}
	if lastCode != http.StatusTooManyRequests {
		t.Fatalf("连续失败后应触发限流 429，得到 %d", lastCode)
	}
}

func TestMeReflectsSession(t *testing.T) {
	e := newTestEnv(t, nil)
	var out sessionInfo
	if code := e.doJSON("GET", "/api/me", nil, &out); code != http.StatusOK || out.Authenticated {
		t.Fatalf("未登录时 authenticated 应为 false: %+v", out)
	}
	e.login()
	if code := e.doJSON("GET", "/api/me", nil, &out); code != http.StatusOK || !out.Authenticated {
		t.Fatalf("登录后 authenticated 应为 true: %+v", out)
	}
	if out.Username != "tester" {
		t.Errorf("用户名错误: %q", out.Username)
	}
}

func TestLogoutClearsSession(t *testing.T) {
	e := newTestEnv(t, nil)
	e.login()
	if code := e.doJSON("POST", "/api/logout", nil, nil); code != http.StatusOK {
		t.Fatalf("登出失败: %d", code)
	}
	resp, _ := e.do("GET", "/api/overview", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("登出后应返回 401，得到 %d", resp.StatusCode)
	}
}

func TestTamperedSessionRejected(t *testing.T) {
	e := newTestEnv(t, nil)
	req, _ := http.NewRequest("GET", e.srv.URL+"/api/overview", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: "abcdef:9999999999:forged"})
	resp, err := e.http.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("伪造会话应被拒绝，得到 %d", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// 任务 API
// ---------------------------------------------------------------------------

func TestTaskCRUDViaAPI(t *testing.T) {
	e := newTestEnv(t, nil)
	e.login()

	// 初始为空
	var list struct {
		Items []json.RawMessage `json:"items"`
		Total int               `json:"total"`
	}
	if code := e.doJSON("GET", "/api/tasks", nil, &list); code != http.StatusOK || list.Total != 0 {
		t.Fatalf("初始任务列表应为空: code=%d total=%d", code, list.Total)
	}

	// 创建
	var created struct {
		Task struct {
			ID       int64    `json:"id"`
			Name     string   `json:"name"`
			Method   string   `json:"method"`
			NextRuns []string `json:"next_runs"`
		} `json:"task"`
	}
	code := e.doJSON("POST", "/api/tasks", samplePayload("夜间备份"), &created)
	if code != http.StatusCreated {
		t.Fatalf("创建任务应返回 201，得到 %d", code)
	}
	if created.Task.ID == 0 || created.Task.Method != "sync/sync" {
		t.Errorf("创建响应错误: %+v", created.Task)
	}
	if len(created.Task.NextRuns) == 0 {
		t.Errorf("应返回未来触发时间预览: %+v", created.Task)
	}

	// 同名冲突
	if code := e.doJSON("POST", "/api/tasks", samplePayload("夜间备份"), nil); code != http.StatusConflict {
		t.Errorf("同名任务应返回 409，得到 %d", code)
	}

	// 详情
	var detail struct {
		Task json.RawMessage `json:"task"`
	}
	if code := e.doJSON("GET", fmt.Sprintf("/api/tasks/%d", created.Task.ID), nil, &detail); code != http.StatusOK {
		t.Fatalf("查询任务失败: %d", code)
	}

	// 更新
	updated := samplePayload("夜间备份-改名")
	updated["cron_expr"] = "*/5 * * * * *"
	updated["kind"] = "copy"
	if code := e.doJSON("PUT", fmt.Sprintf("/api/tasks/%d", created.Task.ID), updated, nil); code != http.StatusOK {
		t.Fatalf("更新任务失败: %d", code)
	}
	task, err := e.store.GetTask(context.Background(), created.Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if task.Name != "夜间备份-改名" || task.Kind != store.KindCopy {
		t.Errorf("更新未生效: %+v", task)
	}

	// 删除
	if code := e.doJSON("DELETE", fmt.Sprintf("/api/tasks/%d", created.Task.ID), nil, nil); code != http.StatusOK {
		t.Fatalf("删除任务失败: %d", code)
	}
	if _, err := e.store.GetTask(context.Background(), created.Task.ID); err == nil {
		t.Error("删除后任务应不存在")
	}
}

func TestTaskValidationViaAPI(t *testing.T) {
	e := newTestEnv(t, nil)
	e.login()

	cases := []struct {
		name    string
		payload map[string]any
		expect  string
	}{
		{"缺名称", map[string]any{"kind": "sync", "source": "a:", "dest": "b:"}, "名称"},
		{"缺源", map[string]any{"name": "x", "kind": "sync", "dest": "b:"}, "源路径"},
		{"缺目标", map[string]any{"name": "x", "kind": "sync", "source": "a:"}, "目标路径"},
		{"非法类型", map[string]any{"name": "x", "kind": "rsync", "source": "a:", "dest": "b:"}, "不支持"},
		{"非法cron", map[string]any{"name": "x", "kind": "sync", "source": "a:", "dest": "b:", "cron_expr": "not-cron"}, "cron"},
		{"保留参数", map[string]any{"name": "x", "kind": "sync", "source": "a:", "dest": "b:",
			"extra_flags": map[string]any{"srcFs": "evil"}}, "保留参数"},
		// 「清理目标重名」只对 sync/copy/move 有意义，其他类型必须被服务端拒绝，
		// 不能只依赖前端把控件藏起来。
		{"非写目标类型开启去重", map[string]any{"name": "x", "kind": "check", "source": "a:", "dest": "b:",
			"dedupe_before": true}, "清理目标重名"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out apiError
			code := e.doJSON("POST", "/api/tasks", tc.payload, &out)
			if code != http.StatusBadRequest {
				t.Fatalf("期望 400，得到 %d（%+v）", code, out)
			}
			if !strings.Contains(out.Error, tc.expect) {
				t.Errorf("错误信息应包含 %q，得到 %q", tc.expect, out.Error)
			}
		})
	}

	// 未知字段应被拒绝（防止前端字段拼写错误被静默忽略）
	var out apiError
	code := e.doJSON("POST", "/api/tasks", map[string]any{
		"name": "x", "kind": "sync", "source": "a:", "dest": "b:", "soruce": "typo",
	}, &out)
	if code != http.StatusBadRequest {
		t.Fatalf("未知字段应返回 400，得到 %d", code)
	}
}

// chainPayload 构造一个三步任务，且刻意不带顶层 kind/source/dest：
// 前端的多步骤表单只提交 steps，顶层字段应该由第一步推导，而不是兜成 sync。
func chainPayload(name string) map[string]any {
	return map[string]any{
		"name": name,
		"steps": []map[string]any{
			{"name": "音乐", "kind": "copy", "source": "src:音乐", "dest": "dst:音乐",
				"delay_after": 5},
			{"name": "清目录", "kind": "purge", "source": "dst:tmp"},
			{"name": "文档", "kind": "sync", "source": "src:文档", "dest": "dst:文档",
				"timeout_seconds": 600, "on_error": "abort"},
		},
		"timeout_seconds": 1800,
	}
}

// TestTaskStepsViaAPI 覆盖任务步骤的读写：顺序、字段、整体替换。
func TestTaskStepsViaAPI(t *testing.T) {
	e := newTestEnv(t, nil)
	e.login()

	var created struct {
		Task struct {
			ID     int64            `json:"id"`
			Kind   string           `json:"kind"`
			Source string           `json:"source"`
			Steps  []store.TaskStep `json:"steps"`
		} `json:"task"`
	}
	if code := e.doJSON("POST", "/api/tasks", chainPayload("编排链"), &created); code != http.StatusCreated {
		t.Fatalf("创建多步骤任务应返回 201，得到 %d", code)
	}
	if len(created.Task.Steps) != 3 {
		t.Fatalf("响应应回带 3 个步骤，得到 %d", len(created.Task.Steps))
	}
	// 顶层镜像必须来自第一步，而不是默认的 sync。
	if created.Task.Kind != "copy" || created.Task.Source != "src:音乐" {
		t.Errorf("顶层字段应由第一步推导，得到 kind=%q source=%q", created.Task.Kind, created.Task.Source)
	}

	task, err := e.store.GetTask(context.Background(), created.Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(task.Steps) != 3 {
		t.Fatalf("应落库 3 个步骤，得到 %d", len(task.Steps))
	}
	want := []struct {
		name    string
		kind    store.TaskKind
		source  string
		dest    string
		delay   int
		onErr   store.StepOnError
		timeout int
	}{
		{"音乐", store.KindCopy, "src:音乐", "dst:音乐", 5, store.OnErrorContinue, 0},
		{"清目录", store.KindPurge, "dst:tmp", "", 0, store.OnErrorContinue, 0},
		{"文档", store.KindSync, "src:文档", "dst:文档", 0, store.OnErrorAbort, 600},
	}
	for i, w := range want {
		got := task.Steps[i]
		if got.Position != i {
			t.Errorf("步骤 %d 的 position 应为 %d，得到 %d", i+1, i, got.Position)
		}
		if got.Name != w.name || got.Kind != w.kind || got.Source != w.source || got.Dest != w.dest {
			t.Errorf("步骤 %d 内容不符：%+v", i+1, got)
		}
		if got.DelayAfter != w.delay {
			t.Errorf("步骤 %d 的 delay_after 应为 %d，得到 %d", i+1, w.delay, got.DelayAfter)
		}
		if got.OnError != w.onErr {
			t.Errorf("步骤 %d 的 on_error 应为 %q，得到 %q", i+1, w.onErr, got.OnError)
		}
		if got.TimeoutSeconds != w.timeout {
			t.Errorf("步骤 %d 的 timeout_seconds 应为 %d，得到 %d", i+1, w.timeout, got.TimeoutSeconds)
		}
	}

	// 更新为两步：旧的第三步必须被删掉，不能残留。
	updated := map[string]any{
		"name":  "编排链",
		"steps": []map[string]any{{"kind": "move", "source": "src:a", "dest": "dst:a"}},
	}
	if code := e.doJSON("PUT", fmt.Sprintf("/api/tasks/%d", created.Task.ID), updated, nil); code != http.StatusOK {
		t.Fatalf("更新任务失败: %d", code)
	}
	task, err = e.store.GetTask(context.Background(), created.Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(task.Steps) != 1 || task.Steps[0].Kind != store.KindMove {
		t.Fatalf("更新后应只剩一个新的步骤，得到 %+v", task.Steps)
	}

	// 删除任务要连步骤一起删，否则复用编号的新任务会带着上一位的步骤。
	if code := e.doJSON("DELETE", fmt.Sprintf("/api/tasks/%d", created.Task.ID), nil, nil); code != http.StatusOK {
		t.Fatalf("删除任务失败: %d", code)
	}
	var n int64
	if err := e.store.DB().QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM task_steps WHERE task_id = ?`, created.Task.ID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("删除任务后应无残留步骤，实际 %d 条", n)
	}
}

// TestTaskStepValidationViaAPI 保证逐步校验的错误能定位到具体步骤。
func TestTaskStepValidationViaAPI(t *testing.T) {
	e := newTestEnv(t, nil)
	e.login()

	cases := []struct {
		name   string
		steps  []map[string]any
		expect string
	}{
		{"第二步类型非法", []map[string]any{
			{"kind": "sync", "source": "a:", "dest": "b:"},
			{"kind": "rsync", "source": "a:", "dest": "b:"},
		}, "步骤 2"},
		{"第二步缺目标", []map[string]any{
			{"kind": "sync", "source": "a:", "dest": "b:"},
			{"kind": "sync", "source": "a:"},
		}, "步骤 2"},
		{"间隔超限", []map[string]any{
			{"kind": "sync", "source": "a:", "dest": "b:", "delay_after": 100000},
		}, "步骤 1"},
		{"策略非法", []map[string]any{
			{"kind": "sync", "source": "a:", "dest": "b:", "on_error": "retry"},
		}, "步骤 1"},
		{"不支持去重的类型", []map[string]any{
			{"kind": "purge", "source": "a:", "dedupe_before": true},
		}, "步骤 1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out apiError
			code := e.doJSON("POST", "/api/tasks", map[string]any{
				"name":  "x",
				"steps": tc.steps,
			}, &out)
			if code != http.StatusBadRequest {
				t.Fatalf("期望 400，得到 %d（%+v）", code, out)
			}
			if !strings.Contains(out.Error, tc.expect) {
				t.Errorf("错误信息应指出 %q，得到 %q", tc.expect, out.Error)
			}
		})
	}

	// 步骤为空的任务要被拒：它既跑不了，也不该退化成一个空壳任务。
	var out apiError
	if code := e.doJSON("POST", "/api/tasks", map[string]any{
		"name":   "x",
		"steps":  []map[string]any{},
		"source": "",
	}, &out); code != http.StatusBadRequest {
		t.Fatalf("无步骤任务应返回 400，得到 %d", code)
	}
}

// TestRunTaskFromStep 覆盖"从失败步骤重跑"的入口校验。
func TestRunTaskFromStep(t *testing.T) {
	e := newTestEnv(t, nil)
	e.login()

	var created struct {
		Task struct {
			ID int64 `json:"id"`
		} `json:"task"`
	}
	if code := e.doJSON("POST", "/api/tasks", chainPayload("编排链"), &created); code != http.StatusCreated {
		t.Fatalf("创建失败: %d", code)
	}

	var out apiError
	code := e.doJSON("POST", fmt.Sprintf("/api/tasks/%d/run?from_step=9", created.Task.ID), nil, &out)
	if code != http.StatusBadRequest {
		t.Fatalf("越界起始步骤应返回 400，得到 %d", code)
	}
	if !strings.Contains(out.Error, "超出范围") {
		t.Errorf("错误信息应说明越界，得到 %q", out.Error)
	}
	if code := e.doJSON("POST", "/api/tasks/1/run?from_step=abc", nil, nil); code != http.StatusBadRequest {
		t.Errorf("非法参数应返回 400，得到 %d", code)
	}

	// 合法值要真的从前两步之外开始跑：此处只验证被接受，执行顺序由 manager 用例覆盖。
	var res struct {
		Run struct {
			StepIndex int `json:"step_index"`
			StepTotal int `json:"step_total"`
		} `json:"run"`
	}
	if code := e.doJSON("POST", fmt.Sprintf("/api/tasks/%d/run?from_step=2", created.Task.ID), nil, &res); code != http.StatusAccepted {
		t.Fatalf("从第 3 步重跑应返回 202，得到 %d", code)
	}
	if res.Run.StepIndex != 2 || res.Run.StepTotal != 3 {
		t.Errorf("运行的步骤定位错误: index=%d total=%d", res.Run.StepIndex, res.Run.StepTotal)
	}
	e.mgr.Wait(5 * time.Second)
}

// TestDedupeBeforeViaAPI 覆盖正向路径：sync 任务开启后要真的落库，
// 并且能在详情里读回来（前端表单需要它来回填复选框）。
func TestDedupeBeforeViaAPI(t *testing.T) {
	e := newTestEnv(t, nil)
	e.login()

	payload := samplePayload("去重备份")
	payload["dedupe_before"] = true
	var created struct {
		Task struct {
			ID           int64 `json:"id"`
			DedupeBefore bool  `json:"dedupe_before"`
		} `json:"task"`
	}
	if code := e.doJSON("POST", "/api/tasks", payload, &created); code != http.StatusCreated {
		t.Fatalf("创建任务应返回 201，得到 %d", code)
	}
	if !created.Task.DedupeBefore {
		t.Error("创建响应未回带 dedupe_before")
	}

	task, err := e.store.GetTask(context.Background(), created.Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !task.DedupeBefore {
		t.Error("dedupe_before 未落库")
	}

	// 关掉后要能改回去。
	payload2 := samplePayload("去重备份")
	payload2["dedupe_before"] = false
	if code := e.doJSON("PUT", fmt.Sprintf("/api/tasks/%d", created.Task.ID), payload2, nil); code != http.StatusOK {
		t.Fatalf("更新任务失败: %d", code)
	}
	task, err = e.store.GetTask(context.Background(), created.Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if task.DedupeBefore {
		t.Error("dedupe_before 关闭后未回写")
	}
}

// TestDedupeBeforeAllowedForCopyAndMoveViaAPI：copy/move 同样把源端对象写到目标端，
// 也会出现"目标同名越堆越多"的问题，服务端必须放行 —— 前端白名单只是提示，
// 不是唯一约束，直接调 API 也要走得通。
func TestDedupeBeforeAllowedForCopyAndMoveViaAPI(t *testing.T) {
	e := newTestEnv(t, nil)
	e.login()

	for _, kind := range []string{"copy", "move"} {
		payload := samplePayload("去重-" + kind)
		payload["kind"] = kind
		payload["dedupe_before"] = true
		var created struct {
			Task struct {
				ID           int64 `json:"id"`
				DedupeBefore bool  `json:"dedupe_before"`
			} `json:"task"`
		}
		if code := e.doJSON("POST", "/api/tasks", payload, &created); code != http.StatusCreated {
			t.Fatalf("%s 任务开启去重应返回 201，得到 %d", kind, code)
		}
		if !created.Task.DedupeBefore {
			t.Errorf("%s 任务 dedupe_before 未回带", kind)
		}
		task, err := e.store.GetTask(context.Background(), created.Task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !task.DedupeBefore {
			t.Errorf("%s 任务 dedupe_before 未落库", kind)
		}
	}
}

// TestDeleteRunAndClearAllViaAPI 覆盖「运行记录」页的两个新入口：
// 单条删除（含 404）与清空全部。
func TestDeleteRunAndClearAllViaAPI(t *testing.T) {
	e := newTestEnv(t, nil)
	e.login()

	var created struct {
		Task struct {
			ID int64 `json:"id"`
		} `json:"task"`
	}
	e.doJSON("POST", "/api/tasks", samplePayload("删除记录"), &created)

	var runIDs []int64
	for i := 0; i < 3; i++ {
		var trig struct {
			Run store.Run `json:"run"`
		}
		code := e.doJSON("POST", fmt.Sprintf("/api/tasks/%d/run", created.Task.ID), nil, &trig)
		if code != http.StatusAccepted {
			t.Fatalf("触发任务应 202，得到 %d", code)
		}
		waitForStatus(t, e.store, trig.Run.ID, store.StatusSuccess)
		runIDs = append(runIDs, trig.Run.ID)
	}

	if code := e.doJSON("DELETE", fmt.Sprintf("/api/runs/%d", runIDs[0]), nil, nil); code != http.StatusOK {
		t.Fatalf("删除运行记录应 200，得到 %d", code)
	}
	if code := e.doJSON("DELETE", fmt.Sprintf("/api/runs/%d", runIDs[0]), nil, nil); code != http.StatusNotFound {
		t.Fatalf("重复删除应 404，得到 %d", code)
	}

	var cleared struct {
		Deleted int64 `json:"deleted"`
	}
	if code := e.doJSON("DELETE", "/api/runs", nil, &cleared); code != http.StatusOK {
		t.Fatalf("清空应 200，得到 %d", code)
	}
	if cleared.Deleted != 2 {
		t.Fatalf("期望清空 2 条，得到 %d", cleared.Deleted)
	}

	var runs struct {
		Total int `json:"total"`
	}
	e.doJSON("GET", "/api/runs", nil, &runs)
	if runs.Total != 0 {
		t.Errorf("清空后应为 0 条，得到 %d", runs.Total)
	}
}

// TestDeleteRunningRunRejected 保证"运行中的记录不能删"落在服务端而不是前端：
// 前端可以不给按钮，但直接调 API 必须被拒，否则观测窗口会凭空消失。
func TestDeleteRunningRunRejected(t *testing.T) {
	e := newTestEnvWith(t, nil, func(rc *stubRC) { rc.neverFinish = true })
	e.login()

	var created struct {
		Task struct {
			ID int64 `json:"id"`
		} `json:"task"`
	}
	e.doJSON("POST", "/api/tasks", samplePayload("运行中任务"), &created)
	var trig struct {
		Run store.Run `json:"run"`
	}
	if code := e.doJSON("POST", fmt.Sprintf("/api/tasks/%d/run", created.Task.ID), nil, &trig); code != http.StatusAccepted {
		t.Fatalf("触发任务应 202，得到 %d", code)
	}

	var out apiError
	if code := e.doJSON("DELETE", fmt.Sprintf("/api/runs/%d", trig.Run.ID), nil, &out); code != http.StatusConflict {
		t.Fatalf("删除运行中的记录应 409，得到 %d（%+v）", code, out)
	}
	if !strings.Contains(out.Error, "取消") {
		t.Errorf("错误信息应引导先取消，得到 %q", out.Error)
	}
	if code := e.doJSON("DELETE", "/api/runs", nil, nil); code != http.StatusConflict {
		t.Fatalf("有运行中任务时清空应 409，得到 %d", code)
	}
}

// TestSetTaskEnabledViaAPI 覆盖列表页开关背后的接口：
// 切换、幂等重复设置，以及停用后调度条目被摘除。
func TestSetTaskEnabledViaAPI(t *testing.T) {
	e := newTestEnv(t, nil)
	e.login()

	var created struct {
		Task store.Task `json:"task"`
	}
	if code := e.doJSON("POST", "/api/tasks", samplePayload("开关任务"), &created); code != http.StatusCreated {
		t.Fatalf("创建任务应 201，得到 %d", code)
	}
	id := created.Task.ID
	if !created.Task.Enabled {
		t.Fatalf("默认应启用")
	}
	path := fmt.Sprintf("/api/tasks/%d/enabled", id)

	var out struct {
		Task store.Task `json:"task"`
	}
	// 重复设置同一状态应幂等成功，而不是报错。
	if code := e.doJSON("POST", path, map[string]any{"enabled": true}, &out); code != http.StatusOK {
		t.Fatalf("重复启用应 200，得到 %d", code)
	}
	if !out.Task.Enabled {
		t.Errorf("重复启用后状态不应变化")
	}

	if code := e.doJSON("POST", path, map[string]any{"enabled": false}, &out); code != http.StatusOK {
		t.Fatalf("停用应 200，得到 %d", code)
	}
	if out.Task.Enabled {
		t.Errorf("停用后 enabled 应为 false: %+v", out.Task)
	}
	// 落库验证，防止只改了内存对象。
	stored, err := e.store.GetTask(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Enabled {
		t.Errorf("停用结果未落库")
	}

	// 停用的任务仍可手动触发吗？不允许 —— manager 会拒绝，这里顺带确认接口语义。
	if code := e.doJSON("POST", fmt.Sprintf("/api/tasks/%d/run", id), nil, nil); code != http.StatusConflict {
		t.Fatalf("停用任务手动触发应 409，得到 %d", code)
	}

	if code := e.doJSON("POST", path, map[string]any{"enabled": true}, &out); code != http.StatusOK {
		t.Fatalf("重新启用应 200，得到 %d", code)
	}
	if !out.Task.Enabled {
		t.Errorf("重新启用后 enabled 应为 true")
	}
}

// TestUpdateRunningTaskRejected 保证"运行中不允许编辑"落在服务端：
// 前端置灰按钮只是体验层，直接调 API 必须被拒。
func TestUpdateRunningTaskRejected(t *testing.T) {
	e := newTestEnvWith(t, nil, func(rc *stubRC) { rc.neverFinish = true })
	e.login()

	var created struct {
		Task struct {
			ID int64 `json:"id"`
		} `json:"task"`
	}
	if code := e.doJSON("POST", "/api/tasks", samplePayload("运行中编辑"), &created); code != http.StatusCreated {
		t.Fatalf("创建任务应 201，得到 %d", code)
	}
	id := created.Task.ID

	// 未运行时编辑正常。
	var upd struct {
		Task store.Task `json:"task"`
	}
	payload := samplePayload("运行中编辑")
	payload["description"] = "运行前改动"
	if code := e.doJSON("PUT", fmt.Sprintf("/api/tasks/%d", id), payload, &upd); code != http.StatusOK {
		t.Fatalf("未运行时编辑应 200，得到 %d", code)
	}

	var trig struct {
		Run store.Run `json:"run"`
	}
	if code := e.doJSON("POST", fmt.Sprintf("/api/tasks/%d/run", id), nil, &trig); code != http.StatusAccepted {
		t.Fatalf("触发任务应 202，得到 %d", code)
	}

	var errOut apiError
	payload = samplePayload("运行中编辑")
	payload["description"] = "运行中改动"
	if code := e.doJSON("PUT", fmt.Sprintf("/api/tasks/%d", id), payload, &errOut); code != http.StatusConflict {
		t.Fatalf("运行中编辑应 409，得到 %d（%+v）", code, errOut)
	}
	if errOut.Code != "task_running" {
		t.Errorf("错误码应为 task_running，得到 %q", errOut.Code)
	}

	// 取消后应恢复可编辑。
	if code := e.doJSON("POST", fmt.Sprintf("/api/tasks/%d/cancel", id), nil, nil); code != http.StatusOK {
		t.Fatalf("取消任务应 200，得到 %d", code)
	}
	deadline := time.Now().Add(5 * time.Second)
	for e.mgr.RunningByTask(id) != nil {
		if time.Now().After(deadline) {
			t.Fatal("取消后任务仍在运行")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if code := e.doJSON("PUT", fmt.Sprintf("/api/tasks/%d", id), payload, &upd); code != http.StatusOK {
		t.Fatalf("取消后编辑应 200，得到 %d", code)
	}
	if upd.Task.Description != "运行中改动" {
		t.Errorf("改动应生效，得到 %q", upd.Task.Description)
	}
}

func TestValidateCronEndpoint(t *testing.T) {
	e := newTestEnv(t, nil)
	e.login()

	var ok struct {
		Valid bool     `json:"valid"`
		Next  []string `json:"next"`
	}
	// 测试环境 scheduler.seconds=true，表达式必须是 6 段（秒 分 时 日 月 周）。
	if code := e.doJSON("POST", "/api/tasks/validate", map[string]any{"cron_expr": "0 0 3 * * *"}, &ok); code != http.StatusOK {
		t.Fatalf("校验接口失败: %d", code)
	}
	if !ok.Valid || len(ok.Next) != 5 {
		t.Errorf("应返回 5 个触发时间: %+v", ok)
	}

	var bad struct {
		Valid bool   `json:"valid"`
		Error string `json:"error"`
	}
	if code := e.doJSON("POST", "/api/tasks/validate", map[string]any{"cron_expr": "bad"}, &bad); code != http.StatusOK {
		t.Fatalf("非法表达式应返回 200 且 valid=false，得到 %d", code)
	}
	if bad.Valid || bad.Error == "" {
		t.Errorf("非法表达式应给出错误信息: %+v", bad)
	}

	var empty struct {
		Valid bool `json:"valid"`
	}
	if code := e.doJSON("POST", "/api/tasks/validate", map[string]any{"cron_expr": ""}, &empty); code != http.StatusOK || !empty.Valid {
		t.Errorf("空表达式应视为有效（仅手动）: code=%d %+v", code, empty)
	}
}

// ---------------------------------------------------------------------------
// 运行 API
// ---------------------------------------------------------------------------

func TestTriggerRunAndProgress(t *testing.T) {
	e := newTestEnv(t, nil)
	e.login()

	var created struct {
		Task struct {
			ID int64 `json:"id"`
		} `json:"task"`
	}
	e.doJSON("POST", "/api/tasks", samplePayload("手动任务"), &created)

	var triggered struct {
		Run store.Run `json:"run"`
	}
	code := e.doJSON("POST", fmt.Sprintf("/api/tasks/%d/run", created.Task.ID), nil, &triggered)
	if code != http.StatusAccepted {
		t.Fatalf("触发任务应返回 202，得到 %d", code)
	}
	if triggered.Run.ID == 0 || triggered.Run.Status != store.StatusPending {
		t.Errorf("触发响应错误: %+v", triggered.Run)
	}

	// 重复触发应被 SkipOverlap 拒绝（或首个已完成，此时允许再次触发）
	waitForStatus(t, e.store, triggered.Run.ID, store.StatusSuccess)

	// 详情 + 进度
	var detail struct {
		Run      store.Run        `json:"run"`
		Progress manager.Progress `json:"progress"`
	}
	code = e.doJSON("GET", fmt.Sprintf("/api/runs/%d", triggered.Run.ID), nil, &detail)
	if code != http.StatusOK {
		t.Fatalf("查询运行详情失败: %d", code)
	}
	if detail.Run.Status != store.StatusSuccess || detail.Run.Bytes != 1024 {
		t.Errorf("运行详情错误: %+v", detail.Run)
	}
	if detail.Progress.Percent != 100 || detail.Progress.TotalBytes != 1024 {
		t.Errorf("进度视图错误: %+v", detail.Progress)
	}

	// 列表 + 状态过滤
	var runs struct {
		Items []store.Run `json:"items"`
		Total int         `json:"total"`
	}
	if code := e.doJSON("GET", "/api/runs?status=success&limit=10", nil, &runs); code != http.StatusOK {
		t.Fatalf("运行列表失败: %d", code)
	}
	if runs.Total != 1 {
		t.Errorf("成功记录应为 1 条，得到 %d", runs.Total)
	}

	if code := e.doJSON("GET", "/api/runs?status=bogus", nil, nil); code != http.StatusBadRequest {
		t.Errorf("非法状态过滤应返回 400，得到 %d", code)
	}

	// 活跃列表
	var active struct {
		Total int `json:"total"`
	}
	if code := e.doJSON("GET", "/api/runs/active", nil, &active); code != http.StatusOK || active.Total != 0 {
		t.Errorf("活跃列表应为空: code=%d %+v", code, active)
	}
}

func TestCancelNonRunningRun(t *testing.T) {
	e := newTestEnv(t, nil)
	e.login()
	if code := e.doJSON("POST", "/api/runs/999/cancel", nil, nil); code != http.StatusNotFound {
		t.Fatalf("取消不存在的运行应返回 404，得到 %d", code)
	}
}

func TestDeleteRunningTaskRequiresForce(t *testing.T) {
	// neverFinish：任务跑不完，才能真正验证"运行中不允许删除"。
	e := newTestEnvWith(t, nil, func(rc *stubRC) { rc.neverFinish = true })

	// 用一个永不结束的任务占住运行态
	var created struct {
		Task struct {
			ID int64 `json:"id"`
		} `json:"task"`
	}
	payload := samplePayload("长任务")
	payload["name"] = "长任务"
	e.login()
	e.doJSON("POST", "/api/tasks", payload, &created)

	// 直接构造一个运行中的记录，模拟任务占用
	task, _ := e.store.GetTask(context.Background(), created.Task.ID)
	if _, err := e.mgr.Trigger(context.Background(), task, store.TriggerManual); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, e.store, 0, "") // 等待启动
	if e.mgr.RunningByTask(task.ID) == nil {
		t.Skip("任务已瞬间完成，无法验证删除保护")
	}

	var out apiError
	if code := e.doJSON("DELETE", fmt.Sprintf("/api/tasks/%d", created.Task.ID), nil, &out); code != http.StatusConflict {
		t.Fatalf("删除运行中的任务应返回 409，得到 %d", code)
	}
	if out.Code != "task_running" {
		t.Errorf("错误码应为 task_running，得到 %q", out.Code)
	}
}

// ---------------------------------------------------------------------------
// 设置 API（运行记录保留策略）
// ---------------------------------------------------------------------------

// TestSettingsEndpoints 覆盖保留策略的读写与"立即清理"：
// 默认值、写入后回读、非法值拒绝、以及清理只删已结束的过期记录。
func TestSettingsEndpoints(t *testing.T) {
	e := newTestEnv(t, nil)
	e.login()
	ctx := context.Background()

	var got struct {
		RetentionHours  int    `json:"retention_hours"`
		RetentionText   string `json:"retention_text"`
		RetentionSource string `json:"retention_source"`
		Pending         int64  `json:"pending"`
		Storage         struct {
			Runs        int64 `json:"runs"`
			DBSizeBytes int64 `json:"db_size_bytes"`
		} `json:"storage"`
	}
	if code := e.doJSON("GET", "/api/settings", nil, &got); code != http.StatusOK {
		t.Fatalf("读取设置应 200，得到 %d", code)
	}
	if got.RetentionHours != 0 || got.RetentionSource != retention.SourceDefault {
		t.Fatalf("默认应为不限制: %+v", got)
	}
	if !strings.Contains(got.RetentionText, "不限制") {
		t.Errorf("默认文案应说明不限制，得到 %q", got.RetentionText)
	}

	// 需要先登录：未认证 401 已在别处覆盖，这里只确认写入需要认证之外的语义。
	// 缺少字段应 400，而不是被当成"不限制"。
	if code := e.doJSON("PUT", "/api/settings", map[string]any{}, nil); code != http.StatusBadRequest {
		t.Fatalf("缺少 retention_hours 应 400，得到 %d", code)
	}
	// 0 表示不限制（不是"没传"）。
	if code := e.doJSON("PUT", "/api/settings", map[string]any{"retention_hours": 0}, nil); code != http.StatusOK {
		t.Fatalf("0 表示不限制，应 200，得到 %d", code)
	}
	// 超过上限（10 年）应 400。
	if code := e.doJSON("PUT", "/api/settings", map[string]any{"retention_hours": 24 * 365 * 11}, nil); code != http.StatusBadRequest {
		t.Fatalf("超长保留时长应 400，得到 %d", code)
	}

	var set struct {
		RetentionHours  int    `json:"retention_hours"`
		RetentionSource string `json:"retention_source"`
	}
	if code := e.doJSON("PUT", "/api/settings", map[string]any{"retention_hours": 24}, &set); code != http.StatusOK {
		t.Fatalf("设置保留 24 小时应 200，得到 %d", code)
	}
	if set.RetentionHours != 24 || set.RetentionSource != retention.SourceUI {
		t.Fatalf("写入后应来自界面设置: %+v", set)
	}

	// 造两条记录：一条 3 天前成功（应过期），一条刚刚成功（应保留）。
	task := &store.Task{Name: "保留策略任务", Kind: store.KindSync, Source: "a:", Dest: "b:", Enabled: true}
	if err := e.store.CreateTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	old := &store.Run{TaskID: task.ID, TaskName: task.Name, Kind: task.Kind,
		Status: store.StatusSuccess, StartedAt: time.Now().UTC().Add(-72 * time.Hour)}
	recent := &store.Run{TaskID: task.ID, TaskName: task.Name, Kind: task.Kind,
		Status: store.StatusSuccess, StartedAt: time.Now().UTC().Add(-time.Hour)}
	running := &store.Run{TaskID: task.ID, TaskName: task.Name, Kind: task.Kind,
		Status: store.StatusRunning, StartedAt: time.Now().UTC().Add(-72 * time.Hour)}
	for _, r := range []*store.Run{old, recent, running} {
		if err := e.store.CreateRun(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	var before struct {
		Pending int64 `json:"pending"`
	}
	if code := e.doJSON("GET", "/api/settings", nil, &before); code != http.StatusOK {
		t.Fatalf("读取设置失败: %d", code)
	}
	if before.Pending != 1 {
		t.Fatalf("应有 1 条待清理，得到 %d", before.Pending)
	}

	var pruned struct {
		Deleted  int64 `json:"deleted"`
		Vacuumed bool  `json:"vacuumed"`
	}
	if code := e.doJSON("POST", "/api/runs/prune", nil, &pruned); code != http.StatusOK {
		t.Fatalf("立即清理应 200，得到 %d", code)
	}
	if pruned.Deleted != 1 || !pruned.Vacuumed {
		t.Fatalf("清理结果错误: %+v", pruned)
	}

	for _, id := range []int64{recent.ID, running.ID} {
		if _, err := e.store.GetRun(ctx, id); err != nil {
			t.Errorf("记录 #%d 不应被清理: %v", id, err)
		}
	}
	if _, err := e.store.GetRun(ctx, old.ID); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("过期记录应被清理，得到 %v", err)
	}
}

// TestSettingsEndpointsRequireAuth 保证保留策略不能被匿名改写。
func TestSettingsEndpointsRequireAuth(t *testing.T) {
	e := newTestEnv(t, nil)
	if resp, _ := e.do("GET", "/api/settings", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("未认证读取设置应 401，得到 %d", resp.StatusCode)
	}
	if resp, _ := e.do("PUT", "/api/settings", map[string]any{"retention_hours": 24}); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("未认证修改设置应 401，得到 %d", resp.StatusCode)
	}
	if resp, _ := e.do("POST", "/api/runs/prune", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("未认证清理应 401，得到 %d", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// 系统 API
// ---------------------------------------------------------------------------

func TestOverview(t *testing.T) {
	e := newTestEnv(t, nil)
	e.login()

	var out struct {
		Counts struct {
			Tasks int `json:"tasks"`
		} `json:"counts"`
		Scheduler struct {
			Enabled  bool   `json:"enabled"`
			Timezone string `json:"timezone"`
		} `json:"scheduler"`
		Rclone struct {
			Endpoint string `json:"endpoint"`
			State    string `json:"state"`
		} `json:"rclone"`
		Version string `json:"version"`
	}
	if code := e.doJSON("GET", "/api/overview", nil, &out); code != http.StatusOK {
		t.Fatalf("概览接口失败: %d", code)
	}
	if !out.Scheduler.Enabled || out.Scheduler.Timezone == "" {
		t.Errorf("调度信息缺失: %+v", out.Scheduler)
	}
	if out.Rclone.Endpoint == "" {
		t.Errorf("rclone 信息缺失: %+v", out.Rclone)
	}
	if out.Version == "" {
		t.Error("应返回版本号")
	}
}

func TestRcloneEndpoints(t *testing.T) {
	e := newTestEnv(t, nil)
	e.login()

	var status struct {
		Status rclone.Status `json:"status"`
		Stats  *rclone.Stats `json:"stats"`
	}
	if code := e.doJSON("GET", "/api/rclone", nil, &status); code != http.StatusOK {
		t.Fatalf("rclone 状态接口失败: %d", code)
	}
	if status.Status.Endpoint == "" {
		t.Error("应返回 RC 地址")
	}

	var remotes struct {
		Remotes []string `json:"remotes"`
	}
	if code := e.doJSON("GET", "/api/rclone/remotes", nil, &remotes); code != http.StatusOK || len(remotes.Remotes) != 2 {
		t.Fatalf("remote 列表错误: code=%d %+v", code, remotes)
	}

	var listing struct {
		Total int `json:"total"`
	}
	if code := e.doJSON("GET", "/api/rclone/list?fs=gdrive:photos", nil, &listing); code != http.StatusOK || listing.Total != 1 {
		t.Fatalf("目录浏览错误: code=%d %+v", code, listing)
	}
	if code := e.doJSON("GET", "/api/rclone/list", nil, nil); code != http.StatusBadRequest {
		t.Errorf("缺少 fs 参数应返回 400，得到 %d", code)
	}

	var about struct {
		About *rclone.About `json:"about"`
	}
	if code := e.doJSON("GET", "/api/rclone/about?fs=gdrive:", nil, &about); code != http.StatusOK || about.About == nil {
		t.Fatalf("容量查询失败: code=%d %+v", code, about)
	}

	var opts struct {
		Available int `json:"available"`
	}
	if code := e.doJSON("GET", "/api/rclone/options", nil, &opts); code != http.StatusOK {
		t.Fatalf("选项查询失败: %d", code)
	}

	var logResp struct {
		Lines []string `json:"lines"`
	}
	if code := e.doJSON("GET", "/api/rclone/log?tail=10", nil, &logResp); code != http.StatusOK {
		t.Fatalf("日志查询失败: %d", code)
	}

	// 测试环境连接的是外部托管的 rclone（AutoStart=false），程序无权重启它，
	// 应返回 409 冲突而非 5xx；真实内建模式下同一接口返回 200。
	var restartResp map[string]any
	if code := e.doJSON("POST", "/api/rclone/restart", nil, &restartResp); code != http.StatusConflict {
		t.Fatalf("外部托管实例重启应返回 409，得到 %d", code)
	}
	if restartResp["code"] != "external_managed" {
		t.Fatalf("重启冲突码应为 external_managed，得到 %v", restartResp["code"])
	}
}

// ---------------------------------------------------------------------------
// SSE
// ---------------------------------------------------------------------------

func TestEventStreamDeliversFrames(t *testing.T) {
	e := newTestEnv(t, nil)
	e.login()

	req, _ := http.NewRequest("GET", e.srv.URL+"/api/events", nil)
	// 复用会话 Cookie
	for _, c := range e.http.Jar.Cookies(req.URL) {
		req.AddCookie(c)
	}
	resp, err := e.http.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type 应为 text/event-stream，得到 %q", ct)
	}

	reader := bufio.NewReader(resp.Body)
	var event, data string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("读取 SSE 失败: %v", err)
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case strings.HasPrefix(line, "event: "):
			event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			data = strings.TrimPrefix(line, "data: ")
		case line == "":
			if event != "" {
				if event != "hello" {
					t.Fatalf("首帧应为 hello，得到 %q", event)
				}
				var payload map[string]any
				if err := json.Unmarshal([]byte(data), &payload); err != nil {
					t.Fatalf("首帧数据不是合法 JSON: %s", data)
				}
				return
			}
		}
	}
	t.Fatal("未在超时内收到 SSE 首帧")
}

// ---------------------------------------------------------------------------
// 工具
// ---------------------------------------------------------------------------

func waitForStatus(t *testing.T, st store.Store, runID int64, want store.RunStatus) {
	t.Helper()
	if runID == 0 {
		// 仅等待调度器/管理器启动完成任务
		time.Sleep(200 * time.Millisecond)
		return
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, err := st.GetRun(context.Background(), runID)
		if err == nil && run.Status == want {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	run, _ := st.GetRun(context.Background(), runID)
	t.Fatalf("等待运行 #%d 变为 %s 超时，当前 %+v", runID, want, run)
}

// TestRunningTaskLocksAllMutations 覆盖「运行中只能取消」这条规则的全部入口：
// 编辑（PUT）、删除（DELETE）、改启用状态（POST /enabled）都必须返回 409，
// 取消之后三者才恢复可用。前端置灰按钮只是体验层，服务端才是约束。
func TestRunningTaskLocksAllMutations(t *testing.T) {
	e := newTestEnvWith(t, nil, func(rc *stubRC) { rc.neverFinish = true })
	e.login()
	id := createTaskViaAPI(t, e, "运行中锁定")

	// 手动触发，把任务钉在运行态。
	if code := e.doJSON("POST", fmt.Sprintf("/api/tasks/%d/run", id), nil, nil); code != http.StatusAccepted {
		t.Fatalf("触发任务应 202，得到 %d", code)
	}
	waitForStatus(t, e.store, 0, "")
	if e.mgr.RunningByTask(id) == nil {
		t.Skip("任务已瞬间完成，无法验证运行态锁定")
	}

	var errOut apiError
	if code := e.doJSON("POST", fmt.Sprintf("/api/tasks/%d/enabled", id),
		map[string]any{"enabled": false}, &errOut); code != http.StatusConflict {
		t.Errorf("运行中改启用状态应 409，得到 %d（%+v）", code, errOut)
	}
	if errOut.Code != "task_running" {
		t.Errorf("错误码应为 task_running，得到 %q", errOut.Code)
	}
	if code := e.doJSON("DELETE", fmt.Sprintf("/api/tasks/%d", id), nil, &errOut); code != http.StatusConflict {
		t.Errorf("运行中删除应 409，得到 %d（%+v）", code, errOut)
	}

	// 取消后恢复可操作。
	if code := e.doJSON("POST", fmt.Sprintf("/api/tasks/%d/cancel", id), nil, nil); code != http.StatusOK {
		t.Fatalf("取消任务应 200，得到 %d", code)
	}
	deadline := time.Now().Add(5 * time.Second)
	for e.mgr.RunningByTask(id) != nil {
		if time.Now().After(deadline) {
			t.Fatal("取消后任务仍在运行")
		}
		time.Sleep(20 * time.Millisecond)
	}
	var out struct {
		Task struct {
			Enabled bool `json:"enabled"`
		} `json:"task"`
	}
	if code := e.doJSON("POST", fmt.Sprintf("/api/tasks/%d/enabled", id),
		map[string]any{"enabled": false}, &out); code != http.StatusOK {
		t.Errorf("取消后改启用状态应 200，得到 %d", code)
	}
	if out.Task.Enabled {
		t.Error("取消后停用应生效")
	}
}

// TestDeleteRunningTaskWithForce：?force=1 是留给运维的后门（先取消再删），
// UI 不暴露，但语义必须保证"不会留下还在跑的孤儿任务"。
func TestDeleteRunningTaskWithForce(t *testing.T) {
	e := newTestEnvWith(t, nil, func(rc *stubRC) { rc.neverFinish = true })
	e.login()
	id := createTaskViaAPI(t, e, "强制删除")

	if code := e.doJSON("POST", fmt.Sprintf("/api/tasks/%d/run", id), nil, nil); code != http.StatusAccepted {
		t.Fatalf("触发任务应 202，得到 %d", code)
	}
	waitForStatus(t, e.store, 0, "")
	if e.mgr.RunningByTask(id) == nil {
		t.Skip("任务已瞬间完成，无法验证强制删除")
	}

	if code := e.doJSON("DELETE", fmt.Sprintf("/api/tasks/%d?force=1", id), nil, nil); code != http.StatusOK {
		t.Fatalf("强制删除应 200，得到 %d", code)
	}
	if _, err := e.store.GetTask(context.Background(), id); err == nil {
		t.Error("强制删除后任务应不存在")
	}
	if e.mgr.RunningByTask(id) != nil {
		t.Error("强制删除后不应还有运行中的实例")
	}
}

// createTaskViaAPI 建一个最小可用任务并返回 ID。
func createTaskViaAPI(t *testing.T, e *testEnv, name string) int64 {
	t.Helper()
	var created struct {
		Task struct {
			ID int64 `json:"id"`
		} `json:"task"`
	}
	payload := samplePayload(name)
	if code := e.doJSON("POST", "/api/tasks", payload, &created); code != http.StatusCreated {
		t.Fatalf("创建任务应 201，得到 %d", code)
	}
	return created.Task.ID
}
