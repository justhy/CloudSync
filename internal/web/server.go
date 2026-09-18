// Package web 提供 Web 管理端：REST API + 内嵌单页前端 + SSE 实时进度推送。
package web

import (
	"context"
	"embed"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"cloudsync/internal/config"
	"cloudsync/internal/logging"
	"cloudsync/internal/manager"
	"cloudsync/internal/rclone"
	"cloudsync/internal/retention"
	"cloudsync/internal/scheduler"
	"cloudsync/internal/store"
)

//go:embed static
var staticFS embed.FS

// Version 是构建版本号，可通过 -ldflags "-X cloudsync/internal/web.Version=..." 注入。
var Version = "dev"

// Server 是 Web 管理端。
type Server struct {
	cfg       *config.Config
	store     store.Store
	manager   *manager.Manager
	scheduler *scheduler.Scheduler
	rclone    *rclone.Supervisor
	retention *retention.Service
	logger    *slog.Logger
	auth      *auth
	limiter   *loginLimiter

	http *http.Server
}

// New 创建 Web 管理端。
func New(
	cfg *config.Config,
	st store.Store,
	mgr *manager.Manager,
	sch *scheduler.Scheduler,
	sup *rclone.Supervisor,
	ret *retention.Service,
	logger *slog.Logger,
) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{
		cfg:       cfg,
		store:     st,
		manager:   mgr,
		scheduler: sch,
		rclone:    sup,
		retention: ret,
		logger:    logger.With("component", "web"),
		auth:      newAuth(cfg.Server.Username, cfg.Server.Password, cfg.Server.SessionSecret, cfg.Server.SessionTTL.D()),
		limiter:   newLoginLimiter(5, 15*time.Minute),
	}
	s.http = &http.Server{
		Addr:         cfg.Server.Addr,
		Handler:      s.routes(),
		ReadTimeout:  cfg.Server.ReadTimeout.D(),
		WriteTimeout: cfg.Server.WriteTimeout.D(),
		IdleTimeout:  120 * time.Second,
	}
	return s
}

// Handler 暴露 HTTP 处理链（便于测试）。
func (s *Server) Handler() http.Handler { return s.http.Handler }

// ListenAndServe 启动 HTTP 服务。
func (s *Server) ListenAndServe() error {
	s.logger.Info("Web 管理端已启动",
		"addr", s.cfg.Server.Addr,
		"base_path", s.cfg.Server.BasePath,
		"username", s.cfg.Server.Username,
	)
	err := s.http.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown 优雅停止 HTTP 服务。
func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}

// ---------------------------------------------------------------------------
// 路由
// ---------------------------------------------------------------------------

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()

	// 无需认证。
	mux.HandleFunc("GET /api/health", s.handleHealth)
	mux.HandleFunc("POST /api/login", s.handleLogin)
	mux.HandleFunc("POST /api/logout", s.handleLogout)
	mux.HandleFunc("GET /api/me", s.handleMe)

	// 需要认证。
	mux.Handle("GET /api/overview", s.requireAuth(s.handleOverview))

	mux.Handle("GET /api/tasks", s.requireAuth(s.handleListTasks))
	mux.Handle("POST /api/tasks", s.requireAuth(s.handleCreateTask))
	mux.Handle("POST /api/tasks/validate", s.requireAuth(s.handleValidateCron))
	mux.Handle("GET /api/tasks/{id}", s.requireAuth(s.handleGetTask))
	mux.Handle("PUT /api/tasks/{id}", s.requireAuth(s.handleUpdateTask))
	mux.Handle("POST /api/tasks/{id}/enabled", s.requireAuth(s.handleSetTaskEnabled))
	mux.Handle("DELETE /api/tasks/{id}", s.requireAuth(s.handleDeleteTask))
	mux.Handle("POST /api/tasks/{id}/run", s.requireAuth(s.handleRunTask))
	mux.Handle("POST /api/tasks/{id}/cancel", s.requireAuth(s.handleCancelTask))

	mux.Handle("GET /api/runs", s.requireAuth(s.handleListRuns))
	mux.Handle("GET /api/runs/active", s.requireAuth(s.handleActiveRuns))
	mux.Handle("GET /api/runs/{id}", s.requireAuth(s.handleGetRun))
	mux.Handle("POST /api/runs/{id}/cancel", s.requireAuth(s.handleCancelRun))
	mux.Handle("DELETE /api/runs", s.requireAuth(s.handleDeleteAllRuns))
	mux.Handle("DELETE /api/runs/{id}", s.requireAuth(s.handleDeleteRun))
	// 按保留策略立即清理（只删已结束且过期的记录）。
	mux.Handle("POST /api/runs/prune", s.requireAuth(s.handlePruneRuns))

	mux.Handle("GET /api/settings", s.requireAuth(s.handleGetSettings))
	mux.Handle("PUT /api/settings", s.requireAuth(s.handleUpdateSettings))

	mux.Handle("GET /api/events", s.requireAuth(s.handleEvents))

	mux.Handle("GET /api/rclone", s.requireAuth(s.handleRcloneStatus))
	mux.Handle("POST /api/rclone/restart", s.requireAuth(s.handleRcloneRestart))
	mux.Handle("GET /api/rclone/remotes", s.requireAuth(s.handleRemotes))
	mux.Handle("GET /api/rclone/log", s.requireAuth(s.handleRcloneLog))
	mux.Handle("GET /api/rclone/list", s.requireAuth(s.handleBrowse))
	mux.Handle("GET /api/rclone/about", s.requireAuth(s.handleAbout))
	mux.Handle("GET /api/rclone/options", s.requireAuth(s.handleRcloneOptions))

	// 静态前端。
	static, err := fs.Sub(staticFS, "static")
	if err != nil {
		// embed 路径固定，不会失败；保底返回一个提示页。
		s.logger.Error("加载内嵌静态资源失败", logging.Err(err))
		static = nil
	}
	if static != nil {
		fileServer := http.FileServer(http.FS(static))
		mux.Handle("GET /", spaHandler(static, fileServer))
	}

	handler := http.Handler(mux)

	basePath := s.cfg.Server.BasePath
	if basePath != "" && basePath != "/" {
		prefix := strings.TrimSuffix(basePath, "/")
		inner := handler
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == prefix {
				http.Redirect(w, r, basePath, http.StatusTemporaryRedirect)
				return
			}
			if !strings.HasPrefix(r.URL.Path, basePath) {
				http.NotFound(w, r)
				return
			}
			http.StripPrefix(prefix, inner).ServeHTTP(w, r)
		})
	}

	return s.requestLogger(s.recoverer(handler))
}

// spaHandler 对未命中的路径回退到 index.html（前端自己做路由）。
func spaHandler(static fs.FS, fileServer http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path == "" {
			serveIndex(w, r, static)
			return
		}
		if _, err := fs.Stat(static, path); err != nil {
			serveIndex(w, r, static)
			return
		}
		// 静态资源允许缓存，index.html 关闭缓存。
		if strings.Contains(path, ".") {
			w.Header().Set("Cache-Control", "public, max-age=300")
		}
		fileServer.ServeHTTP(w, r)
	})
}

func serveIndex(w http.ResponseWriter, r *http.Request, static fs.FS) {
	data, err := fs.ReadFile(static, "index.html")
	if err != nil {
		http.Error(w, "前端资源缺失", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(data)
}

// ---------------------------------------------------------------------------
// 中间件
// ---------------------------------------------------------------------------

func (s *Server) requireAuth(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie(sessionCookie)
		if err != nil {
			s.unauthorized(w, r)
			return
		}
		if err := s.auth.verify(cookie.Value); err != nil {
			s.auth.clearCookie(w, r)
			s.unauthorized(w, r)
			return
		}
		next(w, r)
	})
}

func (s *Server) unauthorized(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/api/") {
		writeErr(w, http.StatusUnauthorized, "unauthorized", "请先登录")
		return
	}
	http.Redirect(w, r, s.cfg.Server.BasePath, http.StatusTemporaryRedirect)
}

// statusRecorder 记录响应状态码用于访问日志。
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
	// hijackDisabled 标记该响应是否为 SSE（长连接）。
	stream bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

// Flush 透传 http.Flusher，SSE 依赖它。
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (s *Server) requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		if strings.HasSuffix(r.URL.Path, "/api/events") {
			rec.stream = true
		}

		next.ServeHTTP(rec, r)

		if rec.status == 0 {
			rec.status = http.StatusOK
		}
		// 静态资源与健康检查不记录访问日志，避免噪音。
		if strings.HasPrefix(r.URL.Path, "/api/") {
			s.logger.Info("http 请求",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"bytes", rec.bytes,
				"ip", clientIP(r, s.cfg.Server.TrustedProxy),
				"duration_ms", time.Since(start).Milliseconds(),
			)
		}
	})
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.logger.Error("HTTP 处理 panic",
					"path", r.URL.Path,
					"recover", rec,
				)
				// 响应头已写出时无法再修改状态码，只能终止本次响应。
				if !responseStarted(w) {
					writeErr(w, http.StatusInternalServerError, "panic", "服务器内部错误")
				}
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// responseStarted 判断响应是否已经开始写出。
func responseStarted(w http.ResponseWriter) bool {
	if rec, ok := w.(*statusRecorder); ok {
		return rec.status != 0
	}
	return false
}

// ---------------------------------------------------------------------------
// 登录限流
// ---------------------------------------------------------------------------

type loginLimiter struct {
	mu       sync.Mutex
	attempts map[string]*attemptRecord
	max      int
	window   time.Duration
}

type attemptRecord struct {
	count int
	first time.Time
}

func newLoginLimiter(max int, window time.Duration) *loginLimiter {
	return &loginLimiter{attempts: make(map[string]*attemptRecord), max: max, window: window}
}

// allow 判断该 IP 是否允许继续尝试登录。
func (l *loginLimiter) allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	rec, ok := l.attempts[ip]
	if !ok || time.Since(rec.first) > l.window {
		return true
	}
	return rec.count < l.max
}

// fail 记录一次失败尝试。
func (l *loginLimiter) fail(ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	rec, ok := l.attempts[ip]
	if !ok || time.Since(rec.first) > l.window {
		l.attempts[ip] = &attemptRecord{count: 1, first: time.Now()}
		return
	}
	rec.count++
}

// reset 登录成功后清除记录。
func (l *loginLimiter) reset(ip string) {
	l.mu.Lock()
	delete(l.attempts, ip)
	l.mu.Unlock()
}

// gc 清理过期记录，避免无限增长。
func (l *loginLimiter) gc() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for ip, rec := range l.attempts {
		if time.Since(rec.first) > l.window {
			delete(l.attempts, ip)
		}
	}
}

// StartGC 周期性清理限流记录。
func (s *Server) StartGC(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.limiter.gc()
		}
	}
}
