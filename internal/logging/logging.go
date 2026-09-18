// Package logging 提供基于 log/slog 的结构化日志。
//
// 日志同时输出到 stderr 与（可选的）文件，格式支持 json / text。
// 所有日志都带有固定的 service 字段，业务日志通过 slog 的 With 附加
// task_id / run_id / job_id 等上下文字段，便于集中检索。
package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Options 描述日志初始化参数。
type Options struct {
	// Level 取值 debug/info/warn/error，默认 info。
	Level string
	// Format 取值 json/text，默认 json。
	Format string
	// File 非空时额外写入该文件（追加）。
	File string
	// AddSource 为 true 时记录调用位置。
	AddSource bool
	// Output 为 nil 时使用 os.Stderr。
	Output io.Writer
	// Service 作为固定字段写入每条日志。
	Service string
}

// ParseLevel 将字符串转换为 slog.Level。
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error", "err", "fatal":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// Setup 构建 *slog.Logger 并设置为 slog 默认 logger。
// 返回的 close 函数用于关闭日志文件。
func Setup(opts Options) (*slog.Logger, func() error, error) {
	out := opts.Output
	if out == nil {
		out = os.Stderr
	}

	var closers []func() error
	writers := []io.Writer{out}

	if opts.File != "" {
		if dir := filepath.Dir(opts.File); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, nil, fmt.Errorf("创建日志目录: %w", err)
			}
		}
		f, err := os.OpenFile(opts.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, nil, fmt.Errorf("打开日志文件 %s: %w", opts.File, err)
		}
		writers = append(writers, f)
		closers = append(closers, f.Close)
	}

	w := io.MultiWriter(writers...)

	level := ParseLevel(opts.Level)
	handlerOpts := &slog.HandlerOptions{Level: level, AddSource: opts.AddSource}

	var handler slog.Handler
	if strings.EqualFold(opts.Format, "text") {
		handler = slog.NewTextHandler(w, handlerOpts)
	} else {
		handler = slog.NewJSONHandler(w, handlerOpts)
	}

	if opts.Service != "" {
		handler = handler.WithAttrs([]slog.Attr{slog.String("service", opts.Service)})
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)

	closeFn := func() error {
		var err error
		for _, c := range closers {
			if e := c(); e != nil && err == nil {
				err = e
			}
		}
		return err
	}
	return logger, closeFn, nil
}

// Discard 返回一个丢弃全部日志的 logger，供测试使用。
func Discard() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

// ---------------------------------------------------------------------------
// 字段辅助函数：统一日志字段命名，避免各处拼写不一致。
// ---------------------------------------------------------------------------

// Task 返回任务相关字段。
func Task(id int64, name string) slog.Attr {
	return slog.Group("task", slog.Int64("id", id), slog.String("name", name))
}

// Run 返回运行记录相关字段。
func Run(id int64, status string) slog.Attr {
	return slog.Group("run", slog.Int64("id", id), slog.String("status", status))
}

// Job 返回 rclone job 相关字段。
func Job(id int64) slog.Attr {
	return slog.Int64("job_id", id)
}

// Err 在 err 为 nil 时返回空 Attr（slog 会忽略空 Attr）。
func Err(err error) slog.Attr {
	if err == nil {
		return slog.Attr{}
	}
	return slog.String("error", err.Error())
}

// Logger 携带一个基础 logger，方便结构体持有。
type Logger struct {
	*slog.Logger
}

// New 包装一个 slog.Logger。
func New(l *slog.Logger) *Logger { return &Logger{Logger: l} }

// WithContext 兼容性占位：保留 context 以便未来接入 trace。
func (l *Logger) WithContext(_ context.Context) *slog.Logger { return l.Logger }

// RingBuffer 是一个固定容量的行缓冲，用于保存子进程最近输出。
// 它同时满足 io.Writer，可安全地被多 goroutine 并发写入。
//
// 每行都会被分配一个单调递增的序号，调用方可用 Mark/Seq 记录游标，
// 之后用 Since 取得「某个时间点之后」的输出，从而把全局日志切分出
// 与单次任务运行对应的片段。
type RingBuffer struct {
	mu    sync.RWMutex
	lines []ringEntry
	max   int
	next  int64
	// partial 保存尚未遇到换行的尾部片段。
	partial string
}

type ringEntry struct {
	seq  int64
	line string
}

// NewRingBuffer 创建一个容量为 max 行的环形缓冲。
func NewRingBuffer(max int) *RingBuffer {
	if max <= 0 {
		max = 1000
	}
	return &RingBuffer{max: max}
}

// Write 按行切分写入缓冲，返回值实现 io.Writer 契约。
func (r *RingBuffer) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	text := r.partial + string(p)
	lines := strings.Split(text, "\n")
	r.partial = lines[len(lines)-1]
	for _, line := range lines[:len(lines)-1] {
		r.pushLocked(line)
	}
	return len(p), nil
}

func (r *RingBuffer) pushLocked(line string) {
	r.lines = append(r.lines, ringEntry{seq: r.next, line: line})
	r.next++
	if len(r.lines) > r.max {
		// 一次性截断，避免频繁拷贝。
		drop := len(r.lines) - r.max
		r.lines = append([]ringEntry(nil), r.lines[drop:]...)
	}
}

// Mark 返回当前游标；之后可用 Since(mark) 取得新增的行。
func (r *RingBuffer) Mark() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.next
}

// Since 返回序号 >= mark 的行。若缓冲区已滚动导致起始行被丢弃，
// overflow 返回 true。
func (r *RingBuffer) Since(mark int64) (lines []string, overflow bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.lines) > 0 && mark < r.lines[0].seq {
		overflow = true
	}
	for _, e := range r.lines {
		if e.seq >= mark {
			lines = append(lines, e.line)
		}
	}
	if r.partial != "" && r.next >= mark {
		lines = append(lines, r.partial)
	}
	return lines, overflow
}

// Lines 返回全部已缓冲的行（拷贝）。
func (r *RingBuffer) Lines() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.lines)+1)
	for _, e := range r.lines {
		out = append(out, e.line)
	}
	if r.partial != "" {
		out = append(out, r.partial)
	}
	return out
}

// Tail 返回最后 n 行。
func (r *RingBuffer) Tail(n int) []string {
	lines := r.Lines()
	if n > 0 && len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return lines
}

// Len 返回当前缓冲行数。
func (r *RingBuffer) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.lines)
}

// Reset 清空缓冲（保留序号单调性，避免已记录的游标失效）。
func (r *RingBuffer) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines = nil
	r.partial = ""
}
