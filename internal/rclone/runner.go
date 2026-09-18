package rclone

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"cloudsync/internal/config"
)

// Runner 执行「一次性」rclone 子命令，例如 lsjson / deletefile。
//
// 之所以不复用 Supervisor：Supervisor 管的是长期常驻的 rcd（探活、自动重启、
// Job Object、进程退出时连带关闭），而这里需要的是"跑一次就结束、输出归调用方"。
//
// 但有一件事必须与 Supervisor 保持一致 —— 二进制路径与 --config。
// 否则会出现「托管的 sync 用 A 二进制/配置，清理步骤用 B 二进制/配置」的错配，
// 症状是"看起来清理了，实际清的是另一个 remote"，极难排查。
type Runner struct {
	binary     string
	configFile string
}

// NewRunner 创建一次性命令执行器。
func NewRunner(cfg config.RcloneConfig) *Runner {
	return &Runner{
		binary:     resolveBinary(cfg),
		configFile: cfg.ConfigFile,
	}
}

// resolveBinary 返回实际执行的 rclone 可执行文件。
// cfg.Path 为空时退回 PATH 查找，判定规则与 Supervisor 完全一致。
func resolveBinary(cfg config.RcloneConfig) string {
	if cfg.Path != "" {
		return cfg.Path
	}
	return "rclone"
}

// Run 执行一次 rclone 命令，把 stdout/stderr 合并写入 out。
//
// args 不含可执行文件名本身，例如 Run(ctx, w, "deletefile", "dst:a/b.mp3")。
// ctx 取消时会连带终止子进程；out 为 nil 时丢弃子进程输出。
func (r *Runner) Run(ctx context.Context, out io.Writer, args ...string) error {
	full := r.buildArgs(args)
	cmd := r.command(ctx, full)
	if out == nil {
		out = io.Discard
	}
	cmd.Stdout = out
	cmd.Stderr = cmd.Stdout

	return r.run(ctx, cmd, full)
}

// Capture 执行一次 rclone 命令并返回标准输出。
//
// 与 Run 的区别是 stderr 单独缓存、不混入 stdout —— 需要解析输出（如 lsjson 的
// JSON）时必须走这里，否则 rclone 打在 stderr 上的 NOTICE/ERROR 会污染 JSON。
func (r *Runner) Capture(ctx context.Context, args ...string) ([]byte, error) {
	full := r.buildArgs(args)
	cmd := r.command(ctx, full)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := r.run(ctx, cmd, full); err != nil {
		if detail := strings.TrimSpace(stderr.String()); detail != "" {
			return nil, fmt.Errorf("%w: %s", err, detail)
		}
		return nil, err
	}
	return stdout.Bytes(), nil
}

func (r *Runner) buildArgs(args []string) []string {
	full := make([]string, 0, len(args)+2)
	if r.configFile != "" {
		full = append(full, "--config", r.configFile)
	}
	return append(full, args...)
}

func (r *Runner) command(ctx context.Context, full []string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, r.binary, full...)
	cmd.Env = os.Environ()
	cmd.Stdin = nil
	prepareCommand(cmd)
	return cmd
}

func (r *Runner) run(ctx context.Context, cmd *exec.Cmd, full []string) error {
	if err := cmd.Start(); err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return fmt.Errorf("找不到 rclone 可执行文件 %q，请设置 rclone.path 或将其加入 PATH: %w",
				r.binary, err)
		}
		return fmt.Errorf("启动 rclone 失败 (%s %s): %w", r.binary, strings.Join(full, " "), err)
	}
	if err := cmd.Wait(); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("rclone 命令被中断 (%s): %w", strings.Join(full, " "), err)
		}
		return fmt.Errorf("rclone 命令失败 (%s): %w", strings.Join(full, " "), err)
	}
	return nil
}
