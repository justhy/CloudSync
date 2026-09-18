//go:build !windows

package rclone

import (
	"os/exec"
	"syscall"
	"time"
)

// afterStart 在 Unix 上无需额外操作：父进程死亡时的回收由 Pdeathsig（Linux）
// 与独立进程组共同保证。
func afterStart(_ *exec.Cmd) error { return nil }

// releaseCommand 释放平台相关资源。
func releaseCommand(_ *exec.Cmd) {}

// forceKill 向整个进程组发送 SIGKILL，确保 rclone 及其子进程一并退出。
func forceKill(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	if pgid, err := syscall.Getpgid(cmd.Process.Pid); err == nil && pgid > 0 {
		if err := syscall.Kill(-pgid, syscall.SIGKILL); err == nil {
			return nil
		}
	}
	return cmd.Process.Kill()
}

// processAlive 判断进程是否仍存在。
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	// 信号 0 不发送信号，仅做存在性与权限检查。
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}

// graceful 先向进程组发送 SIGTERM，在 timeout 内退出则返回 true。
func graceful(cmd *exec.Cmd, timeout time.Duration) bool {
	if cmd == nil || cmd.Process == nil {
		return true
	}
	pid := cmd.Process.Pid
	if pgid, err := syscall.Getpgid(pid); err == nil && pgid > 0 {
		_ = syscall.Kill(-pgid, syscall.SIGTERM)
	} else {
		_ = cmd.Process.Signal(syscall.SIGTERM)
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !processAlive(pid) {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return !processAlive(pid)
}
