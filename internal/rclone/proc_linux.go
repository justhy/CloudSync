package rclone

import (
	"os/exec"
	"syscall"
)

// prepareCommand 在 Linux 上额外设置 Pdeathsig：
// 父进程意外死亡时内核会向子进程发送 SIGTERM，避免 rclone 变成孤儿进程。
func prepareCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid:   true,
		Pdeathsig: syscall.SIGTERM,
	}
}
