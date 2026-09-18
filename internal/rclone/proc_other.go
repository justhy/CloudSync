//go:build !windows && !linux

package rclone

import (
	"os/exec"
	"syscall"
)

// prepareCommand 在类 Unix（macOS/BSD）上把子进程放入独立进程组。
func prepareCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}
