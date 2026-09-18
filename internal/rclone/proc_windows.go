//go:build windows

package rclone

import (
	"fmt"
	"os/exec"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

// Windows 下通过 Job Object 保证父进程退出（含被强杀）时子进程一并回收。
// 设置 JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE 后，只要宿主进程持有的 job 句柄
// 被系统关闭，job 内所有进程都会被终止。
const (
	jobObjectExtendedLimitInformation   = 9
	jobObjectLimitKillOnJobClose        = 0x00002000
	createNewProcessGroup               = 0x00000200
	createNoWindow                      = 0x08000000
	jobObjectBasicAccountingInformation = 1
)

var (
	kernel32                      = syscall.NewLazyDLL("kernel32.dll")
	procCreateJobObjectW          = kernel32.NewProc("CreateJobObjectW")
	procSetInformationJobObject   = kernel32.NewProc("SetInformationJobObject")
	procAssignProcessToJobObject  = kernel32.NewProc("AssignProcessToJobObject")
	procCloseHandle               = kernel32.NewProc("CloseHandle")
	procQueryInformationJobObject = kernel32.NewProc("QueryInformationJobObject")
)

type ioCounters struct {
	ReadOperationCount  uint64
	WriteOperationCount uint64
	OtherOperationCount uint64
	ReadTransferCount   uint64
	WriteTransferCount  uint64
	OtherTransferCount  uint64
}

type jobObjectBasicLimitInformation struct {
	PerProcessUserTimeLimit int64
	PerJobUserTimeLimit     int64
	LimitFlags              uint32
	MinimumWorkingSetSize   uintptr
	MaximumWorkingSetSize   uintptr
	ActiveProcessLimit      uint32
	Affinity                uintptr
	PriorityClass           uint32
	SchedulingClass         uint32
}

type jobObjectExtendedLimitInformationStruct struct {
	BasicLimitInformation jobObjectBasicLimitInformation
	IoInfo                ioCounters
	ProcessMemoryLimit    uintptr
	JobMemoryLimit        uintptr
	PeakProcessMemoryUsed uintptr
	PeakJobMemoryUsed     uintptr
}

// jobs 保存每个子进程对应的 job 句柄。
var jobs sync.Map // map[*exec.Cmd]syscall.Handle

// prepareCommand 设置进程组与无窗口标志。
func prepareCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: createNewProcessGroup | createNoWindow,
		HideWindow:    true,
	}
}

// afterStart 在子进程启动后立即把它加入 Job Object。
func afterStart(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	job, err := createKillOnCloseJob()
	if err != nil {
		// 退化为仅依靠进程组：不阻断启动，但记录原因。
		return fmt.Errorf("创建 Job Object 失败（子进程可能与父进程同时退出不保证）: %w", err)
	}
	if err := assignProcessToJob(job, cmd.Process.Pid); err != nil {
		closeHandle(job)
		return fmt.Errorf("将 rclone 进程加入 Job Object 失败: %w", err)
	}
	jobs.Store(cmd, job)
	return nil
}

// releaseCommand 关闭 job 句柄。
func releaseCommand(cmd *exec.Cmd) {
	if v, ok := jobs.LoadAndDelete(cmd); ok {
		if h, ok := v.(syscall.Handle); ok {
			closeHandle(h)
		}
	}
}

// forceKill 终止子进程（job 句柄会在 releaseCommand 中关闭并顺带清理 job 内进程）。
func forceKill(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	err := cmd.Process.Kill()
	// TerminateProcess 对已退出进程会报错，这里可忽略。
	if err != nil && !processAlive(cmd.Process.Pid) {
		return nil
	}
	return err
}

// processAlive 通过 OpenProcess 判断进程是否存在。
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	const processQueryLimitedInformation = 0x1000
	h, err := syscall.OpenProcess(processQueryLimitedInformation, false, uint32(pid))
	if err != nil {
		return false
	}
	_ = syscall.CloseHandle(h)
	return true
}

func createKillOnCloseJob() (syscall.Handle, error) {
	r, _, err := procCreateJobObjectW.Call(0, 0)
	if r == 0 {
		return 0, err
	}
	job := syscall.Handle(r)

	var info jobObjectExtendedLimitInformationStruct
	info.BasicLimitInformation.LimitFlags = jobObjectLimitKillOnJobClose
	_, _, err = procSetInformationJobObject.Call(
		uintptr(job),
		uintptr(jobObjectExtendedLimitInformation),
		uintptr(unsafe.Pointer(&info)),
		unsafe.Sizeof(info),
	)
	if err != syscall.Errno(0) {
		closeHandle(job)
		return 0, err
	}
	return job, nil
}

func assignProcessToJob(job syscall.Handle, pid int) error {
	const processSetQuota = 0x0100
	const processTerminate = 0x0001
	h, err := syscall.OpenProcess(processSetQuota|processTerminate, false, uint32(pid))
	if err != nil {
		return err
	}
	defer syscall.CloseHandle(h)
	_, _, callErr := procAssignProcessToJobObject.Call(uintptr(job), uintptr(h))
	if callErr != syscall.Errno(0) {
		return callErr
	}
	return nil
}

func closeHandle(h syscall.Handle) {
	if h == 0 {
		return
	}
	_, _, _ = procCloseHandle.Call(uintptr(h))
}

// graceful 在 Windows 上没有等价的优雅信号，
// 直接返回 false 让上层走 TerminateProcess（以及 Job Object 回收）。
func graceful(_ *exec.Cmd, _ time.Duration) bool { return false }
