package main

import (
	"flag"
	"io"
	"reflect"
	"testing"
)

// parseLikeMain 复刻 main() 中除 command 判定外的参数处理路径，
// 返回解析后的参数值与过滤结果。
func parseLikeMain(t *testing.T, args []string) (options, []string) {
	t.Helper()
	var opts options
	fs := newFlagSet(&opts)
	fs.SetOutput(io.Discard)
	filtered := stripUnknownFlags(fs, args)
	if err := fs.Parse(filtered); err != nil {
		t.Fatalf("解析 %q 失败: %v", args, err)
	}
	return opts, filtered
}

// TestStripUnknownFlagsSupervisorCommandLine 用宿主程序实际生成的命令行做回归。
//
// 这是本测试存在的原因：internal/rclone 的 buildArgs 会追加真实 rclone 才有的
// --rc-job-expire-duration / --rc-job-expire-interval。mock 若不忽略它们，会以
// “flag provided but not defined” 退出码 2 秒退，宿主只能看到 context deadline exceeded。
func TestStripUnknownFlagsSupervisorCommandLine(t *testing.T) {
	// 取自 supervisor 的实际输出（见 buildArgs）。
	args := []string{
		"--rc-addr", "127.0.0.1:15572",
		"--log-level", "INFO",
		"--rc-user", "admin", "--rc-pass", "test123",
		"--rc-job-expire-duration", "1h",
		"--rc-job-expire-interval", "5m",
	}

	opts, filtered := parseLikeMain(t, args)

	if opts.rcAddr != "127.0.0.1:15572" {
		t.Errorf("rcAddr = %q，期望 127.0.0.1:15572", opts.rcAddr)
	}
	if opts.logLevel != "INFO" {
		t.Errorf("logLevel = %q，期望 INFO", opts.logLevel)
	}
	if opts.rcUser != "admin" || opts.rcPass != "test123" {
		t.Errorf("凭据 = %q/%q，期望 admin/test123", opts.rcUser, opts.rcPass)
	}
	if opts.noAuth {
		t.Error("noAuth 应为 false")
	}
	want := []string{
		"--rc-addr", "127.0.0.1:15572",
		"--log-level", "INFO",
		"--rc-user", "admin", "--rc-pass", "test123",
	}
	if !reflect.DeepEqual(filtered, want) {
		t.Errorf("过滤结果 = %q，期望 %q", filtered, want)
	}
}

// TestStripUnknownFlagsKeepsLaterKnownFlags 覆盖最容易被忽略的顺序陷阱：
// flag 包遇到第一个非 flag 参数就停止解析。若只丢弃未知 flag 而把它后面的
// 取值留成位置参数，后面所有已知 flag 都会被静默忽略——这比直接报错更难查。
func TestStripUnknownFlagsKeepsLaterKnownFlags(t *testing.T) {
	args := []string{
		"--rc-job-expire-duration", "1h", // 未知，且带取值
		"--rc-addr", "127.0.0.1:6000", // 已知，必须仍被解析
	}

	opts, filtered := parseLikeMain(t, args)

	if opts.rcAddr != "127.0.0.1:6000" {
		t.Errorf("rcAddr = %q，期望 127.0.0.1:6000（未知 flag 的取值不应截断后续解析）", opts.rcAddr)
	}
	if len(filtered) == 0 || filtered[0] != "--rc-addr" {
		t.Errorf("过滤结果 = %q，期望以 --rc-addr 开头", filtered)
	}
}

// TestStripUnknownFlagsInlineAndBoolForms 覆盖 --flag=value 与布尔 flag：
// 布尔 flag 不得吞掉后一个 token。
func TestStripUnknownFlagsInlineAndBoolForms(t *testing.T) {
	args := []string{
		"--rc-no-auth",             // 布尔，不应吞掉下一个 token
		"--rc-addr=127.0.0.1:7000", // 内联取值
		"--transfers", "8",         // 未知（真实 rclone 的参数）
	}

	opts, filtered := parseLikeMain(t, args)

	if !opts.noAuth {
		t.Error("noAuth 应为 true")
	}
	if opts.rcAddr != "127.0.0.1:7000" {
		t.Errorf("rcAddr = %q，期望 127.0.0.1:7000", opts.rcAddr)
	}
	want := []string{"--rc-no-auth", "--rc-addr=127.0.0.1:7000"}
	if !reflect.DeepEqual(filtered, want) {
		t.Errorf("过滤结果 = %q，期望 %q", filtered, want)
	}
}

// TestStripUnknownFlagsPreservesPositionalArgs 保证 rcd 一类的未知位置参数
// 仍被保留，交由 main 判定并沿用“忽略未知位置参数”的既有行为。
func TestStripUnknownFlagsPreservesPositionalArgs(t *testing.T) {
	args := []string{"rcd", "--rc-addr", "127.0.0.1:7001", "extra"}

	var opts options
	fs := newFlagSet(&opts)
	filtered := stripUnknownFlags(fs, args)

	want := []string{"rcd", "--rc-addr", "127.0.0.1:7001", "extra"}
	if !reflect.DeepEqual(filtered, want) {
		t.Errorf("过滤结果 = %q，期望 %q", filtered, want)
	}
}

// TestTakesValueDistinguishesBoolFlags 锁定“哪些 flag 需要取值”的判定，
// 因为 stripUnknownFlags 依赖它来决定是否连带吞掉下一个 token。
func TestTakesValueDistinguishesBoolFlags(t *testing.T) {
	var opts options
	fs := newFlagSet(&opts)

	got := map[string]bool{}
	fs.VisitAll(func(f *flag.Flag) { got[f.Name] = takesValue(f) })

	want := map[string]bool{
		"rc-addr":    true,
		"rc-user":    true,
		"rc-pass":    true,
		"log-level":  true,
		"rc-no-auth": false,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("takesValue = %v，期望 %v", got, want)
	}
}
