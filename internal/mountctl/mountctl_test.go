package mountctl

import (
	"os"
	"strings"
	"testing"
	"time"
)

// TestScanProcMounts: smoke — returns a slice (possibly empty) without
// panicking; content depends on the host.
func TestScanProcMounts(t *testing.T) {
	_ = ScanProcMounts()
}

// TestResolveStdoutLog_Skip: pipe + tty + non-regular targets must
// return empty so we never try to read a non-file as the log.
func TestResolveStdoutLog_Skip(t *testing.T) {
	// pid 1 (init) on systemd hosts has stdout pointing at /dev/null,
	// /dev/console, or a journald socket — none of which are regular
	// files. ResolveStdoutLog should return "" without erroring.
	got := ResolveStdoutLog(1)
	if got != "" {
		// Some hosts have init's stdout in a regular file (rare); just
		// log, don't fail.
		t.Logf("ResolveStdoutLog(1) = %q (host-specific)", got)
	}
}

// TestReadEnvVar_Missing: a process without the requested env var
// should return "" without erroring. Self-test on our own pid.
func TestReadEnvVar_Missing(t *testing.T) {
	got := ReadEnvVar(os.Getpid(), "TELFS_TEST_DEFINITELY_UNSET_ENV_VAR_xyz_123")
	if got != "" {
		t.Fatalf("ReadEnvVar missing key returned %q, want empty", got)
	}
}

// TestProcStartTime: should return a time in the past, never the
// future (sanity check against a parse bug flipping boot time).
func TestProcStartTime(t *testing.T) {
	got := ProcStartTime(os.Getpid())
	if got.IsZero() {
		t.Skip("no /proc/<pid>/stat")
	}
	if got.After(time.Now()) {
		t.Fatalf("ProcStartTime = %v is in the future (now = %v)", got, time.Now())
	}
}

// TestIsMountedNonexistent: a path that can't be a mountpoint reports
// false.
func TestIsMountedNonexistent(t *testing.T) {
	if IsMounted("/definitely/not/a/mountpoint/xyz") {
		t.Fatal("IsMounted returned true for a nonexistent path")
	}
}

// TestStopDaemonNoPidNoMount: with no pid and an unmounted path,
// StopDaemon is a no-op success.
func TestStopDaemonNoPidNoMount(t *testing.T) {
	if err := StopDaemon(0, "/definitely/not/a/mountpoint/xyz", time.Second); err != nil {
		t.Fatalf("StopDaemon no-op returned %v", err)
	}
}

// TestFindByProfileEmpty: empty profile never matches.
func TestFindByProfileEmpty(t *testing.T) {
	if _, _, found, err := FindByProfile(""); found || err != nil {
		t.Fatalf("FindByProfile(\"\") = found %v, err %v", found, err)
	}
}

func TestProfileOfSource(t *testing.T) {
	if p, ok := ProfileOf(Mount{Source: SourceFor("home"), Point: "/x"}); !ok || p != "home" {
		t.Fatalf("telfs:home → %q %v", p, ok)
	}
	// A v0.22-style source with no daemon behind it: unknown.
	if _, ok := ProfileOf(Mount{Source: "telfs", Point: "/definitely/not/mounted"}); ok {
		t.Fatal("a plain \"telfs\" source without a readable daemon must be unknown")
	}
	if got := unescapeMount(`/home/a\040b`); got != "/home/a b" {
		t.Fatalf("unescape: %q", got)
	}
	e := &ErrUnknownMount{Mountpoints: []string{"/m"}}
	if !strings.Contains(e.Error(), "/m") {
		t.Fatal(e.Error())
	}
}
