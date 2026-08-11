// Package mountctl locates and controls telfs mount daemons via /proc.
//
// It is the single home for "find the daemon serving this mountpoint,
// signal it, and make sure the kernel mount actually goes away" logic,
// shared by the web supervisor and the `telfs unmount` CLI so both
// paths behave identically no matter who started the daemon.
package mountctl

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// FusermountBin resolves the FUSE unmount helper: fusermount3 (fuse3)
// first, then legacy fusermount. fuse3-only distros ship no
// "fusermount" name at all, so hardcoding it made every emergency
// unmount a silent no-op.
func FusermountBin() string {
	for _, name := range []string{"fusermount3", "fusermount"} {
		if p, err := exec.LookPath(name); err == nil {
			return p
		}
	}
	return "fusermount"
}

// ScanProcMounts returns every fuse.telfs mountpoint currently in
// /proc/mounts.
func ScanProcMounts() []string {
	f, err := os.Open("/proc/mounts")
	if err != nil {
		return nil
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) >= 3 && fields[2] == "fuse.telfs" {
			out = append(out, fields[1])
		}
	}
	return out
}

// IsMounted reports whether mountpoint is currently a fuse.telfs entry.
func IsMounted(mountpoint string) bool {
	for _, m := range ScanProcMounts() {
		if m == mountpoint {
			return true
		}
	}
	return false
}

// Unmount detaches a fuse.telfs mountpoint: polite `fusermount -u`
// first, then the lazy `-u -z` form on EBUSY (open files keep working
// against a detached mount until closed). Returns nil once the entry
// has left /proc/mounts.
func Unmount(mountpoint string) error {
	if !IsMounted(mountpoint) {
		return nil
	}
	if err := exec.Command(FusermountBin(), "-u", mountpoint).Run(); err == nil {
		return nil
	}
	return LazyUnmount(mountpoint)
}

// LazyUnmount runs `fusermount -u -z` — the mount disappears from the
// namespace immediately; lingering FDs unmap once closed.
func LazyUnmount(mountpoint string) error {
	if err := exec.Command(FusermountBin(), "-u", "-z", mountpoint).Run(); err != nil {
		return fmt.Errorf("%s -u -z %s: %w", filepath.Base(FusermountBin()), mountpoint, err)
	}
	return nil
}

// FindDaemonPid scans /proc/*/cmdline for the telfs daemon that
// mounted `mountpoint`. The daemon's argv contains the "mount"
// subcommand and ends with the mountpoint (see cmd/telfs mount arg
// construction). Returns (pid, true) on match.
func FindDaemonPid(mountpoint string) (int, bool) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, false
	}
	for _, e := range entries {
		name := e.Name()
		if name == "" || name[0] < '0' || name[0] > '9' {
			continue
		}
		cmdline, err := os.ReadFile("/proc/" + name + "/cmdline")
		if err != nil {
			continue
		}
		argv := strings.Split(strings.TrimRight(string(cmdline), "\x00"), "\x00")
		if len(argv) < 2 {
			continue
		}
		if filepath.Base(argv[0]) != "telfs" {
			continue
		}
		hasMount := false
		for _, a := range argv[1:] {
			if a == "mount" {
				hasMount = true
				break
			}
		}
		if !hasMount {
			continue
		}
		if argv[len(argv)-1] != mountpoint {
			continue
		}
		var pid int
		if _, err := fmt.Sscanf(name, "%d", &pid); err == nil {
			return pid, true
		}
	}
	return 0, false
}

// FindByProfile locates a running telfs mount daemon whose
// TELFS_PROFILE matches. Returns the pid and mountpoint of the first
// match.
func FindByProfile(profile string) (pid int, mountpoint string, found bool) {
	if profile == "" {
		return 0, "", false
	}
	for _, mp := range ScanProcMounts() {
		p, ok := FindDaemonPid(mp)
		if !ok {
			continue
		}
		if ReadEnvVar(p, "TELFS_PROFILE") == profile {
			return p, mp, true
		}
	}
	return 0, "", false
}

// ReadEnvVar pulls a single environment variable's value from
// /proc/<pid>/environ. Empty string when missing or unreadable.
func ReadEnvVar(pid int, key string) string {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", pid))
	if err != nil {
		return ""
	}
	prefix := key + "="
	for _, kv := range strings.Split(string(b), "\x00") {
		if strings.HasPrefix(kv, prefix) {
			return kv[len(prefix):]
		}
	}
	return ""
}

// ProcStartTime reads /proc/<pid>/stat's start-time field (jiffies
// since boot) and converts it to wall-clock via /proc/stat's btime.
// Zero time on any parse failure.
func ProcStartTime(pid int) time.Time {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return time.Time{}
	}
	// The comm field (#2) is wrapped in parens and may contain spaces,
	// so split AFTER the closing paren; starttime is then index 19.
	s := string(b)
	cp := strings.LastIndexByte(s, ')')
	if cp < 0 {
		return time.Time{}
	}
	fields := strings.Fields(s[cp+1:])
	if len(fields) < 20 {
		return time.Time{}
	}
	var startTicks int64
	if _, err := fmt.Sscanf(fields[19], "%d", &startTicks); err != nil {
		return time.Time{}
	}
	boot := bootTime()
	if boot.IsZero() {
		return time.Time{}
	}
	// USER_HZ is conventionally 100 on Linux; correct in practice for
	// every kernel TelFS targets.
	const userHz = 100
	return boot.Add(time.Duration(startTicks) * time.Second / userHz)
}

func bootTime() time.Time {
	b, err := os.ReadFile("/proc/stat")
	if err != nil {
		return time.Time{}
	}
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(line, "btime ") {
			continue
		}
		var btime int64
		if _, err := fmt.Sscanf(line, "btime %d", &btime); err == nil {
			return time.Unix(btime, 0)
		}
	}
	return time.Time{}
}

// ResolveStdoutLog returns the file /proc/<pid>/fd/1 points to when
// it's a regular file — the daemon's log when stdout was redirected.
// Empty for ttys and pipes.
func ResolveStdoutLog(pid int) string {
	target, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/1", pid))
	if err != nil {
		return ""
	}
	if target == "" || target[0] != '/' {
		return ""
	}
	if strings.HasPrefix(target, "/dev/") || strings.HasPrefix(target, "pipe:") {
		return ""
	}
	if st, err := os.Stat(target); err != nil || !st.Mode().IsRegular() {
		return ""
	}
	return target
}

// StopDaemon terminates the mount daemon and makes sure the kernel
// mount is gone. SIGTERM first — the daemon's orderly shutdown drains
// uploads and takes a final snapshot, so give it the full timeout
// (its own hard watchdog is 30 s; pass ≥35 s). Escalates to SIGKILL +
// lazy unmount, and verifies the mountpoint actually left /proc/mounts
// either way (the daemon can exit cleanly yet leave the mount behind
// when its own fusermount fails, e.g. EBUSY).
func StopDaemon(pid int, mountpoint string, timeout time.Duration) error {
	if pid > 0 {
		_ = syscall.Kill(pid, syscall.SIGTERM)
		if !waitPidGone(pid, timeout) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
			if !waitPidGone(pid, 5*time.Second) {
				return fmt.Errorf("pid %d survived SIGKILL", pid)
			}
		}
	}
	if mountpoint != "" && IsMounted(mountpoint) {
		if err := LazyUnmount(mountpoint); err != nil {
			return fmt.Errorf("daemon stopped but mount remains: %w", err)
		}
	}
	return nil
}

// waitPidGone polls kill(pid, 0) until the process disappears or the
// timeout elapses.
func waitPidGone(pid int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		if syscall.Kill(pid, 0) != nil {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		<-tick.C
	}
}

// ErrNoDaemon reports that no telfs daemon could be located for a
// mountpoint that is (or was) mounted.
var ErrNoDaemon = errors.New("no telfs mount daemon found")
