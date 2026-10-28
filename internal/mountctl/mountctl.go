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

// SourcePrefix starts the mount source of a TelFS mount made by a P1 or
// newer daemon: "telfs:<profile>". /proc/mounts is readable by every
// process, so the profile is known even when the daemon is non-dumpable
// (its environ is hidden) or dead (an orphaned mount).
const SourcePrefix = "telfs:"

// SourceFor returns the mount source for a profile and its filesystem:
// "telfs:<profile>:<fs_uuid>" (profile names have no ':'). fsUUID may be
// empty.
func SourceFor(profile, fsUUID string) string {
	if fsUUID == "" {
		return SourcePrefix + profile
	}
	return SourcePrefix + profile + ":" + fsUUID
}

// versionField marks a version mount's source.
const versionField = "v="

// SourceForVersion is the mount source of a read-only mount of a
// filesystem's version (spec v1 §2): "telfs:<profile>:<fs_uuid>:v=<name>"
// (version names have no ':'). Older readers see the profile as before
// and an fs_uuid that matches no filesystem.
func SourceForVersion(profile, fsUUID, version string) string {
	return SourceFor(profile, fsUUID) + ":" + versionField + version
}

// Mount is one fuse.telfs entry of /proc/mounts.
type Mount struct {
	Source   string
	Point    string
	ReadOnly bool // mounted with the "ro" option
}

// sourceFields splits a P1+ mount source into profile, fs_uuid, rest.
func sourceFields(m Mount) []string {
	if !strings.HasPrefix(m.Source, SourcePrefix) {
		return nil
	}
	return strings.Split(strings.TrimPrefix(m.Source, SourcePrefix), ":")
}

// FSUUIDOf returns the fs_uuid in a P1+ mount source, or "".
func FSUUIDOf(m Mount) string {
	if f := sourceFields(m); len(f) >= 2 {
		return f[1]
	}
	return ""
}

// VersionOf returns the version a mount serves ("" for a mount of the
// live filesystem).
func VersionOf(m Mount) string {
	f := sourceFields(m)
	if len(f) >= 3 && strings.HasPrefix(f[2], versionField) {
		return strings.TrimPrefix(f[2], versionField)
	}
	return ""
}

// ScanMounts returns every fuse.telfs entry currently in /proc/mounts.
func ScanMounts() []Mount {
	f, err := os.Open("/proc/mounts")
	if err != nil {
		return nil
	}
	defer f.Close()
	var out []Mount
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) >= 3 && fields[2] == "fuse.telfs" {
			m := Mount{Source: unescapeMount(fields[0]), Point: unescapeMount(fields[1])}
			if len(fields) >= 4 {
				for _, o := range strings.Split(fields[3], ",") {
					if o == "ro" {
						m.ReadOnly = true
					}
				}
			}
			out = append(out, m)
		}
	}
	return out
}

// ScanProcMounts returns every fuse.telfs mountpoint currently in
// /proc/mounts.
func ScanProcMounts() []string {
	var out []string
	for _, m := range ScanMounts() {
		out = append(out, m.Point)
	}
	return out
}

// unescapeMount undoes /proc/mounts' octal escapes (\040 for a space).
func unescapeMount(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			var v int
			if _, err := fmt.Sscanf(s[i+1:i+4], "%o", &v); err == nil {
				b.WriteByte(byte(v))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// ProfileOf returns the profile serving a TelFS mount: from its source
// ("telfs:<profile>"), else — for mounts made by v0.22 daemons, whose
// source is plain "telfs" and whose environ is still readable — from the
// daemon's TELFS_PROFILE. ("", false) when it cannot be determined.
func ProfileOf(m Mount) (string, bool) {
	if strings.HasPrefix(m.Source, SourcePrefix) {
		p, _, _ := strings.Cut(strings.TrimPrefix(m.Source, SourcePrefix), ":")
		if p != "" {
			return p, true
		}
	}
	if pid, ok := FindDaemonPid(m.Point); ok {
		if p := ReadEnvVar(pid, "TELFS_PROFILE"); p != "" {
			return p, true
		}
	}
	return "", false
}

// ErrUnknownMount: some TelFS mount's profile cannot be determined, so a
// check of the form "is profile X mounted?" cannot answer "no" safely.
type ErrUnknownMount struct{ Mountpoints []string }

func (e *ErrUnknownMount) Error() string {
	return fmt.Sprintf("cannot tell which profile serves the TelFS mount(s) at %s — unmount them first (telfs unmount <mountpoint>)",
		strings.Join(e.Mountpoints, ", "))
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

// FindByProfile locates a TelFS mount of a profile and its daemon (pid 0
// when the mount is orphaned): the live filesystem's mount, else a
// version mount — every mount of the profile uses its session, so a
// safety check (delete, reset-auth, logout, secret change) must see them
// all. It FAILS CLOSED: when no mount matches but some mount's profile is
// unknown, it returns *ErrUnknownMount — the caller must refuse rather
// than assume "not mounted".
func FindByProfile(profile string) (pid int, mountpoint string, found bool, err error) {
	return findByProfile(ScanMounts(), profile, false)
}

// FindLiveByProfile is FindByProfile for the live filesystem's mount only
// (version mounts are ignored): the mount `telfs unmount <profile>` stops.
func FindLiveByProfile(profile string) (pid int, mountpoint string, found bool, err error) {
	return findByProfile(ScanMounts(), profile, true)
}

func findByProfile(mounts []Mount, profile string, liveOnly bool) (pid int, mountpoint string, found bool, err error) {
	if profile == "" {
		return 0, "", false, nil
	}
	var unknown []string
	version := ""
	for _, m := range mounts {
		p, ok := ProfileOf(m)
		if !ok {
			unknown = append(unknown, m.Point)
			continue
		}
		if p != profile {
			continue
		}
		if VersionOf(m) == "" {
			pid, _ := FindDaemonPid(m.Point)
			return pid, m.Point, true, nil
		}
		if !liveOnly && version == "" {
			version = m.Point
		}
	}
	if version != "" {
		pid, _ := FindDaemonPid(version)
		return pid, version, true, nil
	}
	if len(unknown) > 0 {
		return 0, "", false, &ErrUnknownMount{Mountpoints: unknown}
	}
	return 0, "", false, nil
}

// VersionMounts returns the mounts of a profile's versions, by version
// name (one mountpoint each: a version's lock allows one mount).
func VersionMounts(profile string) map[string]string {
	out := map[string]string{}
	for _, m := range ScanMounts() {
		if p, ok := ProfileOf(m); ok && p == profile {
			if v := VersionOf(m); v != "" {
				out[v] = m.Point
			}
		}
	}
	return out
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

// WriterConflict reports why a new mount of profile/fsUUID must not
// start on this machine (single-writer guard), or "" if it may:
//   - any mount of the same PROFILE (read-only or not): two daemons
//     would share one db.sqlite;
//   - for a read-write mount, any mount of the same FILESYSTEM (fs_uuid),
//     whatever its profile: two writers corrupt each other's snapshots
//     and journal;
//   - fail closed: a TelFS mount whose profile or filesystem cannot be
//     told (made by an older telfs) blocks a read-write mount.
//
// Version mounts are ignored: one serves its own read-only copy of a
// version (its own DB file, lock and cache) and writes nothing to the
// channel, so it never blocks the live mount — nor the boot mount it.
// (A version mount itself is not checked here: its version lock is its
// guard.)
//
// fsUUID may be "" (not known yet, before recovery): then only the
// profile is checked.
func WriterConflict(profile, fsUUID string, readOnly bool) string {
	return writerConflict(ScanMounts(), profile, fsUUID, readOnly)
}

func writerConflict(mounts []Mount, profile, fsUUID string, readOnly bool) string {
	for _, m := range mounts {
		if VersionOf(m) != "" {
			continue
		}
		p, known := ProfileOf(m)
		if known && p == profile {
			return fmt.Sprintf("profile %s is already mounted at %s — one daemon per profile (they would share db.sqlite)", profile, m.Point)
		}
		if readOnly || fsUUID == "" {
			if !known && !readOnly {
				return fmt.Sprintf("cannot tell which profile serves the TelFS mount at %s (an older telfs) — unmount it first", m.Point)
			}
			continue
		}
		other := FSUUIDOf(m)
		switch {
		case other == fsUUID:
			return fmt.Sprintf("filesystem %s is already mounted at %s (profile %s) — a second writer would corrupt it; mount --readonly for a second view", fsUUID, m.Point, p)
		case other == "":
			return fmt.Sprintf("cannot tell which filesystem the TelFS mount at %s holds (an older telfs) — unmount it before a read-write mount", m.Point)
		}
	}
	return ""
}
