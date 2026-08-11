package web

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"telfs/internal/config"
)

// MountProcess describes one telfs mount daemon. Two flavors:
//
//   - Supervised: started by this web server via Supervisor.Start.
//     cmd != nil, exit is observed via cmd.Wait, log path is known
//     up front (we created it). Adopted == false.
//   - Adopted: discovered after the fact in /proc by AdoptExternal.
//     cmd == nil, exit is observed by polling /proc/<pid>/status,
//     log path is read from /proc/<pid>/fd/1 when it's a regular
//     file. Adopted == true. The pid is set so Stop can signal it.
//
// Both shapes share the same Stop / TailLog / Status semantics so
// the UI never needs to branch on origin — the user gets the same
// controls whether they started the daemon from `telfs mount` or
// from /mount/new in the browser.
type MountProcess struct {
	Profile    string
	Mountpoint string
	LogPath    string
	Started    time.Time
	Adopted    bool
	Pid        int

	cmd *exec.Cmd
	// done closes when the process has been observed to exit. For
	// supervised procs this happens in the goroutine that ran
	// cmd.Wait. For adopted procs Status() lazily closes it the
	// first time it sees the pid has died. Multiple observers can
	// race to be "first" (concurrent Status calls, Reconcile), so
	// the close funnels through doneOnce.
	done     chan struct{}
	doneOnce sync.Once
	// exitErr captures the result of cmd.Wait(); read only after <-done.
	// Always nil for adopted procs (we never reaped them).
	exitErr error
}

// markDone closes done exactly once, no matter how many observers
// notice the exit concurrently. A bare close() here used to panic
// when two requests both rendered Status() of a freshly-dead adopted
// pid.
func (m *MountProcess) markDone() {
	m.doneOnce.Do(func() { close(m.done) })
}

// fusermountHelper resolves the FUSE unmount helper: fusermount3
// (fuse3) first, then legacy fusermount. fuse3-only distros ship no
// "fusermount" name at all, so hardcoding it made every emergency
// unmount here a silent no-op.
func fusermountHelper() string {
	for _, name := range []string{"fusermount3", "fusermount"} {
		if p, err := exec.LookPath(name); err == nil {
			return p
		}
	}
	return "fusermount"
}

// Status is the short label shown in the UI. For adopted procs
// (cmd==nil) it lazily detects exit by signal-0 probing the pid,
// closing m.done the first time we see the process is gone so
// future calls are O(1).
func (m *MountProcess) Status() string {
	if m == nil {
		return "stopped"
	}
	select {
	case <-m.done:
		if m.exitErr != nil {
			return "exited: " + m.exitErr.Error()
		}
		return "exited"
	default:
	}
	// Adopted procs: poll the pid. syscall.Kill(pid, 0) returns nil
	// if the process is alive; errors otherwise. Cheap (one syscall).
	if m.Adopted && m.Pid > 0 {
		if err := syscall.Kill(m.Pid, 0); err != nil {
			m.markDone()
			return "exited"
		}
	}
	return "running"
}

// Supervisor owns supervised mount children. One per profile — TelFS
// itself crashes if you try to double-mount the same FS, so we enforce
// it here too with a clearer error.
type Supervisor struct {
	mu       sync.Mutex
	procs    map[string]*MountProcess
	selfPath string // resolved path to the telfs binary for re-exec
}

// NewSupervisor stores the binary path used to re-exec `telfs mount`.
// Pass `os.Args[0]`.
func NewSupervisor(selfPath string) (*Supervisor, error) {
	abs, err := exec.LookPath(selfPath)
	if err != nil {
		// LookPath fails when selfPath is "./bin/telfs" with no PATH;
		// resolve manually.
		abs2, err2 := filepath.Abs(selfPath)
		if err2 != nil {
			return nil, fmt.Errorf("resolve self path %q: %w (lookup: %v)", selfPath, err2, err)
		}
		if _, err := os.Stat(abs2); err != nil {
			return nil, fmt.Errorf("self binary not found at %s: %w", abs2, err)
		}
		abs = abs2
	}
	return &Supervisor{
		procs:    make(map[string]*MountProcess),
		selfPath: abs,
	}, nil
}

// List returns a snapshot of every mount the supervisor knows about
// — supervised (started here) and adopted (discovered in /proc).
// Reconciles first so CLI-started daemons that appeared since the
// last call show up. Sorted by profile for stable template output.
func (s *Supervisor) List() []*MountProcess {
	s.Reconcile()
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*MountProcess, 0, len(s.procs))
	for _, p := range s.procs {
		out = append(out, p)
	}
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j].Profile < out[i].Profile {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

// Get returns the mount process for a profile, or nil. Reconciles
// first so a CLI-started daemon whose TELFS_PROFILE matches is
// findable by name.
func (s *Supervisor) Get(profile string) *MountProcess {
	s.Reconcile()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.procs[profile]
}

// StartOpts controls Supervisor.Start.
type StartOpts struct {
	Profile    string
	Mountpoint string
	Passphrase string // optional; pass-through via TELFS_PASSPHRASE
	Readonly   bool
	AllowOther bool
	Debug      bool
	// MetricsAddr binds the daemon's debug HTTP endpoint (/metrics,
	// /telegram). Empty falls back to DefaultMetricsAddr — that's the
	// URL the web's Telegram tab fetches from, so the default just
	// works for a single supervised mount. Multi-profile setups must
	// pick distinct ports here (the kernel can't share a TCP listen).
	MetricsAddr string
}

// DefaultMetricsAddr is the daemon's debug HTTP bind when StartOpts
// doesn't override. Matches the URL the web's Telegram tab fetches
// from (internal/web/telegram.go), so the Telegram tab "just works"
// after a default-options Start.
const DefaultMetricsAddr = "127.0.0.1:9090"

// Start spawns `telfs mount <mountpoint>` for the given profile,
// captures stdout/stderr to a per-profile log file, and tracks the
// process. Returns an error if a mount for the profile is already
// supervised or if the spawn itself fails.
func (s *Supervisor) Start(opts StartOpts) (*MountProcess, error) {
	if strings.TrimSpace(opts.Profile) == "" {
		return nil, errors.New("profile is required")
	}
	if strings.TrimSpace(opts.Mountpoint) == "" {
		return nil, errors.New("mountpoint is required")
	}

	// Reconcile first so a CLI-started daemon under the same profile
	// shows up as "already running" instead of getting clobbered by a
	// duplicate spawn that would fight for the same FUSE channel.
	s.Reconcile()

	// Reserve the profile slot UNDER THE LOCK before spawning: the
	// old check-unlock-spawn-lock-insert dance let two concurrent
	// Start calls (double-click on the form) both pass the check and
	// spawn two daemons onto one db.sqlite + session.json — a data
	// race at every layer below. The placeholder registers as
	// "running" (done not closed) so the second caller refuses.
	placeholder := &MountProcess{
		Profile:    opts.Profile,
		Mountpoint: opts.Mountpoint,
		Started:    time.Now(),
		done:       make(chan struct{}),
	}
	s.mu.Lock()
	if existing := s.procs[opts.Profile]; existing != nil {
		select {
		case <-existing.done:
			// Previous run exited — purge so we can restart.
			delete(s.procs, opts.Profile)
		default:
			s.mu.Unlock()
			origin := "supervised"
			if existing.Adopted {
				origin = "adopted (started outside this web)"
			}
			return nil, fmt.Errorf("profile %q already has a %s mount at %s — stop it first", opts.Profile, origin, existing.Mountpoint)
		}
	}
	s.procs[opts.Profile] = placeholder
	s.mu.Unlock()
	// From here every failure path must release the reservation.
	unreserve := func() {
		s.mu.Lock()
		if s.procs[opts.Profile] == placeholder {
			delete(s.procs, opts.Profile)
		}
		s.mu.Unlock()
		placeholder.markDone()
	}

	if info, err := os.Stat(opts.Mountpoint); err != nil {
		unreserve()
		return nil, fmt.Errorf("mountpoint %s: %w", opts.Mountpoint, err)
	} else if !info.IsDir() {
		unreserve()
		return nil, fmt.Errorf("mountpoint %s is not a directory", opts.Mountpoint)
	}

	logPath := mountLogPath(opts.Profile)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		unreserve()
		return nil, fmt.Errorf("open log %s: %w", logPath, err)
	}

	args := []string{"mount"}
	if opts.Readonly {
		args = append(args, "--readonly")
	}
	if opts.AllowOther {
		args = append(args, "--allow-other")
	}
	if opts.Debug {
		args = append(args, "--debug")
	}
	metricsAddr := opts.MetricsAddr
	if metricsAddr == "" {
		metricsAddr = DefaultMetricsAddr
	}
	args = append(args, "--metrics-addr", metricsAddr)
	args = append(args, opts.Mountpoint)

	cmd := exec.Command(s.selfPath, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Stdin = nil
	// Detach into its own session so a SIGINT to the web server (Ctrl-C)
	// doesn't propagate to the child.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	env := os.Environ()
	env = scrubEnv(env, config.EnvProfile, "TELFS_PASSPHRASE")
	if opts.Profile != "" && opts.Profile != "(default)" {
		env = append(env, config.EnvProfile+"="+opts.Profile)
	}
	if opts.Passphrase != "" {
		env = append(env, "TELFS_PASSPHRASE="+opts.Passphrase)
	}
	cmd.Env = env

	if err := cmd.Start(); err != nil {
		logFile.Close()
		unreserve()
		return nil, fmt.Errorf("spawn %s mount: %w", s.selfPath, err)
	}

	mp := &MountProcess{
		Profile:    opts.Profile,
		Mountpoint: opts.Mountpoint,
		LogPath:    logPath,
		Started:    time.Now(),
		Pid:        cmd.Process.Pid,
		cmd:        cmd,
		done:       make(chan struct{}),
	}
	go func() {
		mp.exitErr = cmd.Wait()
		logFile.Close()
		mp.markDone()
	}()

	s.mu.Lock()
	s.procs[opts.Profile] = mp // replaces the reservation placeholder
	s.mu.Unlock()
	placeholder.markDone() // release anyone blocked on the reservation
	return mp, nil
}

// Stop signals the mount daemon for the given profile and waits up
// to 35 seconds — comfortably past the daemon's own 30-second
// shutdown watchdog, so orderly teardown (drain + final snapshot)
// always gets to finish before we SIGKILL. (An earlier 25 s value
// undercut the daemon's budget and could SIGKILL it mid-final-
// snapshot, leaving a stale WAL behind.) After that, it SIGKILLs the
// process and runs the fusermount helper as a last-ditch unmount.
//
// Works for both supervised (started by us) and adopted (discovered
// in /proc, started outside this web) procs. The branching is
// minimal: signal delivery uses cmd.Process for supervised (where
// we know the *os.Process) or syscall.Kill for adopted (where we
// only know the pid). Exit detection is the channel close from the
// supervised cmd.Wait goroutine, or a kill-0 poll loop for adopted.
func (s *Supervisor) Stop(profile string) error {
	s.mu.Lock()
	mp := s.procs[profile]
	s.mu.Unlock()
	if mp == nil {
		return fmt.Errorf("no mount tracked for profile %q", profile)
	}
	select {
	case <-mp.done:
		return nil
	default:
	}
	_ = signalProc(mp, syscall.SIGTERM)
	if waitForExit(mp, 35*time.Second) {
		return nil
	}
	// Last-ditch: SIGKILL + lazy unmount.
	_ = signalProc(mp, syscall.SIGKILL)
	if err := exec.Command(fusermountHelper(), "-u", "-z", mp.Mountpoint).Run(); err != nil {
		return fmt.Errorf("kill + lazy unmount failed: %w", err)
	}
	// Mark done for adopted procs so Status flips. markDone is
	// sync.Once-guarded — safe unconditionally.
	mp.markDone()
	return nil
}

// signalProc sends sig to mp's process. Uses cmd.Process for
// supervised procs (where we hold the *os.Process), syscall.Kill
// for adopted procs (where we only have the pid). Returns the
// error from the kernel; caller usually ignores since the next
// step is to confirm exit via waitForExit anyway.
func signalProc(mp *MountProcess, sig syscall.Signal) error {
	if mp.cmd != nil && mp.cmd.Process != nil {
		return mp.cmd.Process.Signal(sig)
	}
	if mp.Pid > 0 {
		return syscall.Kill(mp.Pid, sig)
	}
	return errors.New("no process handle or pid")
}

// waitForExit blocks until mp.done closes or the timeout elapses,
// returning whether the proc actually exited. For adopted procs
// (which have no cmd.Wait goroutine to close done) it polls
// kill(pid, 0) on a short tick and closes done on detection so the
// next Status call sees "exited" immediately.
func waitForExit(mp *MountProcess, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-mp.done:
			return true
		case <-tick.C:
			if mp.Adopted && mp.Pid > 0 {
				if err := syscall.Kill(mp.Pid, 0); err != nil {
					mp.markDone()
					return true
				}
			}
			if time.Now().After(deadline) {
				return false
			}
		}
	}
}

// TailLog returns the last `maxBytes` of the log file. Cheap polling
// target for HTMX.
func (mp *MountProcess) TailLog(maxBytes int64) (string, error) {
	if mp == nil || mp.LogPath == "" {
		return "", errors.New("no log")
	}
	f, err := os.Open(mp.LogPath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return "", err
	}
	if st.Size() > maxBytes {
		if _, err := f.Seek(-maxBytes, io.SeekEnd); err != nil {
			return "", err
		}
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// UnmountExternal unmounts a fuse.telfs mount that was started outside
// this web server. Refuses any path not currently present in
// /proc/mounts as fuse.telfs — defense against being aimed at an
// unrelated mountpoint. Refuses supervised mountpoints too; those go
// through Stop() so their child process gets a clean SIGTERM first.
//
// Tries the polite `fusermount -u` first; on EBUSY (open files) falls
// back to `fusermount -u -z` (lazy detach — mount disappears from the
// namespace immediately, lingering FDs unmap once closed).
func (s *Supervisor) UnmountExternal(mountpoint string) error {
	mountpoint = strings.TrimSpace(mountpoint)
	if mountpoint == "" {
		return errors.New("mountpoint is required")
	}
	// Refuse if this is one of our supervised mounts — Stop() is the
	// right path so the child process exits cleanly first.
	s.mu.Lock()
	for _, p := range s.procs {
		if p.Mountpoint == mountpoint {
			s.mu.Unlock()
			return fmt.Errorf("mountpoint %q is supervised; use Stop on the Mounts page instead", mountpoint)
		}
	}
	s.mu.Unlock()
	// Confirm the mountpoint is actually a fuse.telfs entry right now.
	found := false
	for _, m := range scanProcMounts() {
		if m == mountpoint {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("mountpoint %q is not currently mounted as fuse.telfs", mountpoint)
	}
	if err := exec.Command(fusermountHelper(), "-u", mountpoint).Run(); err == nil {
		return nil
	}
	// EBUSY or stuck — try the lazy form.
	if err := exec.Command(fusermountHelper(), "-u", "-z", mountpoint).Run(); err != nil {
		return fmt.Errorf("fusermount -u -z %s: %w", mountpoint, err)
	}
	return nil
}

// ExternalMounts returns fuse.telfs mountpoints in /proc/mounts that
// the supervisor isn't tracking AT ALL — typically because the daemon
// died unrecognizably (no pid in /proc, no matching cmdline). Real
// CLI-started mounts get adopted into procs via Reconcile and show up
// in List(); this is the residual list of "we see a mount but can't
// find the daemon", surfaced as a warning in the UI.
func (s *Supervisor) ExternalMounts() []string {
	all := scanProcMounts()
	s.mu.Lock()
	tracked := make(map[string]struct{}, len(s.procs))
	for _, p := range s.procs {
		tracked[p.Mountpoint] = struct{}{}
	}
	s.mu.Unlock()
	out := make([]string, 0, len(all))
	for _, m := range all {
		if _, ok := tracked[m]; !ok {
			out = append(out, m)
		}
	}
	return out
}

// Reconcile scans /proc for fuse.telfs mounts not yet in s.procs and
// adopts them. Idempotent — already-tracked mountpoints (whether
// supervised or previously-adopted) are skipped. Stale procs
// (tracked but the pid is gone) get pruned so a future Start under
// the same profile name can succeed.
//
// Cheap enough to call on every UI request: O(/proc/mounts entries)
// plus one /proc walk per unmapped fuse.telfs entry to identify the
// pid. The number of fuse.telfs mounts on a host is small.
func (s *Supervisor) Reconcile() {
	mounts := scanProcMounts()
	s.mu.Lock()
	// Prune dead procs first so a freshly-killed mount frees its
	// profile slot for a re-mount under the same name.
	for prof, mp := range s.procs {
		select {
		case <-mp.done:
			delete(s.procs, prof)
			continue
		default:
		}
		if mp.Adopted && mp.Pid > 0 && syscall.Kill(mp.Pid, 0) != nil {
			mp.markDone()
			delete(s.procs, prof)
		}
	}
	tracked := make(map[string]struct{}, len(s.procs))
	for _, p := range s.procs {
		tracked[p.Mountpoint] = struct{}{}
	}
	s.mu.Unlock()
	for _, mp := range mounts {
		if _, ok := tracked[mp]; ok {
			continue
		}
		adopted := adoptFromProc(mp)
		if adopted == nil {
			continue
		}
		profileKey := adopted.Profile
		if profileKey == "" {
			// Avoid colliding with a "" key (used by Get with no
			// profile name); attribute under a synthetic key derived
			// from the mountpoint instead.
			profileKey = "adopted:" + adopted.Mountpoint
			adopted.Profile = profileKey
		}
		s.mu.Lock()
		// Last-writer-wins. A concurrent Start since we dropped the
		// lock would have inserted under the same key; respect it.
		if _, exists := s.procs[profileKey]; !exists {
			s.procs[profileKey] = adopted
		}
		s.mu.Unlock()
	}
}

// adoptFromProc finds the telfs mount daemon serving `mountpoint`
// by walking /proc/*/cmdline. Returns nil when nothing matches —
// the mountpoint is a fuse.telfs entry but no live telfs process
// holds it (zombie or detached). Reads profile from TELFS_PROFILE
// in /proc/<pid>/environ; falls back to "(unknown)" when absent.
// LogPath is /proc/<pid>/fd/1 IF it resolves to a regular file —
// daemons started with stdout redirected to a file (the supervisor
// pattern, or `telfs mount ... > log.txt`).
func adoptFromProc(mountpoint string) *MountProcess {
	pid, found := findTelfsMountPid(mountpoint)
	if !found {
		return nil
	}
	mp := &MountProcess{
		Mountpoint: mountpoint,
		Pid:        pid,
		Adopted:    true,
		done:       make(chan struct{}),
		Profile:    readEnvVar(pid, config.EnvProfile),
		Started:    procStartTime(pid),
		LogPath:    resolveStdoutLog(pid),
	}
	return mp
}

// findTelfsMountPid scans /proc/*/cmdline for the daemon that mounted
// `mountpoint`. The daemon's argv ends with `<mountpoint>` (see
// cmd/telfs/main.go's mount subcommand and the supervisor's Start
// args construction). Returns (pid, true) on match, (0, false) on
// no match.
func findTelfsMountPid(mountpoint string) (int, bool) {
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
		// First arg should look like a telfs binary; later args
		// should contain "mount" and end with the mountpoint.
		exe := filepath.Base(argv[0])
		if exe != "telfs" {
			continue
		}
		// Quick scan: need both "mount" subcommand and matching final mountpoint.
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

// readEnvVar pulls a single environment variable's value from
// /proc/<pid>/environ. Returns empty string if missing or unreadable
// — typical when the daemon was started without setting the var.
func readEnvVar(pid int, key string) string {
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

// procStartTime reads /proc/<pid>/stat's start-time field (jiffies
// since boot) and converts to a wall-clock time using boot time +
// the system clock tick. Returns zero time on any parse failure;
// the UI just shows "—" in that case.
func procStartTime(pid int) time.Time {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return time.Time{}
	}
	// /proc/<pid>/stat fields are space-separated; field 22 (1-based)
	// is starttime in clock ticks since boot. The comm field (#2) is
	// wrapped in parens and may contain spaces, so split AFTER the
	// closing paren.
	s := string(b)
	cp := strings.LastIndexByte(s, ')')
	if cp < 0 {
		return time.Time{}
	}
	fields := strings.Fields(s[cp+1:])
	// After the close-paren, field offsets shift: starttime is now
	// at index 19 (was 22, minus pid + comm).
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
	// USER_HZ is conventionally 100 on Linux; correct in practice
	// for every kernel TelFS targets.
	const userHz = 100
	return boot.Add(time.Duration(startTicks) * time.Second / userHz)
}

// bootTime returns the system boot wall-clock by parsing /proc/stat's
// "btime" line.
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

// resolveStdoutLog returns the file path that /proc/<pid>/fd/1
// (stdout) points to, when it's a regular file. Daemons that ran
// with stdout redirected to a log file (the supervisor pattern, or
// `telfs mount ... > log` from a shell) get their log surfaced;
// daemons attached to a tty or piping into another process get
// empty (no useful log path).
func resolveStdoutLog(pid int) string {
	target, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/1", pid))
	if err != nil {
		return ""
	}
	if target == "" || target[0] != '/' {
		return ""
	}
	// Skip if it's a tty or pipe (/dev/pts/*, pipe:[...]).
	if strings.HasPrefix(target, "/dev/") || strings.HasPrefix(target, "pipe:") {
		return ""
	}
	if st, err := os.Stat(target); err != nil || !st.Mode().IsRegular() {
		return ""
	}
	return target
}

// scanProcMounts mirrors activeMounts() in handlers.go but lives here
// so the supervisor is self-contained.
func scanProcMounts() []string {
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

// mountLogPath is the per-profile log file. Lives in $XDG_RUNTIME_DIR
// when set (tmpfs, cleaned on session end), otherwise /tmp.
func mountLogPath(profile string) string {
	base := os.Getenv("XDG_RUNTIME_DIR")
	if base == "" {
		base = "/tmp"
	}
	name := profile
	if name == "" {
		name = "default"
	}
	return filepath.Join(base, "telfs-web-"+name+".log")
}

// scrubEnv removes any existing occurrences of the named keys so the
// child inherits exactly what we set.
func scrubEnv(env []string, keys ...string) []string {
	out := env[:0]
	for _, e := range env {
		drop := false
		for _, k := range keys {
			if strings.HasPrefix(e, k+"=") {
				drop = true
				break
			}
		}
		if !drop {
			out = append(out, e)
		}
	}
	return out
}
