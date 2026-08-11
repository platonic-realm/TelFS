package web

import (
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
	"telfs/internal/mountctl"
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
	// Reconcile so a daemon started outside this web (or after a web
	// restart emptied the map) is stoppable by profile name too.
	s.Reconcile()
	s.mu.Lock()
	mp := s.procs[profile]
	s.mu.Unlock()
	if mp == nil {
		return fmt.Errorf("no mount tracked for profile %q", profile)
	}
	select {
	case <-mp.done:
		return ensureUnmounted(mp.Mountpoint)
	default:
	}
	_ = signalProc(mp, syscall.SIGTERM)
	if waitForExit(mp, 35*time.Second) {
		// The daemon can exit cleanly yet leave the kernel mount
		// behind (its own fusermount hit EBUSY). Verify + lazy-detach.
		return ensureUnmounted(mp.Mountpoint)
	}
	// Last-ditch: SIGKILL + lazy unmount.
	_ = signalProc(mp, syscall.SIGKILL)
	if err := mountctl.LazyUnmount(mp.Mountpoint); err != nil && mountctl.IsMounted(mp.Mountpoint) {
		return fmt.Errorf("kill + lazy unmount failed: %w", err)
	}
	// Mark done for adopted procs so Status flips. markDone is
	// sync.Once-guarded — safe unconditionally.
	mp.markDone()
	return nil
}

// ensureUnmounted lazy-detaches the mountpoint if it's still present
// in /proc/mounts after the daemon exited.
func ensureUnmounted(mountpoint string) error {
	if mountpoint == "" || !mountctl.IsMounted(mountpoint) {
		return nil
	}
	if err := mountctl.LazyUnmount(mountpoint); err != nil {
		return fmt.Errorf("daemon exited but mount remains: %w", err)
	}
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
	// Polite unmount, lazy on EBUSY.
	return mountctl.Unmount(mountpoint)
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
// in /proc/<pid>/environ. LogPath is /proc/<pid>/fd/1 IF it
// resolves to a regular file — daemons started with stdout
// redirected to a file (the supervisor pattern, or
// `telfs mount ... > log.txt`).
func adoptFromProc(mountpoint string) *MountProcess {
	pid, found := mountctl.FindDaemonPid(mountpoint)
	if !found {
		return nil
	}
	mp := &MountProcess{
		Mountpoint: mountpoint,
		Pid:        pid,
		Adopted:    true,
		done:       make(chan struct{}),
		Profile:    mountctl.ReadEnvVar(pid, config.EnvProfile),
		Started:    mountctl.ProcStartTime(pid),
		LogPath:    mountctl.ResolveStdoutLog(pid),
	}
	return mp
}

// scanProcMounts is the supervisor's local name for the shared
// /proc/mounts scan.
func scanProcMounts() []string { return mountctl.ScanProcMounts() }

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
