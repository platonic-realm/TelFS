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

// MountProcess describes one supervised `telfs mount` child.
type MountProcess struct {
	Profile    string
	Mountpoint string
	LogPath    string
	Started    time.Time

	cmd *exec.Cmd
	// done closes when Wait() returns. While the process is alive,
	// reading from done blocks. Used by Stop's timeout path.
	done chan struct{}
	// exitErr captures the result of cmd.Wait(); read only after <-done.
	exitErr error
}

// Status is the short label shown in the UI.
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
		return "running"
	}
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

// List returns a snapshot of supervised processes, sorted by profile.
func (s *Supervisor) List() []*MountProcess {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*MountProcess, 0, len(s.procs))
	for _, p := range s.procs {
		out = append(out, p)
	}
	// Stable order for templates.
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j].Profile < out[i].Profile {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

// Get returns the supervised process for a profile, or nil.
func (s *Supervisor) Get(profile string) *MountProcess {
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
}

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

	s.mu.Lock()
	if existing := s.procs[opts.Profile]; existing != nil {
		select {
		case <-existing.done:
			// Previous run exited — purge so we can restart.
			delete(s.procs, opts.Profile)
		default:
			s.mu.Unlock()
			return nil, fmt.Errorf("profile %q already has a supervised mount at %s", opts.Profile, existing.Mountpoint)
		}
	}
	s.mu.Unlock()

	if info, err := os.Stat(opts.Mountpoint); err != nil {
		return nil, fmt.Errorf("mountpoint %s: %w", opts.Mountpoint, err)
	} else if !info.IsDir() {
		return nil, fmt.Errorf("mountpoint %s is not a directory", opts.Mountpoint)
	}

	logPath := mountLogPath(opts.Profile)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
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
		return nil, fmt.Errorf("spawn %s mount: %w", s.selfPath, err)
	}

	mp := &MountProcess{
		Profile:    opts.Profile,
		Mountpoint: opts.Mountpoint,
		LogPath:    logPath,
		Started:    time.Now(),
		cmd:        cmd,
		done:       make(chan struct{}),
	}
	go func() {
		mp.exitErr = cmd.Wait()
		logFile.Close()
		close(mp.done)
	}()

	s.mu.Lock()
	s.procs[opts.Profile] = mp
	s.mu.Unlock()
	return mp, nil
}

// Stop signals the supervised process and waits up to 25 seconds —
// matching the in-mount watchdog. After that, it SIGKILLs the process
// and runs `fusermount -u -z` as a last-ditch unmount.
func (s *Supervisor) Stop(profile string) error {
	s.mu.Lock()
	mp := s.procs[profile]
	s.mu.Unlock()
	if mp == nil {
		return fmt.Errorf("no supervised mount for profile %q", profile)
	}
	select {
	case <-mp.done:
		return nil
	default:
	}
	if mp.cmd != nil && mp.cmd.Process != nil {
		_ = mp.cmd.Process.Signal(syscall.SIGTERM)
	}
	select {
	case <-mp.done:
		return nil
	case <-time.After(25 * time.Second):
	}
	// Last-ditch: kill + lazy unmount.
	if mp.cmd != nil && mp.cmd.Process != nil {
		_ = mp.cmd.Process.Kill()
	}
	if err := exec.Command("fusermount", "-u", "-z", mp.Mountpoint).Run(); err != nil {
		return fmt.Errorf("kill + lazy unmount failed: %w", err)
	}
	return nil
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

// ExternalMounts returns fuse.telfs mountpoints in /proc/mounts that
// are NOT supervised here. Used to flag "ran outside the web UI" mounts
// on the dashboard without offering to stop them.
func (s *Supervisor) ExternalMounts() []string {
	all := scanProcMounts()
	s.mu.Lock()
	supervised := make(map[string]struct{}, len(s.procs))
	for _, p := range s.procs {
		supervised[p.Mountpoint] = struct{}{}
	}
	s.mu.Unlock()
	out := make([]string, 0, len(all))
	for _, m := range all {
		if _, ok := supervised[m]; !ok {
			out = append(out, m)
		}
	}
	return out
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
