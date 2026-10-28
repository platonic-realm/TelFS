// Package secret reads a profile's secret from the source its
// config.toml names (feature 5): an interactive prompt, a secret file
// (the S9 service layout, written by svc-secret), or KeePassXC over the
// Secret Service (`secret-tool lookup Path /<entry>`, as svc-secret
// does).
//
// Rules (agreed 2026-10-25, decision D4):
//   - The secret is never logged, never on a command line, and never
//     put into any environment by TelFS.
//   - TELFS_PASSPHRASE is read and REMOVED from the process environment
//     at startup (CaptureEnv), so no child — mount daemon, fusermount,
//     secret-tool — inherits it. It only skips the prompt of a
//     prompt-source profile.
//   - A secret file must be a regular file (not a symlink), give
//     nothing to "others", be at most 4 KiB and not be empty; one
//     trailing newline is stripped. Owner and group are NOT checked: a
//     boot mount running as svc-<group> reads arash's 0640 file through
//     the group.
//   - Service mode never prompts.
package secret

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"golang.org/x/term"

	"telfs/internal/config"
)

// EnvPassphrase is the environment variable that skips the prompt of a
// prompt-source profile.
const EnvPassphrase = "TELFS_PASSPHRASE"

// MaxFileSize caps a secret file (and a secret read from a pipe).
const MaxFileSize = 4096

// KeePassXCTimeout bounds one Secret Service lookup: long enough to
// unlock KeePassXC or confirm the access in its window.
const KeePassXCTimeout = 120 * time.Second

// WeakLength is the length below which setting a secret draws a
// warning. Length is only a rough proxy for strength.
const WeakLength = 20

var (
	envMu   sync.Mutex
	envPass []byte
	envSet  bool
)

// CaptureEnv moves TELFS_PASSPHRASE out of the process environment into
// memory. Call it first thing in main: every process started afterwards
// (exec.Command with an inherited environment) no longer sees it.
func CaptureEnv() {
	envMu.Lock()
	defer envMu.Unlock()
	if v, ok := os.LookupEnv(EnvPassphrase); ok {
		envPass = []byte(v)
		envSet = true
		_ = os.Unsetenv(EnvPassphrase)
	}
}

// CapturedEnv returns a copy of the captured TELFS_PASSPHRASE, for the
// one place that hands it on: the mount's re-exec with an absolute
// mountpoint, through a pipe (never the environment).
func CapturedEnv() ([]byte, bool) { return envPassphrase() }

// envPassphrase returns a copy of the captured TELFS_PASSPHRASE.
func envPassphrase() ([]byte, bool) {
	envMu.Lock()
	defer envMu.Unlock()
	if !envSet {
		return nil, false
	}
	return append([]byte(nil), envPass...), true
}

// Secret holds a secret's bytes. Zero it as soon as the derivations
// that need it are done.
type Secret struct {
	b []byte
	// Source is where the bytes came from ("prompt", "file",
	// "keepassxc", "pipe", "env").
	Source string
}

// New wraps b (taking ownership) — for tests and for callers that
// already hold the bytes (the web console's form field).
func New(b []byte, source string) *Secret { return &Secret{b: b, Source: source} }

// Bytes returns the secret. The slice is owned by the Secret.
func (s *Secret) Bytes() []byte { return s.b }

// Zero overwrites the secret. Safe to call more than once.
func (s *Secret) Zero() {
	if s == nil {
		return
	}
	for i := range s.b {
		s.b[i] = 0
	}
	s.b = nil
}

// Purpose distinguishes reading an existing secret from setting a new
// one (a new typed secret is asked twice).
type Purpose int

const (
	Unlock Purpose = iota
	SetNew
)

// Request describes one read.
type Request struct {
	// Override replaces config's source for this one command
	// (--secret-from). Empty = use config.
	Override string
	Purpose  Purpose
	// Prompt is the label for a typed secret ("Secret for profile X: ").
	Prompt string
	// PipeFD > 0 reads the secret from that inherited file descriptor
	// (web-started mounts of prompt profiles) and closes it.
	PipeFD int
	// NoPrompt is service mode: a prompt source is an error.
	NoPrompt bool
	// Stderr receives the "waiting for KeePassXC" line and warnings.
	Stderr io.Writer
}

// Error classes, so commands can map them to exit codes.
var (
	// ErrSource: the source could not produce a secret.
	ErrSource = errors.New("secret source failed")
)

// Read returns the profile's secret according to conf and req.
func Read(ctx context.Context, conf config.SecretConfig, req Request) (*Secret, error) {
	if req.Stderr == nil {
		req.Stderr = os.Stderr
	}
	if req.PipeFD > 0 {
		b, err := readFD(req.PipeFD)
		if err != nil {
			return nil, fmt.Errorf("%w: read secret from fd %d: %v", ErrSource, req.PipeFD, err)
		}
		return &Secret{b: b, Source: "pipe"}, nil
	}
	src := conf.EffectiveSource()
	if req.Override != "" {
		src = req.Override
		conf.Source = req.Override
	}
	if err := conf.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSource, err)
	}
	switch src {
	case config.SecretSourceFile:
		path, err := conf.ResolvedFile()
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrSource, err)
		}
		b, err := ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrSource, err)
		}
		return &Secret{b: b, Source: src}, nil
	case config.SecretSourceKeePassXC:
		b, err := lookupKeePassXC(ctx, conf.Entry, req.Stderr)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrSource, err)
		}
		return &Secret{b: b, Source: src}, nil
	default: // prompt
		if b, ok := envPassphrase(); ok {
			if len(b) == 0 {
				return nil, fmt.Errorf("%w: %s is set but empty", ErrSource, EnvPassphrase)
			}
			return &Secret{b: b, Source: "env"}, nil
		}
		if req.NoPrompt {
			return nil, fmt.Errorf(`%w: this profile's secret source is "prompt", which service mode cannot use — configure [secret] source = "file"`, ErrSource)
		}
		b, err := prompt(req)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrSource, err)
		}
		return &Secret{b: b, Source: src}, nil
	}
}

// ReadFile reads a secret file under the rules in the package comment.
// Opening with O_NOFOLLOW and checking the OPENED file (fstat) leaves
// no gap between the check and the read.
func ReadFile(path string) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return nil, fmt.Errorf("secret file %s is a symlink — TelFS reads only a regular file there", path)
		}
		return nil, fmt.Errorf("secret file: %w", err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("secret file %s: %w", path, err)
	}
	if !st.Mode().IsRegular() {
		return nil, fmt.Errorf("secret file %s is not a regular file", path)
	}
	if perm := st.Mode().Perm(); perm&0o007 != 0 {
		return nil, fmt.Errorf("secret file %s has mode %04o — others must have no access (e.g. chmod 0640 or 0600)", path, perm)
	}
	if st.Size() > MaxFileSize {
		return nil, fmt.Errorf("secret file %s is %d bytes — more than the %d allowed", path, st.Size(), MaxFileSize)
	}
	b, err := io.ReadAll(io.LimitReader(f, MaxFileSize+1))
	if err != nil {
		return nil, fmt.Errorf("secret file %s: %w", path, err)
	}
	if len(b) > MaxFileSize {
		return nil, fmt.Errorf("secret file %s grew past %d bytes while reading", path, MaxFileSize)
	}
	b = bytes.TrimSuffix(b, []byte("\n"))
	if len(b) == 0 {
		return nil, fmt.Errorf("secret file %s is empty", path)
	}
	return b, nil
}

// secretToolBin is the KeePassXC helper; a variable so tests can point
// it at a fake.
var secretToolBin = "secret-tool"

// lookupKeePassXC asks the running KeePassXC for an entry's password
// over the Secret Service. The child inherits the environment (it needs
// the D-Bus session address); TELFS_PASSPHRASE was already removed by
// CaptureEnv. The secret comes back through a pipe.
func lookupKeePassXC(ctx context.Context, entry string, stderr io.Writer) ([]byte, error) {
	path := "/" + strings.Trim(entry, "/")
	bin, err := exec.LookPath(secretToolBin)
	if err != nil {
		return nil, fmt.Errorf("KeePassXC lookup needs %s (package libsecret) — not found: %v", secretToolBin, err)
	}
	ctx, cancel := context.WithTimeout(ctx, KeePassXCTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "lookup", "Path", path)
	cmd.Stdin = nil
	cmd.Env = withoutPassphrase(os.Environ())
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", bin, err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var werr error
	select {
	case werr = <-done:
	case <-time.After(2 * time.Second):
		fmt.Fprintf(stderr, "waiting for KeePassXC (unlock it or confirm the access to %q in its window)…\n", path)
		werr = <-done
	}
	if ctx.Err() == context.DeadlineExceeded {
		return nil, fmt.Errorf("KeePassXC did not answer for %q within %s — is it running and unlocked?", path, KeePassXCTimeout)
	}
	b := out.Bytes()
	if out.Len() > MaxFileSize {
		return nil, fmt.Errorf("KeePassXC returned more than %d bytes for %q", MaxFileSize, path)
	}
	b = bytes.TrimSuffix(append([]byte(nil), b...), []byte("\n"))
	for i := range out.Bytes() {
		out.Bytes()[i] = 0
	}
	if werr != nil || len(b) == 0 {
		return nil, fmt.Errorf("KeePassXC gave nothing for %q: is KeePassXC running and unlocked, and is the entry inside the group exposed under Settings → Secret Service Integration?", path)
	}
	return b, nil
}

// withoutPassphrase drops TELFS_PASSPHRASE from an environment list
// (belt and braces: CaptureEnv already removed it from the process).
func withoutPassphrase(env []string) []string {
	out := env[:0:0]
	for _, kv := range env {
		if strings.HasPrefix(kv, EnvPassphrase+"=") {
			continue
		}
		out = append(out, kv)
	}
	return out
}

// StripEnv is withoutPassphrase for callers that build a child's
// environment themselves (the web supervisor).
func StripEnv(env []string) []string { return withoutPassphrase(env) }

func readFD(fd int) ([]byte, error) {
	f := os.NewFile(uintptr(fd), "secret-pipe")
	if f == nil {
		return nil, errors.New("invalid descriptor")
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, MaxFileSize+1))
	if err != nil {
		return nil, err
	}
	if len(b) > MaxFileSize {
		return nil, fmt.Errorf("more than %d bytes", MaxFileSize)
	}
	b = bytes.TrimSuffix(b, []byte("\n"))
	if len(b) == 0 {
		return nil, errors.New("empty")
	}
	return b, nil
}

// stdin is replaceable by tests.
var stdin io.Reader = os.Stdin

// lineReader keeps ONE buffered reader per input, so a second typed
// line (the repeat of a new secret) isn't lost in the first read's
// buffer.
var (
	lineMu  sync.Mutex
	lineSrc io.Reader
	lineBuf *bufio.Reader
)

func lineReader() *bufio.Reader {
	lineMu.Lock()
	defer lineMu.Unlock()
	if lineSrc != stdin || lineBuf == nil {
		lineSrc = stdin
		lineBuf = bufio.NewReader(stdin)
	}
	return lineBuf
}

func prompt(req Request) ([]byte, error) {
	label := req.Prompt
	if label == "" {
		label = "Secret: "
	}
	first, err := readTyped(label)
	if err != nil {
		return nil, err
	}
	if len(first) == 0 {
		return nil, errors.New("the secret must not be empty")
	}
	if req.Purpose != SetNew {
		return first, nil
	}
	second, err := readTyped("Repeat it: ")
	if err != nil {
		zero(first)
		return nil, err
	}
	defer zero(second)
	if !bytes.Equal(first, second) {
		zero(first)
		return nil, errors.New("the two entries do not match")
	}
	return first, nil
}

func readTyped(label string) ([]byte, error) {
	fmt.Fprint(os.Stderr, label)
	if f, ok := stdin.(*os.File); ok && term.IsTerminal(int(f.Fd())) {
		b, err := term.ReadPassword(int(f.Fd()))
		fmt.Fprintln(os.Stderr)
		if err != nil {
			return nil, fmt.Errorf("read secret: %w", err)
		}
		return b, nil
	}
	line, err := lineReader().ReadBytes('\n')
	if err != nil && !(errors.Is(err, io.EOF) && len(line) > 0) {
		return nil, fmt.Errorf("read secret: %w", err)
	}
	return bytes.TrimSuffix(line, []byte("\n")), nil
}

func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

// WarnIfWeak prints the strength warning when a NEW secret is shorter
// than WeakLength characters (whatever its source). A warning, not a
// refusal.
func WarnIfWeak(w io.Writer, s *Secret) {
	if s == nil || utf8.RuneCount(s.b) >= WeakLength {
		return
	}
	fmt.Fprintf(w, "WARNING: this secret is only %d characters long.\n", utf8.RuneCount(s.b))
	fmt.Fprintln(w, "  The filesystem's key header is readable by anyone with access to the channel,")
	fmt.Fprintln(w, "  so a short secret can be guessed offline. Length is only a rough guide, but")
	fmt.Fprintln(w, "  the safe choice is a password generated by KeePassXC (32+ characters), read")
	fmt.Fprintln(w, `  with [secret] source = "keepassxc" (interactive) or "file" (boot mounts).`)
}
