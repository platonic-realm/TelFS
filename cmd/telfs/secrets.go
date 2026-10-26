package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"

	"telfs/internal/config"
	"telfs/internal/crypto"
	"telfs/internal/harden"
	"telfs/internal/secret"
	"telfs/internal/sessionstore"
	"telfs/internal/tg"
)

// Every telfs process: take TELFS_PASSPHRASE out of the environment (no
// child may inherit it) and become non-dumpable, before anything else
// runs (decisions D4/D5).
func init() {
	secret.CaptureEnv()
	if err := harden.NonDumpable(); err != nil {
		fmt.Fprintln(os.Stderr, "telfs: warning:", err)
	}
}

// Global secret options, taken out of os.Args by extractGlobalFlags.
var (
	// secretFrom is --secret-from: overrides [secret] source for one
	// command.
	secretFrom string
	// secretFD is the hidden --secret-fd N: the web console hands a
	// typed passphrase to a mount it starts through an inherited pipe.
	secretFD int
)

// extractGlobalFlags removes --secret-from / --secret-fd (in either
// "--flag value" or "--flag=value" form, anywhere on the line) and
// returns the remaining args.
func extractGlobalFlags(args []string) ([]string, error) {
	out := args[:0:0]
	for i := 0; i < len(args); i++ {
		a := args[i]
		name, val, hasVal := strings.Cut(strings.TrimLeft(a, "-"), "=")
		if !strings.HasPrefix(a, "--") || (name != "secret-from" && name != "secret-fd") {
			out = append(out, a)
			continue
		}
		if !hasVal {
			if i+1 >= len(args) {
				return nil, fmt.Errorf("--%s needs a value", name)
			}
			i++
			val = args[i]
		}
		switch name {
		case "secret-from":
			switch val {
			case config.SecretSourcePrompt, config.SecretSourceFile, config.SecretSourceKeePassXC:
				secretFrom = val
			default:
				return nil, fmt.Errorf(`--secret-from %q: use "prompt", "file" or "keepassxc"`, val)
			}
		case "secret-fd":
			n, err := strconv.Atoi(val)
			if err != nil || n < 3 {
				return nil, fmt.Errorf("--secret-fd %q: want a descriptor number ≥ 3", val)
			}
			secretFD = n
		}
	}
	return out, nil
}

// exitCode maps an error to the process exit status: 2 when the secret
// source failed, 3 when the secret was wrong, 1 otherwise.
func exitCode(err error) int {
	switch {
	case errors.Is(err, secret.ErrSource):
		return 2
	case errors.Is(err, crypto.ErrWrongSecret):
		return 3
	default:
		return 1
	}
}

// readSecret reads the active profile's secret.
func readSecret(ctx context.Context, cfg *config.Config, purpose secret.Purpose) (*secret.Secret, error) {
	label := fmt.Sprintf("Secret for profile %s: ", cfg.ProfileName())
	if purpose == secret.SetNew {
		label = fmt.Sprintf("New secret for profile %s: ", cfg.ProfileName())
	}
	return secret.Read(ctx, cfg.Secret, secret.Request{
		Override: secretFrom,
		Purpose:  purpose,
		Prompt:   label,
		PipeFD:   secretFD,
	})
}

// openSessionStore opens the profile's session.enc with the secret.
// A leftover v0.22 session.json (and no session.enc) is refused.
func openSessionStore(cfg *config.Config, sec *secret.Secret) (*sessionstore.Store, error) {
	if _, err := os.Stat(cfg.SessionPath()); errors.Is(err, os.ErrNotExist) && cfg.HasLegacySession() {
		return nil, sessionstore.ErrLegacy
	}
	st, err := sessionstore.Open(cfg.SessionPath(), sec.Bytes(), crypto.DefaultArgonParamsV2())
	if errors.Is(err, crypto.ErrWrongSecret) {
		// An interrupted `secret change` can leave the session under the
		// NEW secret in session.enc.new (the key header already switched).
		next := cfg.SessionPath() + ".new"
		if _, serr := os.Stat(next); serr == nil {
			if st2, err2 := sessionstore.Open(next, sec.Bytes(), crypto.DefaultArgonParamsV2()); err2 == nil {
				if rerr := os.Rename(next, cfg.SessionPath()); rerr == nil {
					fmt.Fprintln(os.Stderr, "Finished an interrupted `secret change`: session.enc.new is now session.enc.")
					return sessionstore.Open(cfg.SessionPath(), sec.Bytes(), crypto.DefaultArgonParamsV2())
				}
				return st2, nil
			}
		}
	}
	return st, err
}

// newClientWith builds a Telegram client over session.enc opened with
// an already-read secret (the caller zeroes it when it is done with it).
func newClientWith(cfg *config.Config, sec *secret.Secret) (*tg.Client, error) {
	if err := cfg.RequireAPI(); err != nil {
		return nil, err
	}
	st, err := openSessionStore(cfg, sec)
	if err != nil {
		return nil, err
	}
	return tg.New(cfg, st)
}

// newClient reads the secret, opens session.enc, drops the secret, and
// returns a Telegram client: for the commands that need nothing else.
func newClient(ctx context.Context, cfg *config.Config) (*tg.Client, error) {
	if err := cfg.RequireAPI(); err != nil {
		return nil, err
	}
	if !cfg.HasSession() {
		return nil, tg.ErrNotAuthorized
	}
	sec, err := readSecret(ctx, cfg, secret.Unlock)
	if err != nil {
		return nil, err
	}
	defer sec.Zero()
	return newClientWith(cfg, sec)
}

// reexecAbsoluteMount re-executes `telfs mount` (same pid) with the
// mountpoint made absolute. The daemon's command line is how mountctl
// finds it — a non-dumpable daemon's cwd is unreadable, so a relative
// mountpoint there could never be matched again, and `telfs unmount`
// used to detach the mount without stopping the daemon (found in the
// P1 live test). The global secret flags are passed on; a captured
// TELFS_PASSPHRASE of a prompt-source profile goes through a pipe.
func reexecAbsoluteMount(absMountpoint string) error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("re-exec with an absolute mountpoint: %w", err)
	}
	args := append([]string(nil), os.Args...) // global flags already stripped
	args[len(args)-1] = absMountpoint
	var extra []string
	if secretFrom != "" {
		extra = append(extra, "--secret-from", secretFrom)
	}
	if secretFD > 0 {
		extra = append(extra, fmt.Sprintf("--secret-fd=%d", secretFD))
	} else if b, ok := secret.CapturedEnv(); ok && promptSourceActive() {
		var p [2]int
		if err := syscall.Pipe(p[:]); err != nil { // no O_CLOEXEC: the read end survives exec
			return fmt.Errorf("secret pipe: %w", err)
		}
		_, werr := syscall.Write(p[1], b)
		syscall.Close(p[1])
		for i := range b {
			b[i] = 0
		}
		if werr != nil {
			return fmt.Errorf("secret pipe: %w", werr)
		}
		extra = append(extra, fmt.Sprintf("--secret-fd=%d", p[0]))
	}
	newArgs := append(append(append([]string{}, args[:2]...), extra...), args[2:]...)
	return syscall.Exec(exe, newArgs, os.Environ())
}

// promptSourceActive reports whether this command reads a typed secret.
func promptSourceActive() bool {
	if secretFrom != "" {
		return secretFrom == config.SecretSourcePrompt
	}
	cfg, err := config.Load()
	return err == nil && cfg.Secret.EffectiveSource() == config.SecretSourcePrompt
}
