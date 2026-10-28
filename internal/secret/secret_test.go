package secret

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"telfs/internal/config"
)

func writeFile(t *testing.T, dir, name, content string, mode os.FileMode) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, mode); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestReadFileRules(t *testing.T) {
	dir := t.TempDir()
	ok := writeFile(t, dir, "ok", "s3cret\n", 0o640)
	if b, err := ReadFile(ok); err != nil || string(b) != "s3cret" {
		t.Fatalf("ok file: %q, %v", b, err)
	}
	// Only ONE trailing newline is stripped.
	two := writeFile(t, dir, "two", "abc\n\n", 0o600)
	if b, _ := ReadFile(two); string(b) != "abc\n" {
		t.Fatalf("two newlines: got %q", b)
	}
	noNL := writeFile(t, dir, "nonl", "exact", 0o600)
	if b, _ := ReadFile(noNL); string(b) != "exact" {
		t.Fatalf("no newline: got %q", b)
	}
	cases := map[string]string{}
	cases[writeFile(t, dir, "world", "x", 0o644)] = "others must have no access"
	cases[writeFile(t, dir, "empty", "\n", 0o600)] = "is empty"
	cases[writeFile(t, dir, "big", strings.Repeat("a", MaxFileSize+1), 0o600)] = "more than"
	link := filepath.Join(dir, "link")
	if err := os.Symlink(ok, link); err != nil {
		t.Fatal(err)
	}
	cases[link] = "symlink"
	sub := filepath.Join(dir, "adir")
	os.Mkdir(sub, 0o700)
	cases[sub] = "not a regular file"
	cases[filepath.Join(dir, "missing")] = "no such file"
	for p, want := range cases {
		if _, err := ReadFile(p); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want it to mention %q", filepath.Base(p), err, want)
		}
	}
}

func TestReadSourcesAndOverride(t *testing.T) {
	dir := t.TempDir()
	f := writeFile(t, dir, "s", "from-file", 0o600)
	conf := config.SecretConfig{Source: "file", File: f}
	s, err := Read(context.Background(), conf, Request{})
	if err != nil || string(s.Bytes()) != "from-file" || s.Source != "file" {
		t.Fatalf("file source: %v %v", s, err)
	}
	s.Zero()
	if s.Bytes() != nil {
		t.Fatal("Zero must drop the bytes")
	}

	// --secret-from prompt overrides the configured file source.
	stdin = strings.NewReader("typed\n")
	defer func() { stdin = os.Stdin }()
	s, err = Read(context.Background(), conf, Request{Override: "prompt"})
	if err != nil || string(s.Bytes()) != "typed" {
		t.Fatalf("override prompt: %v %v", s, err)
	}

	// Service mode never prompts.
	stdin = strings.NewReader("typed\n")
	_, err = Read(context.Background(), config.SecretConfig{}, Request{NoPrompt: true})
	if !errors.Is(err, ErrSource) || !strings.Contains(err.Error(), "service mode") {
		t.Fatalf("service mode prompt: %v", err)
	}

	// SetNew asks twice and refuses a mismatch.
	stdin = strings.NewReader("one\ntwo\n")
	if _, err := Read(context.Background(), config.SecretConfig{}, Request{Purpose: SetNew}); err == nil {
		t.Fatal("mismatched repeat must fail")
	}
	stdin = strings.NewReader("same\nsame\n")
	if s, err := Read(context.Background(), config.SecretConfig{}, Request{Purpose: SetNew}); err != nil || string(s.Bytes()) != "same" {
		t.Fatalf("matching repeat: %v %v", s, err)
	}

	// A file source without a path is a config error, not a prompt.
	if _, err := Read(context.Background(), config.SecretConfig{Source: "file"}, Request{}); !errors.Is(err, ErrSource) {
		t.Fatalf("file source without path: %v", err)
	}
}

func TestEnvCaptureStripsChildren(t *testing.T) {
	t.Setenv(EnvPassphrase, "env-secret")
	CaptureEnv()
	defer func() { envMu.Lock(); envSet = false; envPass = nil; envMu.Unlock() }()
	if _, ok := os.LookupEnv(EnvPassphrase); ok {
		t.Fatal("CaptureEnv must remove TELFS_PASSPHRASE from the process environment")
	}
	// A child started the ordinary way (inherited environment) must not
	// see it.
	out, err := exec.Command("env").Output()
	if err != nil {
		t.Skip("env not available:", err)
	}
	if bytes.Contains(out, []byte(EnvPassphrase)) {
		t.Fatalf("child environment still contains %s", EnvPassphrase)
	}
	// The prompt source still gets it.
	s, err := Read(context.Background(), config.SecretConfig{}, Request{})
	if err != nil || string(s.Bytes()) != "env-secret" || s.Source != "env" {
		t.Fatalf("env passphrase: %v %v", s, err)
	}
	// …but a file source ignores it.
	f := writeFile(t, t.TempDir(), "s", "file-wins", 0o600)
	s, _ = Read(context.Background(), config.SecretConfig{Source: "file", File: f}, Request{})
	if string(s.Bytes()) != "file-wins" {
		t.Fatalf("file source must not use the env: %q", s.Bytes())
	}
	if got := StripEnv([]string{"A=1", EnvPassphrase + "=x", "B=2"}); len(got) != 2 {
		t.Fatalf("StripEnv: %v", got)
	}
}

// fakeSecretTool installs a shell script as secret-tool.
func fakeSecretTool(t *testing.T, script string) {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "secret-tool")
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"+script), 0o755); err != nil {
		t.Fatal(err)
	}
	old := secretToolBin
	secretToolBin = p
	t.Cleanup(func() { secretToolBin = old })
}

func TestKeePassXC(t *testing.T) {
	conf := config.SecretConfig{Source: "keepassxc", Entry: "Services/TelFS/home"}
	// Hit: the fake checks the exact arguments svc-secret uses.
	fakeSecretTool(t, `[ "$1 $2 $3" = "lookup Path /Services/TelFS/home" ] || exit 2
[ -z "$TELFS_PASSPHRASE" ] || exit 3
printf 'kp-secret\n'`)
	s, err := Read(context.Background(), conf, Request{})
	if err != nil || string(s.Bytes()) != "kp-secret" || s.Source != "keepassxc" {
		t.Fatalf("keepassxc hit: %v %v", s, err)
	}
	// Miss: secret-tool exits 1 with no output.
	fakeSecretTool(t, `exit 1`)
	_, err = Read(context.Background(), conf, Request{})
	if !errors.Is(err, ErrSource) || !strings.Contains(err.Error(), "exposed under Settings") {
		t.Fatalf("keepassxc miss: %v", err)
	}
	// Not installed.
	old := secretToolBin
	secretToolBin = "/nonexistent/secret-tool"
	_, err = Read(context.Background(), conf, Request{})
	secretToolBin = old
	if err == nil || !strings.Contains(err.Error(), "libsecret") {
		t.Fatalf("missing secret-tool: %v", err)
	}
}

func TestWarnIfWeak(t *testing.T) {
	var buf bytes.Buffer
	WarnIfWeak(&buf, New([]byte("short"), "prompt"))
	if !strings.Contains(buf.String(), "KeePassXC") || !strings.Contains(buf.String(), "5 characters") {
		t.Fatalf("weak warning: %q", buf.String())
	}
	buf.Reset()
	WarnIfWeak(&buf, New([]byte(strings.Repeat("x", WeakLength)), "file"))
	if buf.Len() != 0 {
		t.Fatalf("no warning expected at %d characters: %q", WeakLength, buf.String())
	}
}

// systemd encrypted credentials (spec v1): a ${CREDENTIALS_DIRECTORY} path
// resolves inside the unit's credentials directory only — unset variable →
// source error (exit 2), no fallback; `..` cannot climb out.
func TestReadFromCredentialsDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "telfs-secret"), []byte("correct horse battery staple\n"), 0o400); err != nil {
		t.Fatal(err)
	}
	conf := config.SecretConfig{Source: config.SecretSourceFile, File: "${CREDENTIALS_DIRECTORY}/telfs-secret"}
	t.Setenv(config.CredentialsDirVar, dir)
	s, err := Read(context.Background(), conf, Request{NoPrompt: true})
	if err != nil {
		t.Fatal(err)
	}
	if string(s.Bytes()) != "correct horse battery staple" {
		t.Fatalf("secret %q", s.Bytes())
	}
	t.Setenv(config.CredentialsDirVar, "")
	_, err = Read(context.Background(), conf, Request{NoPrompt: true})
	if !errors.Is(err, ErrSource) || !strings.Contains(err.Error(), "CREDENTIALS_DIRECTORY is not set") {
		t.Fatalf("unset variable: %v", err)
	}
	for _, bad := range []string{"${CREDENTIALS_DIRECTORY}/../etc/passwd", "${CREDENTIALS_DIRECTORY}/", "$CREDENTIALS_DIRECTORY/a/../../b"} {
		if err := (config.SecretConfig{Source: config.SecretSourceFile, File: bad}).Validate(); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if err := (config.SecretConfig{Source: config.SecretSourceFile, File: "$CREDENTIALS_DIRECTORY/telfs-secret"}).Validate(); err != nil {
		t.Errorf("plain $VAR form refused: %v", err)
	}
}
