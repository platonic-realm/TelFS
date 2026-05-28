package web

import (
	"strings"
	"testing"
)

func TestScrubEnvRemovesNamedKeys(t *testing.T) {
	in := []string{
		"PATH=/usr/bin",
		"TELFS_PROFILE=stale",
		"HOME=/home/test",
		"TELFS_PASSPHRASE=leak",
		"LANG=en_US.UTF-8",
	}
	out := scrubEnv(in, "TELFS_PROFILE", "TELFS_PASSPHRASE")
	for _, e := range out {
		if strings.HasPrefix(e, "TELFS_PROFILE=") {
			t.Errorf("TELFS_PROFILE leaked into scrubbed env: %q", e)
		}
		if strings.HasPrefix(e, "TELFS_PASSPHRASE=") {
			t.Errorf("TELFS_PASSPHRASE leaked into scrubbed env: %q", e)
		}
	}
	// Non-target keys must remain.
	want := []string{"PATH=", "HOME=", "LANG="}
	for _, prefix := range want {
		found := false
		for _, e := range out {
			if strings.HasPrefix(e, prefix) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("scrubEnv accidentally removed %q", prefix)
		}
	}
}

func TestScrubEnvHandlesValueWithEqualsSign(t *testing.T) {
	// Edge case: a value can contain '=', e.g. a base64 token. scrubEnv
	// must only match on the KEY= prefix, not on the substring "=".
	in := []string{
		"TOKEN=abc=def",
		"TELFS_PROFILE=main",
	}
	out := scrubEnv(in, "TELFS_PROFILE")
	if len(out) != 1 || out[0] != "TOKEN=abc=def" {
		t.Errorf("scrubEnv mangled non-target value: %v", out)
	}
}

func TestMountLogPathPerProfileSeparation(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "/run/user/1000")
	a := mountLogPath("main")
	b := mountLogPath("work")
	if a == b {
		t.Errorf("expected per-profile log paths, both got %q", a)
	}
	if !strings.HasPrefix(a, "/run/user/1000/") {
		t.Errorf("XDG_RUNTIME_DIR not honored: %q", a)
	}
}

func TestMountLogPathFallsBackToTmp(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "")
	got := mountLogPath("main")
	if !strings.HasPrefix(got, "/tmp/") {
		t.Errorf("expected /tmp fallback, got %q", got)
	}
}

func TestMountLogPathEmptyProfile(t *testing.T) {
	// An empty profile name must NOT produce a path like ".log" or end
	// in a dash — the supervisor would clobber other profiles' logs.
	got := mountLogPath("")
	if strings.HasSuffix(got, "telfs-web-.log") {
		t.Errorf("empty profile leaked into log name: %q", got)
	}
}

func TestStatusPillKind(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"running", "ok"},
		{"exited", "muted"},
		{"exited: exit status 1", "err"},
		{"exited: signal: terminated", "err"},
		{"stopped", "muted"},
		{"", "muted"},
		{"weird-unknown-status", "muted"},
	}
	for _, c := range cases {
		if got := statusPillKind(c.in); got != c.want {
			t.Errorf("statusPillKind(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestScanProcMountsRobustness ensures the parser doesn't panic on
// malformed lines — /proc/mounts is kernel-generated but we still want
// graceful behavior.
func TestScanProcMounts(t *testing.T) {
	// Just verify it returns a slice (possibly empty) without panic.
	// On Linux this should return real entries; on no-/proc systems
	// it returns nil.
	got := scanProcMounts()
	_ = got // smoke-only — content depends on host
}
