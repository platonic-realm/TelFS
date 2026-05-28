package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateProfileNameRejectsPathTraversal(t *testing.T) {
	bad := []string{
		"",                  // empty
		"../escape",         // dotdot
		"./local",           // leading dot
		"with space",        // whitespace
		"with/slash",        // separator
		"with\\backslash",   // alt separator
		"null\x00byte",      // NUL
		"name\nwith\nlines", // newline
	}
	for _, b := range bad {
		if err := ValidateProfileName(b); err == nil {
			t.Errorf("ValidateProfileName(%q) accepted; should reject", b)
		}
	}
}

func TestValidateProfileNameAcceptsReasonable(t *testing.T) {
	good := []string{"default", "work", "personal", "bot1", "team-alpha", "a", "Z9"}
	for _, g := range good {
		if err := ValidateProfileName(g); err != nil {
			t.Errorf("ValidateProfileName(%q) rejected: %v", g, err)
		}
	}
}

// TestDefaultDirResolutionOrder verifies the documented precedence:
//
//	1. $TELFS_PROFILE → profile dir
//	2. ~/.config/telfs/active file → profile dir
//	3. $TELFS_DIR → that exact path
//	4. ./.telfs → legacy fallback
func TestDefaultDirResolutionOrder(t *testing.T) {
	// Isolate from the real user's environment.
	xdg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", xdg)
	cwd := t.TempDir()
	if err := os.Chdir(cwd); err != nil {
		t.Fatal(err)
	}

	// 4. Default fallback: $TELFS_DIR unset, no profile, no active file.
	t.Setenv("TELFS_PROFILE", "")
	t.Setenv("TELFS_DIR", "")
	got, err := DefaultDir()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(got, "/.telfs") {
		t.Errorf("fallback: got %q, want suffix /.telfs", got)
	}

	// 3. TELFS_DIR override.
	t.Setenv("TELFS_DIR", "/tmp/explicit")
	got, _ = DefaultDir()
	if got != "/tmp/explicit" {
		t.Errorf("TELFS_DIR: got %q, want /tmp/explicit", got)
	}

	// 2. active file beats TELFS_DIR.
	if err := os.MkdirAll(filepath.Join(xdg, "telfs"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(xdg, "telfs", "active"), []byte("work\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, _ = DefaultDir()
	wantSuffix := "/telfs/profiles/work"
	if !strings.HasSuffix(got, wantSuffix) {
		t.Errorf("active file: got %q, want suffix %s", got, wantSuffix)
	}

	// 1. TELFS_PROFILE beats active file.
	t.Setenv("TELFS_PROFILE", "explicit-env")
	got, _ = DefaultDir()
	wantSuffix = "/telfs/profiles/explicit-env"
	if !strings.HasSuffix(got, wantSuffix) {
		t.Errorf("TELFS_PROFILE: got %q, want suffix %s", got, wantSuffix)
	}
}

func TestSetActiveProfileRejectsBadNames(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	if err := SetActiveProfile("../escape"); err == nil {
		t.Fatalf("SetActiveProfile accepted path-traversal name")
	}
}

func TestActiveProfileRecoversFromCorruptFile(t *testing.T) {
	xdg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", xdg)
	if err := os.MkdirAll(filepath.Join(xdg, "telfs"), 0o700); err != nil {
		t.Fatal(err)
	}
	// Write a junk active file — a malformed name should NOT crash
	// callers; ActiveProfile should report "no active selection."
	_ = os.WriteFile(filepath.Join(xdg, "telfs", "active"), []byte("../malformed\n"), 0o600)
	t.Setenv("TELFS_PROFILE", "")
	if got := ActiveProfile(); got != "" {
		t.Fatalf("malformed active file: ActiveProfile() = %q, want \"\" (fallthrough)", got)
	}
}
