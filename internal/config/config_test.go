package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
//  1. $TELFS_PROFILE → profile dir
//  2. ~/.config/telfs/active file → profile dir
//     none of the above → ErrNoActiveProfile
func TestDefaultDirResolutionOrder(t *testing.T) {
	// Isolate from the real user's environment.
	xdg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", xdg)

	// No env, no active file → ErrNoActiveProfile.
	t.Setenv("TELFS_PROFILE", "")
	if _, err := DefaultDir(); err == nil {
		t.Fatalf("expected ErrNoActiveProfile when nothing is set, got nil")
	}

	// active file resolves — the named profile dir must EXIST (a
	// dangling name is treated as unset; see readActiveProfile).
	if err := os.MkdirAll(filepath.Join(xdg, "telfs", "profiles", "work"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(xdg, "telfs", "active"), []byte("work\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, _ := DefaultDir()
	wantSuffix := "/telfs/profiles/work"
	if !strings.HasSuffix(got, wantSuffix) {
		t.Errorf("active file: got %q, want suffix %s", got, wantSuffix)
	}

	// TELFS_PROFILE beats active file.
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

// TestActiveProfileIgnoresDanglingName: an active file naming a profile
// whose directory no longer exists (deleted with an old binary, hand
// edit) is treated as no selection — it must NOT silently resurrect an
// empty profile dir via the next config.Load.
func TestActiveProfileIgnoresDanglingName(t *testing.T) {
	xdg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", xdg)
	t.Setenv("TELFS_PROFILE", "")
	if err := os.MkdirAll(filepath.Join(xdg, "telfs"), 0o700); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(xdg, "telfs", "active"), []byte("ghost\n"), 0o600)
	if got := ActiveProfile(); got != "" {
		t.Fatalf("dangling active name: ActiveProfile() = %q, want \"\"", got)
	}
	if _, err := DefaultDir(); err == nil {
		t.Fatal("dangling active name: DefaultDir() succeeded, want ErrNoActiveProfile")
	}
}

// TestClearActiveProfile: use → clear → no selection. Clearing twice
// is fine (idempotent).
func TestClearActiveProfile(t *testing.T) {
	xdg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", xdg)
	t.Setenv("TELFS_PROFILE", "")
	if err := os.MkdirAll(filepath.Join(xdg, "telfs", "profiles", "work"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := SetActiveProfile("work"); err != nil {
		t.Fatal(err)
	}
	if got := ActiveProfile(); got != "work" {
		t.Fatalf("ActiveProfile() = %q, want work", got)
	}
	if err := ClearActiveProfile(); err != nil {
		t.Fatal(err)
	}
	if got := ActiveProfile(); got != "" {
		t.Fatalf("after clear: ActiveProfile() = %q, want \"\"", got)
	}
	if err := ClearActiveProfile(); err != nil {
		t.Fatalf("second clear should be idempotent, got %v", err)
	}
}

// TestIdentityRoundTrip: [identity] persists through Save/Load, and
// HasSession flips with the session file's existence.
func TestIdentityRoundTrip(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{DataDir: dir, APIID: 1, APIHash: "h"}

	if cfg.HasSession() {
		t.Fatal("HasSession true with no session file")
	}

	cfg.Identity = &Identity{UserID: 42, Username: "arash", IsBot: false}
	if err := cfg.Save(); err != nil {
		t.Fatal(err)
	}
	// Fake a session file — the identity lock is its existence.
	if err := os.WriteFile(cfg.SessionPath(), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := LoadFromDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got.Identity == nil || got.Identity.UserID != 42 || got.Identity.IsBot || got.Identity.Username != "arash" {
		t.Fatalf("identity round-trip mismatch: %+v", got.Identity)
	}
	if !got.HasSession() {
		t.Fatal("HasSession false with session file present")
	}
	if label := got.Identity.Label(); label != "@arash (user)" {
		t.Fatalf("Label = %q", label)
	}
	// A leftover bot identity (from the removed bot-auth feature) must
	// still label itself honestly so rejection messages name the actor.
	bot := &Identity{UserID: 9, Username: "old_bot", IsBot: true}
	if label := bot.Label(); label != "@old_bot (bot)" {
		t.Fatalf("bot Label = %q", label)
	}
}

// TestConfigIgnoresRemovedBotKeys: configs written while bot auth
// existed carry auth_mode / bot_token keys — decoding must not fail on
// them.
func TestConfigIgnoresRemovedBotKeys(t *testing.T) {
	dir := t.TempDir()
	legacy := "api_id = 7\napi_hash = \"h\"\nauth_mode = \"bot\"\nbot_token = \"123:abc\"\n"
	if err := os.WriteFile(filepath.Join(dir, ConfigFile), []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFromDir(dir)
	if err != nil {
		t.Fatalf("legacy config with bot keys failed to load: %v", err)
	}
	if cfg.APIID != 7 {
		t.Fatalf("APIID = %d, want 7", cfg.APIID)
	}
}

// TestIdentityLabelNil: nil identity renders empty (templates rely on
// this for the {{if .Identity}} guard).
func TestIdentityLabelNil(t *testing.T) {
	var id *Identity
	if got := id.Label(); got != "" {
		t.Fatalf("nil Label = %q, want \"\"", got)
	}
	noName := &Identity{UserID: 9}
	if got := noName.Label(); got != "id=9 (user)" {
		t.Fatalf("no-username Label = %q", got)
	}
}

func TestSecretConfigValidate(t *testing.T) {
	cases := []struct {
		c  SecretConfig
		ok bool
	}{
		{SecretConfig{}, true},
		{SecretConfig{Source: "prompt"}, true},
		{SecretConfig{Source: "file", File: "/srv/x/secrets/telfs"}, true},
		{SecretConfig{Source: "file"}, false},
		{SecretConfig{Source: "file", File: "relative/path"}, false},
		{SecretConfig{Source: "keepassxc", Entry: "Services/TelFS/home"}, true},
		{SecretConfig{Source: "keepassxc", Entry: "/"}, false},
		{SecretConfig{Source: "env"}, false},
	}
	for _, c := range cases {
		if err := c.c.Validate(); (err == nil) != c.ok {
			t.Errorf("%+v: err=%v, want ok=%v", c.c, err, c.ok)
		}
	}
	if (SecretConfig{}).EffectiveSource() != SecretSourcePrompt {
		t.Error("missing source must default to prompt")
	}
}

func TestLegacySessionAndRemove(t *testing.T) {
	dir := t.TempDir()
	c := &Config{DataDir: dir}
	if c.HasSession() {
		t.Fatal("empty profile has no session")
	}
	os.WriteFile(c.LegacySessionPath(), []byte("{}"), 0o600)
	if !c.HasSession() || !c.HasLegacySession() {
		t.Fatal("a leftover session.json still counts as the profile's identity")
	}
	os.WriteFile(c.SessionPath(), []byte("x"), 0o600)
	if err := c.RemoveSessionFiles(); err != nil {
		t.Fatal(err)
	}
	if c.HasSession() {
		t.Fatal("RemoveSessionFiles must remove both")
	}
}

// [snapshot] every_ops / every: defaults when unset, read from
// config.toml, bad values refused; a config without the table saves
// without one.
func TestSnapshotPolicyConfig(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{DataDir: dir, APIID: 1, APIHash: "h"}
	ops, every, err := cfg.SnapshotPolicy()
	if err != nil || ops != 500_000 || every != 24*time.Hour {
		t.Fatalf("defaults: %d %s %v", ops, every, err)
	}
	if err := cfg.Save(); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(cfg.ConfigPath()); strings.Contains(string(b), "[snapshot]") {
		t.Fatalf("an unset [snapshot] must not be written:\n%s", b)
	}
	toml := "api_id = 7\napi_hash = \"h\"\n\n[snapshot]\nevery_ops = 200000\nevery = \"6h\"\n"
	if err := os.WriteFile(filepath.Join(dir, ConfigFile), []byte(toml), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := LoadFromDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if ops, every, err = got.SnapshotPolicy(); err != nil || ops != 200_000 || every != 6*time.Hour {
		t.Fatalf("configured: %d %s %v", ops, every, err)
	}
	for _, bad := range []SnapshotConfig{{EveryOps: -1}, {Every: "1d"}, {Every: "0s"}, {Every: "-1h"}} {
		c := &Config{Snapshot: bad}
		if _, _, err := c.SnapshotPolicy(); err == nil || !strings.Contains(err.Error(), "[snapshot]") {
			t.Errorf("%+v: %v, want an error naming [snapshot]", bad, err)
		}
	}
}
