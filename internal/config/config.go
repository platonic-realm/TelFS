package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
)

// Environment variables. Set any of these to override the on-disk config.
const (
	EnvDir     = "TELFS_DIR"
	EnvProfile = "TELFS_PROFILE"
	EnvAPIID   = "TELFS_API_ID"
	EnvAPIHash = "TELFS_API_HASH"
	EnvPhone   = "TELFS_PHONE"
	EnvDC      = "TELFS_DC"
)

// File names within the data directory.
const (
	DefaultDirName = ".telfs"
	ConfigFile     = "config.toml"
	SessionFile    = "session.json"
	DBFile         = "db.sqlite"
	CacheDir       = "cache"
)

// Profile-related paths.
const (
	// DefaultProfileName is what we use when no profile is selected.
	DefaultProfileName = "default"
	// activeFile holds the name of the currently-active profile,
	// updated by `telfs profile use <name>`. Located in xdgConfigHome().
	activeFile = "active"
)

// Config is the persistent TelFS configuration.
type Config struct {
	APIID   int    `toml:"api_id"`
	APIHash string `toml:"api_hash"`
	Phone   string `toml:"phone,omitempty"`
	// DC overrides gotd's default starting datacenter (which is 2). Useful in
	// environments where DC 2's primary IP is firewalled. Telegram will
	// migrate the connection to the user's home DC after auth regardless of
	// the starting DC.
	DC int `toml:"dc,omitempty"`

	Channel ChannelConfig `toml:"channel"`

	// DataDir is the resolved location of the .telfs/ directory. Not serialized.
	DataDir string `toml:"-"`
}

// ChannelConfig stores the resolved peer reference for the backing channel.
// AccessHash is required by MTProto on every call; we cache it here so we
// don't re-scan dialogs every mount.
type ChannelConfig struct {
	ID         int64  `toml:"id,omitempty"`
	AccessHash int64  `toml:"access_hash,omitempty"`
	Title      string `toml:"title,omitempty"`
	Username   string `toml:"username,omitempty"`
}

// ConfigPath returns the path of the on-disk config file.
func (c *Config) ConfigPath() string { return filepath.Join(c.DataDir, ConfigFile) }

// SessionPath returns the path of the gotd session file.
func (c *Config) SessionPath() string { return filepath.Join(c.DataDir, SessionFile) }

// DBPath returns the path of the SQLite metadata DB.
func (c *Config) DBPath() string { return filepath.Join(c.DataDir, DBFile) }

// CachePath returns the path of the on-disk chunk cache.
func (c *Config) CachePath() string { return filepath.Join(c.DataDir, CacheDir) }

// DefaultDir resolves the data-dir path. Lookup order:
//
//  1. $TELFS_PROFILE set     → ~/.config/telfs/profiles/<value>/
//  2. activeFile exists       → ~/.config/telfs/profiles/<contents>/
//  3. $TELFS_DIR set          → that exact path (legacy)
//  4. fallback                → $PWD/.telfs (legacy)
//
// The legacy paths keep pre-profile workflows working unchanged —
// users who never run `telfs profile use` see the same behavior as
// before this feature landed.
func DefaultDir() (string, error) {
	if name := strings.TrimSpace(os.Getenv(EnvProfile)); name != "" {
		return ProfileDir(name)
	}
	if name, ok := readActiveProfile(); ok {
		return ProfileDir(name)
	}
	if d := os.Getenv(EnvDir); d != "" {
		return d, nil
	}
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return filepath.Join(cwd, DefaultDirName), nil
}

// xdgConfigHome returns the canonical TelFS config root —
// $XDG_CONFIG_HOME/telfs if XDG_CONFIG_HOME is set, otherwise
// ~/.config/telfs.
func xdgConfigHome() (string, error) {
	if x := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME")); x != "" {
		return filepath.Join(x, "telfs"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".config", "telfs"), nil
}

// ProfilesRoot returns the directory containing all named profiles.
func ProfilesRoot() (string, error) {
	root, err := xdgConfigHome()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, "profiles"), nil
}

// ProfileDir returns the data-dir for the named profile. Name is
// validated to prevent path traversal.
func ProfileDir(name string) (string, error) {
	if err := ValidateProfileName(name); err != nil {
		return "", err
	}
	root, err := ProfilesRoot()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, name), nil
}

// ValidateProfileName ensures the name is a single safe identifier —
// letters, digits, dash, underscore — and not empty.
func ValidateProfileName(name string) error {
	if name == "" {
		return fmt.Errorf("profile name must not be empty")
	}
	for _, r := range name {
		ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '-' || r == '_'
		if !ok {
			return fmt.Errorf("profile name %q contains invalid character %q (allowed: a-z A-Z 0-9 - _)", name, r)
		}
	}
	return nil
}

// readActiveProfile reads the profile name written by `telfs profile use`.
// Returns (name, true) on success, (",", false) if the file doesn't exist
// or is empty/invalid (callers should fall back to the next resolution step).
func readActiveProfile() (string, bool) {
	root, err := xdgConfigHome()
	if err != nil {
		return "", false
	}
	b, err := os.ReadFile(filepath.Join(root, activeFile))
	if err != nil {
		return "", false
	}
	name := strings.TrimSpace(string(b))
	if name == "" || ValidateProfileName(name) != nil {
		return "", false
	}
	return name, true
}

// SetActiveProfile writes the named profile to ~/.config/telfs/active so
// it becomes the default for subsequent commands.
func SetActiveProfile(name string) error {
	if err := ValidateProfileName(name); err != nil {
		return err
	}
	root, err := xdgConfigHome()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(root, activeFile), []byte(name+"\n"), 0o600)
}

// ActiveProfile returns the currently-active profile name. Returns
// DefaultProfileName when no profile selection is in effect, or "" if
// the resolution falls back to a legacy ./.telfs / $TELFS_DIR layout.
func ActiveProfile() string {
	if name := strings.TrimSpace(os.Getenv(EnvProfile)); name != "" {
		return name
	}
	if name, ok := readActiveProfile(); ok {
		return name
	}
	return ""
}

// Load reads the on-disk config (if any) and applies env-variable overrides.
// It creates the data dir on first use.
func Load() (*Config, error) {
	dir, err := DefaultDir()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create data dir %s: %w", dir, err)
	}

	c := &Config{DataDir: dir}
	cfgPath := c.ConfigPath()
	if _, err := os.Stat(cfgPath); err == nil {
		if _, err := toml.DecodeFile(cfgPath, c); err != nil {
			return nil, fmt.Errorf("decode %s: %w", cfgPath, err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("stat %s: %w", cfgPath, err)
	}
	// DataDir is not serialized; restore after decode.
	c.DataDir = dir

	if v := strings.TrimSpace(os.Getenv(EnvAPIID)); v != "" {
		id, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid %s=%q: %w", EnvAPIID, v, err)
		}
		c.APIID = id
	}
	if v := strings.TrimSpace(os.Getenv(EnvAPIHash)); v != "" {
		c.APIHash = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvPhone)); v != "" {
		c.Phone = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDC)); v != "" {
		dc, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid %s=%q: %w", EnvDC, v, err)
		}
		c.DC = dc
	}

	return c, nil
}

// Save writes the config back to disk with restrictive permissions.
func (c *Config) Save() error {
	if err := os.MkdirAll(c.DataDir, 0o700); err != nil {
		return err
	}
	tmp := c.ConfigPath() + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if err := toml.NewEncoder(f).Encode(c); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, c.ConfigPath())
}

// RequireAPI checks that the Telegram API credentials are present.
func (c *Config) RequireAPI() error {
	var missing []string
	if c.APIID == 0 {
		missing = append(missing, "api_id")
	}
	if c.APIHash == "" {
		missing = append(missing, "api_hash")
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("missing Telegram credentials: %s\n"+
		"  set via env (%s, %s) or edit %s\n"+
		"  obtain them at https://my.telegram.org/apps",
		strings.Join(missing, ", "), EnvAPIID, EnvAPIHash, c.ConfigPath())
}

// RequireChannel checks that a backing channel has been picked.
func (c *Config) RequireChannel() error {
	if c.Channel.ID == 0 || c.Channel.AccessHash == 0 {
		return fmt.Errorf("no channel configured — run 'telfs channel list' then 'telfs channel set <id>'")
	}
	return nil
}
