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

// DefaultDir returns the data-dir path, honoring $TELFS_DIR; default is
// `$PWD/.telfs`. Mounting from a fresh directory yields a fresh filesystem,
// which is useful during development.
func DefaultDir() (string, error) {
	if d := os.Getenv(EnvDir); d != "" {
		return d, nil
	}
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return filepath.Join(cwd, DefaultDirName), nil
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
