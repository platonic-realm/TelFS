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
	EnvProfile = "TELFS_PROFILE"
	EnvAPIID   = "TELFS_API_ID"
	EnvAPIHash = "TELFS_API_HASH"
	EnvPhone   = "TELFS_PHONE"
	EnvDC      = "TELFS_DC"
)

// File names within the data directory.
const (
	ConfigFile     = "config.toml"
	SessionFile    = "session.json" // v0.22 plaintext; refused, converted by `telfs session encrypt`
	SessionEncFile = "session.enc"
	DBFile         = "db.sqlite"
	CacheDir       = "cache"
)

// Profile-related paths.
const (
	// activeFile holds the name of the currently-active profile,
	// updated by `telfs profile use <name>`. Located in xdgConfigHome().
	activeFile = "active"
)

// Config is the persistent TelFS configuration.
//
// Auth is ALWAYS interactive phone+code+2FA against a real Telegram
// user account. Bot-token auth existed briefly (v0.14 era) and was
// removed: MTProto requires api_id/api_hash even for bots, so a bot
// identity saved nothing over a user login while adding a second auth
// path, an identity-scoped access_hash trap, and channel-admin
// requirements. Configs from that era may still contain auth_mode /
// bot_token keys — the TOML decoder ignores unknown keys, and any
// leftover bot session.json is rejected at login/mount with a
// reset-auth hint.
type Config struct {
	APIID   int    `toml:"api_id"`
	APIHash string `toml:"api_hash"`
	Phone   string `toml:"phone,omitempty"`
	// DC overrides gotd's default starting datacenter (which is 2). Useful in
	// environments where DC 2's primary IP is firewalled. Telegram will
	// migrate the connection to the user's home DC after auth regardless of
	// the starting DC.
	DC int `toml:"dc,omitempty"`

	// Identity records who session.json authenticates as, written after
	// every successful login. Display + consistency checks only — the
	// session file itself is the credential. Nil until the first login
	// (or after `telfs profile reset-auth`).
	Identity *Identity `toml:"identity,omitempty"`

	Channel ChannelConfig `toml:"channel"`

	// Secret says where this profile's secret comes from. The one
	// secret opens both session.enc and (on format-2 filesystems) the
	// FS key. A missing table means source = "prompt".
	Secret SecretConfig `toml:"secret"`

	// CacheCap is the LRU disk-cache size cap for this mount. Empty
	// (or unparseable) falls through to chunk.DefaultCacheCapBytes
	// (1 GiB). Recognized suffixes: B / KiB / MiB / GiB / KB / MB /
	// GB. The CLI flag --cache-cap on `telfs mount` overrides this.
	//
	// Set once at mount; not hot-reloadable. The Reader / Writer
	// hold the cache pointer for the life of the daemon.
	CacheCap string `toml:"cache_cap,omitempty"`

	// Staging is where written-but-not-yet-uploaded data waits, and how
	// much of it may (spec v1, row 9).
	Staging StagingConfig `toml:"staging,omitempty"`

	// DataDir is the resolved location of the .telfs/ directory. Not serialized.
	DataDir string `toml:"-"`
}

// Identity records the authenticated account behind session.json.
// IsBot survives bot-auth removal as a defense flag: a leftover bot
// session from the removed feature must be detected and rejected, not
// silently used.
type Identity struct {
	UserID   int64  `toml:"user_id,omitempty"`
	Username string `toml:"username,omitempty"`
	IsBot    bool   `toml:"is_bot,omitempty"`
}

// Label renders the identity for humans: "@name (user)".
func (id *Identity) Label() string {
	if id == nil {
		return ""
	}
	name := id.Username
	if name == "" {
		name = fmt.Sprintf("id=%d", id.UserID)
	} else {
		name = "@" + name
	}
	kind := "user"
	if id.IsBot {
		kind = "bot"
	}
	return fmt.Sprintf("%s (%s)", name, kind)
}

// HasSession reports whether this profile has authenticated — the
// profile IS that identity until `telfs profile reset-auth` wipes it
// (session + channel binding; the channel access_hash is scoped to
// the identity that resolved it and dies with it).
//
// A leftover plaintext session.json counts too: the identity lock is
// about who the profile IS, and that file still authenticates as them
// (it is refused for use, not ignored).
func (c *Config) HasSession() bool {
	if _, err := os.Stat(c.SessionPath()); err == nil {
		return true
	}
	return c.HasLegacySession()
}

// CacheCapBytes parses Config.CacheCap. Returns 0 (caller falls back
// to its default) on empty / parse error.
func (c *Config) CacheCapBytes() int64 {
	if n, err := ParseHumanBytes(c.CacheCap); err == nil {
		return n
	}
	return 0
}

// ParseHumanBytes accepts strings like "8GiB", "512 MB", "1024" (bare
// bytes), or "" (zero). Recognises binary (KiB / MiB / GiB / TiB) and
// decimal (KB / MB / GB / TB) suffixes case-insensitively. Returns
// (0, error) on parse failure so callers can distinguish "explicit
// zero" (which is the same as no cap, i.e. "use default") from
// "operator typo".
func ParseHumanBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	i := 0
	for i < len(s) && (s[i] == '.' || (s[i] >= '0' && s[i] <= '9')) {
		i++
	}
	numStr := strings.TrimSpace(s[:i])
	suffix := strings.ToLower(strings.TrimSpace(s[i:]))
	if numStr == "" {
		return 0, fmt.Errorf("cache_cap %q: missing number", s)
	}
	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("cache_cap %q: %w", s, err)
	}
	mult := int64(1)
	switch suffix {
	case "", "b":
		mult = 1
	case "k", "kb":
		mult = 1000
	case "kib":
		mult = 1 << 10
	case "m", "mb":
		mult = 1000 * 1000
	case "mib":
		mult = 1 << 20
	case "g", "gb":
		mult = 1000 * 1000 * 1000
	case "gib":
		mult = 1 << 30
	case "t", "tb":
		mult = 1000 * 1000 * 1000 * 1000
	case "tib":
		mult = 1 << 40
	default:
		return 0, fmt.Errorf("cache_cap %q: unknown suffix %q", s, suffix)
	}
	return int64(num * float64(mult)), nil
}

// Secret sources (see internal/secret).
const (
	SecretSourcePrompt    = "prompt"
	SecretSourceFile      = "file"
	SecretSourceKeePassXC = "keepassxc"
)

// SecretConfig is the [secret] table of config.toml. Only the LOCATION
// of the secret lives here — never the secret itself.
type SecretConfig struct {
	// Source is "prompt" (default when empty), "file" or "keepassxc".
	Source string `toml:"source,omitempty"`
	// File is the secret file for source = "file" (e.g. the S9 service
	// layout's secrets/<name>, written by svc-secret).
	File string `toml:"file,omitempty"`
	// Entry is the KeePassXC entry path inside the group exposed over
	// the Secret Service, for source = "keepassxc".
	Entry string `toml:"entry,omitempty"`
}

// EffectiveSource returns Source with the default applied.
func (s SecretConfig) EffectiveSource() string {
	if s.Source == "" {
		return SecretSourcePrompt
	}
	return s.Source
}

// Validate checks that the source is known and has what it needs.
func (s SecretConfig) Validate() error {
	switch s.EffectiveSource() {
	case SecretSourcePrompt:
		return nil
	case SecretSourceFile:
		if s.File == "" {
			return errors.New(`[secret] source = "file" needs file = "<path>"`)
		}
		if rest, ok := credentialRel(s.File); ok {
			if !credentialNameOK(rest) {
				return fmt.Errorf("[secret] file %q must name a file inside the credentials directory", s.File)
			}
			return nil
		}
		if !filepath.IsAbs(s.File) {
			return fmt.Errorf("[secret] file %q must be an absolute path", s.File)
		}
		return nil
	case SecretSourceKeePassXC:
		if strings.Trim(s.Entry, "/") == "" {
			return errors.New(`[secret] source = "keepassxc" needs entry = "<path inside the exposed group>"`)
		}
		return nil
	default:
		return fmt.Errorf(`[secret] source %q is unknown (use "prompt", "file" or "keepassxc")`, s.Source)
	}
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

// SessionPath returns the path of the encrypted session file
// (session.enc). The plaintext session.json of v0.22 and earlier lives
// at LegacySessionPath and is refused until converted.
func (c *Config) SessionPath() string { return filepath.Join(c.DataDir, SessionEncFile) }

// LegacySessionPath returns the path of a v0.22-era plaintext gotd
// session file.
func (c *Config) LegacySessionPath() string { return filepath.Join(c.DataDir, SessionFile) }

// HasLegacySession reports whether a plaintext session.json is present.
func (c *Config) HasLegacySession() bool {
	_, err := os.Stat(c.LegacySessionPath())
	return err == nil
}

// RemoveSessionFiles deletes the session in both its encrypted and its
// legacy plaintext form (reset-auth, logout).
func (c *Config) RemoveSessionFiles() error {
	for _, p := range []string{c.SessionPath(), c.LegacySessionPath()} {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

// ProfileName is the profile's name (the data dir's base name).
func (c *Config) ProfileName() string { return filepath.Base(c.DataDir) }

// DBPath returns the path of the SQLite metadata DB.
func (c *Config) DBPath() string { return filepath.Join(c.DataDir, DBFile) }

// CachePath returns the path of the on-disk chunk cache.
func (c *Config) CachePath() string { return filepath.Join(c.DataDir, CacheDir) }

// ErrNoActiveProfile is returned when no profile selection is in effect.
// Callers should surface a clear "run telfs profile use <name>" hint.
var ErrNoActiveProfile = errors.New("no active TelFS profile — set TELFS_PROFILE or run `telfs profile use <name>`")

// DefaultDir resolves the data-dir path. Lookup order:
//
//  1. $TELFS_PROFILE set → ~/.config/telfs/profiles/<value>/
//  2. activeFile exists  → ~/.config/telfs/profiles/<contents>/
//
// Returns ErrNoActiveProfile when neither is set.
func DefaultDir() (string, error) {
	if name := strings.TrimSpace(os.Getenv(EnvProfile)); name != "" {
		return ProfileDir(name)
	}
	if name, ok := readActiveProfile(); ok {
		return ProfileDir(name)
	}
	return "", ErrNoActiveProfile
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
// Returns (name, true) on success, ("", false) if the file doesn't exist
// or is empty/invalid (callers should fall back to the next resolution step).
//
// A name whose profile DIRECTORY no longer exists is treated as unset:
// a dangling active file (e.g. left by deleting the active profile with
// an old binary) would otherwise silently resurrect an empty profile
// dir on the next config.Load and swallow every command into it.
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
	if dir, err := ProfileDir(name); err != nil {
		return "", false
	} else if _, err := os.Stat(dir); err != nil {
		return "", false
	}
	return name, true
}

// ClearActiveProfile removes the active-profile selection. Called when
// the active profile is deleted so the stale name doesn't dangle.
func ClearActiveProfile() error {
	root, err := xdgConfigHome()
	if err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(root, activeFile)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
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

// ActiveProfile returns the currently-active profile name, or "" if
// none is set.
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
	return LoadFromDir(dir)
}

// LoadFromDir reads the config from an explicit data directory,
// bypassing the global profile resolution. Useful for inspecting a
// non-active profile without env-var tricks.
func LoadFromDir(dir string) (*Config, error) {
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

// StagingConfig is `[staging]` in config.toml (spec v1, row 9).
//
//	[staging]
//	dir = "/srv/…/telfs-staging"  # default <profile>/cache/pending
//	max_bytes = "50GB"            # staged bytes above which writes wait
//	min_free = "5GiB"             # never fill the staging filesystem below this
//
// The directory must be on persistent storage: until a chunk is uploaded,
// its staged wire is the filesystem's only copy (re-submitted at the next
// mount after a crash).
type StagingConfig struct {
	Dir      string `toml:"dir,omitempty"`
	MaxBytes string `toml:"max_bytes,omitempty"`
	MinFree  string `toml:"min_free,omitempty"`
}

// PendingDirName is the staging directory's name under the cache root
// (chunk.PendingDirName is the same).
const PendingDirName = "pending"

// StagingDir is the staging directory: [staging] dir, else
// <profile>/cache/pending.
func (c *Config) StagingDir() string {
	if c.Staging.Dir != "" {
		return c.Staging.Dir
	}
	return filepath.Join(c.CachePath(), PendingDirName)
}

// StagingLimits returns [staging] max_bytes and min_free, with the v1
// defaults (50 GB, 5 GiB) for unset values. max_bytes = 0 means no cap.
func (c *Config) StagingLimits() (maxBytes, minFree int64, err error) {
	maxBytes, minFree = 50_000_000_000, 5<<30
	if c.Staging.MaxBytes != "" {
		if maxBytes, err = ParseHumanBytes(c.Staging.MaxBytes); err != nil {
			return 0, 0, fmt.Errorf("[staging] max_bytes: %w", err)
		}
	}
	if c.Staging.MinFree != "" {
		if minFree, err = ParseHumanBytes(c.Staging.MinFree); err != nil {
			return 0, 0, fmt.Errorf("[staging] min_free: %w", err)
		}
	}
	return maxBytes, minFree, nil
}

// CredentialsDirVar is the variable systemd sets for a unit that loads
// credentials (LoadCredential=/LoadCredentialEncrypted=): the unit's own
// credentials directory (%d in the unit). It holds a path, not a secret.
const CredentialsDirVar = "CREDENTIALS_DIRECTORY"

// credentialRel returns the part after "${CREDENTIALS_DIRECTORY}/" (or
// "$CREDENTIALS_DIRECTORY/") when file starts with it.
func credentialRel(file string) (string, bool) {
	for _, pre := range []string{"${" + CredentialsDirVar + "}/", "$" + CredentialsDirVar + "/"} {
		if strings.HasPrefix(file, pre) {
			return file[len(pre):], true
		}
	}
	return "", false
}

func credentialNameOK(rel string) bool {
	c := filepath.Clean(rel)
	return rel != "" && c != "." && !filepath.IsAbs(c) && c != ".." && !strings.HasPrefix(c, "../")
}

// ResolvedFile is the secret file's path. A file under
// ${CREDENTIALS_DIRECTORY} (systemd encrypted credentials, spec v1) resolves
// inside that directory only: if the variable is unset — the command is not
// running in the unit that loaded the credential — it is an error, never a
// fallback to some other place. Nothing else in the path is expanded.
func (s SecretConfig) ResolvedFile() (string, error) {
	rel, ok := credentialRel(s.File)
	if !ok {
		return s.File, nil
	}
	if !credentialNameOK(rel) {
		return "", fmt.Errorf("[secret] file %q must name a file inside the credentials directory", s.File)
	}
	dir := os.Getenv(CredentialsDirVar)
	if dir == "" {
		return "", fmt.Errorf("[secret] file %q comes from systemd's credentials directory, but $%s is not set — run this from the unit that loads the credential (LoadCredentialEncrypted=)", s.File, CredentialsDirVar)
	}
	return filepath.Join(dir, filepath.Clean(rel)), nil
}
