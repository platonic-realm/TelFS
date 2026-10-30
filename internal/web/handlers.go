package web

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"telfs/internal/config"
	"telfs/internal/crypto"
	"telfs/internal/meta"
	"telfs/internal/mountctl"
	"telfs/internal/snapshot"
)

// dashboardData mirrors the shape of `telfs status` — the same
// information, rendered as HTML.
type dashboardData struct {
	CSRF      string
	Flash     flashData
	Path      string // layout uses this to highlight the active nav tab
	Profile   string
	Identity  string // "@name (user|bot)" — who session.json authenticates as
	DataDir   string
	Channel   channelView
	HasMeta   bool
	FSUUID    string
	ChunkSize int64
	Crypto    cryptoView
	LastSnap  string
	Mounts    []string
	Profiles  []string
	Active    string
	Setup     setupChecklist
	// Monitors is every profile's row (dashboard tables); MountList every
	// TelFS mount on the machine with the profile it serves.
	Monitors  []profileMonitor
	MountList []mountView
}

// setupChecklist surfaces which onboarding steps are still pending so
// the dashboard can guide a new user instead of presenting an empty
// "everything looks fine" view. Each Step is a tuple of (label, done,
// href) — the template walks the slice and renders the first
// non-done item as the next call-to-action.
type setupChecklist struct {
	Complete bool
	Steps    []setupStep
}

type setupStep struct {
	// Key identifies the step for the wizard's inline-form dispatch:
	// profile | api | auth | channel | encrypt | compress | mount.
	Key   string
	Label string
	Done  bool
	Href  string
	Why   string // optional helper text shown for the current pending step
	// Optional — populated only by the wizard. Empty for the dashboard
	// checklist render. Detail is the longer explanation, Outcome is
	// what the user can expect to see after completing the step, and
	// Optional flags steps the user is allowed to skip.
	Detail   string
	Outcome  string
	Optional bool
	CTA      string // label on the wizard's secondary "full page" link
}

// setupReturn resolves a mutating handler's redirect target: forms
// posted from the setup wizard carry from=setup and return there, so
// the whole onboarding stays on one page; everything else keeps the
// handler's default target.
func setupReturn(r *http.Request, def string) string {
	if r.FormValue("from") == "setup" {
		return "/setup"
	}
	return def
}

type channelView struct {
	Set   bool
	ID    int64
	Title string
}

type cryptoView struct {
	Enabled bool
	Mode    string
	Argon   string
}

type flashData struct{ Kind, Msg string }

// dashboardIndex: every profile first (live table, the selected one
// marked), then the setup checklist while a step is open, then every
// profile's channel and filesystem, then every TelFS mount.
func (s *Server) dashboardIndex(w http.ResponseWriter, r *http.Request) {
	d := s.loadDashboard(r.Context(), w, r)
	d.Monitors = monitorProfiles(r.Context(), monFull, profileOf(r))
	d.MountList = mountList()
	s.renderTemplateWith(w, "dashboard.html", d, "templates/partials/profiles.html")
}

// profilesPanelData is the polled profile table's data (the same field
// names as dashboardData, so the block renders from either).
type profilesPanelData struct {
	CSRF     string
	Monitors []profileMonitor
}

// profilesPartial re-renders the dashboard's profile table every 5 s.
// Never through loadDashboard: that consumes the flash before the user
// sees it. EnsureCSRF reuses the browser's token (no rotation), so the
// table's Select buttons keep working across polls.
func (s *Server) profilesPartial(w http.ResponseWriter, r *http.Request) {
	d := profilesPanelData{CSRF: s.state.EnsureCSRF(w, r), Monitors: monitorProfiles(r.Context(), monLive, profileOf(r))}
	s.renderPartialBlock(w, "partials/profiles.html", "profiles_panel", d)
}

// statusPartial re-renders the dashboard's mount list (every 10 s).
func (s *Server) statusPartial(w http.ResponseWriter, r *http.Request) {
	d := struct{ MountList []mountView }{MountList: mountList()}
	s.renderPartialBlock(w, "partials/profiles.html", "mounts_panel", d)
}

// mountView is one fuse.telfs entry in /proc/mounts.
type mountView struct {
	Point    string
	Profile  string // "" when the source does not name one
	Version  string // a version mount's version
	ReadOnly bool
}

// mountList is every TelFS mount on the machine, with the profile it
// serves — bind mounts of a filesystem (S9: /mnt/offsite) included.
func mountList() []mountView {
	var out []mountView
	for _, m := range mountctl.ScanMounts() {
		v := mountView{Point: m.Point, Version: mountctl.VersionOf(m), ReadOnly: m.ReadOnly}
		if strings.HasPrefix(m.Source, mountctl.SourcePrefix) {
			v.Profile, _ = mountctl.ProfileOf(m)
		}
		out = append(out, v)
	}
	return out
}

// setupWizardData drives the /setup page. CurrentIdx is the 0-based
// index of the first not-done (and not-skipped) step; -1 when all
// steps are done.
type setupWizardData struct {
	pageBase
	Steps      []setupStep
	CurrentIdx int
	Complete   bool
	// Skipped is the set of step indices the user has chosen to skip
	// (currently used only for the optional encryption step). Tracked
	// in a cookie so a refresh doesn't lose state.
	Skipped map[int]bool

	// Form state for the CURRENT step's inline form — the wizard IS
	// the form, not a launcher for other pages.
	Active     string // active profile name ("" before the profile step)
	APIID      int    // pre-fill for the api step
	AuthStage  string // "" (phone form) | "code" | "password" — in-flight login
	CryptoOn   bool   // (kept for templates of format-1 profiles)
	Codec      string // init step: current codec pre-selection
	Mountpoint string // mount step: suggested default
	// Secret step: the configured source.
	SecretSource, SecretFile, SecretEntry string
}

const setupSkipCookie = "telfs_setup_skip"

// setupSkipCookieName scopes the skip state to the active profile —
// skipping the optional encryption step on one profile must not mark
// it skipped on every other profile the operator switches to.
// Profile names are [A-Za-z0-9_-], all valid cookie-name chars.
func setupSkipCookieName(profile string) string {
	if profile != "" {
		return setupSkipCookie + "_" + profile
	}
	return setupSkipCookie
}

// setupWizard is the guided first-run flow: one page that carries the
// whole onboarding, each step rendered as an INLINE form posting to
// the step's existing handler with from=setup (see setupReturn) so no
// logic is duplicated and the flow never leaves /setup. The dedicated
// pages remain for power users; each step links to its full page as a
// secondary action.
func (s *Server) setupWizard(w http.ResponseWriter, r *http.Request) {
	d := s.loadDashboard(r.Context(), w, r)
	skipped := readSkipCookie(r)
	currentIdx := -1
	for i, step := range d.Setup.Steps {
		if step.Done {
			continue
		}
		if skipped[i] && step.Optional {
			continue
		}
		currentIdx = i
		break
	}
	pageData := setupWizardData{
		pageBase:   s.basePage(w, r),
		Steps:      d.Setup.Steps,
		CurrentIdx: currentIdx,
		Complete:   currentIdx == -1,
		Skipped:    skipped,
		Active:     d.Active,
		Mountpoint: defaultMountpoint(),
	}
	if cfg, err := loadCfg(r); err == nil {
		pageData.APIID = cfg.APIID
	}
	// In-flight phone login? Render the code/password form instead of
	// the phone form so a reload lands back mid-flow, not at the start.
	if sess, ok := s.activeLoginSession(r); ok {
		pageData.AuthStage = sess.Stage()
	}
	pageData.CryptoOn = d.Crypto.Enabled
	pageData.Codec = "none"
	if cfg, err := loadCfg(r); err == nil {
		pageData.SecretSource = cfg.Secret.EffectiveSource()
		pageData.SecretFile = cfg.Secret.File
		pageData.SecretEntry = cfg.Secret.Entry
	}
	if cfg, err := loadCfg(r); err == nil && fileExists(cfg.DBPath()) {
		if mstore, err := meta.Open(cfg.DBPath()); err == nil {
			if codec, err := mstore.CompressionCodec(r.Context()); err == nil {
				pageData.Codec = codec
			}
			mstore.Close()
		}
	}
	s.renderTemplate(w, "setup.html", pageData)
}

// defaultMountpoint suggests ~/telfs as the mount target — purely a
// pre-fill; the form accepts any absolute path to an existing dir.
func defaultMountpoint() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, "telfs")
}

// setupSkip toggles a step into the skipped set (currently only the
// optional encryption step). Redirects back to /setup so the wizard
// re-evaluates and surfaces the next step. The skip is persisted in a
// session cookie — the user can un-skip by clicking the step in the
// stepper.
func (s *Server) setupSkip(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	idx := r.FormValue("idx")
	skipped := readSkipCookie(r)
	if v, ok := skipped[atoiSafe(idx)]; ok && v {
		delete(skipped, atoiSafe(idx))
	} else {
		skipped[atoiSafe(idx)] = true
	}
	writeSkipCookie(w, profileOf(r), skipped)
	http.Redirect(w, r, "/setup", http.StatusSeeOther)
}

func readSkipCookie(r *http.Request) map[int]bool {
	out := map[int]bool{}
	c, err := r.Cookie(setupSkipCookieName(profileOf(r)))
	if err != nil || c.Value == "" {
		return out
	}
	for _, part := range strings.Split(c.Value, ",") {
		if part == "" {
			continue
		}
		out[atoiSafe(part)] = true
	}
	return out
}

func writeSkipCookie(w http.ResponseWriter, profile string, m map[int]bool) {
	parts := make([]string, 0, len(m))
	for k, v := range m {
		if v {
			parts = append(parts, fmt.Sprintf("%d", k))
		}
	}
	sort.Strings(parts)
	http.SetCookie(w, &http.Cookie{
		Name:     setupSkipCookieName(profile),
		Value:    strings.Join(parts, ","),
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
}

func atoiSafe(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
	}
	return n
}

func (s *Server) loadDashboard(ctx context.Context, w http.ResponseWriter, r *http.Request) dashboardData {
	d := dashboardData{Path: r.URL.Path,
		CSRF: s.state.EnsureCSRF(w, r),
	}
	kind, msg := ConsumeFlash(w, r)
	d.Flash = flashData{Kind: kind, Msg: msg}
	d.Active = profileOf(r)
	cfg, err := loadCfg(r)
	if err == nil {
		d.Profile = nameForDashboard(d.Active)
		d.Identity = cfg.Identity.Label()
		d.DataDir = cfg.DataDir
		if cfg.Channel.ID != 0 {
			d.Channel = channelView{Set: true, ID: cfg.Channel.ID, Title: cfg.Channel.Title}
		}
		// Same stat-first guard as the show page — opening meta
		// auto-bootstraps a db.sqlite, which we don't want on the
		// "no active profile / empty dashboard" path.
		if _, err := os.Stat(cfg.DBPath()); err == nil {
			if mstore, err := openMetaRead(cfg); err == nil {
				d.HasMeta = true
				if u, err := mstore.FSUUID(ctx); err == nil {
					d.FSUUID = u
				}
				if cs, err := mstore.ChunkSize(ctx); err == nil {
					d.ChunkSize = cs
				}
				if mode, err := mstore.GetKV(ctx, crypto.KVMode); err == nil {
					d.Crypto.Enabled = true
					d.Crypto.Mode = string(mode)
					if a, err := mstore.GetKV(ctx, crypto.KVArgon); err == nil {
						d.Crypto.Argon = string(a)
					}
				}
				if id, err := mstore.GetKV(ctx, snapshot.KVCurrentMsgID); err == nil {
					d.LastSnap = strings.TrimSpace(string(id))
				}
				mstore.Close()
			}
		}
	}
	d.Mounts = activeMounts()
	d.Profiles = listProfileNames()
	d.Setup = buildSetupChecklist(d, cfg)
	return d
}

// buildSetupChecklist looks at the loaded dashboard data and produces
// an ordered list of "do this next" hints for new users. Order matches
// the conceptual prerequisites: API creds → login → channel → optional
// encryption → mount. The first not-done step gets a Why message; the
// dashboard template renders that prominently.
func buildSetupChecklist(d dashboardData, cfg *config.Config) setupChecklist {
	apiOK := cfg != nil && cfg.APIID != 0 && cfg.APIHash != ""
	// Authenticated means the LOGIN COMPLETED — [identity] is written
	// only after a successful auth. session.json alone is NOT enough:
	// gotd creates it during the MTProto handshake, i.e. the moment
	// the login code is SENT, which made this step flip to done while
	// the user was still waiting to type their code (the wizard then
	// skipped the code form entirely).
	sessionOK := cfg != nil && fileExists(cfg.SessionPath()) && cfg.Identity != nil
	channelOK := d.Channel.Set
	// The secret step is done once a source is written to config.toml
	// or a session exists (the first login set the secret).
	secretOK := cfg != nil && (cfg.Secret.Source != "" || cfg.HasSession())
	// The filesystem step is done once a local DB exists — made by
	// `init` (a new filesystem) or by a mount's recovery (an existing
	// one). Skipping it means "this channel already holds a filesystem".
	fsOK := cfg != nil && fileExists(cfg.DBPath())
	// "Mount it" is done when THIS profile has written chunks — a
	// reliable signal that the FS has been mounted at some point.
	// (Checking active /proc/mounts entries is unreliable because
	// fuse.telfs entries don't reveal which profile they belong to,
	// and another profile's mount would falsely mark this one done.)
	mountedOK := hasChunksFor(cfg)

	apiHref := "/profiles"
	if d.Active != "" {
		// Deep-link the active profile's page — it has the actual
		// api_id/api_hash form, not just the list.
		apiHref = "/profiles/" + d.Active
	}
	steps := []setupStep{
		{
			Key:     "profile",
			Label:   "Create a profile",
			Done:    d.Active != "",
			Href:    "/profiles",
			Why:     "A profile holds one Telegram identity plus its channel binding and local state.",
			Detail:  "Everything TelFS knows lives under a named profile in ~/.config/telfs/profiles/<name>/ — credentials, session, metadata DB, chunk cache. Create one and it becomes the active profile every page operates on.",
			Outcome: "The masthead shows the new profile name as active.",
			CTA:     "Open profiles",
		},
		{
			Key:     "api",
			Label:   "Telegram API ID + hash",
			Done:    apiOK,
			Href:    apiHref,
			Why:     "Get them once at https://my.telegram.org/apps — one pair works for every profile.",
			Detail:  "TelFS uses the MTProto API directly. That requires a personal API ID + hash, free to obtain at my.telegram.org/apps after logging in with your phone number. The pair identifies the application, not the account, and is written to your profile's config.toml at chmod 0600.",
			Outcome: "config.toml in the active profile has non-zero api_id and a non-empty api_hash.",
			CTA:     "Open profile settings",
		},
		{
			Key:     "secret",
			Label:   "Choose where the secret comes from",
			Done:    secretOK,
			Href:    apiHref,
			Why:     "One secret per profile encrypts the Telegram session and the filesystem key.",
			Detail:  "TelFS never stores the secret itself — only WHERE it comes from. \"prompt\": you type it (fine for occasional use). \"file\": a file only you (and the service's group) can read — what boot mounts use; on S9 svc-secret writes it from KeePassXC. \"keepassxc\": TelFS asks the running KeePassXC for the entry over the Secret Service, as svc-secret does. A KeePassXC-generated password of 32+ characters is the safe choice: the filesystem's key header is readable on the channel, so a short secret can be guessed offline.",
			Outcome: "config.toml's [secret] names the source; the next steps use it.",
			CTA:     "Open profile settings",
		},
		{
			Key:     "auth",
			Label:   "Authenticate (phone)",
			Done:    sessionOK,
			Href:    "/login/phone",
			Why:     "Phone → code → 2FA password (if set). The first login locks the profile to your account.",
			Detail:  "Telegram sends a 5-digit code via SMS or the app's notifications; accounts with two-step verification also enter their password. The resulting session locks the profile to your account.",
			Outcome: "session.enc (encrypted with the profile's secret) appears in the profile directory; subsequent mounts won't prompt for a code.",
			CTA:     "Open login page",
		},
		{
			Key:     "channel",
			Label:   "Bind a private channel",
			Done:    channelOK,
			Href:    "/channel",
			Why:     "Pick the channel TelFS will store chunks in. Creating a private channel first in the Telegram app is the smoothest path.",
			Detail:  "The list below enumerates your channels from your dialogs; picking one resolves its access_hash automatically. Chunks and snapshots for this filesystem will live as messages in that channel.",
			Outcome: "config.toml's [channel] block has id + access_hash set; the dashboard's Channel card lights up.",
			CTA:     "Open channel page",
		},
		{
			Key:      "init",
			Label:    "Create the filesystem",
			Done:     fsOK,
			Href:     "/setup",
			Why:      "A new filesystem on this channel — always encrypted. Skip this if the channel already holds one: the mount recovers it.",
			Detail:   "Creates the filesystem's random data key, wraps it under the profile's secret, posts the key header as a pinned message in the channel, and takes the first snapshot. Everything TelFS posts from now on is encrypted: chunks, captions, snapshots and the journal; only message sizes and times stay visible. The chunk size and compression are fixed from here on. Zstd shrinks text, code and documents before encryption; media and archives gain nothing.",
			Outcome:  "The key message is pinned in the channel; `telfs key export` prints the export to keep with the secret.",
			Optional: true,
			CTA:      "Stay on the wizard",
		},
		{
			Key:     "mount",
			Label:   "Mount it",
			Done:    mountedOK,
			Href:    "/mount/new",
			Why:     "Start the FUSE daemon. Files written to the mountpoint flow through the chunk pipeline to the channel.",
			Detail:  "Pick a mountpoint (any empty directory). The web server launches a `telfs mount` child process and tracks it for you — start, stop, and tail the log from the Mounts page. The mount survives `telfs web` restarts via /proc/mounts reconciliation.",
			Outcome: "An `fuse.telfs` entry shows up in `mount`; the Files page browses live FS contents.",
			CTA:     "Open mount form",
		},
	}
	complete := true
	for _, s := range steps {
		if !s.Done {
			complete = false
			break
		}
	}
	return setupChecklist{Complete: complete, Steps: steps}
}

func fileExists(path string) bool {
	if path == "" {
		return false
	}
	_, err := os.Stat(path)
	return err == nil
}

// compressionConfigured reports whether the profile's DB has the
// compression_codec kv set to a non-default value — i.e. the user has
// made an explicit choice. "none" written explicitly still counts as
// configured (matches the dashboard checklist semantics: any explicit
// codec decision marks the step done).
func compressionConfigured(cfg *config.Config) bool {
	if cfg == nil || !fileExists(cfg.DBPath()) {
		return false
	}
	m, err := meta.Open(cfg.DBPath())
	if err != nil {
		return false
	}
	defer m.Close()
	_, err = m.GetKV(context.Background(), meta.KVCompressionCodec)
	return err == nil
}

// hasChunksFor reports whether the given profile's local DB contains
// at least one chunk_map row. Used by the setup checklist as a proxy
// for "this profile has been mounted and written to" — more reliable
// than /proc/mounts inspection, which can't tell profiles apart.
func hasChunksFor(cfg *config.Config) bool {
	if cfg == nil {
		return false
	}
	if !fileExists(cfg.DBPath()) {
		return false
	}
	m, err := meta.Open(cfg.DBPath())
	if err != nil {
		return false
	}
	defer m.Close()
	has, err := m.HasAnyChunks(context.Background())
	return err == nil && has
}

// activeMounts scans /proc/mounts for fuse.telfs entries.
func activeMounts() []string {
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

func listProfileNames() []string {
	root, err := config.ProfilesRoot()
	if err != nil {
		return nil
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() && config.ValidateProfileName(e.Name()) == nil {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names
}

func nameForDashboard(active string) string {
	if active == "" {
		return "(legacy path / no active profile)"
	}
	return active
}

// dirStats sums file sizes under dir (recursively).
func dirStats(dir string) (int64, int, error) {
	var total int64
	var n int
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		n++
		return nil
	})
	return total, n, err
}

// humanBytesTpl is the template helper. Renders with one decimal so
// "1.99 GiB" doesn't round-truncate to "1 GiB" (a previous version
// used integer right-shift which dropped up to a whole unit of
// precision). Strips a trailing ".0" so whole numbers stay clean.
func humanBytesTpl(n int64) string {
	render := func(unit string, div float64) string {
		v := float64(n) / div
		s := fmt.Sprintf("%.1f %s", v, unit)
		// Strip ".0" for tidy whole-number cases.
		if idx := len(s) - len(unit) - 1; idx > 2 && s[idx-2] == '.' && s[idx-1] == '0' {
			s = s[:idx-2] + s[idx:]
		}
		return s
	}
	switch {
	case n >= 1<<40:
		return render("TiB", float64(int64(1)<<40))
	case n >= 1<<30:
		return render("GiB", float64(int64(1)<<30))
	case n >= 1<<20:
		return render("MiB", float64(int64(1)<<20))
	case n >= 1<<10:
		return render("KiB", float64(int64(1)<<10))
	}
	return fmt.Sprintf("%d B", n)
}

// ── profile handlers ──────────────────────────────────────────────

type profilesListData struct {
	CSRF     string
	Flash    flashData
	Path     string
	Active   string // the profile this browser has selected
	Default  string // the CLI's default (~/.config/telfs/active)
	Profiles []string
}

func (s *Server) profilesList(w http.ResponseWriter, r *http.Request) {
	kind, msg := ConsumeFlash(w, r)
	d := profilesListData{
		CSRF:     s.state.EnsureCSRF(w, r),
		Flash:    flashData{kind, msg},
		Path:     r.URL.Path,
		Active:   profileOf(r),
		Default:  config.ActiveProfile(),
		Profiles: listProfileNames(),
	}
	s.renderTemplate(w, "profile/list.html", d)
}

// pageBase is what every full-page template needs from the layout.
// Embedded in per-page data structs so we don't repeat ourselves.
//
// Path is the request's URL.Path used by the layout to highlight
// the active nav tab. Using path rather than a per-page constant
// keeps handlers from having to remember to set the tab name.
type pageBase struct {
	CSRF  string
	Flash flashData
	Path  string
	// SecretPrompt: the active profile types its secret, so every form
	// that talks to Telegram carries a secret field (derive, act, drop).
	SecretPrompt bool
	// NewSecret: the profile has no session yet — its first login SETS
	// the secret (field asked twice).
	NewSecret bool
}

func (s *Server) basePage(w http.ResponseWriter, r *http.Request) pageBase {
	kind, msg := ConsumeFlash(w, r)
	pb := pageBase{
		CSRF:  s.state.EnsureCSRF(w, r),
		Flash: flashData{Kind: kind, Msg: msg},
		Path:  r.URL.Path,
	}
	if cfg, err := loadCfg(r); err == nil {
		pb.SecretPrompt = secretIsPrompt(cfg)
		pb.NewSecret = !cfg.HasSession()
	}
	return pb
}

type profilesNewData struct {
	pageBase
}

func (s *Server) profilesNewForm(w http.ResponseWriter, r *http.Request) {
	s.renderTemplate(w, "profile/new.html", profilesNewData{pageBase: s.basePage(w, r)})
}

func (s *Server) profilesCreate(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	errBack := setupReturn(r, "/profiles/new")
	name := strings.TrimSpace(r.FormValue("name"))
	if err := config.ValidateProfileName(name); err != nil {
		SetFlash(w, "error", "create: "+err.Error())
		http.Redirect(w, r, errBack, http.StatusSeeOther)
		return
	}
	dir, err := config.ProfileDir(name)
	if err != nil {
		SetFlash(w, "error", "create: "+err.Error())
		http.Redirect(w, r, errBack, http.StatusSeeOther)
		return
	}
	if _, err := os.Stat(dir); err == nil {
		SetFlash(w, "error", fmt.Sprintf("profile %q already exists", name))
		http.Redirect(w, r, errBack, http.StatusSeeOther)
		return
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		SetFlash(w, "error", "create: "+err.Error())
		http.Redirect(w, r, errBack, http.StatusSeeOther)
		return
	}
	// A new profile is the one to configure next: this browser selects it
	// (the wizard's remaining steps act on the selection). It becomes the
	// CLI's default too when there is none yet, so `telfs` commands work.
	setSelected(w, name)
	if config.ActiveProfile() == "" {
		_ = config.SetActiveProfile(name)
	}
	if r.FormValue("from") == "setup" {
		SetFlash(w, "ok", fmt.Sprintf("created profile %q — selected", name))
		http.Redirect(w, r, "/setup", http.StatusSeeOther)
		return
	}
	SetFlash(w, "ok", fmt.Sprintf("created profile %q — selected", name))
	http.Redirect(w, r, "/profiles/"+name, http.StatusSeeOther)
}

type profileShowData struct {
	CSRF       string
	Flash      flashData
	Path       string
	Name       string
	Active     string // the profile this browser has selected
	Default    string // the CLI's default (~/.config/telfs/active)
	Dir        string
	APIID      int
	HasAPI     bool
	Channel    channelView
	Identity   string
	HasAPIHash bool
	HasMeta    bool
	Crypto     cryptoView
	ChunkSize  int64
	Format     int
	// Secret source ([secret]) and session state.
	SecretSource, SecretFile, SecretEntry string
	HasSession, LegacySession             bool
}

func (s *Server) profilesShow(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := config.ValidateProfileName(name); err != nil {
		http.Error(w, "bad profile name", http.StatusBadRequest)
		return
	}
	d := profileShowData{
		CSRF:    s.state.EnsureCSRF(w, r),
		Path:    r.URL.Path,
		Name:    name,
		Active:  profileOf(r),
		Default: config.ActiveProfile(),
	}
	kind, msg := ConsumeFlash(w, r)
	d.Flash = flashData{kind, msg}
	// Read this profile's config WITHOUT changing the global active.
	dir, _ := config.ProfileDir(name)
	d.Dir = dir
	if cfg, ok := readProfileConfig(dir); ok {
		d.APIID = cfg.APIID
		d.HasAPI = cfg.APIID != 0 && cfg.APIHash != ""
		d.HasAPIHash = cfg.APIHash != ""
		if cfg.Channel.ID != 0 {
			d.Channel = channelView{Set: true, ID: cfg.Channel.ID, Title: cfg.Channel.Title}
		}
		d.Identity = cfg.Identity.Label()
		d.SecretSource = cfg.Secret.EffectiveSource()
		d.SecretFile, d.SecretEntry = cfg.Secret.File, cfg.Secret.Entry
		d.HasSession, d.LegacySession = cfg.HasSession(), cfg.HasLegacySession()
		// Stat-before-open so just viewing this profile doesn't
		// auto-bootstrap a db.sqlite (meta.Open creates one when
		// missing). Only inspect the meta when it already exists.
		dbPath := filepath.Join(dir, config.DBFile)
		if _, err := os.Stat(dbPath); err == nil {
			if mstore, err := meta.Open(dbPath); err == nil {
				d.HasMeta = true
				ctx := r.Context()
				if mode, err := mstore.GetKV(ctx, crypto.KVMode); err == nil {
					d.Crypto = cryptoView{Enabled: true, Mode: string(mode)}
				}
				if cs, err := mstore.ChunkSize(ctx); err == nil {
					d.ChunkSize = cs
				}
				d.Format, _ = mstore.Format(ctx)
				mstore.Close()
			}
		}
	}
	s.renderTemplate(w, "profile/show.html", d)
}

// readProfileConfig loads a profile's config.toml directly (bypassing
// the global resolution order so we can show OTHER profiles than the
// active one).
func readProfileConfig(dir string) (*config.Config, bool) {
	cfg, err := config.LoadFromDir(dir)
	if err != nil {
		return nil, false
	}
	return cfg, true
}

// profilesSetAPI saves api_id + api_hash into the named profile's
// config.toml — the web-side equivalent of hand-editing it. App-level
// identity from my.telegram.org, required by MTProto.
func (s *Server) profilesSetAPI(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	name := r.PathValue("name")
	if err := config.ValidateProfileName(name); err != nil {
		http.Error(w, "bad profile name", http.StatusBadRequest)
		return
	}
	dir, err := config.ProfileDir(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		http.Error(w, "no such profile", http.StatusNotFound)
		return
	}
	back := setupReturn(r, "/profiles/"+name)
	cfg, err := config.LoadFromDir(dir)
	if err != nil {
		SetFlash(w, "error", "load config: "+err.Error())
		http.Redirect(w, r, back, http.StatusSeeOther)
		return
	}
	id, err := strconv.Atoi(strings.TrimSpace(r.FormValue("api_id")))
	if err != nil || id <= 0 {
		SetFlash(w, "error", "api_id must be a positive integer (from https://my.telegram.org/apps)")
		http.Redirect(w, r, back, http.StatusSeeOther)
		return
	}
	// Blank hash keeps the stored one — the form doesn't echo secrets
	// back, so "update only the id" must not wipe the hash.
	hash := strings.TrimSpace(r.FormValue("api_hash"))
	if hash == "" && cfg.APIHash == "" {
		SetFlash(w, "error", "api_hash is required (from https://my.telegram.org/apps)")
		http.Redirect(w, r, back, http.StatusSeeOther)
		return
	}
	cfg.APIID = id
	if hash != "" {
		cfg.APIHash = hash
	}
	if err := cfg.Save(); err != nil {
		SetFlash(w, "error", "save: "+err.Error())
		http.Redirect(w, r, back, http.StatusSeeOther)
		return
	}
	SetFlash(w, "ok", "API credentials saved")
	http.Redirect(w, r, back, http.StatusSeeOther)
}

func (s *Server) profilesUse(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	name := r.PathValue("name")
	if !profileExists(name) {
		SetFlash(w, "error", fmt.Sprintf("no profile %q", name))
		http.Redirect(w, r, "/profiles", http.StatusSeeOther)
		return
	}
	// The browser's choice only (v1.1 W1): the CLI's default and other
	// browsers are untouched.
	setSelected(w, name)
	SetFlash(w, "ok", fmt.Sprintf("selected profile %s — the Setup, Browse and State pages act on it", name))
	http.Redirect(w, r, localBack(r, "/"), http.StatusSeeOther)
}

// profilesDefault makes a profile the CLI's default (~/.config/telfs/active,
// what `telfs profile use` writes) — separate from the console's selection.
func (s *Server) profilesDefault(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	name := r.PathValue("name")
	if err := config.SetActiveProfile(name); err != nil {
		SetFlash(w, "error", err.Error())
	} else {
		SetFlash(w, "ok", fmt.Sprintf("%s is now the CLI's default profile (telfs commands without TELFS_PROFILE use it)", name))
	}
	http.Redirect(w, r, "/profiles/"+name, http.StatusSeeOther)
}

// localBack is the form's "back" field when it is a local path, else def
// (never an open redirect).
func localBack(r *http.Request, def string) string {
	b := r.FormValue("back")
	if strings.HasPrefix(b, "/") && !strings.HasPrefix(b, "//") && !strings.Contains(b, "\\") {
		return b
	}
	return def
}

func (s *Server) profilesDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	name := r.PathValue("name")
	dir, err := config.ProfileDir(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		http.Error(w, "no such profile", http.StatusNotFound)
		return
	}
	// Refuse while a daemon for this profile is mounted: RemoveAll
	// under a live daemon leaves it writing to unlinked files, and
	// the next mount recovers from the channel losing everything
	// after the last snapshot.
	for _, mp := range s.sup.List() {
		if mp.Profile == name && mp.Status() == "running" {
			SetFlash(w, "error", fmt.Sprintf("profile %q has a running mount — stop it first", name))
			http.Redirect(w, r, "/profiles", http.StatusSeeOther)
			return
		}
	}
	// Capture BEFORE RemoveAll: once the dir is gone, the active-file
	// reader treats the dangling name as unset.
	wasActive := config.ActiveProfile() == name
	if err := os.RemoveAll(dir); err != nil {
		SetFlash(w, "error", "delete: "+err.Error())
	} else if wasActive {
		// Don't leave the active selection pointing at the void.
		if err := config.ClearActiveProfile(); err != nil {
			SetFlash(w, "error", fmt.Sprintf("deleted profile %q but could not clear the active selection: %v", name, err))
		} else {
			SetFlash(w, "ok", fmt.Sprintf("deleted profile %q — it was the active profile, so none is selected now", name))
		}
	} else {
		SetFlash(w, "ok", fmt.Sprintf("deleted profile %q", name))
	}
	http.Redirect(w, r, "/profiles", http.StatusSeeOther)
}

var bundleFiles = []string{config.ConfigFile, config.SessionEncFile, config.DBFile} // never a plaintext session.json

func (s *Server) profilesExport(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	name := r.PathValue("name")
	dir, err := config.ProfileDir(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Verify everything we need is on disk before sending headers.
	for _, f := range bundleFiles {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			http.Error(w, fmt.Sprintf("export: %s missing — initialize the profile first", f), http.StatusBadRequest)
			return
		}
	}
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="telfs-%s.tar.gz"`, name))
	gz := gzip.NewWriter(w)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()
	for _, f := range bundleFiles {
		path := filepath.Join(dir, f)
		info, err := os.Stat(path)
		if err != nil {
			return
		}
		hdr := &tar.Header{Name: f, Mode: 0o600, Size: info.Size(), ModTime: info.ModTime()}
		if err := tw.WriteHeader(hdr); err != nil {
			return
		}
		src, err := os.Open(path)
		if err != nil {
			return
		}
		_, _ = io.Copy(tw, src)
		src.Close()
	}
}

type profilesImportData struct {
	pageBase
}

func (s *Server) profilesImportForm(w http.ResponseWriter, r *http.Request) {
	s.renderTemplate(w, "profile/import.html", profilesImportData{pageBase: s.basePage(w, r)})
}

func (s *Server) profilesImport(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	if err := r.ParseMultipartForm(64 << 20); err != nil {
		http.Error(w, "import: "+err.Error(), http.StatusBadRequest)
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	if err := config.ValidateProfileName(name); err != nil {
		SetFlash(w, "error", "import: "+err.Error())
		http.Redirect(w, r, "/profiles/import", http.StatusSeeOther)
		return
	}
	file, _, err := r.FormFile("bundle")
	if err != nil {
		SetFlash(w, "error", "import: bundle file missing")
		http.Redirect(w, r, "/profiles/import", http.StatusSeeOther)
		return
	}
	defer file.Close()
	dir, _ := config.ProfileDir(name)
	if _, err := os.Stat(dir); err == nil {
		SetFlash(w, "error", fmt.Sprintf("profile %q already exists; delete it first", name))
		http.Redirect(w, r, "/profiles/import", http.StatusSeeOther)
		return
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		SetFlash(w, "error", "import: "+err.Error())
		http.Redirect(w, r, "/profiles/import", http.StatusSeeOther)
		return
	}
	gz, err := gzip.NewReader(file)
	if err != nil {
		SetFlash(w, "error", "import: not a gzip stream")
		os.RemoveAll(dir)
		http.Redirect(w, r, "/profiles/import", http.StatusSeeOther)
		return
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	got := map[string]bool{}
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			SetFlash(w, "error", "import: "+err.Error())
			os.RemoveAll(dir)
			http.Redirect(w, r, "/profiles/import", http.StatusSeeOther)
			return
		}
		clean := strings.TrimPrefix(hdr.Name, "./")
		if !slicesContains(bundleFiles, clean) {
			continue
		}
		out, err := os.OpenFile(filepath.Join(dir, clean), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			SetFlash(w, "error", "import: "+err.Error())
			os.RemoveAll(dir)
			http.Redirect(w, r, "/profiles/import", http.StatusSeeOther)
			return
		}
		if _, err := io.Copy(out, tr); err != nil {
			out.Close()
			SetFlash(w, "error", "import: "+err.Error())
			os.RemoveAll(dir)
			http.Redirect(w, r, "/profiles/import", http.StatusSeeOther)
			return
		}
		out.Close()
		got[clean] = true
	}
	if len(got) != len(bundleFiles) {
		os.RemoveAll(dir)
		SetFlash(w, "error", "import: bundle is incomplete")
		http.Redirect(w, r, "/profiles/import", http.StatusSeeOther)
		return
	}
	SetFlash(w, "ok", fmt.Sprintf("imported into profile %q", name))
	http.Redirect(w, r, "/profiles/"+name, http.StatusSeeOther)
}

func slicesContains[T comparable](haystack []T, needle T) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}
