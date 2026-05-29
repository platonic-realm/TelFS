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
	"strings"

	"telfs/internal/config"
	"telfs/internal/crypto"
	"telfs/internal/meta"
	"telfs/internal/snapshot"
)

// dashboardData mirrors the shape of `telfs status` — the same
// information, rendered as HTML.
type dashboardData struct {
	CSRF      string
	Flash     flashData
	Profile   string
	DataDir   string
	Channel   channelView
	HasMeta   bool
	FSUUID    string
	ChunkSize int64
	Crypto    cryptoView
	LastSnap  string
	Files     []fileStat
	CacheSize int64
	CacheN    int
	Mounts    []string
	Profiles  []string
	Active    string
	Setup     setupChecklist
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
	CTA      string // call-to-action label on the wizard page; default "Open this step"
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

type fileStat struct {
	Label string
	Size  int64
	Mtime string
}

type flashData struct{ Kind, Msg string }

func (s *Server) dashboardIndex(w http.ResponseWriter, r *http.Request) {
	d := s.loadDashboard(r.Context(), w, r)
	s.renderTemplate(w, "dashboard.html", d)
}

func (s *Server) statusPartial(w http.ResponseWriter, r *http.Request) {
	d := s.loadDashboard(r.Context(), w, r)
	s.renderPartial(w, "partials/status.html", d)
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
}

const setupSkipCookie = "telfs_setup_skip"

// setupWizard is the guided first-run flow. It computes the same
// setupChecklist the dashboard uses, but renders only the first
// not-done step in detail — with prerequisites, expected outcome, and
// a clear "Open this step" CTA pointing at the existing form route.
//
// Why a separate page rather than a multi-step form-of-forms: every
// step's existing handler (login, channel/set, encrypt/init,
// mount/start) already has its own form, redirect logic, and flash
// machinery. Folding them into one page would require rewiring all of
// them to honor a return URL and would duplicate template work.
// Instead, the wizard is a clear narrative landing page that points
// at each step's existing page in turn; on return, the wizard
// advances. This is a thin shell — the work that matters happens in
// the dedicated handlers.
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
	}
	s.renderTemplate(w, "setup.html", pageData)
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
	writeSkipCookie(w, skipped)
	http.Redirect(w, r, "/setup", http.StatusSeeOther)
}

func readSkipCookie(r *http.Request) map[int]bool {
	out := map[int]bool{}
	c, err := r.Cookie(setupSkipCookie)
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

func writeSkipCookie(w http.ResponseWriter, m map[int]bool) {
	parts := make([]string, 0, len(m))
	for k, v := range m {
		if v {
			parts = append(parts, fmt.Sprintf("%d", k))
		}
	}
	sort.Strings(parts)
	http.SetCookie(w, &http.Cookie{
		Name:     setupSkipCookie,
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
	d := dashboardData{
		CSRF: s.state.EnsureCSRF(w, r),
	}
	kind, msg := ConsumeFlash(w, r)
	d.Flash = flashData{Kind: kind, Msg: msg}
	d.Active = config.ActiveProfile()
	cfg, err := config.Load()
	if err == nil {
		d.Profile = nameForDashboard(d.Active)
		d.DataDir = cfg.DataDir
		if cfg.Channel.ID != 0 {
			d.Channel = channelView{Set: true, ID: cfg.Channel.ID, Title: cfg.Channel.Title}
		}
		// Per-file stats.
		for _, f := range []struct {
			Path  string
			Label string
		}{
			{cfg.ConfigPath(), "config.toml"},
			{cfg.SessionPath(), "session.json"},
			{cfg.DBPath(), "db.sqlite"},
		} {
			if info, err := os.Stat(f.Path); err == nil {
				d.Files = append(d.Files, fileStat{Label: f.Label, Size: info.Size(), Mtime: info.ModTime().Format("2006-01-02 15:04:05")})
			} else {
				d.Files = append(d.Files, fileStat{Label: f.Label})
			}
		}
		if total, n, err := dirStats(cfg.CachePath()); err == nil {
			d.CacheSize = total
			d.CacheN = n
		}
		// Same stat-first guard as the show page — opening meta
		// auto-bootstraps a db.sqlite, which we don't want on the
		// "no active profile / empty dashboard" path.
		if _, err := os.Stat(cfg.DBPath()); err == nil {
			if mstore, err := meta.Open(cfg.DBPath()); err == nil {
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
	d.Setup = buildSetupChecklist(d)
	return d
}

// buildSetupChecklist looks at the loaded dashboard data and produces
// an ordered list of "do this next" hints for new users. Order matches
// the conceptual prerequisites: API creds → login → channel → optional
// encryption → mount. The first not-done step gets a Why message; the
// dashboard template renders that prominently.
func buildSetupChecklist(d dashboardData) setupChecklist {
	cfg, _ := config.Load()
	apiOK := cfg != nil && cfg.APIID != 0 && cfg.APIHash != ""
	sessionOK := cfg != nil && fileExists(cfg.SessionPath())
	channelOK := d.Channel.Set
	// Encryption is OPTIONAL — mark as "done" if the FS is either
	// explicitly encrypted OR has chunks already (so opting in later
	// is no longer possible).
	encryptionOK := d.Crypto.Enabled || hasChunksFor(cfg)
	// "Mount it" is done when THIS profile has written chunks — a
	// reliable signal that the FS has been mounted at some point.
	// (Checking active /proc/mounts entries is unreliable because
	// fuse.telfs entries don't reveal which profile they belong to,
	// and another profile's mount would falsely mark this one done.)
	mountedOK := hasChunksFor(cfg)

	steps := []setupStep{
		{
			Label:   "Telegram API ID + hash",
			Done:    apiOK,
			Href:    "/profiles",
			Why:     "Get them at https://my.telegram.org/apps and put them in the active profile's config.toml.",
			Detail:  "TelFS uses the MTProto user/bot API directly (never the HTTP Bot API). That requires a personal API ID + hash, free to obtain at my.telegram.org/apps after logging in with your phone number. The credentials are written to your profile's config.toml at chmod 0600.",
			Outcome: "config.toml in the active profile has non-zero api_id and a non-empty api_hash.",
			CTA:     "Open profile settings",
		},
		{
			Label:   "Authenticate (phone or bot token)",
			Done:    sessionOK,
			Href:    "/login",
			Why:     "Pick phone for a personal account or bot-token for an automation account.",
			Detail:  "Phone auth runs the full Telegram code-then-2FA dance in three pages. Bot auth is a single token field (must be issued by @BotFather; the bot needs admin in the target channel before it can post).",
			Outcome: "session.json appears in the profile directory; subsequent mounts won't prompt for a code.",
			CTA:     "Start authentication",
		},
		{
			Label:   "Bind a private channel",
			Done:    channelOK,
			Href:    "/channel",
			Why:     "Pick the channel TelFS will store chunks in. Creating a private channel first in the Telegram app is the smoothest path.",
			Detail:  "User-authenticated profiles can enumerate your channels (list view). Bot-authenticated profiles must paste the channel id and access_hash explicitly — bots can't list dialogs.",
			Outcome: "config.toml's [channel] block has id + access_hash set; the dashboard's Channel card lights up.",
			CTA:     "Choose a channel",
		},
		{
			Label:    "Encrypt the FS",
			Done:     encryptionOK,
			Href:     "/encrypt",
			Why:      "AES-256-GCM with an Argon2id-derived key. Once any chunk exists, this becomes a one-way choice.",
			Detail:   "Optional but you can only enable it on a fresh filesystem (no chunks yet). Encrypts chunk bytes AND the snapshot blob's metadata. Pre-v0.14 used aes-gcm-v1 (passphrase encrypts everything); from v0.14 onward, aes-gcm-v2 wraps a per-FS DEK so the passphrase can be rotated later without re-uploading chunks. Skipping this step leaves the FS plaintext — Telegram operators and channel members can read your files.",
			Outcome:  "telfs encrypt status reports mode=aes-gcm-v2 and rotation is available.",
			Optional: true,
			CTA:      "Set a passphrase",
		},
		{
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
	chunks, err := m.AllChunkMessageIDs(context.Background())
	if err != nil {
		return false
	}
	return len(chunks) > 0
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

// humanBytesTpl is the template helper; mirrors cmd/telfs/humanBytes.
func humanBytesTpl(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%d GiB", n>>30)
	case n >= 1<<20:
		return fmt.Sprintf("%d MiB", n>>20)
	case n >= 1<<10:
		return fmt.Sprintf("%d KiB", n>>10)
	}
	return fmt.Sprintf("%d B", n)
}

// ── profile handlers ──────────────────────────────────────────────

type profilesListData struct {
	CSRF     string
	Flash    flashData
	Active   string
	Profiles []string
}

func (s *Server) profilesList(w http.ResponseWriter, r *http.Request) {
	kind, msg := ConsumeFlash(w, r)
	d := profilesListData{
		CSRF:     s.state.EnsureCSRF(w, r),
		Flash:    flashData{kind, msg},
		Active:   config.ActiveProfile(),
		Profiles: listProfileNames(),
	}
	s.renderTemplate(w, "profile/list.html", d)
}

// pageBase is what every full-page template needs from the layout.
// Embedded in per-page data structs so we don't repeat ourselves.
type pageBase struct {
	CSRF  string
	Flash flashData
}

func (s *Server) basePage(w http.ResponseWriter, r *http.Request) pageBase {
	kind, msg := ConsumeFlash(w, r)
	return pageBase{
		CSRF:  s.state.EnsureCSRF(w, r),
		Flash: flashData{Kind: kind, Msg: msg},
	}
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
	name := strings.TrimSpace(r.FormValue("name"))
	if err := config.ValidateProfileName(name); err != nil {
		SetFlash(w, "error", "create: "+err.Error())
		http.Redirect(w, r, "/profiles/new", http.StatusSeeOther)
		return
	}
	dir, err := config.ProfileDir(name)
	if err != nil {
		SetFlash(w, "error", "create: "+err.Error())
		http.Redirect(w, r, "/profiles/new", http.StatusSeeOther)
		return
	}
	if _, err := os.Stat(dir); err == nil {
		SetFlash(w, "error", fmt.Sprintf("profile %q already exists", name))
		http.Redirect(w, r, "/profiles/new", http.StatusSeeOther)
		return
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		SetFlash(w, "error", "create: "+err.Error())
		http.Redirect(w, r, "/profiles/new", http.StatusSeeOther)
		return
	}
	SetFlash(w, "ok", fmt.Sprintf("created profile %q", name))
	http.Redirect(w, r, "/profiles/"+name, http.StatusSeeOther)
}

type profileShowData struct {
	CSRF      string
	Flash     flashData
	Name      string
	Active    string
	Dir       string
	APIID     int
	HasAPI    bool
	Channel   channelView
	AuthMode  string
	HasBot    bool
	HasMeta   bool
	Crypto    cryptoView
	ChunkSize int64
}

func (s *Server) profilesShow(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := config.ValidateProfileName(name); err != nil {
		http.Error(w, "bad profile name", http.StatusBadRequest)
		return
	}
	d := profileShowData{
		CSRF:   s.state.EnsureCSRF(w, r),
		Name:   name,
		Active: config.ActiveProfile(),
	}
	kind, msg := ConsumeFlash(w, r)
	d.Flash = flashData{kind, msg}
	// Read this profile's config WITHOUT changing the global active.
	dir, _ := config.ProfileDir(name)
	d.Dir = dir
	if cfg, ok := readProfileConfig(dir); ok {
		d.APIID = cfg.APIID
		d.HasAPI = cfg.APIID != 0 && cfg.APIHash != ""
		if cfg.Channel.ID != 0 {
			d.Channel = channelView{Set: true, ID: cfg.Channel.ID, Title: cfg.Channel.Title}
		}
		d.AuthMode = string(cfg.EffectiveAuthMode())
		d.HasBot = cfg.BotToken != ""
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

func (s *Server) profilesUse(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	name := r.PathValue("name")
	if err := config.SetActiveProfile(name); err != nil {
		SetFlash(w, "error", err.Error())
		http.Redirect(w, r, "/profiles", http.StatusSeeOther)
		return
	}
	SetFlash(w, "ok", fmt.Sprintf("active profile set to %q", name))
	http.Redirect(w, r, "/", http.StatusSeeOther)
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
	if err := os.RemoveAll(dir); err != nil {
		SetFlash(w, "error", "delete: "+err.Error())
	} else {
		SetFlash(w, "ok", fmt.Sprintf("deleted profile %q", name))
	}
	http.Redirect(w, r, "/profiles", http.StatusSeeOther)
}

var bundleFiles = []string{config.ConfigFile, config.SessionFile, config.DBFile}

func (s *Server) profilesExport(w http.ResponseWriter, r *http.Request) {
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
