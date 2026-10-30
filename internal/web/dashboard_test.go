package web

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"telfs/internal/config"
	"telfs/internal/meta"
)

// isolate points the profile root at a temp dir: no test reads the real
// ~/.config/telfs (or a service's profiles) and no TELFS_PROFILE leaks in.
func isolate(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", root)
	t.Setenv("TELFS_PROFILE", "")
	return root
}

// mkProfile makes a profile; withFS gives it a format-2 local DB.
func mkProfile(t *testing.T, name, channel string, withFS bool) {
	t.Helper()
	dir, err := config.ProfileDir(name)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadFromDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if channel != "" {
		cfg.Channel = config.ChannelConfig{ID: 1000 + int64(len(name)), Title: channel}
	}
	if err := cfg.Save(); err != nil {
		t.Fatal(err)
	}
	if withFS {
		m, err := meta.Open(cfg.DBPath())
		if err != nil {
			t.Fatal(err)
		}
		m.PutKV(context.Background(), meta.KVFormat, []byte("2"))
		m.Close()
	}
}

func dashboardFixture(t *testing.T) dashboardData {
	t.Helper()
	isolate(t)
	mkProfile(t, "backup", "TelFS-backup", true)
	mkProfile(t, "cloud", "TelFS-cloud", true)
	mkProfile(t, "spare", "", false)
	if err := config.SetActiveProfile("cloud"); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	return dashboardData{CSRF: "tok", Profile: "cloud", Active: "cloud",
		Monitors: monitorProfiles(ctx, monFull, "cloud"), MountList: []mountView{
			{Point: "/srv/mnt/backup", Profile: "backup"}, {Point: "/mnt/offsite", Profile: "backup", ReadOnly: true}}}
}

// Every profile is monitored on its own, whichever is selected; without a
// daemon a profile with a filesystem is "link down", one without is "no fs".
func TestMonitorProfilesEveryProfile(t *testing.T) {
	d := dashboardFixture(t)
	if len(d.Monitors) != 3 {
		t.Fatalf("monitors %+v", d.Monitors)
	}
	by := map[string]profileMonitor{}
	for _, p := range d.Monitors {
		by[p.Name] = p
	}
	if !by["cloud"].Selected || by["backup"].Selected || by["spare"].Selected {
		t.Error("only the selected profile is marked")
	}
	for _, n := range []string{"backup", "cloud"} {
		p := by[n]
		if !p.HasDB || p.FS.Format != 2 || p.FS.Encryption != "AES-256-GCM" || p.Online || p.LinkLabel != "link down" || !p.Channel.Set {
			t.Errorf("%s: %+v", n, p)
		}
	}
	if s := by["spare"]; s.HasDB || s.LinkLabel != "no fs" {
		t.Errorf("spare: %+v", s)
	}
	// The masthead shows the profiles with a filesystem, not the bare one.
	lamps := lampProfiles(d.Monitors)
	if len(lamps) != 2 || lamps[0].Name != "backup" || lamps[1].Name != "cloud" {
		t.Errorf("lamp profiles %+v", lamps)
	}
}

// The monitoring works without any active profile (the service mounts set
// none): every profile still shows, none marked.
func TestMonitorWithoutActiveProfile(t *testing.T) {
	isolate(t)
	mkProfile(t, "backup", "b", true)
	mkProfile(t, "cloud", "c", true)
	ms := monitorProfiles(context.Background(), monLive, "")
	if len(ms) != 2 || ms[0].Selected || ms[1].Selected {
		t.Fatalf("%+v", ms)
	}
	if out := renderLamps(lampProfiles(ms)); !strings.Contains(out, "backup") || !strings.Contains(out, "cloud") {
		t.Errorf("lamps: %s", out)
	}
}

func TestDashboardRendersEveryProfile(t *testing.T) {
	d := dashboardFixture(t)
	d.Setup = setupChecklist{Complete: true}
	out := render(t, "dashboard.html", d, "templates/partials/profiles.html")
	profiles := strings.Index(out, `id="profiles-panel"`)
	channels := strings.Index(out, "<h3>Channels</h3>")
	filesystems := strings.Index(out, "<h3>Filesystems</h3>")
	if profiles < 0 || channels < profiles || filesystems < channels {
		t.Fatal("order must be profiles, then channels, then filesystems")
	}
	for _, want := range []string{"TelFS-backup", "TelFS-cloud", `href="/telegram?profile=backup"`,
		`action="/profiles/backup/use"`, "/mnt/offsite", `class="selected"`} {
		if !strings.Contains(out, want) {
			t.Errorf("dashboard lacks %q", want)
		}
	}
	if strings.Contains(out, `action="/profiles/cloud/use"`) {
		t.Error("the selected profile gets no Select button")
	}
	if strings.Count(out, `class="selected"`) != 3 {
		t.Error("the selected profile's row is marked in each of the three tables")
	}
}

// The dashboard's setup checklist is for getting started: it shows while a
// step is open and is gone once TelFS is configured (the Setup menu keeps
// every step reachable).
func TestDashboardSetupChecklistOnlyUntilConfigured(t *testing.T) {
	isolate(t)
	steps := []setupStep{{Key: "profile", Label: "Create a profile", Done: true, Href: "/profiles"},
		{Key: "mount", Label: "Mount it", Href: "/mount/new"}}
	open := render(t, "dashboard.html", dashboardData{Setup: setupChecklist{Steps: steps}}, "templates/partials/profiles.html")
	if !strings.Contains(open, "Setup &amp; configuration") || !strings.Contains(open, `id="setup-checklist"`) {
		t.Error("an open step must show the setup checklist")
	}
	steps[1].Done = true
	done := render(t, "dashboard.html", dashboardData{Setup: setupChecklist{Complete: true, Steps: steps}}, "templates/partials/profiles.html")
	if strings.Contains(done, "Setup &amp; configuration") || strings.Contains(done, `id="setup-checklist"`) {
		t.Error("a configured TelFS must not show the setup checklist")
	}
	if !strings.Contains(done, `href="/setup"`) {
		t.Error("the Setup menu must still link the guided wizard")
	}
}

// The masthead carries one lamp set per profile, linking to its page.
func TestRenderLamps(t *testing.T) {
	out := renderLamps([]profileMonitor{
		{Name: "backup", LinkKind: "ok", LinkLabel: "link ok", SyncKind: "ok", SyncLabel: "synced"},
		{Name: "cloud", Selected: true, LinkKind: "err", LinkLabel: "link down", SyncKind: "warn", SyncLabel: "not synced"},
	})
	for _, want := range []string{`id="health-pill"`, `href="/telegram?profile=backup"`, `href="/telegram?profile=cloud"`,
		`class="lamp-set selected"`, "link down", "synced"} {
		if !strings.Contains(out, want) {
			t.Errorf("lamps lack %q: %s", want, out)
		}
	}
	if out := renderLamps(nil); !strings.Contains(out, "no profile") {
		t.Error("no profile at all must say so")
	}
}

// ?profile= picks the profile a page shows; a missing one is an error and
// is never created.
func TestRequestProfile(t *testing.T) {
	root := isolate(t)
	mkProfile(t, "backup", "b", true)
	mkProfile(t, "cloud", "c", true)
	config.SetActiveProfile("cloud")
	cfg, name, err := requestProfile(httptest.NewRequest("GET", "/telegram?profile=backup", nil))
	if err != nil || name != "backup" || filepath.Base(cfg.DataDir) != "backup" {
		t.Fatalf("explicit: %v %q", err, name)
	}
	if _, name, err := requestProfile(httptest.NewRequest("GET", "/telegram", nil)); err != nil || name != "cloud" {
		t.Fatalf("default = the selected profile: %v %q", err, name)
	}
	for _, bad := range []string{"ghost", "../x"} {
		if _, _, err := requestProfile(httptest.NewRequest("GET", "/telegram?profile="+bad, nil)); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "telfs", "profiles", "ghost")); err == nil {
		t.Error("a missing profile's directory was created")
	}
}

// The Telegram page shows one profile with tabs for the others; its poll and
// its rate-limit form carry that profile.
func TestTelegramPageProfileTabs(t *testing.T) {
	isolate(t)
	d := tgPageData{pageBase: pageBase{CSRF: "c"}, Profile: "backup", Profiles: []string{"backup", "cloud"}}
	out := render(t, "telegram.html", d, "templates/partials/telegram.html")
	for _, want := range []string{`class="tabs"`, `href="/telegram?profile=cloud"`, `hx-get="/partials/telegram?profile=backup"`,
		`name="profile" value="backup"`, "profile backup"} {
		if !strings.Contains(out, want) {
			t.Errorf("telegram page lacks %q", want)
		}
	}
}

// The console's selection is per browser (v1.1 W1): cookie, else the CLI's
// default, else the first profile; Select sets only the cookie, the CLI
// default is a separate action; a bad cookie falls through.
func TestProfileSelection(t *testing.T) {
	isolate(t)
	mkProfile(t, "backup", "b", true)
	mkProfile(t, "cloud", "c", true)
	req := func(cookie string) *http.Request {
		r := httptest.NewRequest("GET", "/", nil)
		if cookie != "" {
			r.AddCookie(&http.Cookie{Name: profileCookie, Value: cookie})
		}
		return r
	}
	if got := selectedProfile(req("")); got != "backup" {
		t.Fatalf("no cookie, no CLI default: %q, want the first profile", got)
	}
	config.SetActiveProfile("cloud")
	if got := selectedProfile(req("")); got != "cloud" {
		t.Fatalf("the CLI default: %q", got)
	}
	if got := selectedProfile(req("backup")); got != "backup" {
		t.Fatalf("the browser's choice wins: %q", got)
	}
	if got := selectedProfile(req("../etc")); got != "cloud" {
		t.Fatalf("a bad cookie falls through: %q", got)
	}
	if cfg, err := loadProfileCfg("ghost"); err == nil || cfg != nil {
		t.Fatal("a missing profile loads")
	}

	s := &Server{state: NewState()}
	post := func(target string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", target, strings.NewReader("csrf=0123456789abcdef0123456789abcdef&back=//evil.example"))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.AddCookie(&http.Cookie{Name: "telfs_csrf", Value: "0123456789abcdef0123456789abcdef"})
		r.SetPathValue("name", "backup")
		rec := httptest.NewRecorder()
		if strings.HasSuffix(target, "/use") {
			s.profilesUse(rec, r)
		} else {
			s.profilesDefault(rec, r)
		}
		return rec
	}
	rec := post("/profiles/backup/use")
	if c := rec.Result().Cookies(); len(c) == 0 || !slices.ContainsFunc(c, func(c *http.Cookie) bool { return c.Name == profileCookie && c.Value == "backup" }) {
		t.Fatalf("Select sets no cookie: %v (status %d)", c, rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "/" {
		t.Fatalf("redirect %q: an outside back= must not be followed", loc)
	}
	if config.ActiveProfile() != "cloud" {
		t.Fatal("Select changed the CLI default")
	}
	post("/profiles/backup/default")
	if config.ActiveProfile() != "backup" {
		t.Fatal("Make CLI default did not")
	}
}
