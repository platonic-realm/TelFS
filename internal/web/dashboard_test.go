package web

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"telfs/internal/config"
	"telfs/internal/control"
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
		Monitors: monitorProfiles(ctx, monFull), MountList: []mountView{
			{Point: "/srv/mnt/backup", Profile: "backup"}, {Point: "/mnt/offsite", Profile: "backup", ReadOnly: true}}}
}

// Every profile is monitored on its own; without a daemon a profile with a
// filesystem is "link down", one without is "no fs".
func TestMonitorProfilesEveryProfile(t *testing.T) {
	d := dashboardFixture(t)
	if len(d.Monitors) != 3 {
		t.Fatalf("monitors %+v", d.Monitors)
	}
	by := map[string]profileMonitor{}
	for _, p := range d.Monitors {
		by[p.Name] = p
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

// The monitoring works without any CLI default (the service mounts set
// none): every profile shows.
func TestMonitorWithoutActiveProfile(t *testing.T) {
	isolate(t)
	mkProfile(t, "backup", "b", true)
	mkProfile(t, "cloud", "c", true)
	ms := monitorProfiles(context.Background(), monLive)
	if len(ms) != 2 {
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
		`href="#state-cloud"`, `id="state-cloud"`, `hx-get="/partials/state?profile=cloud"`, "/mnt/offsite",
		`href="/browse/dir?at=%2fsrv%2fmnt%2fbackup"`} {
		if !strings.Contains(out, want) {
			t.Errorf("dashboard lacks %q", want)
		}
	}
	for _, gone := range []string{`/use"`, `class="selected"`, ">selected<", ">Select<"} {
		if strings.Contains(out, gone) {
			t.Errorf("the dashboard selects nothing any more, yet has %q", gone)
		}
	}
}

// The dashboard's setup checklist is for getting started: it shows while a
// step is open and is gone once TelFS is configured (the Setup menu keeps
// every step reachable).
func TestDashboardSetupChecklistOnlyUntilConfigured(t *testing.T) {
	isolate(t)
	steps := []setupStep{{Key: "profile", Label: "Create a profile", Done: true, Href: "/profiles"},
		{Key: "mount", Label: "Mount it", Href: "/mount/new"}}
	open := render(t, "dashboard.html", dashboardData{Setups: []profileSetup{{Profile: "cloud", Setup: setupChecklist{Steps: steps}}}}, "templates/partials/profiles.html")
	if !strings.Contains(open, "Setup &amp; configuration") || !strings.Contains(open, `id="setup-checklist-cloud"`) || !strings.Contains(open, `href="/setup?profile=cloud"`) {
		t.Error("an open step must show that profile's setup checklist")
	}
	steps[1].Done = true
	done := render(t, "dashboard.html", dashboardData{}, "templates/partials/profiles.html")
	if strings.Contains(done, "Setup &amp; configuration") || strings.Contains(done, `id="setup-checklist"`) {
		t.Error("a configured TelFS must not show the setup checklist")
	}
	if !strings.Contains(done, `/setup`) {
		t.Error("the Setup menu must still link the guided wizard")
	}
}

// The masthead carries one lamp set per profile, linking to its page.
func TestRenderLamps(t *testing.T) {
	out := renderLamps([]profileMonitor{
		{Name: "backup", LinkKind: "ok", LinkLabel: "link ok", SyncKind: "ok", SyncLabel: "synced"},
		{Name: "cloud", LinkKind: "err", LinkLabel: "link down", SyncKind: "warn", SyncLabel: "not synced"},
	})
	for _, want := range []string{`id="health-pill"`, `href="/telegram?profile=backup"`, `href="/telegram?profile=cloud"`,
		`class="lamp-set"`, "link down", "synced"} {
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
		t.Fatalf("default = the CLI default: %v %q", err, name)
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

// A page about one profile says which in its address and keeps it: the
// default without ?profile= is the CLI default, else the first profile; the
// profile bar links the same page for every profile; redirects to profile
// pages get the profile appended; the Telegram poll and forms carry it.
func TestPageProfile(t *testing.T) {
	isolate(t)
	mkProfile(t, "backup", "b", true)
	mkProfile(t, "cloud", "c", true)
	get := func(target string) *http.Request { return httptest.NewRequest("GET", target, nil) }
	if got := profileOf(get("/channel")); got != "backup" {
		t.Fatalf("no ?profile=, no CLI default: %q, want the first profile", got)
	}
	config.SetActiveProfile("cloud")
	if got := profileOf(get("/channel")); got != "cloud" {
		t.Fatalf("the CLI default: %q", got)
	}
	if got := profileOf(get("/channel?profile=backup")); got != "backup" {
		t.Fatalf("explicit: %q", got)
	}
	if got := profileOf(get("/channel?profile=../etc")); got != "cloud" {
		t.Fatalf("a bad name falls back: %q", got)
	}
	if cfg, err := loadProfileCfg("ghost"); err == nil || cfg != nil {
		t.Fatal("a missing profile loads")
	}

	// Redirects to a profile page keep the request's profile.
	for _, c := range []struct{ target, loc, want string }{
		{"/channel/set?profile=backup", "/channel", "/channel?profile=backup"},
		{"/setup?profile=backup", "/setup?step=2", "/setup?step=2&profile=backup"},
		{"/channel?profile=backup", "/setup?profile=cloud", "/setup?profile=cloud"},
		{"/channel?profile=backup", "/profiles/backup", "/profiles/backup"},
		{"/channel?profile=backup", "//evil.example/x", "//evil.example/x"},
	} {
		rec := httptest.NewRecorder()
		sr := &statusRecorder{ResponseWriter: rec, req: get(c.target)}
		http.Redirect(sr, sr.req, c.loc, http.StatusSeeOther)
		if got := rec.Header().Get("Location"); got != c.want {
			t.Errorf("%s → %s: Location %q, want %q", c.target, c.loc, got, c.want)
		}
	}

	// The profile bar on a profile page; none elsewhere.
	tabs := profileBar("/channel", "backup")
	if len(tabs) != 2 || tabs[0].URL != "/channel?profile=backup" || !tabs[0].Current || tabs[1].URL != "/channel?profile=cloud" || tabs[1].Current {
		t.Fatalf("bar %+v", tabs)
	}
	if profileBar("/", "backup") != nil || profileBar("/mount", "backup") != nil {
		t.Fatal("the dashboard and the mounts page are about every profile")
	}

	// Rendered: the layout's bar, the page's profile in the poll and forms.
	rec := &statusRecorder{ResponseWriter: httptest.NewRecorder(), req: get("/telegram?profile=backup"), csrf: "0123456789abcdef0123456789abcdef"}
	files := []string{"templates/layout.html", "templates/telegram.html", "templates/partials/telegram.html"}
	tm := template.Must(template.New("").Funcs(funcsFor(rec)).ParseFS(templatesFS, files...))
	var b bytes.Buffer
	if err := tm.ExecuteTemplate(&b, "layout", tgPageData{pageBase: pageBase{CSRF: "c"}, Profile: "backup"}); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{`class="profile-bar"`, `href="/telegram?profile=cloud"`, `class="active" aria-current="page">backup`,
		`hx-get="/partials/telegram?profile=backup"`, `name="profile" value="backup"`, `/static/style.css?v=`} {
		if !strings.Contains(out, want) {
			t.Errorf("telegram page lacks %q", want)
		}
	}
	if strings.Contains(out, "profile-switch") || strings.Contains(out, "/use\"") {
		t.Error("no switcher, no Select")
	}

	// Make CLI default is the one profile action left.
	s := &Server{state: NewState()}
	r := httptest.NewRequest("POST", "/profiles/backup/default", strings.NewReader("csrf=0123456789abcdef0123456789abcdef"))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.AddCookie(&http.Cookie{Name: csrfCookie, Value: "0123456789abcdef0123456789abcdef"})
	r.SetPathValue("name", "backup")
	s.profilesDefault(httptest.NewRecorder(), r)
	if config.ActiveProfile() != "backup" {
		t.Fatal("Make CLI default did not")
	}
}

// UX pass helpers: help text renders `code`, rates read like config.toml,
// the staging meter turns yellow above 80 % and red while writes wait.
func TestUXHelpers(t *testing.T) {
	if got := string(mdCode("run `telfs mount` <now> `x")); got != "run <code>telfs mount</code> &lt;now&gt; `x" {
		t.Fatalf("md: %s", got)
	}
	for bps, want := range map[int64]string{3_870_000: "3.9 MB/s", 820_000: "820 kB/s", 12: "12 B/s"} {
		if got := rateText(bps); got != want {
			t.Errorf("rate %d: %q, want %q", bps, got, want)
		}
	}
	l := &liveStatus{}
	l.Staging.CapBytes, l.Staging.PendingBytes = 100, 50
	if l.StagingKind() != "" {
		t.Error("50 % is no warning")
	}
	l.Staging.PendingBytes = 90
	if l.StagingKind() != "warn" {
		t.Error("90 % warns")
	}
	l.Staging.Blocked = true
	if l.StagingKind() != "err" {
		t.Error("writes waiting is red")
	}
}

// The Telegram page's upload cap form talks to the profile's daemon.
func TestTelegramSetUploadLimit(t *testing.T) {
	isolate(t)
	mkProfile(t, "cloud", "c", true)
	cfg, _ := loadProfileCfg("cloud")
	var got string
	ln, err := net.Listen("unix", control.Path(cfg.DataDir))
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Method + " " + r.URL.RequestURI()
		fmt.Fprint(w, `{"rate_cap_bytes_per_s":3000000,"rate_cap_config_bytes_per_s":5000000}`)
	})}
	go srv.Serve(ln)
	defer srv.Close()
	s := &Server{state: NewState()}
	r := httptest.NewRequest("POST", "/telegram/upload-limit", strings.NewReader("csrf=0123456789abcdef0123456789abcdef&profile=cloud&rate=3MB/s"))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.AddCookie(&http.Cookie{Name: csrfCookie, Value: "0123456789abcdef0123456789abcdef"})
	rec := httptest.NewRecorder()
	s.telegramSetUploadLimit(rec, r)
	if got != "POST /upload/limit?rate=3MB%2Fs" || rec.Header().Get("Location") != "/telegram?profile=cloud" {
		t.Fatalf("daemon got %q, redirect %q", got, rec.Header().Get("Location"))
	}
	if !strings.Contains(rec.Header().Get("Set-Cookie"), "3.0+MB") {
		t.Errorf("flash %q", rec.Header().Get("Set-Cookie"))
	}
}

// The tabs: Dashboard, Telegram, Mounts, Setup — Browse lives in Mounts,
// State in the Dashboard (Arash, 2026-10-30); their old URLs redirect.
func TestNavigation(t *testing.T) {
	isolate(t)
	mkProfile(t, "cloud", "c", true)
	out := render(t, "telegram.html", tgPageData{Profile: "cloud"}, "templates/partials/telegram.html")
	nav := out[strings.Index(out, "<nav>"):strings.Index(out, "</nav>")]
	order := []string{">Dashboard<", ">Telegram<", ">Mounts<", ">Setup ▾<"}
	last := -1
	for _, tab := range order {
		i := strings.Index(nav, tab)
		if i < 0 || i < last {
			t.Fatalf("tab %s missing or out of order in %s", tab, nav)
		}
		last = i
	}
	for _, gone := range []string{">Browse<", ">State<"} {
		if strings.Contains(nav, gone) {
			t.Errorf("%s is no tab any more", gone)
		}
	}

	exe, _ := os.Executable()
	srv, err := New(Options{Listen: "127.0.0.1:0", SelfPath: exe, Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	h := srv.middleware(srv.mux)
	for target, want := range map[string]string{"/state?profile=cloud": "/#state-cloud", "/browse": "/mount"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", target, nil)
		req.Host = "127.0.0.1"
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != want {
			t.Errorf("%s → %d %q, want %q", target, rec.Code, rec.Header().Get("Location"), want)
		}
	}
	// The panel's content: one profile's state as a fragment.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/partials/state?profile=cloud", nil)
	req.Host = "127.0.0.1"
	h.ServeHTTP(rec, req)
	body := rec.Body.String()
	if rec.Code != 200 || strings.Contains(body, "<html") {
		t.Fatalf("state partial: %d %s", rec.Code, body)
	}
	for _, want := range []string{`class="figures"`, ">Files<", `class="checks"`, "on the channel", "no damage", "This machine"} {
		if !strings.Contains(body, want) {
			t.Errorf("state panel lacks %q", want)
		}
	}
}

// Figures read compactly: exact to 9,999, then K and M.
func TestCompactNum(t *testing.T) {
	for n, want := range map[int64]string{0: "0", 999: "999", 1284: "1,284", 12_900: "12.9K", 40_000: "40K", 1_103_308: "1.1M", 2_000_000: "2M"} {
		if got := compactNum(n); got != want {
			t.Errorf("%d: %q, want %q", n, got, want)
		}
	}
}
