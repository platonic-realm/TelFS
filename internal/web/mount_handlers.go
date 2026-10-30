package web

import (
	"fmt"
	"html"
	"net/http"
	"strings"
	"time"

	"telfs/internal/config"
)

// ── /mount (list) ─────────────────────────────────────────────────

type mountListData struct {
	pageBase
	Mounts   []mountRow
	External []string // fuse.telfs entries with no findable daemon process
	Profiles []string
}

type mountRow struct {
	Profile    string
	Mountpoint string
	Status     string
	StatusKind string // ok | err | muted — drives the .pill class
	Started    string
	AgeSeconds int64
	Origin     string // "supervised" | "adopted (started outside this web)"
	Pid        int
	HasLog     bool     // true when LogPath is non-empty — controls whether the detail page link is useful
	AlsoAt     []string // bind views of this mount (v1.1 W2)
}

func (s *Server) mountList(w http.ResponseWriter, r *http.Request) {
	d := mountListData{pageBase: s.basePage(w, r)}
	alsoAt := AlsoVisibleAt()
	for _, mp := range s.sup.List() {
		st := mp.Status()
		origin := "supervised"
		if mp.Adopted {
			origin = "adopted (started outside this web)"
		}
		var started string
		if !mp.Started.IsZero() {
			started = mp.Started.Format(time.RFC3339)
		}
		var age int64
		if !mp.Started.IsZero() {
			age = int64(time.Since(mp.Started).Seconds())
		}
		d.Mounts = append(d.Mounts, mountRow{
			Profile:    mp.Profile,
			Mountpoint: mp.Mountpoint,
			Status:     st,
			StatusKind: statusPillKind(st),
			Started:    started,
			AgeSeconds: age,
			Origin:     origin,
			Pid:        mp.Pid,
			HasLog:     mp.LogPath != "",
			AlsoAt:     alsoAt[mp.Mountpoint],
		})
	}
	d.External = s.sup.ExternalMounts()
	d.Profiles = listProfileNames()
	s.renderTemplate(w, "mount/list.html", d)
}

func statusPillKind(s string) string {
	switch {
	case s == "running":
		return "ok"
	case strings.HasPrefix(s, "exited: ") && s != "exited":
		return "err"
	case s == "exited":
		return "muted"
	}
	return "muted"
}

// ── /mount/new (form) ─────────────────────────────────────────────

type mountNewData struct {
	pageBase
	Profiles       []string
	DefaultProfile string
}

func (s *Server) mountNewForm(w http.ResponseWriter, r *http.Request) {
	d := mountNewData{
		pageBase:       s.basePage(w, r),
		Profiles:       listProfileNames(),
		DefaultProfile: profileOf(r),
	}
	s.renderTemplate(w, "mount/new.html", d)
}

// ── POST /mount/start ─────────────────────────────────────────────

func (s *Server) mountStart(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	errBack := setupReturn(r, "/mount/new")
	profile := strings.TrimSpace(r.FormValue("profile"))
	mountpoint := strings.TrimSpace(r.FormValue("mountpoint"))
	if profile == "" {
		SetFlash(w, "error", "profile is required")
		http.Redirect(w, r, errBack, http.StatusSeeOther)
		return
	}
	if mountpoint == "" {
		SetFlash(w, "error", "mountpoint is required")
		http.Redirect(w, r, errBack, http.StatusSeeOther)
		return
	}
	// Only a prompt-source profile takes a typed secret from the form;
	// the others read their own source in the daemon.
	passphrase := ""
	if dir, derr := config.ProfileDir(profile); derr == nil {
		if pcfg, lerr := config.LoadFromDir(dir); lerr == nil && secretIsPrompt(pcfg) {
			passphrase = r.FormValue("passphrase")
			if passphrase == "" {
				SetFlash(w, "error", "this profile's secret is typed in — enter it in the form")
				http.Redirect(w, r, errBack, http.StatusSeeOther)
				return
			}
		}
	}
	opts := StartOpts{
		Profile:    profile,
		Mountpoint: mountpoint,
		Passphrase: passphrase,
		Readonly:   r.FormValue("readonly") == "on",
		AllowOther: r.FormValue("allow_other") == "on",
		Debug:      r.FormValue("debug") == "on",
	}
	if _, err := s.sup.Start(opts); err != nil {
		SetFlash(w, "error", "start: "+err.Error())
		http.Redirect(w, r, errBack, http.StatusSeeOther)
		return
	}
	SetFlash(w, "ok", fmt.Sprintf("starting mount for %s at %s — tail the log to verify", profile, mountpoint))
	http.Redirect(w, r, "/mount/"+profile, http.StatusSeeOther)
}

// ── POST /mount/unmount-external ──────────────────────────────────
//
// Targets a mount that wasn't started by this web server (no child
// process under our supervision). Validates the mountpoint is in
// /proc/mounts as fuse.telfs and runs `fusermount -u`, falling back
// to `-u -z` on EBUSY. Use the per-profile /mount/{profile}/stop
// path for supervised mounts so their child gets a clean SIGTERM.
func (s *Server) mountUnmountExternal(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	mountpoint := strings.TrimSpace(r.FormValue("mountpoint"))
	redirectTo := r.FormValue("return_to")
	if redirectTo == "" {
		redirectTo = "/mount"
	}
	if err := s.sup.UnmountExternal(mountpoint); err != nil {
		SetFlash(w, "error", "unmount: "+err.Error())
		http.Redirect(w, r, redirectTo, http.StatusSeeOther)
		return
	}
	SetFlash(w, "ok", "unmounted "+mountpoint)
	http.Redirect(w, r, redirectTo, http.StatusSeeOther)
}

// ── POST /mount/stop ──────────────────────────────────────────────
//
// Profile arrives as a form field (see router.go: synthetic
// "adopted:<mountpoint>" keys contain slashes and can't ride in a
// path segment).

func (s *Server) mountStop(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	profile := strings.TrimSpace(r.FormValue("profile"))
	if profile == "" {
		SetFlash(w, "error", "stop: profile is required")
		http.Redirect(w, r, "/mount", http.StatusSeeOther)
		return
	}
	if err := s.sup.Stop(profile); err != nil {
		SetFlash(w, "error", "stop: "+err.Error())
		http.Redirect(w, r, "/mount", http.StatusSeeOther)
		return
	}
	SetFlash(w, "ok", "stopped "+profile)
	http.Redirect(w, r, "/mount", http.StatusSeeOther)
}

// ── GET /mount/{profile} (detail page with HTMX log tail) ─────────

type mountDetailData struct {
	pageBase
	Profile    string
	Mountpoint string
	Status     string
	Started    string
	Origin     string
	Pid        int
	HasLog     bool
	Found      bool
}

func (s *Server) mountDetail(w http.ResponseWriter, r *http.Request) {
	profile := r.PathValue("profile")
	d := mountDetailData{pageBase: s.basePage(w, r), Profile: profile}
	if mp := s.sup.Get(profile); mp != nil {
		d.Found = true
		d.Mountpoint = mp.Mountpoint
		d.Status = mp.Status()
		if !mp.Started.IsZero() {
			d.Started = mp.Started.Format(time.RFC3339)
		}
		d.Pid = mp.Pid
		d.HasLog = mp.LogPath != ""
		d.Origin = "supervised"
		if mp.Adopted {
			d.Origin = "adopted (started outside this web)"
		}
	}
	s.renderTemplate(w, "mount/detail.html", d)
}

// ── GET /mount/{profile}/log — HTMX poll partial ──────────────────

const mountLogTailBytes = 16 * 1024

func (s *Server) mountLog(w http.ResponseWriter, r *http.Request) {
	profile := r.PathValue("profile")
	mp := s.sup.Get(profile)
	if mp == nil {
		http.Error(w, "no supervised mount for "+profile, http.StatusNotFound)
		return
	}
	body, err := mp.TailLog(mountLogTailBytes)
	if err != nil {
		http.Error(w, "log: "+err.Error(), http.StatusInternalServerError)
		return
	}
	// HTMX poll target — a small fragment with the status line plus
	// the tail. Returned as plain HTML, no layout.
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<div class="log-status">status: <code>%s</code></div>`, html.EscapeString(mp.Status()))
	fmt.Fprintf(w, `<pre class="log">%s</pre>`, html.EscapeString(body))
}
