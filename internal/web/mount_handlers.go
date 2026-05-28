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
	Supervised []mountRow
	External   []string
	Profiles   []string
}

type mountRow struct {
	Profile    string
	Mountpoint string
	Status     string
	Started    string
	AgeSeconds int64
}

func (s *Server) mountList(w http.ResponseWriter, r *http.Request) {
	d := mountListData{pageBase: s.basePage(w, r)}
	for _, mp := range s.sup.List() {
		d.Supervised = append(d.Supervised, mountRow{
			Profile:    mp.Profile,
			Mountpoint: mp.Mountpoint,
			Status:     mp.Status(),
			Started:    mp.Started.Format(time.RFC3339),
			AgeSeconds: int64(time.Since(mp.Started).Seconds()),
		})
	}
	d.External = s.sup.ExternalMounts()
	d.Profiles = listProfileNames()
	s.renderTemplate(w, "mount/list.html", d)
}

// ── /mount/new (form) ─────────────────────────────────────────────

type mountNewData struct {
	pageBase
	Profiles      []string
	DefaultProfile string
}

func (s *Server) mountNewForm(w http.ResponseWriter, r *http.Request) {
	d := mountNewData{
		pageBase:       s.basePage(w, r),
		Profiles:       listProfileNames(),
		DefaultProfile: config.ActiveProfile(),
	}
	s.renderTemplate(w, "mount/new.html", d)
}

// ── POST /mount/start ─────────────────────────────────────────────

func (s *Server) mountStart(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	profile := strings.TrimSpace(r.FormValue("profile"))
	mountpoint := strings.TrimSpace(r.FormValue("mountpoint"))
	if profile == "" {
		SetFlash(w, "error", "profile is required")
		http.Redirect(w, r, "/mount/new", http.StatusSeeOther)
		return
	}
	if mountpoint == "" {
		SetFlash(w, "error", "mountpoint is required")
		http.Redirect(w, r, "/mount/new", http.StatusSeeOther)
		return
	}
	opts := StartOpts{
		Profile:    profile,
		Mountpoint: mountpoint,
		Passphrase: r.FormValue("passphrase"),
		Readonly:   r.FormValue("readonly") == "on",
		AllowOther: r.FormValue("allow_other") == "on",
		Debug:      r.FormValue("debug") == "on",
	}
	if _, err := s.sup.Start(opts); err != nil {
		SetFlash(w, "error", "start: "+err.Error())
		http.Redirect(w, r, "/mount/new", http.StatusSeeOther)
		return
	}
	SetFlash(w, "ok", fmt.Sprintf("starting mount for %s at %s — tail the log to verify", profile, mountpoint))
	http.Redirect(w, r, "/mount/"+profile, http.StatusSeeOther)
}

// ── POST /mount/{profile}/stop ────────────────────────────────────

func (s *Server) mountStop(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	profile := r.PathValue("profile")
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
	Found      bool
}

func (s *Server) mountDetail(w http.ResponseWriter, r *http.Request) {
	profile := r.PathValue("profile")
	d := mountDetailData{pageBase: s.basePage(w, r), Profile: profile}
	if mp := s.sup.Get(profile); mp != nil {
		d.Found = true
		d.Mountpoint = mp.Mountpoint
		d.Status = mp.Status()
		d.Started = mp.Started.Format(time.RFC3339)
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
