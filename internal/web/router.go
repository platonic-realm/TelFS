package web

import (
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"net/url"
	"time"

	"telfs/internal/config"
)

// registerRoutes binds the P1 surface. Subsequent phases add login,
// channel, encrypt, mount, browse.
func (s *Server) registerRoutes() {
	// Static assets are served straight out of the embedded FS — no
	// per-file route registration. /static/style.css, /static/htmx.min.js.
	sub, _ := fs.Sub(staticFS, "static")
	s.mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.FS(sub))))

	// Dashboard + status partial (auto-refresh).
	s.mux.HandleFunc("GET /{$}", s.dashboardIndex)
	s.mux.HandleFunc("GET /partials/status", s.statusPartial)
	s.mux.HandleFunc("GET /partials/profiles", s.profilesPartial)
	s.mux.HandleFunc("GET /partials/health", s.healthPartial)

	// First-run setup wizard. Steps are inline forms posting to the
	// dedicated handlers with from=setup; /setup/channels is the
	// HTMX-lazy channel-picker for the channel step.
	s.mux.HandleFunc("GET /setup", s.setupWizard)
	s.mux.HandleFunc("POST /setup/skip", s.setupSkip)
	s.mux.HandleFunc("POST /setup/init", s.setupInit)
	s.mux.HandleFunc("GET /setup/channels", s.setupChannels)
	s.mux.HandleFunc("POST /setup/channels", s.setupChannels)

	// Profile CRUD.
	s.mux.HandleFunc("GET /profiles", s.profilesList)
	s.mux.HandleFunc("GET /profiles/new", s.profilesNewForm)
	s.mux.HandleFunc("POST /profiles", s.profilesCreate)
	s.mux.HandleFunc("GET /profiles/{name}", s.profilesShow)
	s.mux.HandleFunc("POST /profiles/{name}/api", s.profilesSetAPI)
	s.mux.HandleFunc("POST /profiles/{name}/secret", s.profilesSetSecret)
	s.mux.HandleFunc("POST /profiles/{name}/use", s.profilesUse)
	s.mux.HandleFunc("POST /profiles/{name}/delete", s.profilesDelete)
	// POST + CSRF, not GET: the bundle contains session.json — a live
	// MTProto session = full Telegram account takeover. A GET could be
	// triggered cross-origin (img/script/anchor) by any page the
	// operator's browser visits.
	s.mux.HandleFunc("POST /profiles/{name}/export", s.profilesExport)
	s.mux.HandleFunc("GET /profiles/import", s.profilesImportForm)
	s.mux.HandleFunc("POST /profiles/import", s.profilesImport)

	// Login — phone auth is the only flow (bot-token auth removed:
	// MTProto needs api_id/api_hash even for bots, so it saved
	// nothing). /login stays routable for old links; it forwards.
	s.mux.HandleFunc("GET /login", s.loginChoice)
	s.mux.HandleFunc("GET /login/phone", s.loginPhoneForm)
	s.mux.HandleFunc("POST /login/logout", s.loginLogout)
	s.mux.HandleFunc("POST /login/phone/start", s.loginPhoneStart)
	s.mux.HandleFunc("GET /login/phone/code", s.loginPhoneCodeForm)
	s.mux.HandleFunc("POST /login/phone/code", s.loginPhoneCodeSubmit)
	s.mux.HandleFunc("GET /login/phone/password", s.loginPhonePasswordForm)
	s.mux.HandleFunc("POST /login/phone/password", s.loginPhonePasswordSubmit)

	// Channel.
	s.mux.HandleFunc("GET /channel", s.channelShow)
	s.mux.HandleFunc("GET /channel/list", s.channelList)
	s.mux.HandleFunc("POST /channel/list", s.channelList)
	s.mux.HandleFunc("GET /channel/set", s.channelSetForm)
	s.mux.HandleFunc("POST /channel/set", s.channelSetSubmit)

	// Encryption.
	s.mux.HandleFunc("GET /encrypt", s.encryptShow)
	s.mux.HandleFunc("GET /encrypt/init", s.encryptInitForm)
	s.mux.HandleFunc("POST /encrypt/init", s.encryptInitSubmit)

	// Compression.
	s.mux.HandleFunc("GET /compression", s.compressShow)
	s.mux.HandleFunc("GET /compression/init", s.compressInitForm)
	s.mux.HandleFunc("POST /compression/init", s.compressInitSubmit)

	// Telegram connection state + recent requests.
	s.mux.HandleFunc("GET /telegram", s.telegramPage)
	s.mux.HandleFunc("POST /telegram/ratelimit", s.telegramSetRateLimit)
	s.mux.HandleFunc("GET /partials/telegram", s.telegramPartial)

	// FS state + repair.
	s.mux.HandleFunc("GET /state", s.stateShow)
	s.mux.HandleFunc("POST /state/repair", s.stateRepair)
	s.mux.HandleFunc("GET /partials/repair-progress", s.stateRepairProgress)

	// Mount supervisor.
	s.mux.HandleFunc("GET /mount", s.mountList)
	s.mux.HandleFunc("GET /mount/new", s.mountNewForm)
	s.mux.HandleFunc("POST /mount/start", s.mountStart)
	s.mux.HandleFunc("POST /mount/unmount-external", s.mountUnmountExternal)
	// Stop takes the profile as a FORM FIELD, not a path segment:
	// adopted daemons without TELFS_PROFILE get synthetic
	// "adopted:<mountpoint>" keys whose slashes can't ride in a
	// one-segment path wildcard — the old /mount/{profile}/stop
	// route 404'd exactly the mounts that most need stopping.
	s.mux.HandleFunc("POST /mount/stop", s.mountStop)
	s.mux.HandleFunc("GET /mount/{profile}", s.mountDetail)
	s.mux.HandleFunc("GET /mount/{profile}/log", s.mountLog)

	// File browser.
	s.mux.HandleFunc("GET /browse", s.browsePick)
	s.mux.HandleFunc("GET /browse/dir", s.browseDir)
	s.mux.HandleFunc("GET /browse/file", s.browseFile)
	s.mux.HandleFunc("POST /browse/mkdir", s.browseMkdir)
	s.mux.HandleFunc("POST /browse/newfile", s.browseNewFile)
	s.mux.HandleFunc("POST /browse/upload", s.browseUpload)
	s.mux.HandleFunc("POST /browse/delete", s.browseDelete)
	s.mux.HandleFunc("POST /browse/rename", s.browseRename)
}

// renderTemplate parses + executes a template set (always including
// the base layout). Templates live under internal/web/templates/
// and are accessed by their relative path.
//
// Each partial file defines its own "content" block (used by the
// HTMX endpoint via renderPartial). DO NOT add partials to this
// static parse list — every partial's "content" define would
// override the page's own, and you'd see the partial's HTML on
// pages that have nothing to do with it (we shipped that bug once;
// 4c1b4b9's regression). Pages that need to embed a partial inline
// should renderTemplateWith extras for an explicit, per-page parse.
func (s *Server) renderTemplate(w http.ResponseWriter, name string, data any) {
	s.renderTemplateWith(w, name, data)
}

// renderTemplateWith is renderTemplate plus extra template files
// merged into the set. Used by handlers that want to {{template ...}}
// embed a partial in their page. Pass the partial paths relative to
// the templates root.
func (s *Server) renderTemplateWith(w http.ResponseWriter, name string, data any, extras ...string) {
	files := append([]string{"templates/layout.html", "templates/" + name}, extras...)
	t, err := template.New("").Funcs(tmplFuncs()).ParseFS(templatesFS, files...)
	if err != nil {
		s.opts.Logger.Printf("template parse %s: %v", name, err)
		http.Error(w, "template error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := t.ExecuteTemplate(w, "layout", data); err != nil {
		s.opts.Logger.Printf("template exec %s: %v", name, err)
	}
}

// renderPartial parses a single template (no layout) and executes
// its "content" block — for HTMX fragment responses from partials
// that define {{define "content"}}.
func (s *Server) renderPartial(w http.ResponseWriter, name string, data any) {
	s.renderPartialBlock(w, name, "content", data)
}

// renderPartialBlock is the named-block variant. Use when a partial
// defines a uniquely-named block (e.g., "telegram_panel") instead
// of the shared "content" name — that's the right pattern when the
// partial is ALSO embedded into a full page, since two "content"
// defines in one parse set collide.
func (s *Server) renderPartialBlock(w http.ResponseWriter, name, block string, data any) {
	t, err := template.New("").Funcs(tmplFuncs()).
		ParseFS(templatesFS, "templates/"+name)
	if err != nil {
		s.opts.Logger.Printf("partial parse %s: %v", name, err)
		http.Error(w, "template error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := t.ExecuteTemplate(w, block, data); err != nil {
		s.opts.Logger.Printf("partial exec %s [%s]: %v", name, block, err)
	}
}

// tmplFuncs returns the shared template FuncMap. Keep all helpers
// here so a page and its partials see the same functions; otherwise
// {{template ...}} embeds fail when the helper exists on the page's
// FuncMap but not the partial's.
func tmplFuncs() template.FuncMap {
	return template.FuncMap{
		"humanBytes": humanBytesTpl,
		"add1":       func(i int) int { return i + 1 },
		// pathEscape escapes a value for use as a single URL path
		// segment. Needed for adopted-mount profile keys of the form
		// "adopted:/path/to/mnt" — a raw slash would split the
		// segment and 404 the route.
		"pathEscape": url.PathEscape,
		// activeProfile reads the active profile at render time so the
		// masthead can show which profile every page operates on
		// without threading a field through each page's data struct.
		// Cheap: one file read per page render.
		"activeProfile": config.ActiveProfile,
		// ago renders a unix time as a terse age ("3 h ago").
		"ago": ago,
		// pctOf / pctOf64: render a "filled %" for the queue visual
		// bars. Clamped to [0, 100] so an over-target buffer (rare —
		// the packer flushes on threshold) still produces a sane bar.
		"pctOf": func(num, denom int) float64 {
			if denom <= 0 {
				return 0
			}
			p := float64(num) / float64(denom) * 100
			if p < 0 {
				return 0
			}
			if p > 100 {
				return 100
			}
			return p
		},
		"pctOf64": func(num, denom int64) float64 {
			if denom <= 0 {
				return 0
			}
			p := float64(num) / float64(denom) * 100
			if p < 0 {
				return 0
			}
			if p > 100 {
				return 100
			}
			return p
		},
		"ageSeconds": func(now, then int64) int64 {
			if now < then {
				return 0
			}
			return now - then
		},
		// formatTime renders a unix-seconds timestamp as
		// HH:MM:SS in local time — easier to correlate with
		// daemon logs and `date` than a raw epoch number.
		"formatTime": func(unix int64) string {
			if unix == 0 {
				return ""
			}
			return time.Unix(unix, 0).Local().Format("15:04:05")
		},
		// formatDuration renders milliseconds as either "Nms"
		// (for sub-second) or "N.NNs" (for one second or more)
		// so the eye doesn't have to parse "4231 ms" as "~4 s"
		// in a column otherwise full of two-digit numbers.
		"formatDuration": func(ms int64) string {
			if ms < 1000 {
				return fmt.Sprintf("%dms", ms)
			}
			return fmt.Sprintf("%.2fs", float64(ms)/1000.0)
		},
		// activeNav returns "active" when the current request
		// path matches the given nav tab prefix. Used by the
		// layout to mark the selected tab via CSS class. Matches
		// by prefix so /profiles/new and /profiles/<name> both
		// highlight the "Profiles" tab.
		"activeNav": func(currentPath, tabPath string) string {
			if tabPath == "/" {
				if currentPath == "/" {
					return "active"
				}
				return ""
			}
			if currentPath == tabPath ||
				(len(currentPath) > len(tabPath) && currentPath[:len(tabPath)] == tabPath && currentPath[len(tabPath)] == '/') {
				return "active"
			}
			return ""
		},
	}
}
