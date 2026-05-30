package web

import (
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"time"
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

	// First-run setup wizard.
	s.mux.HandleFunc("GET /setup", s.setupWizard)
	s.mux.HandleFunc("POST /setup/skip", s.setupSkip)

	// Profile CRUD.
	s.mux.HandleFunc("GET /profiles", s.profilesList)
	s.mux.HandleFunc("GET /profiles/new", s.profilesNewForm)
	s.mux.HandleFunc("POST /profiles", s.profilesCreate)
	s.mux.HandleFunc("GET /profiles/{name}", s.profilesShow)
	s.mux.HandleFunc("POST /profiles/{name}/use", s.profilesUse)
	s.mux.HandleFunc("POST /profiles/{name}/delete", s.profilesDelete)
	s.mux.HandleFunc("GET /profiles/{name}/export", s.profilesExport)
	s.mux.HandleFunc("GET /profiles/import", s.profilesImportForm)
	s.mux.HandleFunc("POST /profiles/import", s.profilesImport)

	// Login.
	s.mux.HandleFunc("GET /login", s.loginChoice)
	s.mux.HandleFunc("GET /login/phone", s.loginPhoneForm)
	s.mux.HandleFunc("POST /login/phone/start", s.loginPhoneStart)
	s.mux.HandleFunc("GET /login/phone/code", s.loginPhoneCodeForm)
	s.mux.HandleFunc("POST /login/phone/code", s.loginPhoneCodeSubmit)
	s.mux.HandleFunc("GET /login/phone/password", s.loginPhonePasswordForm)
	s.mux.HandleFunc("POST /login/phone/password", s.loginPhonePasswordSubmit)
	s.mux.HandleFunc("GET /login/bot", s.loginBotForm)
	s.mux.HandleFunc("POST /login/bot", s.loginBotSubmit)

	// Channel.
	s.mux.HandleFunc("GET /channel", s.channelShow)
	s.mux.HandleFunc("GET /channel/list", s.channelList)
	s.mux.HandleFunc("GET /channel/set", s.channelSetForm)
	s.mux.HandleFunc("POST /channel/set", s.channelSetSubmit)

	// Encryption.
	s.mux.HandleFunc("GET /encrypt", s.encryptShow)
	s.mux.HandleFunc("GET /encrypt/init", s.encryptInitForm)
	s.mux.HandleFunc("POST /encrypt/init", s.encryptInitSubmit)

	// Telegram connection state + recent requests.
	s.mux.HandleFunc("GET /telegram", s.telegramPage)
	s.mux.HandleFunc("GET /partials/telegram", s.telegramPartial)

	// Mount supervisor.
	s.mux.HandleFunc("GET /mount", s.mountList)
	s.mux.HandleFunc("GET /mount/new", s.mountNewForm)
	s.mux.HandleFunc("POST /mount/start", s.mountStart)
	s.mux.HandleFunc("GET /mount/{profile}", s.mountDetail)
	s.mux.HandleFunc("POST /mount/{profile}/stop", s.mountStop)
	s.mux.HandleFunc("GET /mount/{profile}/log", s.mountLog)

	// File browser.
	s.mux.HandleFunc("GET /browse", s.browsePick)
	s.mux.HandleFunc("GET /browse/dir", s.browseDir)
	s.mux.HandleFunc("GET /browse/file", s.browseFile)
	s.mux.HandleFunc("POST /browse/mkdir", s.browseMkdir)
	s.mux.HandleFunc("POST /browse/upload", s.browseUpload)
	s.mux.HandleFunc("POST /browse/delete", s.browseDelete)
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
	}
}
