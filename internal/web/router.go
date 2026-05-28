package web

import (
	"html/template"
	"io/fs"
	"net/http"
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
func (s *Server) renderTemplate(w http.ResponseWriter, name string, data any) {
	t, err := template.New("").Funcs(template.FuncMap{
		"humanBytes": humanBytesTpl,
	}).ParseFS(templatesFS, "templates/layout.html", "templates/"+name)
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

// renderPartial parses a single template (no layout) — for HTMX
// fragment responses.
func (s *Server) renderPartial(w http.ResponseWriter, name string, data any) {
	t, err := template.New("").Funcs(template.FuncMap{
		"humanBytes": humanBytesTpl,
	}).ParseFS(templatesFS, "templates/"+name)
	if err != nil {
		s.opts.Logger.Printf("partial parse %s: %v", name, err)
		http.Error(w, "template error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := t.ExecuteTemplate(w, "content", data); err != nil {
		s.opts.Logger.Printf("partial exec %s: %v", name, err)
	}
}
