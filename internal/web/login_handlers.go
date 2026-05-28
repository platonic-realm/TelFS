package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"telfs/internal/config"
	"telfs/internal/tg"
)

// ── /login ────────────────────────────────────────────────────────

type loginChoiceData struct {
	pageBase
	HasAPI  bool
	Profile string
	Note    string
}

func (s *Server) loginChoice(w http.ResponseWriter, r *http.Request) {
	d := loginChoiceData{pageBase: s.basePage(w, r), Profile: config.ActiveProfile()}
	if cfg, err := config.Load(); err == nil {
		d.HasAPI = cfg.RequireAPI() == nil
		if !d.HasAPI {
			d.Note = "Set api_id and api_hash in config.toml first (from https://my.telegram.org/apps)."
		}
	}
	s.renderTemplate(w, "login/choice.html", d)
}

// ── phone login: step 1 — enter phone ─────────────────────────────

type phone1Data struct{ pageBase }

func (s *Server) loginPhoneForm(w http.ResponseWriter, r *http.Request) {
	s.renderTemplate(w, "login/phone1.html", phone1Data{s.basePage(w, r)})
}

func (s *Server) loginPhoneStart(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	phone := strings.TrimSpace(r.FormValue("phone"))
	if phone == "" {
		SetFlash(w, "error", "phone number is required")
		http.Redirect(w, r, "/login/phone", http.StatusSeeOther)
		return
	}
	cfg, err := config.Load()
	if err != nil {
		SetFlash(w, "error", "config: "+err.Error())
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	if err := cfg.RequireAPI(); err != nil {
		SetFlash(w, "error", err.Error())
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	// User mode for phone login. The CLI persists auth_mode in config
	// on `login --bot`; mirror that for `login` (no --bot) so a
	// subsequent mount doesn't accidentally bot-auth.
	cfg.AuthMode = config.AuthModeUser
	cfg.BotToken = ""
	if err := cfg.Save(); err != nil {
		SetFlash(w, "error", "save config: "+err.Error())
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}

	sess := newLoginSession(config.ActiveProfile())
	s.logins.Add(sess)
	http.SetCookie(w, &http.Cookie{
		Name:     loginCookie,
		Value:    sess.ID,
		Path:     "/login",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   int(loginSessionTTL.Seconds()),
	})

	// Spawn the gotd login goroutine. It blocks inside LoginWith until
	// the user supplies phone/code/(password), then exits.
	client, err := tg.New(cfg)
	if err != nil {
		SetFlash(w, "error", "tg.New: "+err.Error())
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	go func() {
		err := client.LoginWith(sess.ctx, webAuth{sess: sess})
		select {
		case sess.EventCh <- loginEvent{Kind: evDone, Err: err}:
		default:
		}
	}()

	// Submit the phone and wait for "code sent".
	sess.PhoneCh <- phone
	ev, err := waitForEvent(sess, loginWaitTimeout)
	if err != nil {
		SetFlash(w, "error", "telegram: "+err.Error())
		s.logins.Remove(sess.ID)
		http.Redirect(w, r, "/login/phone", http.StatusSeeOther)
		return
	}
	switch ev.Kind {
	case evCodeSent:
		http.Redirect(w, r, "/login/phone/code", http.StatusSeeOther)
	case evDone:
		// Already authorized — gotd short-circuited via IfNecessary.
		s.logins.Remove(sess.ID)
		if ev.Err != nil {
			SetFlash(w, "error", "auth: "+ev.Err.Error())
			http.Redirect(w, r, "/login/phone", http.StatusSeeOther)
			return
		}
		SetFlash(w, "ok", "already authenticated")
		http.Redirect(w, r, "/", http.StatusSeeOther)
	default:
		SetFlash(w, "error", fmt.Sprintf("unexpected state %s", ev.Kind))
		s.logins.Remove(sess.ID)
		http.Redirect(w, r, "/login/phone", http.StatusSeeOther)
	}
}

// ── phone login: step 2 — enter code ──────────────────────────────

type phone2Data struct {
	pageBase
}

func (s *Server) loginPhoneCodeForm(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.activeLoginSession(r); !ok {
		SetFlash(w, "error", "login session expired or missing")
		http.Redirect(w, r, "/login/phone", http.StatusSeeOther)
		return
	}
	s.renderTemplate(w, "login/phone2.html", phone2Data{s.basePage(w, r)})
}

func (s *Server) loginPhoneCodeSubmit(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	sess, ok := s.activeLoginSession(r)
	if !ok {
		SetFlash(w, "error", "login session expired")
		http.Redirect(w, r, "/login/phone", http.StatusSeeOther)
		return
	}
	code := strings.TrimSpace(r.FormValue("code"))
	if code == "" {
		SetFlash(w, "error", "code is required")
		http.Redirect(w, r, "/login/phone/code", http.StatusSeeOther)
		return
	}
	sess.CodeCh <- code
	ev, err := waitForEvent(sess, loginWaitTimeout)
	if err != nil {
		SetFlash(w, "error", "telegram: "+err.Error())
		http.Redirect(w, r, "/login/phone/code", http.StatusSeeOther)
		return
	}
	switch ev.Kind {
	case evPasswordNeeded:
		http.Redirect(w, r, "/login/phone/password", http.StatusSeeOther)
	case evDone:
		s.logins.Remove(sess.ID)
		clearLoginCookie(w)
		if ev.Err != nil {
			SetFlash(w, "error", "auth: "+ev.Err.Error())
			http.Redirect(w, r, "/login/phone", http.StatusSeeOther)
			return
		}
		SetFlash(w, "ok", "logged in successfully")
		http.Redirect(w, r, "/", http.StatusSeeOther)
	default:
		SetFlash(w, "error", fmt.Sprintf("unexpected state %s", ev.Kind))
		http.Redirect(w, r, "/login/phone/code", http.StatusSeeOther)
	}
}

// ── phone login: step 3 — 2FA password ────────────────────────────

type phone3Data struct{ pageBase }

func (s *Server) loginPhonePasswordForm(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.activeLoginSession(r); !ok {
		SetFlash(w, "error", "login session expired")
		http.Redirect(w, r, "/login/phone", http.StatusSeeOther)
		return
	}
	s.renderTemplate(w, "login/phone3.html", phone3Data{s.basePage(w, r)})
}

func (s *Server) loginPhonePasswordSubmit(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	sess, ok := s.activeLoginSession(r)
	if !ok {
		SetFlash(w, "error", "login session expired")
		http.Redirect(w, r, "/login/phone", http.StatusSeeOther)
		return
	}
	pass := r.FormValue("password")
	if pass == "" {
		SetFlash(w, "error", "password is required")
		http.Redirect(w, r, "/login/phone/password", http.StatusSeeOther)
		return
	}
	sess.PassCh <- pass
	ev, err := waitForEvent(sess, loginWaitTimeout)
	if err != nil {
		SetFlash(w, "error", "telegram: "+err.Error())
		http.Redirect(w, r, "/login/phone/password", http.StatusSeeOther)
		return
	}
	if ev.Kind == evDone {
		s.logins.Remove(sess.ID)
		clearLoginCookie(w)
		if ev.Err != nil {
			SetFlash(w, "error", "auth: "+ev.Err.Error())
			http.Redirect(w, r, "/login/phone/password", http.StatusSeeOther)
			return
		}
		SetFlash(w, "ok", "logged in successfully")
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	SetFlash(w, "error", fmt.Sprintf("unexpected state %s", ev.Kind))
	http.Redirect(w, r, "/login/phone/password", http.StatusSeeOther)
}

// activeLoginSession looks up the current login session by cookie.
// Returns nil + false if missing or expired.
func (s *Server) activeLoginSession(r *http.Request) (*loginSession, bool) {
	c, err := r.Cookie(loginCookie)
	if err != nil {
		return nil, false
	}
	sess := s.logins.Get(c.Value)
	if sess == nil {
		return nil, false
	}
	return sess, true
}

func clearLoginCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:   loginCookie,
		Value:  "",
		Path:   "/login",
		MaxAge: -1,
	})
}

// ── bot login (single shot) ───────────────────────────────────────

type botLoginData struct{ pageBase }

func (s *Server) loginBotForm(w http.ResponseWriter, r *http.Request) {
	s.renderTemplate(w, "login/bot.html", botLoginData{s.basePage(w, r)})
}

func (s *Server) loginBotSubmit(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	token := strings.TrimSpace(r.FormValue("token"))
	if token == "" {
		SetFlash(w, "error", "bot token is required")
		http.Redirect(w, r, "/login/bot", http.StatusSeeOther)
		return
	}
	cfg, err := config.Load()
	if err != nil {
		SetFlash(w, "error", "config: "+err.Error())
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	if err := cfg.RequireAPI(); err != nil {
		SetFlash(w, "error", err.Error())
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	// Apply auth_mode + token to an in-memory cfg copy; only persist
	// after a successful auth round-trip. Otherwise a bad token would
	// poison the profile.
	cfg.AuthMode = config.AuthModeBot
	cfg.BotToken = token
	client, err := tg.New(cfg)
	if err != nil {
		SetFlash(w, "error", "tg.New: "+err.Error())
		http.Redirect(w, r, "/login/bot", http.StatusSeeOther)
		return
	}
	// Force a fresh handshake: an existing user-mode session.json would
	// short-circuit IfNecessary's auth check, and the bot token would
	// never be validated — the request would falsely report success.
	// Switching auth_mode is an explicit user choice, so it's correct
	// to invalidate the prior session at this point.
	sessPath := cfg.SessionPath()
	backupPath := sessPath + ".prebot"
	hadSession := false
	if _, err := os.Stat(sessPath); err == nil {
		_ = os.Rename(sessPath, backupPath)
		hadSession = true
	}
	restoreOnFail := func() {
		_ = os.Remove(sessPath)
		if hadSession {
			_ = os.Rename(backupPath, sessPath)
		}
	}
	// 60-second hard budget for bot auth (one round-trip with
	// floodwait margin); avoids tying up the request indefinitely.
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	if err := client.Login(ctx); err != nil {
		restoreOnFail()
		// Recognize the specific "bad token" return so the message is
		// useful.
		switch {
		case strings.Contains(err.Error(), "ACCESS_TOKEN_INVALID"):
			SetFlash(w, "error", "Telegram rejected the bot token (ACCESS_TOKEN_INVALID). Check the token from @BotFather.")
		case errors.Is(err, context.DeadlineExceeded):
			SetFlash(w, "error", "bot login timed out (60s)")
		default:
			SetFlash(w, "error", "auth: "+err.Error())
		}
		http.Redirect(w, r, "/login/bot", http.StatusSeeOther)
		return
	}
	// Auth succeeded: commit the new auth_mode + token, drop the backup.
	if err := cfg.Save(); err != nil {
		SetFlash(w, "error", "save config: "+err.Error())
		http.Redirect(w, r, "/login/bot", http.StatusSeeOther)
		return
	}
	if hadSession {
		_ = os.Remove(backupPath)
	}
	SetFlash(w, "ok", "bot logged in")
	http.Redirect(w, r, "/", http.StatusSeeOther)
}
