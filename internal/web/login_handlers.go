package web

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"telfs/internal/mountctl"
	"telfs/internal/secret"
)

// ── /login → phone flow (the only auth flow) ──────────────────────
//
// Bot-token login was removed: MTProto requires api_id/api_hash even
// for bots, so a bot identity saved nothing over a user login while
// adding a second auth path and an identity-scoped access_hash trap.

// loginChoice is kept as the /login route target for old links and
// the nav; it just forwards to the phone flow.
func (s *Server) loginChoice(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/login/phone", http.StatusSeeOther)
}

// ── phone login: step 1 — enter phone ─────────────────────────────

type phone1Data struct {
	pageBase
	Profile string
	// NeedAPI: the profile has no api_id/api_hash yet, so the form
	// collects them inline — login shouldn't bounce the user to a
	// different page for a prerequisite it can gather right here.
	NeedAPI bool
	APIID   int
	// LoggedIn: the profile already holds a completed session — the
	// page offers logout instead of a second login (a profile is ONE
	// identity; stacking logins over a live session is never right).
	LoggedIn bool
	Identity string
	// Mounted blocks logout: the running daemon holds the session.
	Mounted    bool
	Mountpoint string
}

func (s *Server) loginPhoneForm(w http.ResponseWriter, r *http.Request) {
	d := phone1Data{pageBase: s.basePage(w, r), Profile: profileOf(r)}
	if cfg, err := loadCfg(r); err == nil {
		d.NeedAPI = cfg.RequireAPI() != nil
		d.APIID = cfg.APIID
		if cfg.HasSession() && cfg.Identity != nil {
			d.LoggedIn = true
			d.Identity = cfg.Identity.Label()
			if _, mp, ok, ferr := mountctl.FindByProfile(d.Profile); ok {
				d.Mounted = true
				d.Mountpoint = mp
			} else if ferr != nil {
				// Unknown-profile mount: show as mounted (fail closed).
				d.Mounted = true
				d.Mountpoint = "(unknown — " + ferr.Error() + ")"
			}
		}
	}
	s.renderTemplate(w, "login/phone1.html", d)
}

// loginLogout removes the active profile's session + recorded
// identity so a fresh login is possible. The channel binding is
// KEPT: logging back into the SAME account leaves it valid, and if a
// different account logs in, mount preflight rejects the stale
// access_hash with a re-bind hint. Refused while the profile's
// daemon is mounted — the running session is in active use, and gotd
// would rewrite session.json under us anyway.
func (s *Server) loginLogout(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	cfg, err := loadCfg(r)
	if err != nil {
		SetFlash(w, "error", "config: "+err.Error())
		http.Redirect(w, r, "/login/phone", http.StatusSeeOther)
		return
	}
	profile := profileOf(r)
	if pid, mp, ok, ferr := mountctl.FindByProfile(profile); ferr != nil {
		SetFlash(w, "error", fmt.Sprintf("profile %q: %v", profile, ferr))
		http.Redirect(w, r, "/login/phone", http.StatusSeeOther)
		return
	} else if ok {
		SetFlash(w, "error", fmt.Sprintf("profile %q is mounted at %s (pid %d) — stop the mount before logging out", profile, mp, pid))
		http.Redirect(w, r, "/login/phone", http.StatusSeeOther)
		return
	}
	if err := cfg.RemoveSessionFiles(); err != nil {
		SetFlash(w, "error", "remove session: "+err.Error())
		http.Redirect(w, r, "/login/phone", http.StatusSeeOther)
		return
	}
	cfg.Identity = nil
	if err := cfg.Save(); err != nil {
		SetFlash(w, "error", "save config: "+err.Error())
		http.Redirect(w, r, "/login/phone", http.StatusSeeOther)
		return
	}
	SetFlash(w, "ok", "logged out — the channel binding was kept; log back into the same account and everything just works (a different account must re-bind the channel)")
	http.Redirect(w, r, "/login/phone", http.StatusSeeOther)
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
	cfg, err := loadCfg(r)
	if err != nil {
		SetFlash(w, "error", "config: "+err.Error())
		http.Redirect(w, r, "/login/phone", http.StatusSeeOther)
		return
	}
	// No double login: a completed session means this profile IS that
	// identity — log out first (button on the login page).
	if cfg.HasSession() && cfg.Identity != nil {
		SetFlash(w, "error", fmt.Sprintf("already logged in as %s — log out first to log in again", cfg.Identity.Label()))
		http.Redirect(w, r, setupReturn(r, "/login/phone"), http.StatusSeeOther)
		return
	}
	// Inline API credentials: when the profile has none, the form
	// collected them alongside the phone number. Persist immediately —
	// they are app-level identity (my.telegram.org), valid regardless
	// of whether this login attempt completes.
	if cfg.RequireAPI() != nil {
		idStr := strings.TrimSpace(r.FormValue("api_id"))
		hash := strings.TrimSpace(r.FormValue("api_hash"))
		if idStr != "" || hash != "" {
			id, err := strconv.Atoi(idStr)
			if err != nil || id <= 0 {
				SetFlash(w, "error", "api_id must be a positive integer (from https://my.telegram.org/apps)")
				http.Redirect(w, r, "/login/phone", http.StatusSeeOther)
				return
			}
			if hash == "" {
				SetFlash(w, "error", "api_hash is required (from https://my.telegram.org/apps)")
				http.Redirect(w, r, "/login/phone", http.StatusSeeOther)
				return
			}
			cfg.APIID = id
			cfg.APIHash = hash
			if err := cfg.Save(); err != nil {
				SetFlash(w, "error", "save API credentials: "+err.Error())
				http.Redirect(w, r, "/login/phone", http.StatusSeeOther)
				return
			}
		}
	}
	if err := cfg.RequireAPI(); err != nil {
		SetFlash(w, "error", err.Error())
		http.Redirect(w, r, "/login/phone", http.StatusSeeOther)
		return
	}

	sess := newLoginSession(profileOf(r))
	s.logins.Add(sess)
	http.SetCookie(w, &http.Cookie{
		Name:  loginCookie,
		Value: sess.ID,
		// Path "/" (not "/login"): the setup wizard at /setup reads
		// the in-flight session to render the code/password form
		// inline; a /login-scoped cookie would be invisible there.
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   int(loginSessionTTL.Seconds()),
	})

	// Spawn the gotd login goroutine. It blocks inside LoginWith until
	// the user supplies phone/code/(password), then exits.
	// The first login SETS the profile's secret (session.enc is
	// created with it); a re-login after logout opens the same secret.
	purpose := secret.Unlock
	if !cfg.HasSession() {
		purpose = secret.SetNew
	}
	client, err := actionClient(r, cfg, purpose)
	if err != nil {
		s.logins.Remove(sess.ID)
		SetFlash(w, "error", err.Error())
		http.Redirect(w, r, setupReturn(r, "/login/phone"), http.StatusSeeOther)
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
		http.Redirect(w, r, setupReturn(r, "/login/phone"), http.StatusSeeOther)
		return
	}
	switch ev.Kind {
	case evCodeSent:
		sess.SetStage("code")
		http.Redirect(w, r, setupReturn(r, "/login/phone/code"), http.StatusSeeOther)
	case evDone:
		// Already authorized — gotd short-circuited via IfNecessary.
		s.logins.Remove(sess.ID)
		if ev.Err != nil {
			SetFlash(w, "error", "auth: "+ev.Err.Error())
			http.Redirect(w, r, setupReturn(r, "/login/phone"), http.StatusSeeOther)
			return
		}
		SetFlash(w, "ok", "already authenticated")
		http.Redirect(w, r, setupReturn(r, "/"), http.StatusSeeOther)
	default:
		SetFlash(w, "error", fmt.Sprintf("unexpected state %s", ev.Kind))
		s.logins.Remove(sess.ID)
		http.Redirect(w, r, setupReturn(r, "/login/phone"), http.StatusSeeOther)
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
		http.Redirect(w, r, setupReturn(r, "/login/phone/code"), http.StatusSeeOther)
		return
	}
	switch ev.Kind {
	case evPasswordNeeded:
		sess.SetStage("password")
		http.Redirect(w, r, setupReturn(r, "/login/phone/password"), http.StatusSeeOther)
	case evDone:
		s.logins.Remove(sess.ID)
		clearLoginCookie(w)
		if ev.Err != nil {
			SetFlash(w, "error", "auth: "+ev.Err.Error())
			http.Redirect(w, r, setupReturn(r, "/login/phone"), http.StatusSeeOther)
			return
		}
		SetFlash(w, "ok", "logged in successfully")
		http.Redirect(w, r, setupReturn(r, "/"), http.StatusSeeOther)
	default:
		SetFlash(w, "error", fmt.Sprintf("unexpected state %s", ev.Kind))
		http.Redirect(w, r, setupReturn(r, "/login/phone/code"), http.StatusSeeOther)
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
		http.Redirect(w, r, setupReturn(r, "/login/phone/password"), http.StatusSeeOther)
		return
	}
	if ev.Kind == evDone {
		s.logins.Remove(sess.ID)
		clearLoginCookie(w)
		if ev.Err != nil {
			SetFlash(w, "error", "auth: "+ev.Err.Error())
			http.Redirect(w, r, setupReturn(r, "/login/phone/password"), http.StatusSeeOther)
			return
		}
		SetFlash(w, "ok", "logged in successfully")
		http.Redirect(w, r, setupReturn(r, "/"), http.StatusSeeOther)
		return
	}
	SetFlash(w, "error", fmt.Sprintf("unexpected state %s", ev.Kind))
	http.Redirect(w, r, setupReturn(r, "/login/phone/password"), http.StatusSeeOther)
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
		Path:   "/",
		MaxAge: -1,
	})
}
