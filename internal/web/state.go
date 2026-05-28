package web

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// flashCookie is a one-shot status message rendered on the next page
// load — used by POST→redirect→GET to surface "Profile created"
// type confirmation.
const flashCookie = "telfs_flash"

// csrfCookie holds the per-session token that must echo back in every
// POST form's `csrf` field. Generated lazily on first GET.
const csrfCookie = "telfs_csrf"

// State is the in-memory state the HTTP layer needs across requests.
// Currently just the CSRF token map; phone-login sessions will live
// here too in P2.
type State struct {
	mu    sync.Mutex
	csrfs map[string]time.Time // token → last-seen
}

// NewState returns an initialized in-memory session store.
func NewState() *State {
	s := &State{csrfs: make(map[string]time.Time)}
	go s.janitorLoop()
	return s
}

// EnsureCSRF returns the request's existing CSRF token from cookie, or
// mints a new one and writes a Set-Cookie header. Should be called by
// every GET handler that renders a form.
func (s *State) EnsureCSRF(w http.ResponseWriter, r *http.Request) string {
	if c, err := r.Cookie(csrfCookie); err == nil && c.Value != "" {
		s.mu.Lock()
		if _, ok := s.csrfs[c.Value]; ok {
			s.csrfs[c.Value] = time.Now()
			s.mu.Unlock()
			return c.Value
		}
		s.mu.Unlock()
	}
	t := randomHex(16)
	s.mu.Lock()
	s.csrfs[t] = time.Now()
	s.mu.Unlock()
	http.SetCookie(w, &http.Cookie{
		Name:     csrfCookie,
		Value:    t,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   60 * 60 * 24,
	})
	return t
}

// CheckCSRF verifies the form's `csrf` field matches the cookie's
// token (both must be present and equal). Returns nil on success.
func (s *State) CheckCSRF(r *http.Request) error {
	c, err := r.Cookie(csrfCookie)
	if err != nil || c.Value == "" {
		return fmt.Errorf("missing csrf cookie")
	}
	form := r.FormValue("csrf")
	if form == "" {
		return fmt.Errorf("missing csrf form field")
	}
	if subtle.ConstantTimeCompare([]byte(c.Value), []byte(form)) != 1 {
		return fmt.Errorf("csrf mismatch")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.csrfs[c.Value]; !ok {
		return fmt.Errorf("csrf token expired")
	}
	s.csrfs[c.Value] = time.Now()
	return nil
}

// SetFlash queues a short status message rendered on the next page.
// Value is URL-encoded so spaces / special chars don't make the
// browser/curl wrap it in quotes (which breaks subsequent parsing).
func SetFlash(w http.ResponseWriter, kind, msg string) {
	http.SetCookie(w, &http.Cookie{
		Name:     flashCookie,
		Value:    url.QueryEscape(kind + "|" + msg),
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   60,
	})
}

// ConsumeFlash returns the previous flash (if any) and clears it.
func ConsumeFlash(w http.ResponseWriter, r *http.Request) (kind, msg string) {
	c, err := r.Cookie(flashCookie)
	if err != nil {
		return "", ""
	}
	http.SetCookie(w, &http.Cookie{
		Name:   flashCookie,
		Value:  "",
		Path:   "/",
		MaxAge: -1,
	})
	decoded, err := url.QueryUnescape(c.Value)
	if err != nil {
		return "", ""
	}
	for i := 0; i < len(decoded); i++ {
		if decoded[i] == '|' {
			return decoded[:i], decoded[i+1:]
		}
	}
	return "", decoded
}

// janitorLoop reaps CSRF tokens unused for more than 24 hours so the
// in-memory map doesn't grow without bound.
func (s *State) janitorLoop() {
	t := time.NewTicker(15 * time.Minute)
	defer t.Stop()
	for range t.C {
		cutoff := time.Now().Add(-24 * time.Hour)
		s.mu.Lock()
		for k, last := range s.csrfs {
			if last.Before(cutoff) {
				delete(s.csrfs, k)
			}
		}
		s.mu.Unlock()
	}
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// vanishingly unlikely; fall back to a timestamp + counter
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
