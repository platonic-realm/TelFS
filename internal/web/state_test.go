package web

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestEnsureCSRFMintsAndReusesToken(t *testing.T) {
	st := NewState()
	w1 := httptest.NewRecorder()
	r1 := httptest.NewRequest("GET", "/", nil)
	t1 := st.EnsureCSRF(w1, r1)
	if len(t1) < 16 {
		t.Fatalf("token too short: %q", t1)
	}
	// Cookie should be Set on the response.
	if cookie := w1.Header().Get("Set-Cookie"); !strings.Contains(cookie, csrfCookie) {
		t.Errorf("Set-Cookie missing csrf cookie: %q", cookie)
	}
	// A second request that ECHOES BACK the cookie reuses the token —
	// otherwise a chatty form would burn through tokens.
	w2 := httptest.NewRecorder()
	r2 := httptest.NewRequest("GET", "/", nil)
	r2.AddCookie(&http.Cookie{Name: csrfCookie, Value: t1})
	t2 := st.EnsureCSRF(w2, r2)
	if t2 != t1 {
		t.Errorf("expected reuse of token %q, got new %q", t1, t2)
	}
}

func TestCheckCSRFAcceptsMatchingPair(t *testing.T) {
	st := NewState()
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	tok := st.EnsureCSRF(w, r)

	// Now post a form with the matching token + cookie.
	body := strings.NewReader("csrf=" + url.QueryEscape(tok))
	post := httptest.NewRequest("POST", "/x", body)
	post.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	post.AddCookie(&http.Cookie{Name: csrfCookie, Value: tok})

	if err := st.CheckCSRF(post); err != nil {
		t.Errorf("CheckCSRF rejected a valid pair: %v", err)
	}
}

func TestCheckCSRFRejectsMismatch(t *testing.T) {
	st := NewState()
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	tok := st.EnsureCSRF(w, r)

	cases := []struct {
		name   string
		cookie string
		form   string
	}{
		{"no-cookie", "", tok},
		{"no-form", tok, ""},
		{"wrong-form", tok, "deadbeefdeadbeefdeadbeefdeadbeef"},
		{"unknown-cookie", "00000000000000000000000000000000", tok},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			body := strings.NewReader("csrf=" + url.QueryEscape(c.form))
			post := httptest.NewRequest("POST", "/x", body)
			post.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			if c.cookie != "" {
				post.AddCookie(&http.Cookie{Name: csrfCookie, Value: c.cookie})
			}
			if err := st.CheckCSRF(post); err == nil {
				t.Errorf("CheckCSRF accepted bad pair (cookie=%q form=%q)", c.cookie, c.form)
			}
		})
	}
}

func TestFlashRoundtripUTF8AndSpaces(t *testing.T) {
	// The flash cookie value is URL-encoded; verify a value with spaces
	// + UTF-8 + reserved chars round-trips byte-identically. This
	// regressed once on whitespace.
	w1 := httptest.NewRecorder()
	msg := "saved 5 files, médée's bundle (mode=0600)"
	SetFlash(w1, "ok", msg)
	// Replay the Set-Cookie on a fresh request to mimic the browser.
	cookie := w1.Result().Cookies()[0]
	r := httptest.NewRequest("GET", "/", nil)
	r.AddCookie(cookie)

	w2 := httptest.NewRecorder()
	kind, got := ConsumeFlash(w2, r)
	if kind != "ok" || got != msg {
		t.Errorf("flash roundtrip: kind=%q msg=%q (want ok / %q)", kind, got, msg)
	}
	// ConsumeFlash should also send a clearing Set-Cookie.
	clearing := w2.Header().Get("Set-Cookie")
	if !strings.Contains(clearing, "Max-Age=0") {
		t.Errorf("ConsumeFlash didn't clear cookie: %q", clearing)
	}
}

func TestConsumeFlashNoCookie(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	kind, msg := ConsumeFlash(w, r)
	if kind != "" || msg != "" {
		t.Errorf("missing-cookie consume: kind=%q msg=%q (want empty/empty)", kind, msg)
	}
}

// TestLoginRegistryAddGetRemove covers the happy-path lifecycle of a
// login session.
func TestLoginRegistryAddGetRemove(t *testing.T) {
	r := newLoginRegistry()
	s := newLoginSession("main")
	r.Add(s)
	if got := r.Get(s.ID); got != s {
		t.Errorf("Get returned %v, want %v", got, s)
	}
	r.Remove(s.ID)
	if got := r.Get(s.ID); got != nil {
		t.Errorf("Get after Remove returned %v, want nil", got)
	}
}

// TestLoginRegistryRemoveCancelsCtx ensures the gotd goroutine wedged
// inside Phone()/Code()/Password() unblocks when we drop the session.
func TestLoginRegistryRemoveCancelsCtx(t *testing.T) {
	r := newLoginRegistry()
	s := newLoginSession("main")
	r.Add(s)
	r.Remove(s.ID)
	select {
	case <-s.ctx.Done():
		// OK — cancel propagated.
	case <-time.After(200 * time.Millisecond):
		t.Errorf("Remove didn't cancel session ctx within 200ms")
	}
}

func TestLoginRegistryGetTouchesLastSeen(t *testing.T) {
	r := newLoginRegistry()
	s := newLoginSession("main")
	r.Add(s)
	original := s.LastTouch
	time.Sleep(5 * time.Millisecond)
	_ = r.Get(s.ID)
	if !s.LastTouch.After(original) {
		t.Errorf("Get didn't bump LastTouch (orig=%v now=%v)", original, s.LastTouch)
	}
}
