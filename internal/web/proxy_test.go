package web

import (
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// Behind a reverse proxy (v1.1 W3; S9: Cloudflare Access → tunnel →
// 127.0.0.1:8400 with the Host rewritten): pages load, the CSRF cookie
// survives the proxy's cross-site login redirect (Lax), and a form posted
// from the public HTTPS origin passes the CSRF check — nothing checks
// Origin. A Host the proxy did not rewrite is refused (DNS rebinding).
func TestBehindReverseProxy(t *testing.T) {
	isolate(t)
	mkProfile(t, "cloud", "c", true)
	exe, _ := os.Executable()
	s, err := New(Options{Listen: "127.0.0.1:8400", SelfPath: exe, Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	h := s.middleware(s.mux)

	get := httptest.NewRequest("GET", "/", nil)
	get.Host = "127.0.0.1:8400"
	get.Header.Set("X-Forwarded-Proto", "https")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, get)
	if rec.Code != 200 {
		t.Fatalf("GET / behind the proxy: %d", rec.Code)
	}
	var csrf *http.Cookie
	for _, c := range rec.Result().Cookies() {
		if c.Name == csrfCookie {
			csrf = c
		}
	}
	if csrf == nil || csrf.SameSite != http.SameSiteLaxMode || !csrf.HttpOnly {
		t.Fatalf("csrf cookie %+v: want HttpOnly, SameSite=Lax", csrf)
	}

	post := httptest.NewRequest("POST", "/profiles/cloud/use", strings.NewReader("csrf="+csrf.Value))
	post.Host = "127.0.0.1:8400"
	post.Header.Set("Origin", "https://tfs.fatehi.dev")
	post.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	post.AddCookie(csrf)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, post)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("a form posted from the public origin: %d %s", rec.Code, rec.Body.String())
	}

	foreign := httptest.NewRequest("GET", "/", nil)
	foreign.Host = "tfs.fatehi.dev"
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, foreign)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("an unrewritten public Host: %d, want 403", rec.Code)
	}
}
