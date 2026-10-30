package web

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// Options configures a Server. Defaults are loopback-only, no token,
// no TLS — the most paranoid sane defaults.
type Options struct {
	// Listen address: "host:port". Empty → 127.0.0.1:8080.
	Listen string
	// Token: if non-empty, every request must carry
	// `Authorization: Bearer <token>` (constant-time compared). When
	// empty, Listen is forced to a loopback address.
	Token string
	// TLSCert / TLSKey: optional in-process TLS termination.
	TLSCert string
	TLSKey  string
	// Logger: defaults to log.Default.
	Logger *log.Logger
	// SelfPath: filesystem path to the running telfs binary. The mount
	// supervisor re-execs it to spawn `telfs mount` children. Defaults
	// to os.Executable() (with os.Args[0] as last resort) when empty.
	SelfPath string
}

// Server bundles the HTTP server, state, and lifecycle handles.
type Server struct {
	opts   Options
	state  *State
	logins *loginRegistry
	sup    *Supervisor
	repair *RepairTracker
	mux    *http.ServeMux
	srv    *http.Server
}

// New returns a configured but not-yet-running Server.
func New(opts Options) (*Server, error) {
	if opts.Listen == "" {
		opts.Listen = "127.0.0.1:8080"
	}
	if opts.Logger == nil {
		opts.Logger = log.Default()
	}
	if opts.Token == "" {
		if err := enforceLoopback(opts.Listen); err != nil {
			return nil, err
		}
	}
	selfPath := opts.SelfPath
	if selfPath == "" {
		// os.Executable resolves /proc/self/exe — immune to argv[0]
		// weirdness. Resolving argv[0] by hand broke for bare names
		// the shell found but Go's LookPath couldn't (e.g. literal
		// `~/...` PATH entries that the shell expands and LookPath
		// doesn't): the fallback made "telfs" absolute against the
		// cwd and errored with "self binary not found at ~/telfs".
		if exe, err := os.Executable(); err == nil {
			selfPath = exe
		} else {
			selfPath = os.Args[0]
		}
	}
	sup, err := NewSupervisor(selfPath)
	if err != nil {
		return nil, fmt.Errorf("supervisor: %w", err)
	}
	s := &Server{
		opts:   opts,
		state:  NewState(),
		logins: newLoginRegistry(),
		sup:    sup,
		repair: &RepairTracker{},
		mux:    http.NewServeMux(),
	}
	s.registerRoutes()
	s.srv = &http.Server{
		Addr:              opts.Listen,
		Handler:           s.middleware(s.mux),
		ReadHeaderTimeout: 15 * time.Second,
	}
	return s, nil
}

// Run blocks serving HTTP until ctx is canceled, then shuts down with
// a 10-second grace period.
func (s *Server) Run(ctx context.Context) error {
	scheme := "http"
	if s.opts.TLSCert != "" {
		scheme = "https"
	}
	s.opts.Logger.Printf("telfs web: listening on %s://%s", scheme, s.opts.Listen)
	if s.opts.Token == "" {
		s.opts.Logger.Print("telfs web: bound to loopback, no token required")
	} else {
		s.opts.Logger.Print("telfs web: token-protected (clients must send Authorization: Bearer)")
	}

	errCh := make(chan error, 1)
	go func() {
		if s.opts.TLSCert != "" {
			errCh <- s.srv.ListenAndServeTLS(s.opts.TLSCert, s.opts.TLSKey)
		} else {
			errCh <- s.srv.ListenAndServe()
		}
	}()
	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return s.srv.Shutdown(shutdownCtx)
	}
}

// middleware composes Host validation + auth + recover + request
// logging.
func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// DNS-rebinding defense: a malicious page can point its own
		// hostname at 127.0.0.1 and become same-origin with this UI
		// in the victim's browser — full read access plus a harvested
		// CSRF token for destructive POSTs (delete tree, export
		// session.json). Browsers always send the Host header of the
		// URL they *think* they're talking to, so rejecting foreign
		// hosts closes the hole. Non-browser clients (curl) send the
		// dial address, which passes.
		if !s.hostAllowed(r.Host) {
			http.Error(w, "forbidden: unrecognized Host header (DNS rebinding?)", http.StatusForbidden)
			return
		}
		// auth: Bearer token, when configured
		if s.opts.Token != "" {
			h := r.Header.Get("Authorization")
			const prefix = "Bearer "
			if !strings.HasPrefix(h, prefix) ||
				subtle.ConstantTimeCompare([]byte(h[len(prefix):]), []byte(s.opts.Token)) != 1 {
				w.Header().Set("WWW-Authenticate", `Bearer realm="telfs"`)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		// panic recover
		defer func() {
			if p := recover(); p != nil {
				s.opts.Logger.Printf("PANIC %s %s: %v", r.Method, r.URL.Path, p)
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}
		}()
		// The profile this request acts on (per browser, v1.1 W1).
		r, sel := withSelection(r)
		// request log (cheap)
		start := time.Now()
		ww := &statusRecorder{ResponseWriter: w, status: 200, profile: sel, path: r.URL.RequestURI()}
		if r.Method == http.MethodGet {
			// Every page's masthead carries the profile switcher (a POST
			// form): make sure the browser has a CSRF token. Reuses an
			// existing one — never rotates it under open forms.
			ww.csrf = s.state.EnsureCSRF(ww, r)
		}
		next.ServeHTTP(ww, r)
		s.opts.Logger.Printf("%s %s %d %s", r.Method, r.URL.Path, ww.status, time.Since(start))
	})
}

// hostAllowed reports whether the request's Host header names an
// address this server plausibly serves: loopback names/IPs, the
// configured listen host, or (any-host binds like 0.0.0.0) any
// literal IP — hostNAMES other than localhost are what rebinding
// attacks use, so those stay rejected unless they match the listen
// host exactly.
func (s *Server) hostAllowed(hostport string) bool {
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if host == "localhost" || host == "127.0.0.1" || host == "::1" || host == "[::1]" {
		return true
	}
	if listenHost, _, err := net.SplitHostPort(s.opts.Listen); err == nil {
		if strings.EqualFold(host, listenHost) {
			return true
		}
	}
	// A literal IP in Host means the client typed the address (or a
	// reverse proxy passed it through) — an attacker-controlled DNS
	// name can't produce it.
	if ip := net.ParseIP(strings.Trim(host, "[]")); ip != nil {
		return true
	}
	return false
}

type statusRecorder struct {
	http.ResponseWriter
	status  int
	profile string // the request's selected profile (selection.go)
	csrf    string // the browser's CSRF token (GET requests)
	path    string // the request URI (the profile switcher returns there)
}

func (sr *statusRecorder) WriteHeader(c int) {
	sr.status = c
	sr.ResponseWriter.WriteHeader(c)
}

// enforceLoopback refuses to bind to a non-loopback address unless a
// token is configured. Default-safe.
func enforceLoopback(listen string) error {
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		return fmt.Errorf("invalid --listen %q: %w", listen, err)
	}
	if host == "" {
		return fmt.Errorf("--listen %q has no host; use 127.0.0.1:%s to bind loopback", listen, listen)
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip != nil && ip.IsLoopback() {
		return nil
	}
	return fmt.Errorf("--listen %q binds to a non-loopback address without --token; either bind to 127.0.0.1 or set --token", listen)
}
