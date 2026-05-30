package metrics

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

// Server is a minimal HTTP server that serves /metrics from the
// Default registry. It runs in the same process as the mount daemon
// so counters incremented by chunk uploads, downloads, FLOOD_WAITs,
// and gauge providers (cache size, pending uploads) reflect that
// daemon's live state — there is no IPC.
//
// Bind addr is up to the caller; 127.0.0.1:9090 is the default in
// `telfs mount --metrics-addr`. Off by default; opt-in keeps the
// surface of the mount daemon small.
type Server struct {
	addr   string
	srv    *http.Server
	done   chan struct{}
	listen net.Listener
}

// NewServer constructs an unstarted Server bound to addr. Use
// Start to actually listen; Shutdown to drain.
func NewServer(addr string) *Server {
	return &Server{addr: addr, done: make(chan struct{})}
}

// Start opens the listener and begins serving in a goroutine. Returns
// an error if the bind fails. The returned Server can then be
// Shutdown by the daemon's teardown.
func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_ = Default.WriteProm(w)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintln(w, "telfs metrics — see /metrics")
	})
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("metrics: listen %s: %w", s.addr, err)
	}
	s.listen = ln
	s.srv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		defer close(s.done)
		if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Printf("metrics: serve: %v\n", err)
		}
	}()
	return nil
}

// Addr returns the bound address (useful in tests where addr=":0").
func (s *Server) Addr() string {
	if s.listen != nil {
		return s.listen.Addr().String()
	}
	return s.addr
}

// Shutdown stops the server, capped at ctx's deadline.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.srv == nil {
		return nil
	}
	err := s.srv.Shutdown(ctx)
	<-s.done
	return err
}
