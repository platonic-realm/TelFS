package tg

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"
)

// SessionManager owns the long-lived Session struct in daemon mode
// and re-runs gotd's telegram.Client.Run under the covers on each
// reconnect. Callers hold a stable *Session via Session(); the
// underlying *tg.Client swaps atomically when the manager
// re-establishes the MTProto link.
//
// Two reasons to reconnect: (1) Run returns (gotd's client crashed,
// network glitch, etc.), (2) external request via Reconnect() from
// the stall watchdog. Either way the manager builds a new
// telegram.Client, runs it, and swaps it into the Session on
// successful first connect.
type SessionManager struct {
	client    *Client
	session   *Session
	reconnect chan struct{}
	stop      chan struct{}
	connected atomic.Bool
}

// NewSessionManager constructs an unstarted manager. Run starts the
// connect loop.
func (c *Client) NewSessionManager() *SessionManager {
	return &SessionManager{
		client:    c,
		session:   c.newSession(),
		reconnect: make(chan struct{}, 1),
		stop:      make(chan struct{}),
	}
}

// Session returns the long-lived session handle. The underlying
// gotd client may be nil during reconnect windows; methods on
// Session return ErrNotConnected in that case, which callers should
// treat as transient.
func (m *SessionManager) Session() *Session { return m.session }

// Connected reports whether the manager has a live gotd client
// bound to the Session right now.
func (m *SessionManager) Connected() bool { return m.connected.Load() }

// Reconnect signals the manager to tear down the current gotd
// connection and re-establish a fresh one. Non-blocking — multiple
// concurrent calls coalesce.
func (m *SessionManager) Reconnect() {
	select {
	case m.reconnect <- struct{}{}:
	default:
	}
}

// Run blocks until ctx is canceled, repeatedly running gotd's
// telegram.Client.Run inside a loop. Each iteration: build a new
// gotd client (with our middleware chain), swap it into the Session,
// wait for either ctx cancel or a Reconnect signal, then unwind.
//
// On error from gotd's Run, backs off briefly before retrying so we
// don't spin if the auth file is missing or the DC is unreachable.
//
// The first connect must succeed before returning success to the
// caller, so initialError surfaces an early-exit condition like
// ErrNotAuthorized.
func (m *SessionManager) Run(ctx context.Context, onFirstConnect func() error) error {
	first := true
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, err := os.Stat(m.client.cfg.SessionPath()); errors.Is(err, os.ErrNotExist) {
			return ErrNotAuthorized
		}
		// One gotd client per iteration. The session.json carries
		// auth state across reconnects so we don't re-do the auth
		// flow.
		tgc := m.client.newTG()
		runCtx, cancelRun := context.WithCancel(ctx)
		runErr := make(chan error, 1)
		go func() {
			runErr <- tgc.Run(runCtx, func(innerCtx context.Context) error {
				status, err := tgc.Auth().Status(innerCtx)
				if err != nil {
					return fmt.Errorf("auth status: %w", err)
				}
				if !status.Authorized {
					return ErrNotAuthorized
				}
				// Bind the new client to the stable Session so
				// callers' next call picks it up.
				m.session.swapAPI(tgc.API())
				// The upload pool lives exactly as long as this
				// connection cycle: opened on its client, unbound and
				// closed when the cycle ends (a reconnect builds the
				// next one). Until it is bound, and if it can't be
				// opened, uploads use the main connection. A read-only
				// session uploads nothing and gets none.
				if !m.session.ReadOnly() {
					pool, upAPI, err := m.client.openUploadPool(tgc)
					switch {
					case err != nil:
						fmt.Fprintf(os.Stderr, "[upload] no upload connections this session (%v); uploads use the main connection\n", err)
					case pool != nil:
						m.session.swapUploadAPI(upAPI)
						defer func() {
							m.session.swapUploadAPI(nil)
							closePoolBounded(pool, uploadPoolCloseWait)
						}()
					}
				}
				m.connected.Store(true)
				if first {
					first = false
					if onFirstConnect != nil {
						if err := onFirstConnect(); err != nil {
							return err
						}
					}
				}
				select {
				case <-innerCtx.Done():
					return nil
				case <-m.reconnect:
					return errReconnect
				}
			})
		}()
		select {
		case <-ctx.Done():
			cancelRun()
			<-runErr
			m.session.swapAPI(nil)
			m.connected.Store(false)
			return ctx.Err()
		case err := <-runErr:
			cancelRun()
			m.session.swapAPI(nil)
			m.connected.Store(false)
			if errors.Is(err, ErrNotAuthorized) {
				return err
			}
			if errors.Is(err, errReconnect) {
				fmt.Fprintln(os.Stderr, "[reconnect] tearing down MTProto session and re-establishing…")
				continue
			}
			if first {
				// First connect failed for a reason other than
				// ErrNotAuthorized — surface so cmdMount aborts
				// the mount rather than spinning forever.
				if err == nil {
					return errors.New("tg: session ended before first connect")
				}
				return err
			}
			// Subsequent failures: back off and retry. gotd's Run
			// returning at all here usually means a transport-level
			// error we can recover from.
			fmt.Fprintf(os.Stderr, "[reconnect] session ended (%v); reconnecting in 5s…\n", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
			}
		}
	}
}

// uploadPoolCloseWait bounds how long a cycle's end waits for its upload
// connections to close. A reconnect is often the answer to a wedged
// connection; it must not wedge on one in turn.
const uploadPoolCloseWait = 10 * time.Second

// closePoolBounded closes p, waiting at most d for its connections to end
// (the rest finish in the background).
func closePoolBounded(p io.Closer, d time.Duration) {
	done := make(chan struct{})
	go func() {
		_ = p.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		fmt.Fprintf(os.Stderr, "[upload] upload connections still closing after %v; going on\n", d)
	}
}

// errReconnect is the sentinel returned from the inner gotd callback
// when Reconnect() fires. Triggers the outer loop to re-run.
var errReconnect = errors.New("tg: reconnect requested")
