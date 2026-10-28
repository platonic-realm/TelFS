package tg

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/telegram/dcs"
)

// dialGate spaces out the connection attempts of a Client after they
// fail. gotd's pool (the upload connections) replaces a connection that
// dies at once — a refused or unroutable dial (network cut, REJECT,
// EMFILE), or a link closed before the server ever answered — with a new
// one in the SAME call, with no pause: one waiting upload part built
// ~100,000 connections a second, and every part in flight spun like
// that until the outage ended. Every dial of the client (main connection
// and pool) goes through the gate: after a failure the next attempt
// waits base, doubling per further failure up to max; a connection that
// delivers its first byte (the server answered the handshake) resets it.
type dialGate struct {
	dial      dcs.DialFunc
	base, max time.Duration

	mu       sync.Mutex
	failures int
	nextAt   time.Time // no dial starts before this
}

// Dial backoff after failed connection attempts.
const (
	dialBackoffBase = 200 * time.Millisecond
	dialBackoffMax  = 5 * time.Second
)

func newDialGate(dial dcs.DialFunc) *dialGate {
	if dial == nil {
		var d net.Dialer
		dial = d.DialContext
	}
	return &dialGate{dial: dial, base: dialBackoffBase, max: dialBackoffMax}
}

// Dial is a dcs.DialFunc.
func (g *dialGate) Dial(ctx context.Context, network, addr string) (net.Conn, error) {
	g.mu.Lock()
	wait := time.Until(g.nextAt)
	g.mu.Unlock()
	if wait > 0 {
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		case <-t.C:
		}
	}
	conn, err := g.dial(ctx, network, addr)
	if err != nil {
		if ctx.Err() == nil {
			g.failed()
		}
		return nil, err
	}
	return &gatedConn{Conn: conn, g: g}, nil
}

func (g *dialGate) failed() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.failures++
	d := g.max
	if g.failures <= 16 {
		if b := g.base << (g.failures - 1); b < d {
			d = b
		}
	}
	g.nextAt = time.Now().Add(d)
}

func (g *dialGate) succeeded() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.failures = 0
	g.nextAt = time.Time{}
}

// gatedConn reports to its gate whether the server ever answered: the
// first byte read is a success; an error or a close before it, a
// failure.
type gatedConn struct {
	net.Conn
	g     *dialGate
	state atomic.Int32 // 0 open, nothing read yet; 1 answered; 2 failed before an answer
}

func (c *gatedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		if c.state.CompareAndSwap(0, 1) {
			c.g.succeeded()
		}
	} else if err != nil && c.state.CompareAndSwap(0, 2) {
		c.g.failed()
	}
	return n, err
}

func (c *gatedConn) Close() error {
	if c.state.CompareAndSwap(0, 2) {
		c.g.failed()
	}
	return c.Conn.Close()
}
