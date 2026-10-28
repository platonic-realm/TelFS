package tg

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/pool"
	"github.com/gotd/td/tg"
)

// refusingDial fails at once, as a dial into a cut network does, and
// counts its calls.
type refusingDial struct{ n atomic.Int64 }

func (d *refusingDial) dial(context.Context, string, string) (net.Conn, error) {
	d.n.Add(1)
	return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
}

func testGate(dial func(context.Context, string, string) (net.Conn, error)) *dialGate {
	g := newDialGate(dial)
	g.base, g.max = 20*time.Millisecond, 100*time.Millisecond
	return g
}

// Many callers dialing into a dead network are spaced out: a bounded
// number of attempts, not a hot loop.
func TestDialGateBoundsFailingDials(t *testing.T) {
	d := &refusingDial{}
	g := testGate(d.dial)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				g.Dial(ctx, "tcp", "149.154.167.50:443")
			}
		}()
	}
	wg.Wait()
	if n := d.n.Load(); n > 200 || n < 8 {
		t.Fatalf("%d dials in 1 s from 8 callers, want a bounded number (≤ 200)", n)
	}
}

// fakeConn is a gotd pool connection whose Run dials (through the gate)
// and dies at once, as the pool's connections do when the network is
// cut.
type fakeConn struct {
	g     *dialGate
	ready chan struct{}
}

func (c *fakeConn) Run(ctx context.Context) error {
	conn, err := c.g.Dial(ctx, "tcp", "149.154.167.50:443")
	if err != nil {
		return err
	}
	conn.Close()
	return errors.New("connection closed")
}

func (c *fakeConn) Invoke(context.Context, bin.Encoder, bin.Decoder) error { return nil }
func (c *fakeConn) Ping(context.Context) error                             { return nil }
func (c *fakeConn) Ready() <-chan struct{}                                 { return c.ready }

// The review's reproduction: one call on gotd's pool (8 connections)
// whose connections die at once redials in a hot loop inside the call
// (~100,000 connections a second). Through the gate the dials stay few.
func TestDialGateThroughGotdPool(t *testing.T) {
	d := &refusingDial{}
	g := testGate(d.dial)
	var made atomic.Int64
	dc := pool.NewDC(context.Background(), 2, func() pool.Conn {
		made.Add(1)
		return &fakeConn{g: g, ready: make(chan struct{})}
	}, pool.DCOptions{MaxOpenConnections: 8})
	defer dc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var out tg.BoolBox
	if err := dc.Invoke(ctx, &tg.UploadSaveBigFilePartRequest{}, &out); err == nil {
		t.Fatal("a call on dead connections succeeded")
	}
	if n := d.n.Load(); n > 100 {
		t.Fatalf("%d dials (%d connections) in 1 s for one call, want a bounded number", n, made.Load())
	}
}

// A connection the server closes before answering counts as a failure
// (the next dial waits); one that answers resets the gate.
func TestDialGateHandshake(t *testing.T) {
	var server net.Conn
	g := testGate(func(context.Context, string, string) (net.Conn, error) {
		c, s := net.Pipe()
		server = s
		return c, nil
	})
	ctx := context.Background()
	c, err := g.Dial(ctx, "tcp", "x")
	if err != nil {
		t.Fatal(err)
	}
	server.Close() // closed before any answer
	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatal("read from a closed pipe")
	}
	start := time.Now()
	c, _ = g.Dial(ctx, "tcp", "x")
	if waited := time.Since(start); waited < 15*time.Millisecond {
		t.Fatalf("the dial after an unanswered connection waited %v, want the backoff", waited)
	}
	go server.Write([]byte{1}) // the server answers
	if n, err := c.Read(make([]byte, 1)); n != 1 || err != nil {
		t.Fatalf("read %d %v", n, err)
	}
	start = time.Now()
	if _, err := g.Dial(ctx, "tcp", "x"); err != nil {
		t.Fatal(err)
	}
	if waited := time.Since(start); waited > 10*time.Millisecond {
		t.Fatalf("after an answered connection the next dial waited %v", waited)
	}
	// A canceled wait returns at once.
	g.failed()
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := g.Dial(cctx, "tcp", "x"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled dial: %v", err)
	}
}

// hangingPool is an upload pool whose connections never answer.
type hangingPool struct{ calls atomic.Int64 }

func (h *hangingPool) Invoke(ctx context.Context, _ bin.Encoder, _ bin.Decoder) error {
	h.calls.Add(1)
	<-ctx.Done()
	return ctx.Err()
}
func (h *hangingPool) Close() error { return nil }

// When no upload connection answers (they cannot be opened while the
// main one works), the pool is suspended and uploads go over the main
// connection instead of stalling; a call that succeeds ends it.
func TestUploadPoolSuspendsWhenNothingAnswers(t *testing.T) {
	hang := &hangingPool{}
	p := &uploadPool{inv: hang, callTimeout: 50 * time.Millisecond}
	c := &Client{tracker: NewTracker(16), rateLimit: NewRateLimiter(0, 0), byteLimit: NewByteLimiter(0)}
	mainTG := &fakeTelegram{}
	s := uploadTestSession(2)
	s.swapAPI(tg.NewClient(mainTG))
	s.swapUploadAPI(c.wrapUploadInvoker(p), p)
	stderr := captureStderr(t)
	body := bytes.Repeat([]byte{1}, 3*uploadPartMax)
	id, err := s.UploadDocument(context.Background(), bytes.NewReader(body), "blob", "c")
	out := stderr()
	if err != nil || id != 77 {
		t.Fatalf("upload: %d %v", id, err)
	}
	if hang.calls.Load() == 0 {
		t.Fatal("the pool was never tried")
	}
	if n := mainTG.count("UploadSaveBigFilePartRequest"); n != 3 {
		t.Fatalf("main connection got %d parts, want all 3 after the pool was suspended", n)
	}
	if p.usable() {
		t.Fatal("the pool must be suspended")
	}
	if !bytes.Contains([]byte(out), []byte("uploads use the main connection")) {
		t.Fatalf("stderr %q: the suspension is not said", out)
	}
	if _, threads := s.partsAPI(tg.NewClient(mainTG)); threads != 1 {
		t.Fatalf("suspended: %d threads, want main's 1", threads)
	}
	// The pool answers again: usable.
	p.inv = fakeCloser{&fakeTelegram{}}
	var ok tg.BoolBox
	if err := p.Invoke(context.Background(), &tg.UploadSaveBigFilePartRequest{Bytes: []byte{1}}, &ok); err != nil {
		t.Fatal(err)
	}
	if !p.usable() {
		t.Fatal("a call that succeeded must end the suspension")
	}
	if _, threads := s.partsAPI(tg.NewClient(mainTG)); threads != 2 {
		t.Fatalf("after the pool answered: %d threads, want the pool's 2", threads)
	}
}
