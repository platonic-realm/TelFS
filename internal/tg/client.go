package tg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"

	"telfs/internal/config"
)

// Client is a façade over gotd's telegram.Client. It is responsible for:
//
//   - One-shot CLI operations (PostMessage, ListChannels, ...). Each one
//     opens a fresh telegram.Client via RunSession, runs the op, and
//     closes the client. gotd's *telegram.Client is single-shot — once
//     its Run completes, the next Run on the same instance fails with
//     "client already closed".
//
//   - Long-lived daemon mode: RunSession exposes a *Session to a callback
//     that lives as long as the callback runs. The FUSE mount uses this
//     to hold one MTProto connection for the daemon's lifetime.
//
// The teardown contract: callers of RunSession MUST ensure that all of
// their consumers (background goroutines that issue Session ops) have
// drained before they return from the callback. After return, the
// Session's underlying client is closed and any further use will fail.
type Client struct {
	cfg       *config.Config
	tracker   *Tracker
	rateLimit *RateLimiter
	// byteLimit caps upload parts in bytes/s ([upload] max_rate), on
	// every connection of this client.
	byteLimit *ByteLimiter
	// upload is [upload] resolved: the upload pool's size and the parts
	// in flight per upload.
	upload config.UploadSettings
	// storage holds the MTProto session. Since P1 it is always the
	// encrypted session.enc (internal/sessionstore); the caller opens
	// it with the profile's secret and passes it in.
	storage session.Storage
	// dials spaces out connection attempts after failures (every
	// connection of the client: main and upload pool).
	dials *dialGate
}

// ErrNotAuthorized is returned by RunSession when no valid local session
// file exists. Callers should invoke Login first.
var ErrNotAuthorized = errors.New("not authorized — run 'telfs login' first")

// New constructs a Client over the given session storage (the profile's
// session.enc, opened with its secret). Returns an error if the config
// is missing the Telegram API credentials or no storage is given.
func New(cfg *config.Config, storage session.Storage) (*Client, error) {
	if err := cfg.RequireAPI(); err != nil {
		return nil, err
	}
	if storage == nil {
		return nil, errors.New("tg: no session storage (open session.enc with the profile's secret first)")
	}
	up, err := cfg.UploadSettings()
	if err != nil {
		return nil, err
	}
	return &Client{
		cfg:       cfg,
		tracker:   NewTracker(128),
		rateLimit: NewRateLimiter(0, 0),
		byteLimit: NewByteLimiter(up.MaxRate),
		upload:    up,
		storage:   storage,
		dials:     newDialGate(nil),
	}, nil
}

// UploadSettings is the client's [upload]: pool size, parts in flight per
// upload, byte-rate cap.
func (c *Client) UploadSettings() config.UploadSettings { return c.upload }

// newSession is a Session of this client, with no connection bound yet.
func (c *Client) newSession() *Session {
	return &Session{cfg: c.cfg, tracker: c.tracker, uploadThreads: c.upload.PartsPerUpload}
}

// Tracker returns the request log + connection-state tracker. The
// daemon hands this to the web layer for /telegram visibility.
func (c *Client) Tracker() *Tracker { return c.tracker }

// RateLimiter returns the shared rate limiter. The daemon spawns a
// poller that watches the meta_kv key and applies SetPerMinute(N) so
// changes made via CLI or web take effect without a remount.
func (c *Client) RateLimiter() *RateLimiter { return c.rateLimit }

// ByteLimiter is the process-wide byte-rate cap on upload parts ([upload]
// max_rate; changeable while running).
func (c *Client) ByteLimiter() *ByteLimiter { return c.byteLimit }

// newTG builds a fresh underlying gotd client honoring cfg.DC and the
// session storage path. Our FloodWaiter (internal/tg/floodwait.go)
// replaces gotd/contrib's SimpleWaiter — the contrib version capped
// waits at 2 min and bounced anything longer, leaving callers to
// retry in tight loops that never made progress. Ours honors waits
// up to MaxFloodWait (30 min) and logs progress so a long sleep is
// visible in the daemon log instead of looking wedged.
func (c *Client) newTG() *telegram.Client {
	storage := c.storage
	// Middleware order is bottom-up: the LAST entry in the slice
	// wraps the lowest level. TrackingMiddleware sits BELOW
	// FloodWaiter so it records the raw FLOOD_WAIT response (before
	// the waiter swallows it via retry). That way the request log
	// shows every wire-level attempt, not just the user-facing
	// outcome.
	// Middlewares are evaluated from FIRST to LAST in the slice with
	// the FIRST entry wrapping everything below it. So RateLimiter
	// (first) is the outermost gate: requests wait at the limiter
	// BEFORE flooding into FloodWaiter's retry loop or being counted
	// by TrackingMiddleware as "in flight". That keeps the daemon's
	// outbound rate bounded by the operator-controlled budget rather
	// than by Telegram's punitive FLOOD_WAITs. The ByteLimiter comes
	// next: upload parts wait there for their bytes ([upload] max_rate),
	// also before they count as in flight.
	opts := telegram.Options{
		SessionStorage: storage,
		Middlewares:    c.middlewares(),
	}
	if c.dials != nil {
		// gotd's default resolver, dialing through the gate.
		opts.Resolver = dcs.Plain(dcs.PlainOptions{Dial: c.dials.Dial})
	}
	if c.cfg.DC != 0 {
		opts.DC = c.cfg.DC
	}
	return telegram.NewClient(c.cfg.APIID, c.cfg.APIHash, opts)
}

// middlewares is TelFS's invoker chain, outermost first (see newTG for the
// order): the RPC-per-minute limiter, the upload byte-rate cap, the flood
// waiter, the tracker. The main connection gets it through
// telegram.Options; the upload pool through wrapUploadInvoker — gotd's
// Client.Pool invokes its connections directly, WITHOUT the client's
// middlewares, so without that wrapper uploads would skip flood waits,
// the limiters and the tracker (status, adaptive limiter, stall watchdog).
func (c *Client) middlewares() []telegram.Middleware {
	return []telegram.Middleware{
		c.rateLimit,
		c.byteLimit,
		NewFloodWaiter(),
		&TrackingMiddleware{Tracker: c.tracker},
	}
}

// chainInvoker wraps inv in mws, the first outermost (gotd's own order).
func chainInvoker(inv tg.Invoker, mws ...telegram.Middleware) tg.Invoker {
	for i := len(mws) - 1; i >= 0; i-- {
		inv = mws[i].Handle(inv)
	}
	return inv
}

// wrapUploadInvoker is an API over raw (an upload pool's invoker) that
// runs every call through the client's middleware chain.
func (c *Client) wrapUploadInvoker(raw tg.Invoker) *tg.Client {
	return tg.NewClient(chainInvoker(raw, c.middlewares()...))
}

// uploadPool is one connection cycle's pool of upload connections (gotd
// Client.Pool: N connections to the account's data centre, sharing the
// main connection's authorization). Once closed, its errors read as
// ErrNotConnected, which UploadDocument retries — on the next cycle's
// pool or on the main connection.
//
// A call that gets no answer within callTimeout (no connection of the
// pool could be made or none answered — gotd's pool keeps redialing
// inside the call) suspends the pool: uploads use the main connection
// for a while (1 min, doubling to 10 min while it keeps failing; a
// call that succeeds ends it), so they do not stall behind connections
// that cannot be opened while the main one works.
type uploadPool struct {
	inv         telegram.CloseInvoker
	closed      atomic.Bool
	callTimeout time.Duration // 0 = uploadPoolCallTimeout

	mu        sync.Mutex
	until     time.Time     // suspended until
	suspendIn time.Duration // the next suspension's length
}

// Upload pool breaker.
const (
	uploadPoolCallTimeout = 2 * time.Minute
	uploadPoolSuspendMin  = time.Minute
	uploadPoolSuspendMax  = 10 * time.Minute
)

// Invoke implements tg.Invoker.
func (p *uploadPool) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	if p.closed.Load() {
		return ErrNotConnected
	}
	limit := p.callTimeout
	if limit <= 0 {
		limit = uploadPoolCallTimeout
	}
	cctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	err := p.inv.Invoke(cctx, input, output)
	switch {
	case err == nil:
		p.answered()
		return nil
	case p.closed.Load():
		return fmt.Errorf("%w (upload connections closed: %v)", ErrNotConnected, err)
	case ctx.Err() == nil && cctx.Err() != nil:
		// Our bound, not the caller's: nothing of the pool answered.
		d := p.suspend()
		return fmt.Errorf("%w (no upload connection answered within %s; uploads use the main connection for %s)", ErrNotConnected, limit, d)
	}
	return err
}

// usable reports whether uploads may go through the pool now.
func (p *uploadPool) usable() bool {
	if p.closed.Load() {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return !time.Now().Before(p.until)
}

func (p *uploadPool) suspend() time.Duration {
	p.mu.Lock()
	d := p.suspendIn
	if d < uploadPoolSuspendMin {
		d = uploadPoolSuspendMin
	}
	p.suspendIn = min(2*d, uploadPoolSuspendMax)
	newly := !time.Now().Before(p.until)
	p.until = time.Now().Add(d)
	p.mu.Unlock()
	if newly {
		fmt.Fprintf(os.Stderr, "[upload] no upload connection answers; uploads use the main connection for %s\n", d)
	}
	return d
}

func (p *uploadPool) answered() {
	p.mu.Lock()
	p.suspendIn = 0
	p.until = time.Time{}
	p.mu.Unlock()
}

// Close closes the pool's connections (waits for them to end).
func (p *uploadPool) Close() error {
	p.closed.Store(true)
	return p.inv.Close()
}

// openUploadPool opens the upload pool of a connection cycle on tgc (a
// running client) and returns it with the API that uploads through it.
// (nil, nil, nil) when [upload] connections = 0. gotd dials the pool's
// connections lazily, on the first uploads.
func (c *Client) openUploadPool(tgc *telegram.Client) (*uploadPool, *tg.Client, error) {
	if c.upload.Connections <= 0 {
		return nil, nil, nil
	}
	inv, err := tgc.Pool(int64(c.upload.Connections))
	if err != nil {
		return nil, nil, err
	}
	p := &uploadPool{inv: inv}
	return p, c.wrapUploadInvoker(p), nil
}

// RunSession runs fn within an authenticated MTProto session. fn receives
// a *Session valid only for the duration of the callback. If no local
// session file exists, fn is not invoked and ErrNotAuthorized is returned
// without any network round-trip.
//
// gotd's telegram.Client.Run swallows context.Canceled to nil; we
// re-surface it via ctx.Err() so callers can distinguish a clean cancel
// from "the work succeeded with empty results."
func (c *Client) RunSession(ctx context.Context, fn func(ctx context.Context, sess *Session) error) error {
	if _, err := os.Stat(c.cfg.SessionPath()); errors.Is(err, os.ErrNotExist) {
		return ErrNotAuthorized
	}
	tgc := c.newTG()
	err := tgc.Run(ctx, func(ctx context.Context) error {
		status, err := tgc.Auth().Status(ctx)
		if err != nil {
			return fmt.Errorf("auth status: %w", err)
		}
		if !status.Authorized {
			return ErrNotAuthorized
		}
		sess := c.newSession()
		sess.api = tgc.API()
		return fn(ctx, sess)
	})
	if err == nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// ListChannels is a one-shot wrapper around Session.ListChannels.
func (c *Client) ListChannels(ctx context.Context) ([]ChannelInfo, error) {
	var out []ChannelInfo
	err := c.RunSession(ctx, func(ctx context.Context, s *Session) error {
		var err error
		out, err = s.ListChannels(ctx)
		return err
	})
	return out, err
}

// SetChannel is a one-shot wrapper around Session.SetChannel.
func (c *Client) SetChannel(ctx context.Context, id, accessHash int64) (ChannelInfo, error) {
	var out ChannelInfo
	err := c.RunSession(ctx, func(ctx context.Context, s *Session) error {
		var err error
		out, err = s.SetChannel(ctx, id, accessHash)
		return err
	})
	return out, err
}

// PostMessage is a one-shot wrapper around Session.PostMessage.
func (c *Client) PostMessage(ctx context.Context, text string) (int, error) {
	var id int
	err := c.RunSession(ctx, func(ctx context.Context, s *Session) error {
		var err error
		id, err = s.PostMessage(ctx, text)
		return err
	})
	return id, err
}

// GetMessageText is a one-shot wrapper around Session.GetMessageText.
func (c *Client) GetMessageText(ctx context.Context, msgID int) (string, error) {
	var text string
	err := c.RunSession(ctx, func(ctx context.Context, s *Session) error {
		var err error
		text, err = s.GetMessageText(ctx, msgID)
		return err
	})
	return text, err
}
