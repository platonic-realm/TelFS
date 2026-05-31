package tg

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"

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
}

// ErrNotAuthorized is returned by RunSession when no valid local session
// file exists. Callers should invoke Login first.
var ErrNotAuthorized = errors.New("not authorized — run 'telfs login' first")

// New constructs a Client. Returns an error if the config is missing the
// Telegram API credentials.
func New(cfg *config.Config) (*Client, error) {
	if err := cfg.RequireAPI(); err != nil {
		return nil, err
	}
	return &Client{
		cfg:       cfg,
		tracker:   NewTracker(128),
		rateLimit: NewRateLimiter(0, 0),
	}, nil
}

// Tracker returns the request log + connection-state tracker. The
// daemon hands this to the web layer for /telegram visibility.
func (c *Client) Tracker() *Tracker { return c.tracker }

// RateLimiter returns the shared rate limiter. The daemon spawns a
// poller that watches the meta_kv key and applies SetPerMinute(N) so
// changes made via CLI or web take effect without a remount.
func (c *Client) RateLimiter() *RateLimiter { return c.rateLimit }

// newTG builds a fresh underlying gotd client honoring cfg.DC and the
// session storage path. Our FloodWaiter (internal/tg/floodwait.go)
// replaces gotd/contrib's SimpleWaiter — the contrib version capped
// waits at 2 min and bounced anything longer, leaving callers to
// retry in tight loops that never made progress. Ours honors waits
// up to MaxFloodWait (30 min) and logs progress so a long sleep is
// visible in the daemon log instead of looking wedged.
func (c *Client) newTG() *telegram.Client {
	storage := &session.FileStorage{Path: c.cfg.SessionPath()}
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
	// than by Telegram's punitive FLOOD_WAITs.
	opts := telegram.Options{
		SessionStorage: storage,
		Middlewares: []telegram.Middleware{
			c.rateLimit,
			NewFloodWaiter(),
			&TrackingMiddleware{Tracker: c.tracker},
		},
	}
	if c.cfg.DC != 0 {
		opts.DC = c.cfg.DC
	}
	return telegram.NewClient(c.cfg.APIID, c.cfg.APIHash, opts)
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
		return fn(ctx, &Session{api: tgc.API(), cfg: c.cfg, tracker: c.tracker})
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
