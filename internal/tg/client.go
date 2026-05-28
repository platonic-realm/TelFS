package tg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/gotd/contrib/middleware/floodwait"
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
	cfg *config.Config
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
	return &Client{cfg: cfg}, nil
}

// newTG builds a fresh underlying gotd client honoring cfg.DC and the
// session storage path. The FLOOD_WAIT middleware transparently retries
// any RPC that Telegram answers with FLOOD_WAIT_N (rate limit) after
// sleeping N seconds — without it, burst writes would surface raw
// tgerr errors as EIO to FUSE callers.
func (c *Client) newTG() *telegram.Client {
	storage := &session.FileStorage{Path: c.cfg.SessionPath()}
	waiter := floodwait.NewSimpleWaiter().
		WithMaxRetries(10).
		WithMaxWait(2 * time.Minute)
	opts := telegram.Options{
		SessionStorage: storage,
		Middlewares:    []telegram.Middleware{waiter},
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
		return fn(ctx, &Session{api: tgc.API(), cfg: c.cfg})
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
