package tg

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"

	"telfs/internal/config"
)

// Client wraps a gotd Telegram client with TelFS-specific operations.
//
// The underlying telegram.Client is not started until Run, Login, or one of
// the high-level helpers (ListChannels, PostMessage, ...) is called.
//
// TODO(M3): the public helpers (PostMessage, GetMessageText, ListChannels,
// SetChannel) each spin up their own MTProto session per call. That's fine
// for one-shot CLI commands but unsuitable for the FUSE data path, which
// will issue thousands of chunk ops per mount. When wiring M3 (read path),
// hold a single long-lived c.tg.Run for the daemon's lifetime and expose
// the live *tg.Client to the chunk pipeline directly.
type Client struct {
	cfg *config.Config
	tg  *telegram.Client
}

// ErrNotAuthorized is returned by Run when no valid local session exists.
// Callers should invoke Login first.
var ErrNotAuthorized = errors.New("not authorized — run 'telfs login' first")

// New constructs a Client. Returns an error if the config is missing the
// Telegram API credentials.
func New(cfg *config.Config) (*Client, error) {
	if err := cfg.RequireAPI(); err != nil {
		return nil, err
	}
	storage := &session.FileStorage{Path: cfg.SessionPath()}
	tgc := telegram.NewClient(cfg.APIID, cfg.APIHash, telegram.Options{
		SessionStorage: storage,
	})
	return &Client{cfg: cfg, tg: tgc}, nil
}

// Run executes fn within an authenticated MTProto session. If no session
// file exists, fn is not invoked and ErrNotAuthorized is returned without
// any network round-trip.
//
// gotd's telegram.Client.Run swallows context.Canceled to nil; we
// re-surface it via ctx.Err() so callers can distinguish a clean cancel
// from "the work succeeded with empty results."
func (c *Client) Run(ctx context.Context, fn func(ctx context.Context, api *tg.Client) error) error {
	if _, err := os.Stat(c.cfg.SessionPath()); errors.Is(err, os.ErrNotExist) {
		return ErrNotAuthorized
	}
	err := c.tg.Run(ctx, func(ctx context.Context) error {
		status, err := c.tg.Auth().Status(ctx)
		if err != nil {
			return fmt.Errorf("auth status: %w", err)
		}
		if !status.Authorized {
			return ErrNotAuthorized
		}
		return fn(ctx, c.tg.API())
	})
	if err == nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
