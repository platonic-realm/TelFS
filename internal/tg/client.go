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
// Each call to Run (and Login) spins up a fresh *telegram.Client because
// gotd's Client is single-shot: after one Run completes, its internal ctx
// is canceled and subsequent calls fail with "client already closed".
// That's fine for one-shot CLI commands.
//
// TODO(M3): the FUSE daemon will issue thousands of chunk ops per mount,
// so paying a full MTProto handshake per call is unworkable. When wiring
// M3 (read path), hold a single long-lived telegram.Client for the
// daemon's lifetime and expose the live *tg.Client to the chunk pipeline
// directly.
type Client struct {
	cfg *config.Config
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
	return &Client{cfg: cfg}, nil
}

// newTG builds a fresh underlying gotd client honoring cfg.DC and the
// session storage path.
func (c *Client) newTG() *telegram.Client {
	storage := &session.FileStorage{Path: c.cfg.SessionPath()}
	opts := telegram.Options{SessionStorage: storage}
	if c.cfg.DC != 0 {
		opts.DC = c.cfg.DC
	}
	return telegram.NewClient(c.cfg.APIID, c.cfg.APIHash, opts)
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
	tgc := c.newTG()
	err := tgc.Run(ctx, func(ctx context.Context) error {
		status, err := tgc.Auth().Status(ctx)
		if err != nil {
			return fmt.Errorf("auth status: %w", err)
		}
		if !status.Authorized {
			return ErrNotAuthorized
		}
		return fn(ctx, tgc.API())
	})
	if err == nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
