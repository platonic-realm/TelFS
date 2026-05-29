// Package snapshot's journal_poster.go runs the background drainer
// that takes pending entries out of meta.journal and posts them to the
// channel as journal-op messages. Pair with the snapshot Manager: the
// snapshot is the periodic full image; the journal entries between
// snapshots are the WAL that recovery replays to close the up-to-
// snapshot-interval crash window.
package snapshot

import (
	"context"
	"fmt"
	"log"
	"time"

	"telfs/internal/meta"
	"telfs/internal/tg"
)

// DefaultPosterInterval is how often the poster drains the local
// journal table. We don't post per-op (each journal mutation would
// then synchronously block on a network RPC) — instead we batch
// every interval so a busy workload (e.g. `tar xf` extracting many
// small files) keeps front-end latency low while the back-end fills
// the channel WAL. Tradeoff: in-flight ops since the last drain are
// lost on crash, narrowing the window from 5 minutes to ~5 seconds.
const DefaultPosterInterval = 5 * time.Second

// Poster posts pending journal entries to the channel. One Poster
// per FS; co-runs alongside the snapshot Manager on the same gotd
// session.
type Poster struct {
	Meta     *meta.Store
	Session  *tg.Session
	Interval time.Duration
	Logger   *log.Logger
}

// Run drains the local journal periodically until ctx is canceled.
// Errors are logged but never abort the loop — best-effort posting
// is fine because the snapshot manager's full image is the
// authoritative recovery source and the journal is purely additive.
func (p *Poster) Run(ctx context.Context) error {
	interval := p.Interval
	if interval <= 0 {
		interval = DefaultPosterInterval
	}
	logger := p.Logger
	if logger == nil {
		logger = log.Default()
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			// Best-effort final drain so a clean shutdown gets every
			// op into the channel before the gotd session tears down.
			// Use a short standalone timeout — we don't want the
			// shutdown sequence to wedge here.
			drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := p.drainOnce(drainCtx); err != nil {
				logger.Printf("[journal-poster] final drain: %v", err)
			}
			cancel()
			return nil
		case <-t.C:
			if err := p.drainOnce(ctx); err != nil {
				logger.Printf("[journal-poster] drain: %v", err)
			}
		}
	}
}

// drainOnce reads every pending journal entry and posts them to the
// channel, marking each posted before moving on. We post serially —
// gotd serializes RPCs at the connection layer anyway, and journal
// entries are tiny enough that parallel uploads wouldn't meaningfully
// speed this up.
func (p *Poster) drainOnce(ctx context.Context) error {
	pending, err := p.Meta.PendingJournal(ctx)
	if err != nil {
		return fmt.Errorf("read pending: %w", err)
	}
	if len(pending) == 0 {
		return nil
	}
	fsUUID, err := p.Meta.FSUUID(ctx)
	if err != nil {
		return fmt.Errorf("fs_uuid: %w", err)
	}
	for _, e := range pending {
		_, err := p.Session.UploadJournalOp(ctx, e.OpJSON, e.Seq, time.Now().Unix(), fsUUID)
		if err != nil {
			// Stop on first error — likely transient (network blip,
			// FLOOD_WAIT). Next tick will retry from where we stopped.
			return fmt.Errorf("post seq=%d: %w", e.Seq, err)
		}
		if err := p.Meta.MarkJournalPosted(ctx, e.Seq, time.Now().Unix()); err != nil {
			return fmt.Errorf("mark posted seq=%d: %w", e.Seq, err)
		}
	}
	return nil
}
