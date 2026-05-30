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

// DefaultPosterBatchSize is how many journal entries get packed
// into one TG channel message. ~200 bytes per op + JSON overhead
// means 1000 ops ≈ 250 KB body — small enough that one batch
// upload finishes in well under a second on residential uplinks,
// large enough that a 10k-file unzip becomes 10 channel messages
// instead of 10000.
const DefaultPosterBatchSize = 1000

// Poster posts pending journal entries to the channel. One Poster
// per FS; co-runs alongside the snapshot Manager on the same gotd
// session.
type Poster struct {
	Meta      *meta.Store
	Session   *tg.Session
	Interval  time.Duration
	BatchSize int // 0 → DefaultPosterBatchSize
	Logger    *log.Logger
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

// drainOnce reads pending journal entries and posts them to the
// channel in BATCHES — one TG message per batch instead of one per
// op. A 10k-file unzip used to generate 10k+ TG messages here;
// with batching it's ~10 (depending on BatchSize).
//
// Each batch is one UploadJournalOpBatch call. If a batch fails
// mid-loop, the entries in batches BEFORE the failure are already
// marked posted; the failed batch's entries stay unposted and the
// next tick retries them. Per-batch (not per-op) granularity means
// up to BatchSize ops might be re-posted after a transient error,
// which is acceptable: recovery dedupes by seq.
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
	batchSize := p.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultPosterBatchSize
	}
	for start := 0; start < len(pending); start += batchSize {
		end := start + batchSize
		if end > len(pending) {
			end = len(pending)
		}
		batch := pending[start:end]
		entries := make([]tg.JournalBatchEntry, len(batch))
		for i, e := range batch {
			entries[i] = tg.JournalBatchEntry{Seq: e.Seq, Op: e.OpJSON}
		}
		if _, err := p.Session.UploadJournalOpBatch(ctx, entries, time.Now().Unix(), fsUUID); err != nil {
			return fmt.Errorf("post batch first_seq=%d last_seq=%d: %w",
				batch[0].Seq, batch[len(batch)-1].Seq, err)
		}
		// Mark every seq in the batch posted. We do this serially
		// after the upload returns success; if MarkJournalPosted
		// fails partway, the next tick re-posts the not-yet-marked
		// tail (same recovery model as per-op behavior). At most
		// batchSize ops are duplicated on the channel for a single
		// crash — orphan dedup is `gc`'s job.
		nowUnix := time.Now().Unix()
		for _, e := range batch {
			if err := p.Meta.MarkJournalPosted(ctx, e.Seq, nowUnix); err != nil {
				return fmt.Errorf("mark posted seq=%d: %w", e.Seq, err)
			}
		}
	}
	return nil
}
