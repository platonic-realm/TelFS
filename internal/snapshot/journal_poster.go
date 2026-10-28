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

	"github.com/gotd/td/tgerr"

	"telfs/internal/meta"
	"telfs/internal/tg"
	"telfs/internal/wire"
)

// channelLost reports whether err means the bound channel is
// unreachable for this session's identity (kicked from the channel,
// identity-scoped access_hash gone stale).
// NOT transient: retrying on the normal cadence can't succeed until
// the operator fixes the binding or identity, so callers back off and
// say so once instead of spamming the log every tick.
func channelLost(err error) bool {
	return tgerr.Is(err, "CHANNEL_INVALID") || tgerr.Is(err, "CHANNEL_PRIVATE")
}

// Channel-lost backoff bounds for the poster loop.
const (
	channelLostBackoffMin = 1 * time.Minute
	channelLostBackoffMax = 5 * time.Minute
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

// DefaultPosterBatchBytes2 bounds a FORMAT-2 batch by the plain JSON of
// its ops instead of by count. Format-2 bodies are zstd-compressed
// (about 9:1 on journal JSON), so 1,000 ops came out at ~11 KB — the
// 1,000-op cap above was sized for ~250 KB of PLAIN JSON, and an rsync
// of 50,000 files took 400 messages (P1 scale pilot). 2 MiB of op JSON
// compresses to roughly 200–250 KB: the size the cap was meant to give.
const DefaultPosterBatchBytes2 = 2 << 20

// Poster posts pending journal entries to the channel. One Poster
// per FS; co-runs alongside the snapshot Manager on the same gotd
// session.
type Poster struct {
	Meta      *meta.Store
	Session   *tg.Session
	Interval  time.Duration
	BatchSize int // 0 → DefaultPosterBatchSize
	Logger    *log.Logger
	// Keys, when set, posts FORMAT-2 batches: body sealed under the
	// meta key, `blob` document, encrypted caption (decision D3 — the
	// journal carried names, folders and sizes in clear before).
	Keys *wire.Keys
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
	// Channel-lost backoff state: when the channel is unreachable for
	// this identity (CHANNEL_INVALID / CHANNEL_PRIVATE), retrying every
	// interval is pure log spam — nothing succeeds until the operator
	// fixes the binding. Log once per backoff window and double it up
	// to the cap. Any success (or non-lost error) resets.
	var lostUntil time.Time
	var lostBackoff time.Duration
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
			if time.Now().Before(lostUntil) {
				continue
			}
			err := p.drainOnce(ctx)
			switch {
			case err == nil:
				lostBackoff = 0
			case channelLost(err):
				if lostBackoff == 0 {
					lostBackoff = channelLostBackoffMin
				} else if lostBackoff *= 2; lostBackoff > channelLostBackoffMax {
					lostBackoff = channelLostBackoffMax
				}
				lostUntil = time.Now().Add(lostBackoff)
				logger.Printf("[journal-poster] channel unreachable for this identity (%v) — pausing %s. Fix the channel binding or identity (`telfs status`, `telfs channel set`), journal entries stay queued locally.", err, lostBackoff)
			default:
				lostBackoff = 0
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
	batchSize, batchBytes := p.BatchSize, 0
	if batchSize <= 0 {
		batchSize = DefaultPosterBatchSize
	}
	if p.Keys != nil {
		batchSize, batchBytes = 0, DefaultPosterBatchBytes2
	}
	for _, r := range journalBatches(pending, batchSize, batchBytes) {
		batch := pending[r[0]:r[1]]
		entries := make([]tg.JournalBatchEntry, len(batch))
		for i, e := range batch {
			entries[i] = tg.JournalBatchEntry{Seq: e.Seq, Op: e.OpJSON}
		}
		var err error
		if p.Keys != nil {
			_, err = UploadJournal2(ctx, p.Session, p.Keys, entries)
		} else {
			_, err = p.Session.UploadJournalOpBatch(ctx, entries, time.Now().Unix(), fsUUID)
		}
		if err != nil {
			return fmt.Errorf("post batch first_seq=%d last_seq=%d: %w",
				batch[0].Seq, batch[len(batch)-1].Seq, err)
		}
		// Mark every seq in the batch posted in ONE batched UPDATE.
		// The previous per-seq loop kept the (single) DB connection
		// busy for hundreds of ms while it issued N small UPDATEs,
		// blocking every FUSE op queued behind it long enough for
		// some kernel-side contexts to time out — surfacing in
		// userspace as EIO from rm/tar under load. One batched
		// UPDATE finishes in a couple of ms regardless of batch size.
		seqs := make([]int64, len(batch))
		for i, e := range batch {
			seqs[i] = e.Seq
		}
		if err := p.Meta.BatchMarkJournalPosted(ctx, seqs, time.Now().Unix()); err != nil {
			return fmt.Errorf("mark posted (batch first_seq=%d last_seq=%d): %w",
				batch[0].Seq, batch[len(batch)-1].Seq, err)
		}
	}
	return nil
}

// journalBatches cuts pending into consecutive [start, end) ranges of at
// most maxOps ops (0 = no count limit) and at most maxBytes bytes of op
// JSON (0 = no byte limit). A single op larger than maxBytes still forms
// its own batch.
func journalBatches(pending []meta.JournalEntry, maxOps, maxBytes int) [][2]int {
	var out [][2]int
	for start := 0; start < len(pending); {
		end, n := start, 0
		for end < len(pending) {
			if maxOps > 0 && end-start >= maxOps {
				break
			}
			sz := len(pending[end].OpJSON)
			if maxBytes > 0 && end > start && n+sz > maxBytes {
				break
			}
			n += sz
			end++
		}
		out = append(out, [2]int{start, end})
		start = end
	}
	return out
}
