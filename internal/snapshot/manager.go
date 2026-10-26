package snapshot

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"telfs/internal/crypto"
	"telfs/internal/meta"
	"telfs/internal/tg"
	"telfs/internal/wire"
)

// KVCurrentMsgID is the meta_kv key under which we persist the
// message-id of the most recently posted snapshot. Recovery uses it to
// jump straight to the latest without scanning the channel.
const KVCurrentMsgID = "snap_msg_id"

// KVCurrentJournalSeq records the journal seq that was current at the
// moment of the most recently posted snapshot. The cadence loop skips
// a snapshot when LastJournalSeq() hasn't advanced past this value
// (no mutations since last snap), saving an upload + a delete RPC +
// snapshot-sized TG bandwidth per idle 5-minute tick. The key is also
// the recovery anchor: replay any journal entries with seq > this
// value.
const KVCurrentJournalSeq = "snap_journal_seq"

// DefaultInterval is how often the cadence goroutine snapshots.
const DefaultInterval = 5 * time.Minute

// DefaultRetention is how many recent snapshots to keep on the
// channel. At the DefaultInterval cadence this is ~1h of recoverable
// history — enough to roll back a "rm -rf went wrong 30 min ago"
// without paying for unbounded channel storage. Each snapshot is the
// gzipped SQLite DB plus the TFSE envelope overhead (typically tens
// of KB to a few MB depending on FS size).
const DefaultRetention = 12

// Manager runs periodic snapshots for the lifetime of a mounted FUSE
// daemon. One snapshot is taken on entry (so a freshly-recovered DB
// gets re-uploaded promptly), then every DefaultInterval, then one
// final snapshot when ctx is canceled (clean unmount).
type Manager struct {
	Meta     *meta.Store
	Session  *tg.Session
	Interval time.Duration
	// Retention is how many recent snapshots to keep on the channel
	// after each successful upload. <=0 falls back to DefaultRetention.
	// Older snapshots are deleted at the end of Once().
	Retention int
	// Cipher encrypts the snapshot body before upload when the FS has
	// encryption enabled. nil → plaintext snapshot (chunk_map still
	// uses whatever cipher the chunk pipeline has).
	Cipher crypto.Cipher
	// Keys, when set, makes this a FORMAT-2 manager: the snapshot body
	// is sealed under the meta key, posted as a `blob` with an
	// encrypted caption, and retention follows the locally recorded
	// snapshot ids (KVSnapMsgIDs) instead of a channel scan.
	Keys *wire.Keys
	// Logger receives a one-line update per snapshot attempt; set to nil
	// to fall back to log.Default.
	Logger *log.Logger

	// QuiesceCheck, when non-nil, reports whether the upload queue is
	// fully drained (no packer buffer, no staged wires, no in-flight
	// uploads, no unposted journal ops). Run's loop uses it to take an
	// EARLY snapshot shortly after the FS quiesces dirty, instead of
	// making the operator wait out the full cadence interval to reach
	// the "channel holds everything" state the web sync indicator
	// reports.
	QuiesceCheck func(ctx context.Context) bool

	// mu guards the change-detection baseline below. Once (cadence
	// goroutine) writes it; Synced (metrics HTTP goroutines) reads it.
	mu sync.Mutex
	// lastSnapCounter is Meta.WriteCounter() as of the last successful
	// snapshot THIS manager took. Together with the journal-seq check
	// it decides whether a cycle can be skipped: journal seq alone is
	// blind to chunk_map writes (PutChunk is not journaled and lands
	// AFTER the Flush-time SetSize), so seq-only skipping could
	// permanently exclude a file's chunks from every future snapshot —
	// including the final unmount one. haveSnapCounter distinguishes
	// "no snapshot yet this process" from a zero counter.
	lastSnapCounter int64
	haveSnapCounter bool
}

// baseline returns the change-detection state under the lock.
func (m *Manager) baseline() (counter int64, have bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastSnapCounter, m.haveSnapCounter
}

// setBaseline records the post-snapshot change-detection state.
func (m *Manager) setBaseline(counter int64) {
	m.mu.Lock()
	m.lastSnapCounter = counter
	m.haveSnapCounter = true
	m.mu.Unlock()
}

// Synced reports whether the channel already holds everything the
// local DB knows — the same condition Once uses to skip a cycle: the
// journal seq equals the seq captured by the last posted snapshot AND
// no counted DB writes have happened since that snapshot landed.
// False before this process has posted its first snapshot: a baseline
// it never recorded is a baseline it can't vouch for. Cheap (one KV
// read + one MAX(seq) + one atomic), fine on a 2 s poll.
func (m *Manager) Synced(ctx context.Context) bool {
	lastCounter, have := m.baseline()
	if !have || m.Meta.WriteCounter() != lastCounter {
		return false
	}
	currentSeq, err := m.Meta.LastJournalSeq(ctx)
	if err != nil {
		return false
	}
	prevSeq, hadPrev, err := loadCurrentJournalSeq(ctx, m.Meta)
	if err != nil || !hadPrev {
		return false
	}
	return currentSeq == prevSeq
}

// Run executes the periodic snapshot loop until ctx is canceled. It
// does NOT take a final snapshot on shutdown — by the time ctx is
// canceled, the underlying gotd session is being torn down and uploads
// fail with "engine closed". Callers should drive the final snapshot
// synchronously via Once before signaling shutdown to gotd.
func (m *Manager) Run(ctx context.Context) error {
	if m.Interval <= 0 {
		m.Interval = DefaultInterval
	}
	logger := m.Logger
	if logger == nil {
		logger = log.Default()
	}

	// Take one snapshot immediately. If this is a freshly-recovered
	// mount, the local DB was just restored from a (now-stale) channel
	// snapshot; re-uploading now gives us a fresh baseline.
	if err := m.Once(ctx); err != nil {
		logger.Printf("[snapshot] initial snapshot failed: %v%s", err, channelLostHint(err))
	}

	// Tick at a fraction of the interval so quiesce detection is
	// responsive; only snapshot when the full interval elapsed OR the
	// FS just went idle-but-dirty (upload queue drained with changes
	// not yet covered by a snapshot). The early path is what lets the
	// web's channel-sync indicator turn green within seconds of the
	// last upload landing, instead of after the next cadence tick —
	// and Once's own skip check keeps repeated quiesce ticks free.
	tick := m.Interval / 10
	if tick < 5*time.Second {
		tick = 5 * time.Second
	}
	const quiesceMinGap = 30 * time.Second
	lastAttempt := time.Now()
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			due := time.Since(lastAttempt) >= m.Interval
			if !due && m.QuiesceCheck != nil && time.Since(lastAttempt) >= quiesceMinGap {
				due = m.QuiesceCheck(ctx) && !m.Synced(ctx)
			}
			if !due {
				continue
			}
			lastAttempt = time.Now()
			if err := m.Once(ctx); err != nil {
				logger.Printf("[snapshot] periodic snapshot failed: %v%s", err, channelLostHint(err))
			}
		}
	}
}

// channelLostHint appends the "this won't fix itself" explanation to
// snapshot failure logs when the error is the identity-scoped
// channel-access kind. The manager's cadence is already minutes-long,
// so no extra backoff — just an actionable message.
func channelLostHint(err error) string {
	if !channelLost(err) {
		return ""
	}
	return " — channel unreachable for this identity; fix the binding or identity (`telfs status`, `telfs channel set`)"
}

// Once performs a single snapshot cycle: build, optionally encrypt
// under the FS key, upload, record new msg_id, and prune snapshots
// older than the retention window so the channel doesn't accumulate
// indefinitely.
//
// Skips the cycle entirely when NOTHING changed since the last
// snapshot this process took: the journal seq is unchanged AND the
// store's write counter is unchanged. The counter matters because
// chunk_map writes aren't journaled and land after the Flush-time
// SetSize — a seq-only skip could exclude a file's chunks from every
// future snapshot (including the final unmount one) even on a clean
// shutdown. The first Once() of a process never skips, so a chunk
// that landed just before a crash is captured by the next mount's
// initial snapshot.
func (m *Manager) Once(ctx context.Context) error {
	currentSeq, err := m.Meta.LastJournalSeq(ctx)
	if err != nil {
		return fmt.Errorf("read last journal seq: %w", err)
	}
	prevSeq, hadPrev, err := loadCurrentJournalSeq(ctx, m.Meta)
	if err != nil {
		return fmt.Errorf("read prev snap journal seq: %w", err)
	}
	counterNow := m.Meta.WriteCounter()
	lastCounter, haveCounter := m.baseline()
	if hadPrev && currentSeq == prevSeq && haveCounter && counterNow == lastCounter {
		logger := m.Logger
		if logger == nil {
			logger = log.Default()
		}
		logger.Printf("[snapshot] skipped: no new journal entries since seq %d and no DB writes", currentSeq)
		return nil
	}
	snap, err := Take(ctx, m.Meta)
	if err != nil {
		return fmt.Errorf("take: %w", err)
	}

	var newID int
	if m.Keys != nil {
		if newID, err = m.once2(ctx, snap); err != nil {
			return err
		}
	} else {
		// If the FS is encrypted, wrap the gzipped blob in an envelope
		// that bundles the KDF state so recovery can decrypt with just
		// the user's passphrase. Plaintext filesystems upload the
		// gzipped DB directly (still compatible with M5-vintage clients).
		body := snap.Bytes
		if m.shouldEncrypt() {
			opts, kerr := loadWrapOpts(ctx, m.Meta)
			if kerr != nil {
				return fmt.Errorf("snapshot encryption: %w", kerr)
			}
			wrapped, werr := Wrap(m.Cipher, opts, snap.Bytes)
			if werr != nil {
				return fmt.Errorf("wrap snapshot: %w", werr)
			}
			body = wrapped
		}
		newID, err = m.Session.UploadSnapshot(ctx, body, snap.JournalSeq, time.Now().Unix(), snap.FSUUID)
		if err != nil {
			return fmt.Errorf("upload: %w", err)
		}
	}
	if err := storeCurrentMsgID(ctx, m.Meta, newID); err != nil {
		return fmt.Errorf("persist new snap msg_id: %w", err)
	}
	if err := storeCurrentJournalSeq(ctx, m.Meta, snap.JournalSeq); err != nil {
		return fmt.Errorf("persist new snap journal seq: %w", err)
	}
	// Local journal pruning: rows with seq <= this snapshot's seq that
	// have ALSO been posted to the channel are superseded — their
	// effects are inside the snapshot we just uploaded, and channel-
	// side replay only ever asks for seq > snapshot seq. Without this
	// the journal table grows forever and is re-VACUUMed, re-gzipped
	// and re-uploaded inside every snapshot (this was the dominant
	// driver of snapshot bloat). Unposted rows stay: an older retained
	// snapshot restored later replays from the CHANNEL's journal
	// messages, so those must reach the channel before deletion.
	if pruned, err := m.Meta.DeleteJournalUpTo(ctx, snap.JournalSeq); err != nil {
		if m.Logger != nil {
			m.Logger.Printf("[snapshot] journal prune: %v", err)
		}
	} else if pruned > 0 && m.Logger != nil {
		m.Logger.Printf("[snapshot] pruned %d superseded journal rows", pruned)
	}
	// Format 2: record the id and prune by the local list (a bookkeeping
	// write, so it happens BEFORE the baseline below).
	if m.Keys != nil {
		if err := m.pruneRetention2(ctx, newID); err != nil && m.Logger != nil {
			m.Logger.Printf("[snapshot] retention prune: %v", err)
		}
	}
	// Record the change-detection baseline AFTER our own bookkeeping
	// writes so they don't count as "new changes" next cycle.
	m.setBaseline(m.Meta.WriteCounter())
	if m.Keys != nil {
		return nil
	}
	// Retention pruning: list every snapshot for this fs_uuid, keep the
	// `retention` newest, delete the rest. Best-effort — if delete
	// fails the orphan stays on the channel but the FS keeps working;
	// `telfs gc` will reap them eventually.
	if err := m.pruneRetention(ctx, snap.FSUUID, newID); err != nil && m.Logger != nil {
		m.Logger.Printf("[snapshot] retention prune: %v", err)
	}
	return nil
}

// retention returns the effective snapshot retention count.
func (m *Manager) retention() int {
	if m.Retention <= 0 {
		return DefaultRetention
	}
	return m.Retention
}

// pruneRetention deletes channel-side snapshots beyond the retention
// window. The newest `retention` snapshots stay; the rest are deleted.
// We list with cap = retention + 64 so even a chatty channel can't push
// every old snapshot beyond our visibility in one call; older-than-cap
// orphans are picked up by `telfs gc` instead.
func (m *Manager) pruneRetention(ctx context.Context, fsUUID string, justUploaded int) error {
	keep := m.retention()
	snaps, err := m.Session.ListSnapshots(ctx, fsUUID, keep+64)
	if err != nil {
		return fmt.Errorf("list: %w", err)
	}
	if len(snaps) <= keep {
		return nil
	}
	// snaps is newest-first; everything past index `keep` is stale.
	toDelete := make([]int, 0, len(snaps)-keep)
	for _, s := range snaps[keep:] {
		// Never delete the snapshot we JUST uploaded — defensive guard
		// in case ListSnapshots' newest-first ordering surprises us.
		if s.MessageID == justUploaded {
			continue
		}
		toDelete = append(toDelete, s.MessageID)
	}
	if len(toDelete) == 0 {
		return nil
	}
	return m.Session.DeleteChannelMessages(ctx, toDelete...)
}

func loadCurrentMsgID(ctx context.Context, m *meta.Store) (int, error) {
	v, err := m.GetKV(ctx, KVCurrentMsgID)
	if err != nil {
		return 0, err
	}
	id, err := strconv.Atoi(string(v))
	if err != nil {
		return 0, err
	}
	return id, nil
}

func storeCurrentMsgID(ctx context.Context, m *meta.Store, id int) error {
	return m.PutKV(ctx, KVCurrentMsgID, []byte(strconv.Itoa(id)))
}

// loadCurrentJournalSeq reads the journal seq captured by the most
// recent snapshot. The second return value is false when no snapshot
// has ever been taken on this FS (key missing), which the caller uses
// to suppress the no-op skip on the first cycle.
func loadCurrentJournalSeq(ctx context.Context, m *meta.Store) (int64, bool, error) {
	v, err := m.GetKV(ctx, KVCurrentJournalSeq)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			return 0, false, nil
		}
		return 0, false, err
	}
	seq, err := strconv.ParseInt(string(v), 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("malformed %s kv (%q): %w", KVCurrentJournalSeq, string(v), err)
	}
	return seq, true, nil
}

func storeCurrentJournalSeq(ctx context.Context, m *meta.Store, seq int64) error {
	return m.PutKV(ctx, KVCurrentJournalSeq, []byte(strconv.FormatInt(seq, 10)))
}

// shouldEncrypt reports whether Once should encrypt the snapshot
// body. True only when the Manager has a non-Noop cipher AND the FS
// itself has crypto_mode set (so plaintext FSes never accidentally
// produce encrypted snapshots that recovery can't read without a
// passphrase).
func (m *Manager) shouldEncrypt() bool {
	if m.Cipher == nil {
		return false
	}
	if _, ok := m.Cipher.(crypto.NoopCipher); ok {
		return false
	}
	return true
}

// loadWrapOpts reads the public crypto state from meta_kv into a
// WrapOpts. For v2 FSes this also fetches the wrappedDEK so cold
// recovery is self-contained — the envelope on the channel carries
// everything needed to unwrap, given the user's passphrase.
func loadWrapOpts(ctx context.Context, m *meta.Store) (WrapOpts, error) {
	mode, err := m.GetKV(ctx, crypto.KVMode)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			return WrapOpts{}, fmt.Errorf("crypto_mode missing — FS is in inconsistent state")
		}
		return WrapOpts{}, err
	}
	salt, err := m.GetKV(ctx, crypto.KVSalt)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			return WrapOpts{}, fmt.Errorf("crypto_salt missing — FS is in inconsistent state")
		}
		return WrapOpts{}, err
	}
	argonJSON, err := m.GetKV(ctx, crypto.KVArgon)
	if err != nil {
		return WrapOpts{}, err
	}
	canary, err := m.GetKV(ctx, crypto.KVCanary)
	if err != nil {
		return WrapOpts{}, err
	}
	opts := WrapOpts{
		Mode: string(mode), Salt: salt, Argon: argonJSON, Canary: canary,
	}
	// v2 and v3 FSes carry the wrapped DEK so cold recovery can produce
	// the DEK from (passphrase, salt, argon, wrappedDEK) without any
	// local state. v1 envelopes omit it — the passphrase derives the
	// cipher key directly.
	if string(mode) == crypto.ModeAESGCMv2 || string(mode) == crypto.ModeAESGCMv3 {
		wrapped, err := m.GetKV(ctx, crypto.KVWrappedDEK)
		if err != nil {
			return WrapOpts{}, fmt.Errorf("wrapped DEK missing on v2 FS — inconsistent state: %w", err)
		}
		opts.WrappedDEK = wrapped
	}
	return opts, nil
}
