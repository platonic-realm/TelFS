package snapshot

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"telfs/internal/crypto"
	"telfs/internal/meta"
	"telfs/internal/tg"
)

// KVCurrentMsgID is the meta_kv key under which we persist the
// message-id of the most recently posted snapshot. Recovery uses it to
// jump straight to the latest without scanning the channel.
const KVCurrentMsgID = "snap_msg_id"

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
	// Logger receives a one-line update per snapshot attempt; set to nil
	// to fall back to log.Default.
	Logger *log.Logger
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
		logger.Printf("[snapshot] initial snapshot failed: %v", err)
	}

	t := time.NewTicker(m.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := m.Once(ctx); err != nil {
				logger.Printf("[snapshot] periodic snapshot failed: %v", err)
			}
		}
	}
}

// Once performs a single snapshot cycle: build, optionally encrypt
// under the FS key, upload, record new msg_id, and prune snapshots
// older than the retention window so the channel doesn't accumulate
// indefinitely.
func (m *Manager) Once(ctx context.Context) error {
	snap, err := Take(ctx, m.Meta)
	if err != nil {
		return fmt.Errorf("take: %w", err)
	}

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

	newID, err := m.Session.UploadSnapshot(ctx, body, snap.JournalSeq, time.Now().Unix(), snap.FSUUID)
	if err != nil {
		return fmt.Errorf("upload: %w", err)
	}
	if err := storeCurrentMsgID(ctx, m.Meta, newID); err != nil {
		return fmt.Errorf("persist new snap msg_id: %w", err)
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
