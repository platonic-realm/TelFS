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
// message-id of the most recently posted snapshot. Used to delete the
// superseded snapshot after a new one lands.
const KVCurrentMsgID = "snap_msg_id"

// DefaultInterval is how often the cadence goroutine snapshots.
const DefaultInterval = 5 * time.Minute

// Manager runs periodic snapshots for the lifetime of a mounted FUSE
// daemon. One snapshot is taken on entry (so a freshly-recovered DB
// gets re-uploaded promptly), then every DefaultInterval, then one
// final snapshot when ctx is canceled (clean unmount).
type Manager struct {
	Meta     *meta.Store
	Session  *tg.Session
	Interval time.Duration
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
// under the FS key, upload, record new msg_id, delete the previous
// snapshot's message.
func (m *Manager) Once(ctx context.Context) error {
	snap, err := Take(ctx, m.Meta)
	if err != nil {
		return fmt.Errorf("take: %w", err)
	}
	prevID, _ := loadCurrentMsgID(ctx, m.Meta)

	// If the FS is encrypted, wrap the gzipped blob in an envelope
	// that bundles the KDF state so recovery can decrypt with just
	// the user's passphrase. Plaintext filesystems upload the
	// gzipped DB directly (still compatible with M5-vintage clients).
	body := snap.Bytes
	if m.shouldEncrypt() {
		salt, argonJSON, canary, kerr := loadEnvelopeKDF(ctx, m.Meta)
		if kerr != nil {
			return fmt.Errorf("snapshot encryption: %w", kerr)
		}
		wrapped, werr := Wrap(m.Cipher, salt, argonJSON, canary, snap.Bytes)
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
	if prevID != 0 && prevID != newID {
		// Best-effort: if delete fails, the old snapshot is orphaned but
		// the new one is already authoritative.
		if err := m.Session.DeleteChannelMessages(ctx, prevID); err != nil {
			if m.Logger != nil {
				m.Logger.Printf("[snapshot] could not delete prior snap msg %d: %v", prevID, err)
			}
		}
	}
	return nil
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

// loadEnvelopeKDF reads the public Argon2id state from meta_kv. These
// are the same fields `telfs encrypt init` persisted; we copy them
// into every encrypted snapshot envelope so recovery doesn't need
// the local DB to derive the key.
func loadEnvelopeKDF(ctx context.Context, m *meta.Store) (salt, argonJSON, canary []byte, err error) {
	if salt, err = m.GetKV(ctx, crypto.KVSalt); err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			return nil, nil, nil, fmt.Errorf("crypto_salt missing — FS is in inconsistent state")
		}
		return nil, nil, nil, err
	}
	if argonJSON, err = m.GetKV(ctx, crypto.KVArgon); err != nil {
		return nil, nil, nil, err
	}
	if canary, err = m.GetKV(ctx, crypto.KVCanary); err != nil {
		return nil, nil, nil, err
	}
	return salt, argonJSON, canary, nil
}
