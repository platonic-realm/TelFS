package snapshot

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

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

// Once performs a single snapshot cycle: build, upload, record new
// msg_id, delete the previous snapshot's message.
func (m *Manager) Once(ctx context.Context) error {
	snap, err := Take(ctx, m.Meta)
	if err != nil {
		return fmt.Errorf("take: %w", err)
	}
	prevID, _ := loadCurrentMsgID(ctx, m.Meta)

	newID, err := m.Session.UploadSnapshot(ctx, snap.Bytes, snap.JournalSeq, time.Now().Unix(), snap.FSUUID)
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
