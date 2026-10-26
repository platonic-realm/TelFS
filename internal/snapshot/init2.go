package snapshot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"telfs/internal/compress"
	"telfs/internal/crypto"
	"telfs/internal/meta"
	"telfs/internal/tg"
	"telfs/internal/wire"
)

// Init2 creates a format-2 filesystem: a fresh local DB at dbPath, a
// random DEK wrapped under secret (key message posted + pinned), and the
// first snapshot. Refuses a channel that already holds a TelFS
// filesystem (format 1 or 2) and a local DB that already holds one.
// Returns the new fs_uuid.
func Init2(ctx context.Context, dbPath string, secret []byte, sess *tg.Session, chunkSize int64, codecName string, params crypto.ArgonParams) (string, error) {
	if _, err := os.Stat(dbPath); err == nil {
		m, err := meta.Open(dbPath)
		if err != nil {
			return "", err
		}
		has, _ := m.HasAnyChunks(ctx)
		f, _ := m.Format(ctx)
		m.Close()
		if has || f == 2 {
			return "", fmt.Errorf("init: %s already holds a filesystem — use a new profile (and a new channel) for a new one", dbPath)
		}
		// An empty leftover DB (e.g. from an old `telfs init`): replace it.
		for _, p := range []string{dbPath, dbPath + "-wal", dbPath + "-shm"} {
			_ = os.Remove(p)
		}
	}
	// Refuse a channel that already holds a TelFS filesystem.
	if keys, err := FindKeyMessages(ctx, sess); err != nil {
		return "", err
	} else if len(keys) > 0 {
		return "", fmt.Errorf("init: this channel already holds a TelFS filesystem (key message %d) — mount it to recover, or use a new channel", keys[0].MsgID)
	}
	var format1 bool
	if err := sess.WalkChannelMessages(ctx, ScanPages, func(cm tg.ChannelMessage) error {
		if cm.Kind == tg.KindSnapshot || cm.Kind == tg.KindJournal {
			format1 = true
			return tg.ErrStopWalk
		}
		return nil
	}); err != nil {
		return "", err
	}
	if format1 {
		return "", errors.New("init: this channel holds an older (format 1) TelFS filesystem — mount it as it is, or use a new channel for a new filesystem")
	}

	m, err := meta.Open(dbPath)
	if err != nil {
		return "", err
	}
	defer m.Close()
	if err := meta.ValidateChunkSize(chunkSize); err != nil {
		return "", err
	}
	if err := m.SetChunkSize(ctx, chunkSize); err != nil {
		return "", err
	}
	if codecName != "" {
		if _, err := compress.New(codecName); err != nil {
			return "", err
		}
		if err := m.SetCompressionCodec(ctx, codecName); err != nil {
			return "", err
		}
	}
	fsUUID, err := m.FSUUID(ctx)
	if err != nil {
		return "", err
	}
	dek, err := crypto.NewDEK()
	if err != nil {
		return "", err
	}
	defer zero(dek)
	h, err := wire.NewKeyHeader(secret, dek, fsUUID, params)
	if err != nil {
		return "", err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	for k, v := range map[string]string{meta.KVFormat: "2", meta.KVCreated: now, meta.KVSecretSet: now} {
		if err := m.PutKV(ctx, k, []byte(v)); err != nil {
			return "", err
		}
	}
	if err := m.PutKV(ctx, meta.KVKeyHeader, h.JSON()); err != nil {
		return "", err
	}
	id, err := PostKeyMessage(ctx, sess, h)
	if err != nil {
		return "", err
	}
	if err := m.PutKV(ctx, meta.KVKeyMsgID, []byte(fmt.Sprint(id))); err != nil {
		return "", err
	}
	counters := &wire.Counters{}
	keys, err := wire.DeriveKeys(dek, fsUUID, counters)
	if err != nil {
		return "", err
	}
	mgr := &Manager{Meta: m, Session: sess, Keys: keys}
	if err := mgr.Once(ctx); err != nil {
		return "", fmt.Errorf("first snapshot: %w", err)
	}
	b, _ := json.Marshal(map[string]uint64{"chunk": counters.Chunk.Load(), "meta": counters.Meta.Load(), "caption": counters.Caption.Load()})
	_ = m.PutKVUncounted(ctx, meta.KVUses, b)
	return fsUUID, nil
}

func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
