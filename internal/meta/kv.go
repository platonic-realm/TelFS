package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// GetKV reads a meta_kv value. Returns ErrNotFound if key doesn't exist.
func (s *Store) GetKV(ctx context.Context, key string) ([]byte, error) {
	var v []byte
	err := s.db.QueryRowContext(ctx,
		`SELECT value FROM meta_kv WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get kv %q: %w", key, err)
	}
	return v, nil
}

// PutKV upserts a meta_kv entry.
func (s *Store) PutKV(ctx context.Context, key string, value []byte) error {
	_, err := s.execWithRetry(ctx,
		`INSERT INTO meta_kv(key, value) VALUES (?,?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		key, value)
	if err != nil {
		return fmt.Errorf("put kv %q: %w", key, err)
	}
	return nil
}

// PutKVUncounted stores an ADVISORY blob (telemetry — e.g. the
// tracker's lifetime stats, persisted every 60 s) without bumping the
// store's write counter. The counter feeds snapshot change-detection;
// when the stats persister went through the counted path, every
// cadence tick saw "DB changed" and an idle daemon re-uploaded
// byte-identical snapshots forever — and the channel-sync indicator
// could never report clean. Never use for filesystem content: a write
// this path hides from the counter is a write snapshots may miss.
func (s *Store) PutKVUncounted(ctx context.Context, key string, value []byte) error {
	_, err := s.execWithRetryUncounted(ctx,
		`INSERT INTO meta_kv(key, value) VALUES (?,?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		key, value)
	if err != nil {
		return fmt.Errorf("put kv %q: %w", key, err)
	}
	return nil
}

// DeleteKV removes a key. Returns ErrNotFound if absent.
func (s *Store) DeleteKV(ctx context.Context, key string) error {
	res, err := s.execWithRetry(ctx,
		`DELETE FROM meta_kv WHERE key = ?`, key)
	if err != nil {
		return fmt.Errorf("delete kv %q: %w", key, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}
