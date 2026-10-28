package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
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

// KVTx is meta_kv access inside one transaction (Store.UpdateKV): a
// record made of several keys — a snapshot's message id, seq, time and
// journal boundary; a version registry and the mark that the anchor
// does not list it yet — lands all together or not at all, and no
// reader ever sees half of it.
type KVTx struct {
	ctx context.Context
	tx  *sql.Tx
}

// Get reads a value (ErrNotFound when the key is missing).
func (k *KVTx) Get(key string) ([]byte, error) {
	var v []byte
	err := k.tx.QueryRowContext(k.ctx, `SELECT value FROM meta_kv WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get kv %q: %w", key, err)
	}
	return v, nil
}

// Put upserts a value.
func (k *KVTx) Put(key string, value []byte) error {
	if _, err := k.tx.ExecContext(k.ctx,
		`INSERT INTO meta_kv(key, value) VALUES (?,?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value); err != nil {
		return fmt.Errorf("put kv %q: %w", key, err)
	}
	return nil
}

// Delete removes a key; a missing key is not an error.
func (k *KVTx) Delete(key string) error {
	if _, err := k.tx.ExecContext(k.ctx, `DELETE FROM meta_kv WHERE key = ?`, key); err != nil {
		return fmt.Errorf("delete kv %q: %w", key, err)
	}
	return nil
}

// AdvanceJournalSeq is Store.AdvanceJournalSeq inside the transaction.
func (k *KVTx) AdvanceJournalSeq(seq int64) error {
	return advanceJournalSeqTx(k.ctx, k.tx, seq)
}

// UpdateKV runs fn in one transaction (retried on SQLITE_BUSY, so fn
// must be repeatable): every change it makes lands, or none does.
func (s *Store) UpdateKV(ctx context.Context, fn func(*KVTx) error) error {
	return s.WithTx(ctx, func(tx *sql.Tx) error { return fn(&KVTx{ctx: ctx, tx: tx}) })
}

// GetKVs reads several keys in ONE statement — one consistent view, so a
// record written by UpdateKV is seen whole or not at all. Missing keys
// are absent from the map.
func (s *Store) GetKVs(ctx context.Context, keys ...string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	q := `SELECT key, value FROM meta_kv WHERE key IN (?` + strings.Repeat(",?", len(keys)-1) + `)`
	args := make([]any, len(keys))
	for i, k := range keys {
		args[i] = k
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("get kvs: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		var v []byte
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("get kvs: %w", err)
		}
		out[k] = v
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("get kvs: %w", err)
	}
	return out, nil
}
