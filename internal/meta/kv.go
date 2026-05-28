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
	_, err := s.db.ExecContext(ctx,
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
	res, err := s.db.ExecContext(ctx,
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
