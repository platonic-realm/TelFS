package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// SetXattr inserts or replaces an extended attribute. Values may be any
// byte sequence including empty. The (ino, name) PRIMARY KEY ensures one
// row per name per inode.
//
// TODO(M5): values must round-trip through a Telegram meta-op message
// (JSON-encoded, base64-wrapped). If we ever accept xattr values larger
// than the channel message text limit (~4 KiB), we'll need to promote
// large values to chunks. Linux's xattr cap is 64 KiB per value.
func (s *Store) SetXattr(ctx context.Context, ino int64, name string, value []byte) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO xattrs(ino, name, value) VALUES (?,?,?)
		 ON CONFLICT(ino, name) DO UPDATE SET value = excluded.value`,
		ino, name, value)
	if err != nil {
		return fmt.Errorf("set xattr %d/%q: %w", ino, name, err)
	}
	return nil
}

// GetXattr returns the value for a given (ino, name). Returns ErrNotFound
// if the xattr doesn't exist.
func (s *Store) GetXattr(ctx context.Context, ino int64, name string) ([]byte, error) {
	var v []byte
	err := s.db.QueryRowContext(ctx,
		`SELECT value FROM xattrs WHERE ino = ? AND name = ?`, ino, name).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get xattr %d/%q: %w", ino, name, err)
	}
	return v, nil
}

// ListXattrs returns the names of all xattrs on an inode in lexical order.
func (s *Store) ListXattrs(ctx context.Context, ino int64) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT name FROM xattrs WHERE ino = ? ORDER BY name`, ino)
	if err != nil {
		return nil, fmt.Errorf("list xattrs %d: %w", ino, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// RemoveXattr deletes an xattr. Returns ErrNotFound if it didn't exist.
func (s *Store) RemoveXattr(ctx context.Context, ino int64, name string) error {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM xattrs WHERE ino = ? AND name = ?`, ino, name)
	if err != nil {
		return fmt.Errorf("remove xattr %d/%q: %w", ino, name, err)
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
