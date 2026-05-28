package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// PutChunk inserts or replaces a chunk_map entry. The (ino, idx) primary
// key means re-uploading chunk idx of an inode overwrites the previous
// tg_message_id; callers are responsible for deleting the old Telegram
// message separately (TelFS chunks are immutable in the channel, so
// the old message id is what gets posted to the channel as a delete op).
func (s *Store) PutChunk(ctx context.Context, c Chunk) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO chunk_map(ino, idx, tg_message_id, size)
		   VALUES (?,?,?,?)
		 ON CONFLICT(ino, idx) DO UPDATE SET
		   tg_message_id = excluded.tg_message_id,
		   size          = excluded.size`,
		c.Ino, c.Idx, c.TGMessageID, c.Size)
	if err != nil {
		return fmt.Errorf("put chunk %d/%d: %w", c.Ino, c.Idx, err)
	}
	return nil
}

// GetChunk returns the chunk_map entry for (ino, idx), or ErrNotFound.
func (s *Store) GetChunk(ctx context.Context, ino int64, idx int32) (Chunk, error) {
	var c Chunk
	err := s.db.QueryRowContext(ctx,
		`SELECT ino, idx, tg_message_id, size FROM chunk_map WHERE ino = ? AND idx = ?`,
		ino, idx).Scan(&c.Ino, &c.Idx, &c.TGMessageID, &c.Size)
	if errors.Is(err, sql.ErrNoRows) {
		return Chunk{}, ErrNotFound
	}
	if err != nil {
		return Chunk{}, fmt.Errorf("get chunk %d/%d: %w", ino, idx, err)
	}
	return c, nil
}

// ListChunks returns all chunks for an inode in ascending idx order.
func (s *Store) ListChunks(ctx context.Context, ino int64) ([]Chunk, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT ino, idx, tg_message_id, size FROM chunk_map WHERE ino = ? ORDER BY idx`, ino)
	if err != nil {
		return nil, fmt.Errorf("list chunks %d: %w", ino, err)
	}
	defer rows.Close()
	var out []Chunk
	for rows.Next() {
		var c Chunk
		if err := rows.Scan(&c.Ino, &c.Idx, &c.TGMessageID, &c.Size); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// DeleteChunk removes a single chunk_map entry. Returns ErrNotFound if
// the entry didn't exist.
func (s *Store) DeleteChunk(ctx context.Context, ino int64, idx int32) error {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM chunk_map WHERE ino = ? AND idx = ?`, ino, idx)
	if err != nil {
		return fmt.Errorf("delete chunk %d/%d: %w", ino, idx, err)
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

// AllChunks returns every row of chunk_map in (ino, idx) order. Used
// by `telfs fsck` to walk the full chunk set and verify each one's
// channel reachability. Cost is O(N) in chunk count — small DBs even
// for multi-GB filesystems.
func (s *Store) AllChunks(ctx context.Context) ([]Chunk, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT ino, idx, tg_message_id, size FROM chunk_map ORDER BY ino, idx`)
	if err != nil {
		return nil, fmt.Errorf("all chunks: %w", err)
	}
	defer rows.Close()
	var out []Chunk
	for rows.Next() {
		var c Chunk
		if err := rows.Scan(&c.Ino, &c.Idx, &c.TGMessageID, &c.Size); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// AllChunkMessageIDs returns the distinct set of Telegram message ids
// currently referenced by the chunk_map. Used by `telfs gc` to identify
// orphan chunk messages in the channel.
func (s *Store) AllChunkMessageIDs(ctx context.Context) (map[int64]struct{}, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT tg_message_id FROM chunk_map`)
	if err != nil {
		return nil, fmt.Errorf("all chunk message ids: %w", err)
	}
	defer rows.Close()
	out := make(map[int64]struct{})
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = struct{}{}
	}
	return out, rows.Err()
}

// DeleteChunksAbove removes every chunk with idx >= startIdx for the
// given inode. Used by truncate when the file shrinks past a chunk
// boundary. Returns the number of rows deleted.
func (s *Store) DeleteChunksAbove(ctx context.Context, ino int64, startIdx int32) (int, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM chunk_map WHERE ino = ? AND idx >= ?`, ino, startIdx)
	if err != nil {
		return 0, fmt.Errorf("delete chunks above %d/%d: %w", ino, startIdx, err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}
