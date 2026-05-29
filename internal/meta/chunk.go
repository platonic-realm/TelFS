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

// ReuseChunkByHash atomically reuses an existing channel chunk for a
// new (ino, idx) slot if the content hash matches a previously uploaded
// blob AND that blob's channel message is still referenced by some
// chunk_map row (i.e., the GC hasn't reaped it).
//
// Returns (reused=true, ...) when the chunk_blob index found a live
// match and a chunk_map row was written; the caller then skips the
// upload entirely. Returns (reused=false, ...) when no match exists,
// or when the indexed entry pointed at a dead message — the caller
// uploads as usual and then calls RecordChunkBlob with the new msg id.
//
// The check + insert happen in one transaction so the GC (running
// against `chunk_map` as the source of truth) can never delete the
// shared message between the aliveness check and the new row's commit.
func (s *Store) ReuseChunkByHash(ctx context.Context, ino int64, idx int32, hash []byte) (reused bool, msgID int64, size int32, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, 0, 0, err
	}
	defer tx.Rollback()

	var blobMsgID int64
	var blobSize int32
	err = tx.QueryRowContext(ctx,
		`SELECT tg_message_id, size FROM chunk_blob WHERE hash = ?`, hash).
		Scan(&blobMsgID, &blobSize)
	if errors.Is(err, sql.ErrNoRows) {
		return false, 0, 0, tx.Commit()
	}
	if err != nil {
		return false, 0, 0, fmt.Errorf("reuse lookup: %w", err)
	}
	// Aliveness gate: the blob index can outlive the underlying message
	// if `telfs gc` ran since the blob was indexed. Only reuse when at
	// least one chunk_map row still references the message.
	var probe int
	err = tx.QueryRowContext(ctx,
		`SELECT 1 FROM chunk_map WHERE tg_message_id = ? LIMIT 1`, blobMsgID).Scan(&probe)
	if errors.Is(err, sql.ErrNoRows) {
		// Stale entry: chunk_map no longer holds it, treat as miss. We
		// don't delete the row here — the upload path replaces it with
		// the fresh msg id.
		return false, 0, 0, tx.Commit()
	}
	if err != nil {
		return false, 0, 0, fmt.Errorf("reuse alive-check: %w", err)
	}
	// Live hit. Insert the new chunk_map row in the same tx. Use
	// INSERT OR REPLACE in case (ino, idx) is being overwritten.
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO chunk_map(ino, idx, tg_message_id, size)
		   VALUES (?,?,?,?)
		 ON CONFLICT(ino, idx) DO UPDATE SET
		   tg_message_id = excluded.tg_message_id,
		   size          = excluded.size`,
		ino, idx, blobMsgID, blobSize); err != nil {
		return false, 0, 0, fmt.Errorf("reuse put chunk: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, 0, 0, err
	}
	return true, blobMsgID, blobSize, nil
}

// RecordChunkBlob upserts the (hash → msg_id, size) index entry after a
// fresh upload. Called by the writer's upload path immediately after
// PutChunk lands the chunk_map row, so subsequent identical writes can
// dedup.
func (s *Store) RecordChunkBlob(ctx context.Context, hash []byte, msgID int64, size int32) error {
	if len(hash) == 0 {
		return errors.New("RecordChunkBlob: empty hash")
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO chunk_blob(hash, tg_message_id, size)
		   VALUES (?,?,?)
		 ON CONFLICT(hash) DO UPDATE SET
		   tg_message_id = excluded.tg_message_id,
		   size          = excluded.size`,
		hash, msgID, size)
	if err != nil {
		return fmt.Errorf("record chunk blob: %w", err)
	}
	return nil
}

// PruneStaleChunkBlobs deletes chunk_blob index entries whose
// tg_message_id no longer appears in chunk_map. Run after `telfs gc`
// to keep the dedup index honest; safe to skip (the ReuseChunkByHash
// path re-checks aliveness in-line either way). Returns the number of
// stale rows removed.
func (s *Store) PruneStaleChunkBlobs(ctx context.Context) (int, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM chunk_blob
		  WHERE tg_message_id NOT IN (SELECT DISTINCT tg_message_id FROM chunk_map)`)
	if err != nil {
		return 0, fmt.Errorf("prune chunk blobs: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// CountChunkBlobs returns the number of distinct content blobs we've
// indexed. Useful for the status command and tests.
func (s *Store) CountChunkBlobs(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunk_blob`).Scan(&n)
	if err != nil {
		return 0, err
	}
	return n, nil
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
