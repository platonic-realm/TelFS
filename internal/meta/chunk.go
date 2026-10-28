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
//
// For packed chunks the caller fills Chunk.PackOffset and PackSize too;
// for unpacked chunks they stay zero and the row stores SQL NULL so
// pre-packer rows decode identically.
//
// Format 2 journals the placement (a `chunk` op) in the SAME transaction,
// so the newest snapshot plus the journal after it always holds every
// chunk the filesystem references. Replay and rebuild don't journal.
func (s *Store) PutChunk(ctx context.Context, c Chunk) error {
	var err error
	if s.journalV2.Load() && !s.replaying.Load() {
		err = s.WithTx(ctx, func(tx *sql.Tx) error {
			if err := putChunk(ctx, tx, c); err != nil {
				return err
			}
			_, err := s.appendJournalTx(ctx, tx, JournalOp{Op: OpChunk, Ino: c.Ino, Idx: c.Idx, MsgID: c.TGMessageID,
				PackOffset: c.PackOffset, PackSize: c.PackSize, ChunkLen: c.Size, Hash: c.Hash})
			return err
		})
	} else {
		packOff, packSize := nullablePackFields(c)
		_, err = s.execWithRetry(ctx, putChunkSQL,
			c.Ino, c.Idx, c.TGMessageID, c.Size, packOff, packSize, nullableHash(c.Hash))
	}
	if err != nil {
		return fmt.Errorf("put chunk %d/%d: %w", c.Ino, c.Idx, err)
	}
	return nil
}

const putChunkSQL = `INSERT INTO chunk_map(ino, idx, tg_message_id, size, pack_offset, pack_size, hash)
		   VALUES (?,?,?,?,?,?,?)
		 ON CONFLICT(ino, idx) DO UPDATE SET
		   tg_message_id = excluded.tg_message_id,
		   size          = excluded.size,
		   pack_offset   = excluded.pack_offset,
		   pack_size     = excluded.pack_size,
		   hash          = excluded.hash`

func putChunk(ctx context.Context, tx *sql.Tx, c Chunk) error {
	packOff, packSize := nullablePackFields(c)
	_, err := tx.ExecContext(ctx, putChunkSQL, c.Ino, c.Idx, c.TGMessageID, c.Size, packOff, packSize, nullableHash(c.Hash))
	return err
}

// chunkCols is the column list every chunk_map read uses; scanChunk
// matches it.
const chunkCols = `ino, idx, tg_message_id, size, pack_offset, pack_size, hash`

type rowScanner interface{ Scan(dest ...any) error }

func scanChunk(r rowScanner) (Chunk, error) {
	var c Chunk
	var packOff, packSize sql.NullInt64
	var hash []byte
	if err := r.Scan(&c.Ino, &c.Idx, &c.TGMessageID, &c.Size, &packOff, &packSize, &hash); err != nil {
		return Chunk{}, err
	}
	if packSize.Valid {
		c.PackOffset = packOff.Int64
		c.PackSize = int32(packSize.Int64)
	}
	if len(hash) > 0 {
		c.Hash = hash
	}
	return c, nil
}

func scanChunks(rows *sql.Rows) ([]Chunk, error) {
	defer rows.Close()
	var out []Chunk
	for rows.Next() {
		c, err := scanChunk(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// nullableHash stores an empty hash (format 1) as NULL.
func nullableHash(h []byte) any {
	if len(h) == 0 {
		return nil
	}
	return h
}

// nullablePackFields returns SQL NULL placeholders when the chunk is
// unpacked. Zero pack_offset with non-zero pack_size is a valid
// packed-extent at the start of a blob; treat pack_size as the
// canonical "is this a packed row" signal.
func nullablePackFields(c Chunk) (any, any) {
	if c.PackSize == 0 {
		return nil, nil
	}
	return c.PackOffset, c.PackSize
}

// GetChunk returns the chunk_map entry for (ino, idx), or ErrNotFound.
func (s *Store) GetChunk(ctx context.Context, ino int64, idx int32) (Chunk, error) {
	c, err := scanChunk(s.db.QueryRowContext(ctx,
		`SELECT `+chunkCols+` FROM chunk_map WHERE ino = ? AND idx = ?`, ino, idx))
	if errors.Is(err, sql.ErrNoRows) {
		return Chunk{}, ErrNotFound
	}
	if err != nil {
		return Chunk{}, fmt.Errorf("get chunk %d/%d: %w", ino, idx, err)
	}
	return c, nil
}

// PackedExtentsFrom returns packed chunk_map rows that share
// tgMessageID and start at pack_offset >= fromOffset, ordered by
// pack_offset, at most limit rows. Powers the blob-locality
// prefetcher: files packed into one blob were written together (tar /
// directory order), so the extents after a just-missed one are the
// files most likely to be read next.
func (s *Store) PackedExtentsFrom(ctx context.Context, tgMessageID int64, fromOffset int64, limit int) ([]Chunk, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+chunkCols+`
		   FROM chunk_map
		  WHERE tg_message_id = ? AND pack_size IS NOT NULL AND pack_offset >= ?
		  ORDER BY pack_offset
		  LIMIT ?`,
		tgMessageID, fromOffset, limit)
	if err != nil {
		return nil, fmt.Errorf("packed extents msg %d: %w", tgMessageID, err)
	}
	out, err := scanChunks(rows)
	if err != nil {
		return nil, fmt.Errorf("packed extents msg %d: scan: %w", tgMessageID, err)
	}
	return out, nil
}

// ListChunks returns all chunks for an inode in ascending idx order.
func (s *Store) ListChunks(ctx context.Context, ino int64) ([]Chunk, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+chunkCols+` FROM chunk_map WHERE ino = ? ORDER BY idx`, ino)
	if err != nil {
		return nil, fmt.Errorf("list chunks %d: %w", ino, err)
	}
	return scanChunks(rows)
}

// GetChunksRange returns chunk_map entries for (ino) where idx is in
// [fromIdx, fromIdx+count). Result is in ascending idx order; missing
// rows are simply omitted (the caller correlates by Chunk.Idx). Used
// by the Reader to pre-fetch metadata for a sequential read + its
// prefetch window in one round trip, replacing N+1 GetChunk calls
// with a single SELECT … BETWEEN.
//
// Negative or zero count returns an empty slice. The bound is
// inclusive on fromIdx and exclusive on fromIdx+count, matching Go
// slicing semantics.
func (s *Store) GetChunksRange(ctx context.Context, ino int64, fromIdx int32, count int) ([]Chunk, error) {
	if count <= 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+chunkCols+`
		   FROM chunk_map
		  WHERE ino = ? AND idx >= ? AND idx < ?
		  ORDER BY idx`,
		ino, fromIdx, int64(fromIdx)+int64(count))
	if err != nil {
		return nil, fmt.Errorf("get chunks range %d [%d,%d): %w", ino, fromIdx, int64(fromIdx)+int64(count), err)
	}
	return scanChunks(rows)
}

// DeleteChunk removes a single chunk_map entry. Returns ErrNotFound if
// the entry didn't exist.
func (s *Store) DeleteChunk(ctx context.Context, ino int64, idx int32) error {
	res, err := s.execWithRetry(ctx,
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
		`SELECT `+chunkCols+` FROM chunk_map ORDER BY ino, idx`)
	if err != nil {
		return nil, fmt.Errorf("all chunks: %w", err)
	}
	return scanChunks(rows)
}

// HasAnyChunks reports whether chunk_map has at least one row. Use
// this for emptiness probes instead of AllChunkMessageIDs — at TB
// scale the latter loads millions of ids into a map just to test
// len() > 0.
func (s *Store) HasAnyChunks(ctx context.Context) (bool, error) {
	var one int
	err := s.db.QueryRowContext(ctx,
		`SELECT 1 FROM chunk_map LIMIT 1`).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("has any chunks: %w", err)
	}
	return true, nil
}

// CountChunks returns the total chunk_map row count. One COUNT(*)
// index walk — display-friendly without materializing ids.
func (s *Store) CountChunks(ctx context.Context) (int64, error) {
	var n int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunk_map`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count chunks: %w", err)
	}
	return n, nil
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
	// Route through WithTx so we inherit retry-on-SQLITE_BUSY. The
	// dedup hot path runs from many writer goroutines concurrently
	// and used to ad-hoc BeginTx, which hit "database is locked"
	// under the new MaxOpenConns(8) setting because raw BeginTx
	// doesn't retry. The closure captures the outputs.
	var (
		outReused bool
		outMsgID  int64
		outSize   int32
	)
	err = s.WithTx(ctx, func(tx *sql.Tx) error {
		var blobMsgID int64
		var blobSize int32
		qerr := tx.QueryRowContext(ctx,
			`SELECT tg_message_id, size FROM chunk_blob WHERE hash = ?`, hash).
			Scan(&blobMsgID, &blobSize)
		if errors.Is(qerr, sql.ErrNoRows) {
			return nil
		}
		if qerr != nil {
			return fmt.Errorf("reuse lookup: %w", qerr)
		}
		// Aliveness gate: the blob index can outlive the underlying
		// message if `telfs gc` ran since the blob was indexed. Only
		// reuse when at least one chunk_map row still references the
		// message.
		var probe int
		aerr := tx.QueryRowContext(ctx,
			`SELECT 1 FROM chunk_map WHERE tg_message_id = ? LIMIT 1`, blobMsgID).Scan(&probe)
		if errors.Is(aerr, sql.ErrNoRows) {
			// Stale entry: chunk_map no longer holds it, treat as
			// miss. We don't delete the row here — the upload path
			// replaces it with the fresh msg id.
			return nil
		}
		if aerr != nil {
			return fmt.Errorf("reuse alive-check: %w", aerr)
		}
		// Live hit. Insert the new chunk_map row in the same tx.
		// pack_offset/pack_size are explicitly cleared to NULL — if
		// the previous occupant of this slot was a packed extent and
		// we're now reusing a NON-packed message via dedup, leaving
		// the old pack fields in place would point the Reader at a
		// stale offset in the wrong message.
		// hash is stored too: on format 2 it is the chunk's AAD, so the
		// new slot must carry it to be readable.
		if _, ierr := tx.ExecContext(ctx,
			`INSERT INTO chunk_map(ino, idx, tg_message_id, size, pack_offset, pack_size, hash)
			   VALUES (?,?,?,?,NULL,NULL,?)
			 ON CONFLICT(ino, idx) DO UPDATE SET
			   tg_message_id = excluded.tg_message_id,
			   size          = excluded.size,
			   pack_offset   = NULL,
			   pack_size     = NULL,
			   hash          = excluded.hash`,
			ino, idx, blobMsgID, blobSize, hash); ierr != nil {
			return fmt.Errorf("reuse put chunk: %w", ierr)
		}
		if s.journalV2.Load() {
			if _, jerr := s.appendJournalTx(ctx, tx, JournalOp{Op: OpRef, Ino: ino, Idx: idx,
				MsgID: blobMsgID, ChunkLen: blobSize, Hash: hash}); jerr != nil {
				return jerr
			}
		}
		outReused = true
		outMsgID = blobMsgID
		outSize = blobSize
		return nil
	})
	return outReused, outMsgID, outSize, err
}

// RecordChunkBlob upserts the (hash → msg_id, size) index entry after a
// fresh upload. Called by the writer's upload path immediately after
// PutChunk lands the chunk_map row, so subsequent identical writes can
// dedup.
func (s *Store) RecordChunkBlob(ctx context.Context, hash []byte, msgID int64, size int32) error {
	if len(hash) == 0 {
		return errors.New("RecordChunkBlob: empty hash")
	}
	_, err := s.execWithRetry(ctx,
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
	res, err := s.execWithRetry(ctx,
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
	res, err := s.execWithRetry(ctx,
		`DELETE FROM chunk_map WHERE ino = ? AND idx >= ?`, ino, startIdx)
	if err != nil {
		return 0, fmt.Errorf("delete chunks above %d/%d: %w", ino, startIdx, err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}
