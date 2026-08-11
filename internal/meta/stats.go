package meta

import (
	"context"
	"database/sql"
	"fmt"
)

// UsageSummary is the cheap aggregate statfs needs: total logical
// bytes across regular files and the live inode count.
type UsageSummary struct {
	FileBytes int64
	Inodes    int64
}

// Usage returns the current usage aggregate in one scan of `inodes`
// (never chunk_map — at TB scale that's the big table). Callers that
// serve statfs should cache the result briefly; file managers poll
// statfs aggressively.
func (s *Store) Usage(ctx context.Context) (UsageSummary, error) {
	var u UsageSummary
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(CASE WHEN kind='file' THEN size ELSE 0 END),0), COUNT(*) FROM inodes`).
		Scan(&u.FileBytes, &u.Inodes)
	if err != nil {
		return UsageSummary{}, fmt.Errorf("usage: %w", err)
	}
	return u, nil
}

// Stats is a read-only summary of the local metadata DB shaped for
// the web /state page. None of these counts are authoritative for
// correctness — they're for human inspection only. All fields are
// cheap aggregate queries; the call should complete in milliseconds
// even on a million-row chunk_map.
type Stats struct {
	// Inodes by kind. Total = files + dirs + symlinks.
	InodesTotal    int64
	InodesFiles    int64
	InodesDirs     int64
	InodesSymlinks int64

	// chunk_map summary.
	ChunkRows       int64 // total row count
	DistinctChunks  int64 // distinct (ino, idx) pairs — same as ChunkRows in a healthy FS
	DistinctTGMsgs  int64 // distinct tg_message_id values referenced
	TotalChunkBytes int64 // SUM(size) across all rows

	// chunk_blob dedup table (plaintext content-addressed dedup).
	// Zero on encrypted FS — dedup is keyed differently there.
	ChunkBlobs int64

	// Journal state.
	JournalRows     int64 // total rows including posted ones
	JournalUnposted int64 // rows with posted_at IS NULL
	LastJournalSeq  int64 // highest seq
	LastPostedAt    int64 // unix seconds of the most recent post

	// Snapshots.
	LastSnapshotMsgID string // from meta_kv; decimal string as stored
}

// Stats computes the summary in a handful of aggregate queries. Each
// is small; the whole call is one round-trip-per-query (~5).
func (s *Store) Stats(ctx context.Context) (Stats, error) {
	var out Stats
	// Inode counts by kind.
	rows, err := s.db.QueryContext(ctx, `SELECT kind, COUNT(*) FROM inodes GROUP BY kind`)
	if err != nil {
		return out, fmt.Errorf("stats inodes: %w", err)
	}
	for rows.Next() {
		var kind string
		var n int64
		if err := rows.Scan(&kind, &n); err != nil {
			rows.Close()
			return out, err
		}
		switch kind {
		case "file":
			out.InodesFiles = n
		case "dir":
			out.InodesDirs = n
		case "symlink":
			out.InodesSymlinks = n
		}
		out.InodesTotal += n
	}
	rows.Close()

	// chunk_map: one query collects rows + distinct counts + total bytes.
	if err := s.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COUNT(DISTINCT ino || ':' || idx),
			COUNT(DISTINCT tg_message_id),
			COALESCE(SUM(size), 0)
		FROM chunk_map`).Scan(
		&out.ChunkRows, &out.DistinctChunks, &out.DistinctTGMsgs, &out.TotalChunkBytes,
	); err != nil {
		return out, fmt.Errorf("stats chunk_map: %w", err)
	}

	// chunk_blob dedup table — may not exist on older FSes.
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunk_blob`).Scan(&out.ChunkBlobs); err != nil {
		// Treat "no such table" as zero; surfaces gracefully on legacy DBs.
		out.ChunkBlobs = 0
	}

	// Journal counts. SQLite's FILTER clause is supported by
	// modernc.org/sqlite. posted_at IS NULL marks an entry that has
	// not yet been mirrored to the channel.
	if err := s.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE posted_at IS NULL),
			COALESCE(MAX(seq), 0),
			COALESCE(MAX(posted_at), 0)
		FROM journal`).Scan(
		&out.JournalRows, &out.JournalUnposted, &out.LastJournalSeq, &out.LastPostedAt,
	); err != nil {
		out.JournalRows, out.JournalUnposted, out.LastJournalSeq, out.LastPostedAt = 0, 0, 0, 0
	}
	return out, nil
}

// MetaKVDump returns every (key, value) pair in meta_kv with values
// rendered as strings where reasonable (UTF-8 only; binary values
// like the encryption salt/canary are reported as their hex form +
// byte length). For the web /state debug view.
func (s *Store) MetaKVDump(ctx context.Context) ([]MetaKVEntry, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT key, value FROM meta_kv ORDER BY key`)
	if err != nil {
		return nil, fmt.Errorf("meta_kv dump: %w", err)
	}
	defer rows.Close()
	var out []MetaKVEntry
	for rows.Next() {
		var e MetaKVEntry
		var v sql.RawBytes
		if err := rows.Scan(&e.Key, &v); err != nil {
			return nil, err
		}
		e.Bytes = len(v)
		e.Value = renderKVValue(v)
		out = append(out, e)
	}
	return out, rows.Err()
}

// MetaKVEntry is one row of MetaKVDump.
type MetaKVEntry struct {
	Key   string
	Value string // human-readable rendering — UTF-8 inline or hex prefix for binary
	Bytes int    // raw byte length, useful when Value is truncated
}

// renderKVValue returns a human-friendly rendering. Plain UTF-8 short
// values come through as-is; everything else gets a hex prefix +
// byte-length annotation so the page doesn't blow up on binary blobs.
func renderKVValue(v []byte) string {
	if len(v) == 0 {
		return ""
	}
	if len(v) <= 96 && isPrintable(v) {
		return string(v)
	}
	const maxHex = 16
	if len(v) <= maxHex {
		return fmt.Sprintf("0x%x", v)
	}
	return fmt.Sprintf("0x%x… (%d bytes)", v[:maxHex], len(v))
}

func isPrintable(b []byte) bool {
	for _, c := range b {
		if c < 0x20 || c > 0x7e {
			if c == '\n' || c == '\t' {
				continue
			}
			return false
		}
	}
	return true
}
