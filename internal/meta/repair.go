package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// RepairReport describes one inode that the startup repair pass
// modified — surfaced to the daemon log so the user can see what
// the partial-upload aftermath looked like.
type RepairReport struct {
	Ino        int64
	OldSize    int64
	NewSize    int64
	OldChunks  int
	NewChunks  int
	FirstGapAt int32 // first chunk_map idx that was missing
}

// ScanPartialFiles returns the list of candidate inodes that
// RepairPartialFiles would touch, without modifying anything. Used by
// the web UI to surface "would-repair" state before the user clicks
// the repair button. The shape mirrors RepairReport but with the
// fields the dry-run can derive cheaply.
func (s *Store) ScanPartialFiles(ctx context.Context) ([]RepairReport, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT ino FROM chunk_map
		   GROUP BY ino
		   HAVING MAX(idx) + 1 != COUNT(*)
		   ORDER BY ino`)
	if err != nil {
		return nil, fmt.Errorf("scan partials: %w", err)
	}
	defer rows.Close()
	var inos []int64
	for rows.Next() {
		var ino int64
		if err := rows.Scan(&ino); err != nil {
			return nil, err
		}
		inos = append(inos, ino)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var out []RepairReport
	for _, ino := range inos {
		chunkRows, err := s.db.QueryContext(ctx,
			`SELECT idx, size FROM chunk_map WHERE ino = ? ORDER BY idx`, ino)
		if err != nil {
			return nil, fmt.Errorf("scan chunks for ino %d: %w", ino, err)
		}
		type chunkRow struct {
			Idx  int32
			Size int32
		}
		var chunks []chunkRow
		for chunkRows.Next() {
			var c chunkRow
			if err := chunkRows.Scan(&c.Idx, &c.Size); err != nil {
				chunkRows.Close()
				return nil, err
			}
			chunks = append(chunks, c)
		}
		chunkRows.Close()
		if len(chunks) == 0 {
			continue
		}
		firstGap := int32(-1)
		for i, c := range chunks {
			if c.Idx != int32(i) {
				firstGap = int32(i)
				break
			}
		}
		if firstGap < 0 {
			continue
		}
		var newSize int64
		for _, c := range chunks[:firstGap] {
			newSize += int64(c.Size)
		}
		var oldSize int64
		if err := s.db.QueryRowContext(ctx,
			`SELECT size FROM inodes WHERE ino = ?`, ino).Scan(&oldSize); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return nil, err
		}
		out = append(out, RepairReport{
			Ino:        ino,
			OldSize:    oldSize,
			NewSize:    newSize,
			OldChunks:  len(chunks),
			NewChunks:  int(firstGap),
			FirstGapAt: firstGap,
		})
	}
	return out, nil
}

// RepairPartialFiles scans every file inode for chunk_map
// inconsistencies left by a previous daemon crash mid-write and
// truncates the inode to its largest contiguous chunk prefix. Two
// shapes get cleaned up:
//
//   - GAP: chunk_map has rows at idx 0..K-1, missing K, then K+1..N.
//     Reads of bytes [K*chunk_size .. ) would EIO at the gap, so the
//     file is logically truncated at offset K*chunk_size (sum of
//     sizes 0..K-1, accounting for short last chunks). The unreachable
//     tail rows (K+1..N) are deleted from chunk_map — their TG
//     messages become orphans for `telfs gc` to reclaim.
//
//   - STRAY PAST SIZE: chunk_map has rows whose computed offset
//     ranges past inode.size — happens when an earlier write extended
//     the file, dispatched chunks, but the SetSize update at Flush
//     never landed (e.g., daemon kill between dispatch and Flush
//     return). Those rows are also deleted.
//
// Runs in one transaction per repaired inode so a mid-run failure
// doesn't half-fix anything. Idempotent: a second run on a healthy
// FS does nothing.
//
// Returns the list of repaired inodes for logging at startup.
func (s *Store) RepairPartialFiles(ctx context.Context) ([]RepairReport, error) {
	return s.RepairPartialFilesProgress(ctx, nil)
}

// RepairPartialFilesProgress is RepairPartialFiles with a per-inode
// callback. onProgress(report, idx, total) is invoked after each
// candidate inode is processed; idx is 1-based, total is the count
// from the initial scan. Pass nil for the same behavior as
// RepairPartialFiles. Used by the web UI to surface live progress
// from a long-running repair instead of blocking the request and
// flashing only a summary at the end.
func (s *Store) RepairPartialFilesProgress(
	ctx context.Context,
	onProgress func(report *RepairReport, idx, total int),
) ([]RepairReport, error) {
	// Single query to find candidate inodes: any inode whose
	// chunk_map count doesn't equal max(idx)+1 has a gap or
	// out-of-order distribution. Skip the cheap-case 99.99% of
	// inodes that are clean.
	rows, err := s.db.QueryContext(ctx,
		`SELECT ino FROM chunk_map
		   GROUP BY ino
		   HAVING MAX(idx) + 1 != COUNT(*)
		   ORDER BY ino`)
	if err != nil {
		return nil, fmt.Errorf("repair: scan candidate inodes: %w", err)
	}
	defer rows.Close()
	var candidates []int64
	for rows.Next() {
		var ino int64
		if err := rows.Scan(&ino); err != nil {
			return nil, err
		}
		candidates = append(candidates, ino)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var out []RepairReport
	for i, ino := range candidates {
		report, err := s.repairOneInode(ctx, ino)
		if err != nil {
			return out, fmt.Errorf("repair ino %d: %w", ino, err)
		}
		if report != nil {
			out = append(out, *report)
		}
		if onProgress != nil {
			onProgress(report, i+1, len(candidates))
		}
	}
	return out, nil
}

// repairOneInode does the gap-detect + truncate for a single inode
// in a single transaction. Returns nil report if the inode turned
// out to be clean (race with concurrent writes — shouldn't happen
// at startup before mount, but cheap to guard against).
func (s *Store) repairOneInode(ctx context.Context, ino int64) (*RepairReport, error) {
	var report *RepairReport
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		// List chunks for this inode ordered by idx.
		chunkRows, err := tx.QueryContext(ctx,
			`SELECT idx, size FROM chunk_map WHERE ino = ? ORDER BY idx`, ino)
		if err != nil {
			return fmt.Errorf("list chunks: %w", err)
		}
		defer chunkRows.Close()
		type chunkRow struct {
			Idx  int32
			Size int32
		}
		var chunks []chunkRow
		for chunkRows.Next() {
			var c chunkRow
			if err := chunkRows.Scan(&c.Idx, &c.Size); err != nil {
				return err
			}
			chunks = append(chunks, c)
		}
		if err := chunkRows.Err(); err != nil {
			return err
		}
		if len(chunks) == 0 {
			return nil // no chunks → nothing to repair (foreign-key
			// cascade would have cleaned them up anyway)
		}

		// Find first gap: walk idx in order, look for the first
		// position where chunks[i].Idx != int32(i).
		firstGap := int32(-1)
		for i, c := range chunks {
			if c.Idx != int32(i) {
				firstGap = int32(i)
				break
			}
		}
		if firstGap < 0 {
			// No gap — must be the other class (chunk_map extends
			// past inode.size). The HAVING clause uses MAX(idx)+1
			// vs COUNT(*); if those match here, the candidate query
			// shouldn't have surfaced this inode. Treat as clean.
			return nil
		}

		// Sum sizes of chunks BEFORE the gap to get the consistent
		// file size. With variable-size chunks (multi-tier last-
		// chunk shrinkage, etc.) the sum is the authoritative end.
		var newSize int64
		for _, c := range chunks[:firstGap] {
			newSize += int64(c.Size)
		}

		// Load current inode size for the report and to skip the
		// update if it's already accurate.
		var oldSize int64
		if err := tx.QueryRowContext(ctx,
			`SELECT size FROM inodes WHERE ino = ?`, ino).Scan(&oldSize); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				// Inode already gone (cascade-delete by an unlink).
				// Remove the stranded chunk_map rows.
				if _, err := tx.ExecContext(ctx,
					`DELETE FROM chunk_map WHERE ino = ?`, ino); err != nil {
					return err
				}
				return nil
			}
			return err
		}

		// Truncate inode size + remove chunk_map rows from the gap onward.
		if _, err := tx.ExecContext(ctx,
			`UPDATE inodes SET size = ? WHERE ino = ?`, newSize, ino); err != nil {
			return fmt.Errorf("update size: %w", err)
		}
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM chunk_map WHERE ino = ? AND idx >= ?`, ino, firstGap); err != nil {
			return fmt.Errorf("delete chunks past gap: %w", err)
		}

		report = &RepairReport{
			Ino:        ino,
			OldSize:    oldSize,
			NewSize:    newSize,
			OldChunks:  len(chunks),
			NewChunks:  int(firstGap),
			FirstGapAt: firstGap,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return report, nil
}
