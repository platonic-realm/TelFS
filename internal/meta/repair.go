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
	FirstGapAt int32 // first chunk_map idx that was missing (-1 for non-gap shapes)
}

// StagedChecker reports whether a pre-upload wire for (ino, idx) is
// staged on disk (cache/pending/) awaiting recovery. Repair MUST NOT
// truncate through a gap that a staged wire is about to fill — the
// previous behavior ran repair before pending recovery and turned
// fully-recoverable crashes into permanent silent truncations. A nil
// checker means "nothing staged".
type StagedChecker func(ino int64, idx int32) bool

// ScanPartialFiles returns the list of candidate inodes that
// RepairPartialFiles would touch, without modifying anything. Used by
// the web UI to surface "would-repair" state before the user clicks
// the repair button.
func (s *Store) ScanPartialFiles(ctx context.Context, staged StagedChecker) ([]RepairReport, error) {
	candidates, err := s.repairCandidates(ctx)
	if err != nil {
		return nil, err
	}
	var out []RepairReport
	for _, ino := range candidates {
		report, err := s.repairOneInode(ctx, ino, staged, true)
		if err != nil {
			return nil, fmt.Errorf("scan ino %d: %w", ino, err)
		}
		if report != nil {
			out = append(out, *report)
		}
	}
	return out, nil
}

// RepairPartialFiles scans every file inode for chunk_map
// inconsistencies left by a previous daemon crash mid-write and
// brings size and chunk_map back into agreement. Shapes handled:
//
//   - GAP: chunk_map has rows at idx 0..K-1, missing K, then K+1..N.
//     Reads of bytes [K*chunk_size ..) would EIO at the gap, so the
//     file is logically truncated at the end of the contiguous prefix.
//     The unreachable tail rows (K+1..N) are deleted from chunk_map —
//     their TG messages become orphans for `telfs gc` to reclaim.
//
//   - STRAY PAST SIZE: chunk_map rows whose slot starts at/after
//     inode.size — a crash between dispatch and the Flush-time SetSize,
//     or an interrupted shrink. Those rows are deleted so a later
//     re-grow doesn't resurrect stale bytes.
//
//   - MISSING TAIL: a contiguous prefix that covers less than
//     inode.size (the tail chunks never landed and no wires remain).
//     Size is truncated down to the covered extent.
//
//   - ZERO CHUNKS: size > 0 with no chunk rows at all — size drops
//     to 0.
//
// Sparse files (inode.sparse = 1) are entirely exempt: their gaps are
// legitimate holes that read as zeros, not corruption.
//
// Any inode with a staged pending wire for a missing slot is SKIPPED —
// recovery (which re-submits staged wires) runs after this pass and
// will fill the gap; repairing first would destroy recoverable data.
//
// Runs in one transaction per repaired inode so a mid-run failure
// doesn't half-fix anything. Idempotent: a second run on a healthy
// FS does nothing.
func (s *Store) RepairPartialFiles(ctx context.Context, staged StagedChecker) ([]RepairReport, error) {
	return s.RepairPartialFilesProgress(ctx, staged, nil)
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
	staged StagedChecker,
	onProgress func(report *RepairReport, idx, total int),
) ([]RepairReport, error) {
	candidates, err := s.repairCandidates(ctx)
	if err != nil {
		return nil, err
	}
	var out []RepairReport
	for i, ino := range candidates {
		report, err := s.repairOneInode(ctx, ino, staged, false)
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

// repairCandidates finds every non-sparse file inode whose chunk_map
// disagrees with its size in any of the shapes above. Three cheap
// index-driven queries; the per-inode detail work happens later.
func (s *Store) repairCandidates(ctx context.Context) ([]int64, error) {
	seen := make(map[int64]struct{})
	var out []int64
	collect := func(query string) error {
		rows, err := s.db.QueryContext(ctx, query)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var ino int64
			if err := rows.Scan(&ino); err != nil {
				return err
			}
			if _, dup := seen[ino]; !dup {
				seen[ino] = struct{}{}
				out = append(out, ino)
			}
		}
		return rows.Err()
	}
	// Shape 1+2+3: any mismatch between the rows present and the rows
	// the size implies — gaps (count < max+1), strays and missing
	// tails (max+1 != expected count from size).
	if err := collect(
		`SELECT c.ino FROM chunk_map c
		   JOIN inodes i ON i.ino = c.ino
		  WHERE i.kind = 'file' AND i.sparse = 0
		  GROUP BY c.ino
		 HAVING MAX(c.idx) + 1 != COUNT(*)
		     OR MAX(c.idx) + 1 != (i.size + i.chunk_size - 1) / i.chunk_size`); err != nil {
		return nil, fmt.Errorf("repair: scan candidates: %w", err)
	}
	// Shape 4: size > 0 with zero chunk rows.
	if err := collect(
		`SELECT i.ino FROM inodes i
		  WHERE i.kind = 'file' AND i.sparse = 0 AND i.size > 0
		    AND NOT EXISTS (SELECT 1 FROM chunk_map c WHERE c.ino = i.ino)`); err != nil {
		return nil, fmt.Errorf("repair: scan zero-chunk candidates: %w", err)
	}
	return out, nil
}

// repairOneInode diagnoses and (unless dryRun) fixes a single inode in
// one transaction. Returns nil report if the inode turned out to be
// clean, sparse, or protected by staged wires.
func (s *Store) repairOneInode(ctx context.Context, ino int64, staged StagedChecker, dryRun bool) (*RepairReport, error) {
	var report *RepairReport
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		var oldSize, chunkSize int64
		var sparse int
		if err := tx.QueryRowContext(ctx,
			`SELECT size, chunk_size, sparse FROM inodes WHERE ino = ?`, ino).
			Scan(&oldSize, &chunkSize, &sparse); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				// Inode already gone (cascade-delete by an unlink).
				// Remove any stranded chunk_map rows.
				if dryRun {
					return nil
				}
				_, err := tx.ExecContext(ctx, `DELETE FROM chunk_map WHERE ino = ?`, ino)
				return err
			}
			return err
		}
		if sparse != 0 {
			return nil // holes are legitimate; nothing to repair
		}
		if chunkSize <= 0 {
			chunkSize = DefaultChunkSize
		}

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

		expected := (oldSize + chunkSize - 1) / chunkSize

		// Staged-wire shield: if ANY slot the size implies is missing
		// from chunk_map but has a wire staged on disk, recovery will
		// fill it after this pass — leave the whole inode alone.
		if staged != nil {
			have := make(map[int32]struct{}, len(chunks))
			for _, c := range chunks {
				have[c.Idx] = struct{}{}
			}
			for idx := int64(0); idx < expected; idx++ {
				if _, ok := have[int32(idx)]; ok {
					continue
				}
				if staged(ino, int32(idx)) {
					return nil
				}
			}
		}

		// ZERO CHUNKS: size says data, chunk_map says none, nothing
		// staged. The bytes are unrecoverable locally — record honest
		// emptiness rather than serving EIO forever.
		if len(chunks) == 0 {
			if oldSize == 0 {
				return nil
			}
			if !dryRun {
				if _, err := tx.ExecContext(ctx,
					`UPDATE inodes SET size = 0 WHERE ino = ?`, ino); err != nil {
					return fmt.Errorf("zero size: %w", err)
				}
			}
			report = &RepairReport{Ino: ino, OldSize: oldSize, NewSize: 0, FirstGapAt: -1}
			return nil
		}

		// GAP: truncate to the contiguous prefix.
		firstGap := int32(-1)
		for i, c := range chunks {
			if c.Idx != int32(i) {
				firstGap = int32(i)
				break
			}
		}
		if firstGap >= 0 {
			var newSize int64
			for _, c := range chunks[:firstGap] {
				newSize += int64(c.Size)
			}
			if !dryRun {
				if _, err := tx.ExecContext(ctx,
					`UPDATE inodes SET size = ? WHERE ino = ?`, newSize, ino); err != nil {
					return fmt.Errorf("update size: %w", err)
				}
				if _, err := tx.ExecContext(ctx,
					`DELETE FROM chunk_map WHERE ino = ? AND idx >= ?`, ino, firstGap); err != nil {
					return fmt.Errorf("delete chunks past gap: %w", err)
				}
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
		}

		// Contiguous prefix 0..N-1. Compare against what size implies.
		n := int64(len(chunks))
		switch {
		case n > expected:
			// STRAY PAST SIZE: rows whose slot begins at/after EOF.
			// Delete them so a future re-grow reads zeros, not stale
			// bytes. (Their TG messages become gc fodder.)
			if !dryRun {
				if _, err := tx.ExecContext(ctx,
					`DELETE FROM chunk_map WHERE ino = ? AND idx >= ?`, ino, expected); err != nil {
					return fmt.Errorf("delete stray chunks: %w", err)
				}
			}
			report = &RepairReport{
				Ino:        ino,
				OldSize:    oldSize,
				NewSize:    oldSize,
				OldChunks:  len(chunks),
				NewChunks:  int(expected),
				FirstGapAt: -1,
			}
		case n < expected:
			// MISSING TAIL: size promises more chunks than landed.
			var covered int64
			for _, c := range chunks {
				covered += int64(c.Size)
			}
			if covered >= oldSize {
				return nil // short-but-covering last chunk; consistent
			}
			if !dryRun {
				if _, err := tx.ExecContext(ctx,
					`UPDATE inodes SET size = ? WHERE ino = ?`, covered, ino); err != nil {
					return fmt.Errorf("truncate to covered: %w", err)
				}
			}
			report = &RepairReport{
				Ino:        ino,
				OldSize:    oldSize,
				NewSize:    covered,
				OldChunks:  len(chunks),
				NewChunks:  len(chunks),
				FirstGapAt: -1,
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return report, nil
}
