package meta

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// JournalEntry is a row in the `journal` table — a write-ahead record of a
// metadata mutation that may or may not yet have been posted to the
// channel as a meta-op message.
type JournalEntry struct {
	Seq      int64
	OpJSON   []byte
	PostedAt sql.NullInt64 // unix seconds; null means not yet posted
}

// AppendJournal inserts a new journal entry and returns the assigned seq.
// The entry is not posted (posted_at IS NULL).
func (s *Store) AppendJournal(ctx context.Context, opJSON []byte) (int64, error) {
	res, err := s.execWithRetry(ctx,
		`INSERT INTO journal(op_json) VALUES (?)`, opJSON)
	if err != nil {
		return 0, fmt.Errorf("append journal: %w", err)
	}
	return res.LastInsertId()
}

// appendJournalTx records a journal op inside an existing transaction.
// Used by mutation methods so the journal row and the mutation it
// describes commit atomically — if the tx rolls back, the journal row
// disappears too. Returns the assigned seq.
//
// During replay (ReplayOp) journaling is suppressed: recovery applies
// ops that ALREADY live on the channel, and re-journaling them would
// re-post every replayed op as fresh journal messages.
func (s *Store) appendJournalTx(ctx context.Context, tx *sql.Tx, op JournalOp) (int64, error) {
	if s.replaying.Load() {
		return 0, nil
	}
	payload, err := json.Marshal(op)
	if err != nil {
		return 0, fmt.Errorf("marshal journal op: %w", err)
	}
	res, err := tx.ExecContext(ctx,
		`INSERT INTO journal(op_json) VALUES (?)`, payload)
	if err != nil {
		return 0, fmt.Errorf("tx append journal: %w", err)
	}
	return res.LastInsertId()
}

// PendingJournal returns journal entries that have not yet been posted to
// the channel, in seq order. M5's poster drains this list.
func (s *Store) PendingJournal(ctx context.Context) ([]JournalEntry, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT seq, op_json, posted_at FROM journal WHERE posted_at IS NULL ORDER BY seq`)
	if err != nil {
		return nil, fmt.Errorf("pending journal: %w", err)
	}
	defer rows.Close()
	var out []JournalEntry
	for rows.Next() {
		var e JournalEntry
		if err := rows.Scan(&e.Seq, &e.OpJSON, &e.PostedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// MarkJournalPosted records that seq has been posted to the channel.
// Returns ErrNotFound if no such seq exists.
func (s *Store) MarkJournalPosted(ctx context.Context, seq int64, postedAt int64) error {
	res, err := s.execWithRetry(ctx,
		`UPDATE journal SET posted_at = ? WHERE seq = ?`, postedAt, seq)
	if err != nil {
		return fmt.Errorf("mark journal %d posted: %w", seq, err)
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

// BatchMarkJournalPosted marks every seq in seqs as posted at the
// given time, in ONE single UPDATE statement and ONE transaction.
//
// The naive form — calling MarkJournalPosted in a loop — issues N
// separate UPDATEs on the same shared DB connection, which under
// MaxOpenConns(1) holds the connection for the entire loop. With a
// 1000-op batch from the journal poster that's hundreds of
// milliseconds of head-of-line blocking for every FUSE op queued
// behind it, surfacing to userspace as EIO when those ops' contexts
// time out waiting for the connection.
//
// Empty slice is a no-op. Does not validate that every seq exists;
// the journal poster only feeds in seqs it just received from
// PendingJournal so they're guaranteed live.
func (s *Store) BatchMarkJournalPosted(ctx context.Context, seqs []int64, postedAt int64) error {
	if len(seqs) == 0 {
		return nil
	}
	// One placeholder per seq + the postedAt: build the IN clause.
	// SQLite tolerates up to 999 host parameters by default; chunk if
	// the batch is larger.
	const chunkSz = 800 // leave headroom under SQLITE_LIMIT_VARIABLE_NUMBER
	for start := 0; start < len(seqs); start += chunkSz {
		end := start + chunkSz
		if end > len(seqs) {
			end = len(seqs)
		}
		chunk := seqs[start:end]
		// `?,?,?,?,...` with len(chunk) entries.
		placeholders := strings.Repeat("?,", len(chunk))
		placeholders = placeholders[:len(placeholders)-1] // trim trailing comma
		query := "UPDATE journal SET posted_at = ? WHERE seq IN (" + placeholders + ")"
		args := make([]any, 0, 1+len(chunk))
		args = append(args, postedAt)
		for _, s := range chunk {
			args = append(args, s)
		}
		if _, err := s.execWithRetry(ctx, query, args...); err != nil {
			return fmt.Errorf("batch mark journal posted (chunk %d..%d): %w", chunk[0], chunk[len(chunk)-1], err)
		}
	}
	return nil
}

// DeleteJournalUpTo removes posted journal rows with seq <= upTo. Called
// after a snapshot supersedes them.
func (s *Store) DeleteJournalUpTo(ctx context.Context, upTo int64) (int, error) {
	res, err := s.execWithRetry(ctx,
		`DELETE FROM journal WHERE seq <= ? AND posted_at IS NOT NULL`, upTo)
	if err != nil {
		return 0, fmt.Errorf("delete journal <= %d: %w", upTo, err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// LastJournalSeq returns the highest seq ever assigned (including posted
// and deleted entries). Useful for snapshot metadata. Returns 0 if the
// journal has never been used.
func (s *Store) LastJournalSeq(ctx context.Context) (int64, error) {
	var seq sql.NullInt64
	err := s.db.QueryRowContext(ctx,
		`SELECT seq FROM sqlite_sequence WHERE name = 'journal'`).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("last journal seq: %w", err)
	}
	return seq.Int64, nil
}

// AdvanceJournalSeq makes the journal sequence continue after seq (if it
// is behind). Recovery replays channel ops WITHOUT re-journaling them,
// so the local sequence stays at the snapshot's; without this the next
// local change would reuse sequence numbers already on the channel, and
// the snapshot taken right after recovery would claim to cover only the
// old number — a later recovery would then replay the already-applied
// ops a second time (a setsize could shrink a file back).
func (s *Store) AdvanceJournalSeq(ctx context.Context, seq int64) error {
	if seq <= 0 {
		return nil
	}
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE sqlite_sequence SET seq = ? WHERE name = 'journal' AND seq < ?`, seq, seq)
		if err != nil {
			return fmt.Errorf("advance journal seq: %w", err)
		}
		if n, _ := res.RowsAffected(); n > 0 {
			return nil
		}
		var cur sql.NullInt64
		err = tx.QueryRowContext(ctx, `SELECT seq FROM sqlite_sequence WHERE name = 'journal'`).Scan(&cur)
		if errors.Is(err, sql.ErrNoRows) {
			_, err = tx.ExecContext(ctx, `INSERT INTO sqlite_sequence(name, seq) VALUES ('journal', ?)`, seq)
		}
		return err
	})
}

// UnpostedUpTo counts the journal rows up to seq that have not been posted
// to the channel yet (`telfs sync`).
func (s *Store) UnpostedUpTo(ctx context.Context, seq int64) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal WHERE seq <= ? AND posted_at IS NULL`, seq).Scan(&n)
	return n, err
}
