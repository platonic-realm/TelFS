package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO journal(op_json) VALUES (?)`, opJSON)
	if err != nil {
		return 0, fmt.Errorf("append journal: %w", err)
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
	res, err := s.db.ExecContext(ctx,
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

// DeleteJournalUpTo removes posted journal rows with seq <= upTo. Called
// after a snapshot supersedes them.
func (s *Store) DeleteJournalUpTo(ctx context.Context, upTo int64) (int, error) {
	res, err := s.db.ExecContext(ctx,
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
