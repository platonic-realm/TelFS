package meta

import (
	"bytes"
	"testing"
	"time"
)

func TestJournalAppendAndDrain(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	seq1, err := s.AppendJournal(ctx, []byte(`{"op":"a"}`))
	if err != nil {
		t.Fatal(err)
	}
	seq2, err := s.AppendJournal(ctx, []byte(`{"op":"b"}`))
	if err != nil {
		t.Fatal(err)
	}
	if seq2 <= seq1 {
		t.Fatalf("seq not monotonic: %d -> %d", seq1, seq2)
	}
	pending, err := s.PendingJournal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending = %d, want 2", len(pending))
	}
	if !bytes.Equal(pending[0].OpJSON, []byte(`{"op":"a"}`)) {
		t.Fatalf("pending[0] = %s", pending[0].OpJSON)
	}
	// Mark first posted; pending should drop to 1.
	if err := s.MarkJournalPosted(ctx, seq1, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	pending, _ = s.PendingJournal(ctx)
	if len(pending) != 1 || pending[0].Seq != seq2 {
		t.Fatalf("after mark: pending = %+v", pending)
	}
	// DeleteJournalUpTo only removes posted entries.
	n, err := s.DeleteJournalUpTo(ctx, seq2)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("deleted %d, want 1 (only seq1 was posted)", n)
	}
}

func TestLastJournalSeqIsMonotonicAcrossDeletes(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	for i := 0; i < 3; i++ {
		if _, err := s.AppendJournal(ctx, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	// Mark all posted, then delete them.
	pending, _ := s.PendingJournal(ctx)
	for _, e := range pending {
		_ = s.MarkJournalPosted(ctx, e.Seq, 1)
	}
	if _, err := s.DeleteJournalUpTo(ctx, 1_000_000); err != nil {
		t.Fatal(err)
	}
	// Next AppendJournal must not reuse a seq.
	seq, err := s.AppendJournal(ctx, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if seq <= 3 {
		t.Fatalf("seq %d <= 3; AUTOINCREMENT not respected", seq)
	}
	last, err := s.LastJournalSeq(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if last != seq {
		t.Fatalf("LastJournalSeq = %d, want %d", last, seq)
	}
}
