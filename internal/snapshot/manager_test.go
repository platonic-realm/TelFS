package snapshot

import (
	"context"
	"path/filepath"
	"testing"

	"telfs/internal/meta"
)

// TestCurrentJournalSeqRoundTrip exercises the load/store helpers
// that drive the no-op snapshot skip. Missing key reports "no prev"
// (hadPrev=false) so the manager doesn't skip the first snapshot on
// a fresh FS; a stored value round-trips exactly so subsequent skips
// compare like-with-like.
func TestCurrentJournalSeqRoundTrip(t *testing.T) {
	ctx := context.Background()
	m, err := meta.Open(filepath.Join(t.TempDir(), "telfs.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	seq, hadPrev, err := loadCurrentJournalSeq(ctx, m)
	if err != nil {
		t.Fatalf("loadCurrentJournalSeq (missing): %v", err)
	}
	if hadPrev || seq != 0 {
		t.Fatalf("missing key: want (0,false,nil), got (%d,%v,nil)", seq, hadPrev)
	}

	if err := storeCurrentJournalSeq(ctx, m, 12345); err != nil {
		t.Fatalf("storeCurrentJournalSeq: %v", err)
	}
	seq, hadPrev, err = loadCurrentJournalSeq(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	if !hadPrev || seq != 12345 {
		t.Fatalf("after store: want (12345,true,nil), got (%d,%v,nil)", seq, hadPrev)
	}

	// Overwriting works (a later snapshot supersedes the previous record).
	if err := storeCurrentJournalSeq(ctx, m, 99999); err != nil {
		t.Fatal(err)
	}
	seq, _, _ = loadCurrentJournalSeq(ctx, m)
	if seq != 99999 {
		t.Fatalf("overwrite: want 99999, got %d", seq)
	}
}
