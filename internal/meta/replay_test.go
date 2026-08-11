package meta

import (
	"context"
	"path/filepath"
	"testing"
)

func newReplayStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "m.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestReplayCreateUsesJournaledIno: a replayed create must land at the
// exact ino the journal recorded, even when AUTOINCREMENT would have
// assigned something else — otherwise every later op that references
// inodes by number applies to the wrong file.
func TestReplayCreateUsesJournaledIno(t *testing.T) {
	ctx := context.Background()
	s := newReplayStore(t)

	// Simulate a replay stream whose first op (ino 5) was preceded by
	// ops the snapshot already covered — AUTOINCREMENT here is at 1.
	err := s.ReplayOp(ctx, JournalOp{
		Op: OpCreateChild, Parent: RootIno, Name: "a.txt", Ino: 5,
		Kind: KindFile, Mode: 0o100644, Mtime: 1234, ChunkSize: 16 << 20,
	})
	if err != nil {
		t.Fatalf("replay create: %v", err)
	}
	in, err := s.Lookup(ctx, RootIno, "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if in.Ino != 5 {
		t.Fatalf("replayed create landed at ino %d, want journaled ino 5", in.Ino)
	}
	if in.ChunkSize != 16<<20 {
		t.Fatalf("chunk size = %d, want 16 MiB (journaled tier lost)", in.ChunkSize)
	}
	if in.Mtime != 1234 {
		t.Fatalf("mtime = %d, want journaled 1234", in.Mtime)
	}

	// A later setsize referencing ino 5 must hit this file.
	if err := s.ReplayOp(ctx, JournalOp{Op: OpSetSize, Ino: 5, Size: 999}); err != nil {
		t.Fatalf("replay setsize: %v", err)
	}
	in, _ = s.GetInode(ctx, 5)
	if in.Size != 999 {
		t.Fatalf("size = %d, want 999", in.Size)
	}

	// AUTOINCREMENT continues past the explicit ino for organic creates.
	organic, err := s.CreateChild(ctx, RootIno, "b.txt", Inode{Kind: KindFile, Mode: 0o100644})
	if err != nil {
		t.Fatal(err)
	}
	if organic <= 5 {
		t.Fatalf("organic create got ino %d, want > 5 (sequence must advance past explicit inserts)", organic)
	}
}

// TestReplaySurvivesSkippedCreate: with explicit-ino replay, an op
// whose create was already satisfied (or an unrelated failure that the
// caller chose to tolerate) must NOT shift later creates onto wrong
// inos.
func TestReplaySurvivesSkippedCreate(t *testing.T) {
	ctx := context.Background()
	s := newReplayStore(t)

	// "Snapshot" already contains a.txt at ino 7 (idempotent-replay
	// case: the create for seq N was included in the snapshot).
	if err := s.createChildAt(ctx, RootIno, "a.txt", Inode{Kind: KindFile, Mode: 0o100644, Mtime: 1}, 7); err != nil {
		t.Fatal(err)
	}
	// Replay both creates: a.txt (dup — tolerated) then b.txt at 8.
	if err := s.ReplayOp(ctx, JournalOp{Op: OpCreateChild, Parent: RootIno, Name: "a.txt", Ino: 7, Kind: KindFile, Mode: 0o100644, Mtime: 1}); err != nil {
		t.Fatalf("dup create should be tolerated: %v", err)
	}
	if err := s.ReplayOp(ctx, JournalOp{Op: OpCreateChild, Parent: RootIno, Name: "b.txt", Ino: 8, Kind: KindFile, Mode: 0o100644, Mtime: 2}); err != nil {
		t.Fatal(err)
	}
	in, err := s.Lookup(ctx, RootIno, "b.txt")
	if err != nil {
		t.Fatal(err)
	}
	if in.Ino != 8 {
		t.Fatalf("b.txt at ino %d, want 8 — replay renumbered after a tolerated dup", in.Ino)
	}
}

// TestReplayDoesNotReJournal: replayed ops must not append fresh
// journal rows — they already exist on the channel, and re-journaling
// re-posts every one of them after recovery.
func TestReplayDoesNotReJournal(t *testing.T) {
	ctx := context.Background()
	s := newReplayStore(t)

	before, err := s.LastJournalSeq(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ops := []JournalOp{
		{Op: OpCreateChild, Parent: RootIno, Name: "f", Ino: 3, Kind: KindFile, Mode: 0o100644, Mtime: 1},
		{Op: OpSetSize, Ino: 3, Size: 42},
		{Op: OpSetSparse, Ino: 3, Sparse: true},
		{Op: OpUnlink, Parent: RootIno, Name: "f"},
	}
	for _, op := range ops {
		if err := s.ReplayOp(ctx, op); err != nil {
			t.Fatalf("replay %s: %v", op.Op, err)
		}
	}
	after, err := s.LastJournalSeq(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("replay appended %d journal rows; must append none", after-before)
	}
}

// TestReplaySetSparse round-trips the sparse flag.
func TestReplaySetSparse(t *testing.T) {
	ctx := context.Background()
	s := newReplayStore(t)
	if err := s.ReplayOp(ctx, JournalOp{Op: OpCreateChild, Parent: RootIno, Name: "s", Ino: 4, Kind: KindFile, Mode: 0o100644, Mtime: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplayOp(ctx, JournalOp{Op: OpSetSparse, Ino: 4, Sparse: true}); err != nil {
		t.Fatal(err)
	}
	in, _ := s.GetInode(ctx, 4)
	if !in.Sparse {
		t.Fatal("sparse flag not restored by replay")
	}
	// setsparse for a since-unlinked inode is tolerated.
	if err := s.ReplayOp(ctx, JournalOp{Op: OpSetSparse, Ino: 999}); err != nil {
		t.Fatalf("setsparse on missing inode should be tolerated: %v", err)
	}
}
