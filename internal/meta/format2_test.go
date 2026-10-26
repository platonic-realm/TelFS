package meta

import (
	"context"
	"path/filepath"
	"testing"
)

func TestPathOfAndMemo(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	home, _ := s.CreateChild(ctx, RootIno, "home", Inode{Kind: KindDir, Mode: 0o40755})
	docs, _ := s.CreateChild(ctx, home, "docs", Inode{Kind: KindDir, Mode: 0o40755})
	f, _ := s.CreateChild(ctx, docs, "z.txt", Inode{Kind: KindFile, Mode: 0o100644})
	g, _ := s.CreateChild(ctx, docs, "g.txt", Inode{Kind: KindFile, Mode: 0o100644})
	if p, ok := s.PathOf(ctx, f); !ok || p != "/home/docs/z.txt" {
		t.Fatalf("PathOf: %q %v", p, ok)
	}
	// A hard link with a smaller name wins (deterministic first dirent).
	if err := s.Link(ctx, docs, "a-link.txt", f); err != nil {
		t.Fatal(err)
	}
	if p, _ := s.PathOf(ctx, f); p != "/home/docs/a-link.txt" {
		t.Fatalf("hard link: %q", p)
	}
	memo := map[int64]string{}
	if p, _ := s.PathOfCached(ctx, g, memo); p != "/home/docs/g.txt" {
		t.Fatalf("memo first: %q", p)
	}
	if memo[docs] != "/home/docs" || memo[home] != "/home" {
		t.Fatalf("memo filled wrong: %v", memo)
	}
	if p, _ := s.PathOfCached(ctx, f, memo); p != "/home/docs/a-link.txt" {
		t.Fatalf("memo second: %q", p)
	}
	if p, ok := s.PathOf(ctx, RootIno); !ok || p != "/" {
		t.Fatalf("root: %q", p)
	}
	// Unlinked → no name.
	s.Unlink(ctx, docs, "g.txt")
	if _, ok := s.PathOf(ctx, g); ok {
		t.Fatal("an unlinked inode has no path")
	}
}

func TestRefOpJournaledAndReplayed(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.SetJournalRefs(true)
	a, _ := s.CreateChild(ctx, RootIno, "a", Inode{Kind: KindFile, Mode: 0o100644})
	b, _ := s.CreateChild(ctx, RootIno, "b", Inode{Kind: KindFile, Mode: 0o100644})
	h := make([]byte, 32)
	h[0] = 7
	if err := s.PutChunk(ctx, Chunk{Ino: a, Idx: 0, TGMessageID: 42, Size: 100, Hash: h}); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordChunkBlob(ctx, h, 42, 100); err != nil {
		t.Fatal(err)
	}
	reused, msg, _, err := s.ReuseChunkByHash(ctx, b, 0, h)
	if err != nil || !reused || msg != 42 {
		t.Fatalf("reuse: %v %d %v", reused, msg, err)
	}
	row, _ := s.GetChunk(ctx, b, 0)
	if string(row.Hash) != string(h) {
		t.Fatal("a reused row must carry the hash (format 2 AAD)")
	}
	pend, _ := s.PendingJournal(ctx)
	var ref JournalOp
	for _, e := range pend {
		if op, _ := DecodeOp(e.OpJSON); op.Op == OpRef {
			ref = op
		}
	}
	if ref.Ino != b || ref.MsgID != 42 || string(ref.Hash) != string(h) {
		t.Fatalf("ref op: %+v", ref)
	}
	// Replay into a second store recreates the row.
	s2, _ := Open(filepath.Join(t.TempDir(), "db2.sqlite"))
	defer s2.Close()
	s2.ReplayOp(ctx, JournalOp{Op: OpCreateChild, Parent: RootIno, Name: "b", Ino: b, Kind: KindFile, Mode: 0o100644})
	if err := s2.ReplayOp(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if row, err := s2.GetChunk(ctx, b, 0); err != nil || row.TGMessageID != 42 {
		t.Fatalf("replayed ref: %+v %v", row, err)
	}
	// A ref for an inode that's gone is tolerated.
	if err := s2.ReplayOp(ctx, JournalOp{Op: OpRef, Ino: 999, Idx: 0, MsgID: 1, Hash: h}); err != nil {
		t.Fatalf("ref on a missing inode: %v", err)
	}
	if f, _ := s.Format(ctx); f != 1 {
		t.Fatalf("a fresh DB is format 1 until init: %d", f)
	}
}
