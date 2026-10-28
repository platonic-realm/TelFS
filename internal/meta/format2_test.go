package meta

import (
	"bytes"
	"context"
	"fmt"
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
	s.SetJournalV2(true)
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

func TestAdvanceJournalSeq(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// A fresh DB has no journal row in sqlite_sequence yet.
	if err := s.AdvanceJournalSeq(ctx, 500); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.LastJournalSeq(ctx); n != 500 {
		t.Fatalf("after advance: %d", n)
	}
	// Replayed ops don't journal; the next real change continues after 500.
	s.ReplayOp(ctx, JournalOp{Op: OpCreateChild, Parent: RootIno, Name: "r", Ino: 50, Kind: KindFile, Mode: 0o100644})
	if _, err := s.CreateChild(ctx, RootIno, "x", Inode{Kind: KindFile, Mode: 0o100644}); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.LastJournalSeq(ctx); n != 501 {
		t.Fatalf("next op seq = %d, want 501", n)
	}
	// Never moves backwards.
	s.AdvanceJournalSeq(ctx, 10)
	if n, _ := s.LastJournalSeq(ctx); n != 501 {
		t.Fatalf("went backwards: %d", n)
	}
}

func TestReplaySetSizeKeepsMtimeAndChunkSizeJournaled(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.SetJournalV2(true)
	ino, _ := s.CreateChild(ctx, RootIno, "f", Inode{Kind: KindFile, Mode: 0o100644})
	if err := s.ReplayOp(ctx, JournalOp{Op: OpSetSize, Ino: ino, Size: 99, Mtime: 1600000000}); err != nil {
		t.Fatal(err)
	}
	in, _ := s.GetInode(ctx, ino)
	if in.Size != 99 || in.Mtime != 1600000000 {
		t.Fatalf("replayed setsize: size %d mtime %d, want 99 / 1600000000 (not the replay time)", in.Size, in.Mtime)
	}
	if err := s.SetInodeChunkSize(ctx, ino, 16<<20); err != nil {
		t.Fatal(err)
	}
	pend, _ := s.PendingJournal(ctx)
	var found bool
	for _, e := range pend {
		if op, _ := DecodeOp(e.OpJSON); op.Op == OpSetChunkSize && op.Ino == ino && op.ChunkSize == 16<<20 {
			found = true
		}
	}
	if !found {
		t.Fatal("a chunk-size promotion must be journaled on format 2")
	}
	s2, _ := Open(filepath.Join(t.TempDir(), "db2.sqlite"))
	defer s2.Close()
	s2.ReplayOp(ctx, JournalOp{Op: OpCreateChild, Parent: RootIno, Name: "f", Ino: ino, Kind: KindFile, Mode: 0o100644})
	s2.ReplayOp(ctx, JournalOp{Op: OpSetChunkSize, Ino: ino, ChunkSize: 16 << 20})
	if in, _ := s2.GetInode(ctx, ino); in.ChunkSize != 16<<20 {
		t.Fatalf("replayed chunk size %d", in.ChunkSize)
	}
}

// rsync's chmod of a temp file to the mode it already has and the
// kernel's repeats of the current mtime change nothing: no journal op,
// and the live row stays what a replay of the journal produces.
func TestSetAttrNoOpsNotJournaled(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.SetJournalV2(true)
	ino, err := s.CreateChild(ctx, RootIno, "f", Inode{Kind: KindFile, Mode: 0o100600, Mtime: 1000})
	if err != nil {
		t.Fatal(err)
	}
	u32 := func(v uint32) *uint32 { return &v }
	i64 := func(v int64) *int64 { return &v }
	setattrs := func() []JournalOp {
		pend, _ := s.PendingJournal(ctx)
		var out []JournalOp
		for _, e := range pend {
			if op, _ := DecodeOp(e.OpJSON); op.Op == OpSetAttr {
				out = append(out, op)
			}
		}
		return out
	}
	s.SetAttrs(ctx, ino, SetAttrsPatch{Mode: u32(0o600)}) // go-fuse passes permission bits only
	s.SetAttrs(ctx, ino, SetAttrsPatch{Mtime: i64(1000)}) // the current mtime again
	if ops := setattrs(); len(ops) != 0 {
		t.Fatalf("no-op setattrs journaled: %+v", ops)
	}
	s.SetAttrs(ctx, ino, SetAttrsPatch{Mode: u32(0o644), Mtime: i64(1000)})
	ops := setattrs()
	if len(ops) != 1 || len(ops[0].Set) != 1 || ops[0].Set[0] != "mode" || ops[0].Mode != 0o644 {
		t.Fatalf("want one setattr of the mode only, got %+v", ops)
	}
	live, _ := s.GetInode(ctx, ino)
	s2, _ := Open(filepath.Join(t.TempDir(), "db2.sqlite"))
	defer s2.Close()
	pend, _ := s.PendingJournal(ctx)
	for _, e := range pend {
		op, _ := DecodeOp(e.OpJSON)
		if err := s2.ReplayOp(ctx, op); err != nil {
			t.Fatal(err)
		}
	}
	rep, _ := s2.GetInode(ctx, ino)
	if rep.Mode != live.Mode || rep.Mtime != live.Mtime {
		t.Fatalf("replayed mode %o mtime %d, live mode %o mtime %d", rep.Mode, rep.Mtime, live.Mode, live.Mtime)
	}
}

// Format 2 journals every chunk placement in the same transaction as
// the row; replay restores it field for field (pack offset/size and the
// keyed hash included); format 1 journals none.
func TestChunkOpJournaledAndReplayed(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.SetJournalV2(true)
	ino, _ := s.CreateChild(ctx, RootIno, "f", Inode{Kind: KindFile, Mode: 0o100644})
	c := Chunk{Ino: ino, Idx: 0, TGMessageID: 77, Size: 190, PackOffset: 4096, PackSize: 230, Hash: bytes.Repeat([]byte{7}, 32)}
	if err := s.PutChunk(ctx, c); err != nil {
		t.Fatal(err)
	}
	var ops []JournalOp
	pend, _ := s.PendingJournal(ctx)
	for _, e := range pend {
		op, _ := DecodeOp(e.OpJSON)
		ops = append(ops, op)
	}
	if len(ops) != 2 || ops[1].Op != OpChunk || ops[1].MsgID != 77 || ops[1].PackOffset != 4096 ||
		ops[1].PackSize != 230 || ops[1].ChunkLen != 190 || !bytes.Equal(ops[1].Hash, c.Hash) {
		t.Fatalf("journal: %+v", ops)
	}
	s2, _ := Open(filepath.Join(t.TempDir(), "db2.sqlite"))
	defer s2.Close()
	s2.SetJournalV2(true)
	for _, op := range ops {
		if err := s2.ReplayOp(ctx, op); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s2.GetChunk(ctx, ino, 0)
	if err != nil || got.TGMessageID != 77 || got.PackOffset != 4096 || got.PackSize != 230 || got.Size != 190 || !bytes.Equal(got.Hash, c.Hash) {
		t.Fatalf("replayed row %+v, %v", got, err)
	}
	if pend, _ := s2.PendingJournal(ctx); len(pend) != 0 {
		t.Fatalf("replay journaled %d op(s)", len(pend))
	}
	s1, _ := Open(filepath.Join(t.TempDir(), "db1.sqlite")) // format 1
	defer s1.Close()
	ino1, _ := s1.CreateChild(ctx, RootIno, "f", Inode{Kind: KindFile, Mode: 0o100644})
	s1.PutChunk(ctx, Chunk{Ino: ino1, Idx: 0, TGMessageID: 5, Size: 10})
	pend1, _ := s1.PendingJournal(ctx)
	for _, e := range pend1 {
		if op, _ := DecodeOp(e.OpJSON); op.Op == OpChunk {
			t.Fatal("format 1 must not journal chunk placements")
		}
	}
}

// The P1 1M-test scenario in miniature: a snapshot taken BEFORE files
// were uploaded, plus the journal after it, must give back every chunk
// (it used to give back sizes without chunks, and the startup repair
// emptied the files).
func TestSnapshotPlusJournalHoldsEveryChunk(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "live.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.SetJournalV2(true)
	d, _ := s.CreateChild(ctx, RootIno, "d", Inode{Kind: KindDir, Mode: 0o40755})
	snapSeq, _ := s.LastJournalSeq(ctx)
	if _, err := s.DB().ExecContext(ctx, `VACUUM INTO ?`, filepath.Join(dir, "snap.sqlite")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		ino, err := s.CreateChild(ctx, d, fmt.Sprintf("f%d", i), Inode{Kind: KindFile, Mode: 0o100644, ChunkSize: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		for idx := int32(0); idx < 2; idx++ {
			if err := s.PutChunk(ctx, Chunk{Ino: ino, Idx: idx, TGMessageID: int64(100 + i), Size: 1 << 20,
				PackOffset: int64(idx) << 20, PackSize: 1<<20 + 40, Hash: bytes.Repeat([]byte{byte(i)}, 32)}); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.SetSize(ctx, ino, 2<<20); err != nil {
			t.Fatal(err)
		}
	}
	r, err := Open(filepath.Join(dir, "snap.sqlite")) // the "restored snapshot"
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	pend, _ := s.PendingJournal(ctx)
	for _, e := range pend {
		if e.Seq <= snapSeq {
			continue
		}
		op, _ := DecodeOp(e.OpJSON)
		if err := r.ReplayOp(ctx, op); err != nil {
			t.Fatal(err)
		}
	}
	if scan, err := s.ScanPartialFiles(ctx, nil); err != nil || len(scan) != 0 {
		t.Fatalf("live DB needs repair (bad test data): %+v, %v", scan, err)
	}
	if scan, err := r.ScanPartialFiles(ctx, nil); err != nil || len(scan) != 0 {
		t.Fatalf("recovered DB needs repair: %+v, %v", scan, err)
	}
	if a, b := countChunks(t, s), countChunks(t, r); a != 10 || b != a {
		t.Fatalf("chunk rows: live %d, recovered %d", a, b)
	}
}

func countChunks(t *testing.T, s *Store) int {
	var n int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM chunk_map`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
