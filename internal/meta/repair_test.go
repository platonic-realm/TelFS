package meta

import (
	"context"
	"path/filepath"
	"testing"
)

// TestRepairPartialFiles exercises the canonical crash artifact:
// chunks 0..K-1, missing K, then K+1..N. Repair should truncate
// the inode size to the size covered by the contiguous prefix
// (sum of chunk sizes 0..K-1) and delete the unreachable tail
// from chunk_map.
func TestRepairPartialFiles(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "m.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Create a file at the default chunk_size.
	ino, err := s.CreateChild(ctx, RootIno, "broken", Inode{
		Kind: KindFile, Mode: 0o100644, Size: 100 * 16 * 1024 * 1024, // claim 100×16MiB
	})
	if err != nil {
		t.Fatal(err)
	}
	// Seed chunks 0..4, skip 5, 6..9. Each 16 MiB.
	const chunkSz = 16 * 1024 * 1024
	for _, idx := range []int32{0, 1, 2, 3, 4, 6, 7, 8, 9} {
		if err := s.PutChunk(ctx, Chunk{Ino: ino, Idx: idx, TGMessageID: int64(1000 + idx), Size: chunkSz}); err != nil {
			t.Fatal(err)
		}
	}
	// Sanity: 9 chunks but max idx is 9 → MAX+1=10 ≠ COUNT=9 → candidate.
	reports, err := s.RepairPartialFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 {
		t.Fatalf("repair returned %d reports, want 1", len(reports))
	}
	r := reports[0]
	if r.Ino != ino || r.FirstGapAt != 5 || r.NewChunks != 5 {
		t.Fatalf("report %+v: want Ino=%d FirstGap=5 NewChunks=5", r, ino)
	}
	// 5 chunks × 16 MiB = 80 MiB.
	wantSize := int64(5 * chunkSz)
	if r.NewSize != wantSize {
		t.Fatalf("NewSize = %d, want %d", r.NewSize, wantSize)
	}
	// Confirm DB state matches.
	in, _ := s.GetInode(ctx, ino)
	if in.Size != wantSize {
		t.Fatalf("inode size after repair = %d, want %d", in.Size, wantSize)
	}
	chunks, _ := s.ListChunks(ctx, ino)
	if len(chunks) != 5 {
		t.Fatalf("chunks after repair = %d, want 5", len(chunks))
	}
	for i, c := range chunks {
		if c.Idx != int32(i) {
			t.Fatalf("chunks[%d].Idx = %d, want %d (must be contiguous after repair)", i, c.Idx, i)
		}
	}
	// Second run is a no-op.
	reports2, err := s.RepairPartialFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports2) != 0 {
		t.Fatalf("repair (second pass) returned %d reports, want 0 (must be idempotent)", len(reports2))
	}
}

// TestRepairCleanFileUntouched: a file with no gaps doesn't even
// show up in the candidate query, so size and chunk_map stay
// pristine.
func TestRepairCleanFileUntouched(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "m.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ino, _ := s.CreateChild(ctx, RootIno, "clean", Inode{
		Kind: KindFile, Mode: 0o100644, Size: 3 * 4 * 1024 * 1024,
	})
	for idx := int32(0); idx < 3; idx++ {
		_ = s.PutChunk(ctx, Chunk{Ino: ino, Idx: idx, TGMessageID: int64(100 + idx), Size: 4 * 1024 * 1024})
	}
	reports, err := s.RepairPartialFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 0 {
		t.Fatalf("repair touched a clean inode: %+v", reports)
	}
}
