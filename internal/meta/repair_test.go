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
	reports, err := s.RepairPartialFiles(ctx, nil)
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
	reports2, err := s.RepairPartialFiles(ctx, nil)
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
	reports, err := s.RepairPartialFiles(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 0 {
		t.Fatalf("repair touched a clean inode: %+v", reports)
	}
}

// TestRepairSkipsStagedGap: a gap whose missing chunk has a wire
// staged in the pending dir must NOT be truncated — recovery will
// re-submit the wire and the row will land right after mount.
func TestRepairSkipsStagedGap(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "m.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const cs = 4 * 1024 * 1024
	ino, _ := s.CreateChild(ctx, RootIno, "recoverable", Inode{
		Kind: KindFile, Mode: 0o100644, Size: 3 * cs,
	})
	for _, idx := range []int32{0, 2} { // gap at 1
		_ = s.PutChunk(ctx, Chunk{Ino: ino, Idx: idx, TGMessageID: int64(100 + idx), Size: cs})
	}
	staged := func(gotIno int64, idx int32) bool { return gotIno == ino && idx == 1 }
	reports, err := s.RepairPartialFiles(ctx, staged)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 0 {
		t.Fatalf("repair truncated a recoverable file: %+v", reports)
	}
	in, _ := s.GetInode(ctx, ino)
	if in.Size != 3*cs {
		t.Fatalf("size changed to %d despite staged wire", in.Size)
	}
	if chunks, _ := s.ListChunks(ctx, ino); len(chunks) != 2 {
		t.Fatalf("chunk rows changed: %d, want 2", len(chunks))
	}
	// Without the staged wire the same shape IS repaired.
	reports, err = s.RepairPartialFiles(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || reports[0].FirstGapAt != 1 {
		t.Fatalf("expected gap repair at idx 1, got %+v", reports)
	}
}

// TestRepairZeroChunks: size > 0 with no chunk rows at all (the
// mass-corruption incident shape) — size drops to 0 so reads report
// honest emptiness instead of EIO forever.
func TestRepairZeroChunks(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "m.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ino, _ := s.CreateChild(ctx, RootIno, "empty-liar", Inode{
		Kind: KindFile, Mode: 0o100644, Size: 12345,
	})
	reports, err := s.RepairPartialFiles(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || reports[0].Ino != ino || reports[0].NewSize != 0 {
		t.Fatalf("want zero-chunk repair to size 0, got %+v", reports)
	}
	in, _ := s.GetInode(ctx, ino)
	if in.Size != 0 {
		t.Fatalf("size = %d, want 0", in.Size)
	}
}

// TestRepairStrayPastSize: contiguous rows extending past inode.size
// are deleted so a future re-grow reads zeros, not resurrected bytes.
func TestRepairStrayPastSize(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "m.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const cs = 4 * 1024 * 1024
	ino, _ := s.CreateChild(ctx, RootIno, "shrunk", Inode{
		Kind: KindFile, Mode: 0o100644, Size: cs, // one chunk's worth
	})
	for idx := int32(0); idx < 3; idx++ { // but three rows on record
		_ = s.PutChunk(ctx, Chunk{Ino: ino, Idx: idx, TGMessageID: int64(100 + idx), Size: cs})
	}
	reports, err := s.RepairPartialFiles(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 {
		t.Fatalf("want 1 stray repair, got %+v", reports)
	}
	chunks, _ := s.ListChunks(ctx, ino)
	if len(chunks) != 1 || chunks[0].Idx != 0 {
		t.Fatalf("chunks after stray repair = %+v, want just idx 0", chunks)
	}
	in, _ := s.GetInode(ctx, ino)
	if in.Size != cs {
		t.Fatalf("size should be untouched by stray repair, got %d", in.Size)
	}
}

// TestRepairLeavesSparseAlone: sparse files legitimately have holes —
// no shape applies to them.
func TestRepairLeavesSparseAlone(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "m.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const cs = 4 * 1024 * 1024
	ino, _ := s.CreateChild(ctx, RootIno, "holey", Inode{
		Kind: KindFile, Mode: 0o100644, Size: 10 * cs, Sparse: true,
	})
	_ = s.PutChunk(ctx, Chunk{Ino: ino, Idx: 7, TGMessageID: 700, Size: cs})
	reports, err := s.RepairPartialFiles(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 0 {
		t.Fatalf("repair touched a sparse file: %+v", reports)
	}
}
