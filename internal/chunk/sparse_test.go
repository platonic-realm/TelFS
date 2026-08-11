package chunk

import (
	"bytes"
	"context"
	"testing"
)

// TestSparseWriteSkipsHoleChunks: a write far past EOF must NOT
// materialize (or upload) the intermediate zero chunks — it marks the
// inode sparse, uploads only the written chunk, and the hole reads
// back as zeros.
func TestSparseWriteSkipsHoleChunks(t *testing.T) {
	w, m, up, cache, ino := setupWriter(t, 10)
	ctx := context.Background()

	// Write 1 byte at offset 95 (chunk 9 of a 10-byte-chunk file).
	if _, err := w.WriteAt(ctx, []byte("x"), 95); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Only ONE chunk uploaded — the old behavior materialized and
	// uploaded chunks 0..8 as zeros.
	if got := up.count(); got != 1 {
		t.Fatalf("uploaded %d chunks, want 1 (holes must not upload)", got)
	}
	chunks, _ := m.ListChunks(ctx, ino)
	if len(chunks) != 1 || chunks[0].Idx != 9 {
		t.Fatalf("chunk_map = %+v, want single row at idx 9", chunks)
	}
	in, err := m.GetInode(ctx, ino)
	if err != nil {
		t.Fatal(err)
	}
	if !in.Sparse {
		t.Fatal("inode not marked sparse after cross-chunk hole write")
	}
	if in.Size != 96 {
		t.Fatalf("size = %d, want 96", in.Size)
	}

	// The whole file reads back: 95 zeros then 'x'.
	r := NewReader(m, cache, nil, 10)
	dest := bytes.Repeat([]byte{0xAA}, 96) // poisoned buffer — zeros must be written explicitly
	n, err := r.ReadAt(ctx, ino, dest, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != 96 {
		t.Fatalf("read %d bytes, want 96", n)
	}
	for i := 0; i < 95; i++ {
		if dest[i] != 0 {
			t.Fatalf("dest[%d] = %#x, want 0 (hole must read as zeros)", i, dest[i])
		}
	}
	if dest[95] != 'x' {
		t.Fatalf("dest[95] = %#x, want 'x'", dest[95])
	}
}

// TestTruncateGrowReadsZeros: growing a file with truncate creates a
// hole that must read as zeros — not EIO ("data lost"), which is what
// the reader used to return for any missing chunk inside size.
func TestTruncateGrowReadsZeros(t *testing.T) {
	w, m, _, cache, ino := setupWriter(t, 10)
	ctx := context.Background()

	if _, err := w.WriteAt(ctx, []byte("hello"), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := w.Truncate(ctx, 25); err != nil {
		t.Fatal(err)
	}

	in, _ := m.GetInode(ctx, ino)
	if !in.Sparse || in.Size != 25 {
		t.Fatalf("inode after grow: sparse=%v size=%d, want sparse=true size=25", in.Sparse, in.Size)
	}

	r := NewReader(m, cache, nil, 10)
	dest := bytes.Repeat([]byte{0xAA}, 25)
	n, err := r.ReadAt(ctx, ino, dest, 0)
	if err != nil {
		t.Fatalf("ReadAt after truncate-grow: %v", err)
	}
	if n != 25 {
		t.Fatalf("read %d bytes, want 25", n)
	}
	want := append([]byte("hello"), make([]byte, 20)...)
	if !bytes.Equal(dest, want) {
		t.Fatalf("read %q, want %q", dest, want)
	}
}

// TestTruncateShrinkRegrowReadsZeros: shrinking past data and growing
// again must NOT resurrect the old bytes — POSIX says the re-grown
// region reads as zeros. The shrink has to trim the PERSISTED boundary
// chunk, not just dirty state.
func TestTruncateShrinkRegrowReadsZeros(t *testing.T) {
	w, m, _, cache, ino := setupWriter(t, 10)
	ctx := context.Background()

	payload := append(bytes.Repeat([]byte("A"), 10), bytes.Repeat([]byte("B"), 10)...)
	if _, err := w.WriteAt(ctx, payload, 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	// Shrink to 15 (mid-chunk-1), then grow back to 20.
	if err := w.Truncate(ctx, 15); err != nil {
		t.Fatal(err)
	}
	if err := w.Truncate(ctx, 20); err != nil {
		t.Fatal(err)
	}
	// Dispatch the trimmed boundary chunk.
	if err := w.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	r := NewReader(m, cache, nil, 10)
	dest := bytes.Repeat([]byte{0xAA}, 20)
	n, err := r.ReadAt(ctx, ino, dest, 0)
	if err != nil {
		t.Fatalf("ReadAt after shrink+regrow: %v", err)
	}
	if n != 20 {
		t.Fatalf("read %d bytes, want 20", n)
	}
	want := append(bytes.Repeat([]byte("A"), 10), []byte("BBBBB\x00\x00\x00\x00\x00")...)
	if !bytes.Equal(dest, want) {
		t.Fatalf("read %q, want %q (old bytes must not resurrect)", dest, want)
	}

	// The persisted boundary chunk itself must have been trimmed.
	chunks, _ := m.ListChunks(ctx, ino)
	for _, c := range chunks {
		if c.Idx == 1 && c.Size != 5 {
			t.Fatalf("boundary chunk row size = %d, want 5 (trimmed)", c.Size)
		}
	}
}

// TestNonSparseMissingChunkStillEIO: loss detection must survive the
// sparse work — a NON-sparse file with a missing chunk inside size
// keeps returning an explicit error, not silent zeros.
func TestNonSparseMissingChunkStillEIO(t *testing.T) {
	w, m, _, cache, ino := setupWriter(t, 10)
	ctx := context.Background()

	if _, err := w.WriteAt(ctx, bytes.Repeat([]byte("A"), 20), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	// Simulate corruption: drop chunk 0's row outright.
	if err := m.DeleteChunk(ctx, ino, 0); err != nil {
		t.Fatal(err)
	}
	r := NewReader(m, cache, nil, 10)
	dest := make([]byte, 20)
	if _, err := r.ReadAt(ctx, ino, dest, 0); err == nil {
		t.Fatal("expected data-lost error for missing chunk of non-sparse file, got nil")
	}
}
