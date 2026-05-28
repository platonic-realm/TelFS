package chunk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"telfs/internal/meta"
)

// fakeUploader records uploads to a map and can be configured to fail.
type fakeUploader struct {
	uploads map[int]byteBlob
	nextID  atomic.Int64
	// failFromIdx >= 0 means: fail every upload whose filename ends in
	// idx >= failFromIdx (to simulate "chunk N onwards fails").
	failFromIdx int32
	failErr     error
}

type byteBlob struct {
	name string
	data []byte
}

func newFakeUploader() *fakeUploader {
	return &fakeUploader{uploads: make(map[int]byteBlob), failFromIdx: -1}
}

func (f *fakeUploader) UploadDocument(_ context.Context, r io.Reader, filename, _ string) (int, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	if f.failFromIdx >= 0 {
		// Parse the trailing -idx<N> from the filename our Writer uses.
		var idx int32
		if err := parseIdxSuffix(filename, &idx); err == nil && idx >= f.failFromIdx {
			return 0, f.failErr
		}
	}
	id := int(f.nextID.Add(1))
	f.uploads[id] = byteBlob{name: filename, data: data}
	return id, nil
}

// parseIdxSuffix extracts N from "...-idxN" filenames.
func parseIdxSuffix(name string, out *int32) error {
	i := bytes.LastIndex([]byte(name), []byte("-idx"))
	if i < 0 {
		return errors.New("no -idx suffix")
	}
	var n int32
	for _, c := range name[i+4:] {
		if c < '0' || c > '9' {
			return errors.New("non-digit in idx")
		}
		n = n*10 + int32(c-'0')
	}
	*out = n
	return nil
}

// writerFetcher uses fakeUploader's records as the read backing store so
// that Writer's preload-on-first-touch sees what's been uploaded.
type writerFetcher struct{ uploader *fakeUploader }

func (f writerFetcher) Fetch(_ context.Context, _ Key, msgID int64) ([]byte, error) {
	blob, ok := f.uploader.uploads[int(msgID)]
	if !ok {
		return nil, errors.New("no such msg")
	}
	out := make([]byte, len(blob.data))
	copy(out, blob.data)
	return out, nil
}

func setupWriter(t *testing.T, chunkSize int64) (*Writer, *meta.Store, *fakeUploader, *Cache, int64) {
	t.Helper()
	m := newTestMeta(t)
	ctx := context.Background()
	ino, err := m.CreateChild(ctx, meta.RootIno, "f", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	if err != nil {
		t.Fatal(err)
	}
	up := newFakeUploader()
	cache := newTestCache(t, writerFetcher{uploader: up})
	w, err := NewWriter(ctx, m, cache, up, ino, chunkSize, 0)
	if err != nil {
		t.Fatal(err)
	}
	return w, m, up, cache, ino
}

// TestWriteAtSingleChunkRoundTrip: write a small payload, flush, verify
// chunk_map + size are updated and the uploaded bytes match.
func TestWriteAtSingleChunkRoundTrip(t *testing.T) {
	w, m, up, _, ino := setupWriter(t, 10)
	ctx := context.Background()
	n, err := w.WriteAt(ctx, []byte("hello"), 0)
	if err != nil || n != 5 {
		t.Fatalf("WriteAt: n=%d err=%v", n, err)
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if w.Size() != 5 {
		t.Fatalf("size = %d, want 5", w.Size())
	}
	chunks, _ := m.ListChunks(ctx, ino)
	if len(chunks) != 1 || chunks[0].Size != 5 {
		t.Fatalf("chunks = %+v", chunks)
	}
	if blob := up.uploads[int(chunks[0].TGMessageID)]; string(blob.data) != "hello" {
		t.Fatalf("uploaded blob = %q, want %q", blob.data, "hello")
	}
	in, _ := m.GetInode(ctx, ino)
	if in.Size != 5 {
		t.Fatalf("meta size = %d, want 5", in.Size)
	}
}

// TestWriteAtCrossChunkBoundary writes a payload that straddles two
// chunks. Both chunks should land on disk + chunk_map.
func TestWriteAtCrossChunkBoundary(t *testing.T) {
	w, m, _, _, ino := setupWriter(t, 10)
	ctx := context.Background()
	// Write 15 bytes at offset 5: chunk 0 takes bytes 5..9 (5 bytes),
	// chunk 1 takes bytes 0..9 (10 bytes). Total 15 bytes.
	payload := []byte("AAAAABBBBBBBBBB") // 5 A + 10 B
	n, err := w.WriteAt(ctx, payload, 5)
	if err != nil || n != 15 {
		t.Fatalf("WriteAt: n=%d err=%v", n, err)
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if w.Size() != 20 {
		t.Fatalf("size = %d, want 20", w.Size())
	}
	chunks, _ := m.ListChunks(ctx, ino)
	if len(chunks) != 2 {
		t.Fatalf("got %d chunks, want 2", len(chunks))
	}
	// Chunk 0: first 5 bytes zero (hole), then 5 A's.
	if chunks[0].Size != 10 {
		t.Fatalf("chunk0.size = %d, want 10", chunks[0].Size)
	}
	// Chunk 1: 10 B's.
	if chunks[1].Size != 10 {
		t.Fatalf("chunk1.size = %d, want 10", chunks[1].Size)
	}
}

// TestWriteAtReadModifyWritePreservesOtherBytes: write to middle of
// existing chunk, the rest of the chunk should survive.
func TestWriteAtReadModifyWritePreservesOtherBytes(t *testing.T) {
	w, _, _, _, _ := setupWriter(t, 10)
	ctx := context.Background()
	if _, err := w.WriteAt(ctx, []byte("AAAAAAAAAA"), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// Overwrite bytes 2..5 with 'B'.
	if _, err := w.WriteAt(ctx, []byte("BBBB"), 2); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// Read it back via Reader.
	r := NewReader(w.meta, w.cache, w.chunkSz)
	dest := make([]byte, 10)
	got, err := r.ReadAt(ctx, w.ino, dest, 0)
	if err != nil || got != 10 {
		t.Fatalf("ReadAt: %d %v", got, err)
	}
	want := []byte("AABBBBAAAA")
	if !bytes.Equal(dest, want) {
		t.Fatalf("got %q, want %q", dest, want)
	}
}

// TestFlushPartialFailureLeavesLaterChunksDirty is the advisor's
// discriminator: if upload fails on chunk N, chunks 0..N-1 land in
// chunk_map and chunk N onwards remain dirty for retry.
func TestFlushPartialFailureLeavesLaterChunksDirty(t *testing.T) {
	w, m, up, _, ino := setupWriter(t, 10)
	ctx := context.Background()
	// Three chunks of 10 bytes each.
	for i := 0; i < 3; i++ {
		off := int64(i * 10)
		if _, err := w.WriteAt(ctx, bytes.Repeat([]byte{byte('A' + i)}, 10), off); err != nil {
			t.Fatal(err)
		}
	}
	// Configure uploader to fail starting at chunk idx 1.
	up.failFromIdx = 1
	up.failErr = errors.New("simulated FLOOD_WAIT")

	err := w.Flush(ctx)
	if err == nil {
		t.Fatalf("Flush should have errored on chunk 1")
	}
	chunks, _ := m.ListChunks(ctx, ino)
	if len(chunks) != 1 || chunks[0].Idx != 0 {
		t.Fatalf("after partial failure, chunk_map = %+v, want one entry for idx 0", chunks)
	}
	// Chunks 1 and 2 still in dirty.
	if len(w.dirty) != 2 {
		t.Fatalf("after partial failure, dirty count = %d, want 2", len(w.dirty))
	}
	// Recover: clear failure mode and retry Flush. Should succeed.
	up.failFromIdx = -1
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("retry Flush: %v", err)
	}
	chunks, _ = m.ListChunks(ctx, ino)
	if len(chunks) != 3 {
		t.Fatalf("after retry, chunk_map = %+v, want 3 entries", chunks)
	}
}

// TestFlushInvalidatesCache: after a chunk is uploaded with new content,
// a subsequent ReadAt should NOT see stale cached bytes from a previous
// download.
func TestFlushInvalidatesCache(t *testing.T) {
	w, _, _, _, _ := setupWriter(t, 10)
	ctx := context.Background()
	if _, err := w.WriteAt(ctx, []byte("OLDOLDOLDX"), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// Prime the cache by reading through Reader.
	r := NewReader(w.meta, w.cache, w.chunkSz)
	dest := make([]byte, 10)
	_, _ = r.ReadAt(ctx, w.ino, dest, 0)
	if string(dest) != "OLDOLDOLDX" {
		t.Fatalf("primed read = %q", dest)
	}
	// Overwrite via Writer.
	if _, err := w.WriteAt(ctx, []byte("NEW"), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// Reader should now see the new bytes; if the cache wasn't
	// invalidated, this read returns the stale OLD bytes.
	dest2 := make([]byte, 10)
	_, _ = r.ReadAt(ctx, w.ino, dest2, 0)
	if string(dest2) != "NEWOLDOLDX" {
		t.Fatalf("post-overwrite read = %q, want NEWOLDOLDX", dest2)
	}
}

// TestTruncateShrinkRemovesChunks: truncating below a chunk boundary
// deletes all chunks fully past the new size and trims the boundary
// chunk's recorded size.
func TestTruncateShrinkRemovesChunks(t *testing.T) {
	w, m, _, _, ino := setupWriter(t, 10)
	ctx := context.Background()
	if _, err := w.WriteAt(ctx, bytes.Repeat([]byte{'A'}, 25), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// File now: 3 chunks (10, 10, 5).
	if err := w.Truncate(ctx, 12); err != nil {
		t.Fatal(err)
	}
	// Should leave chunks 0 (10 bytes) and 1 (trimmed to 2 bytes).
	chunks, _ := m.ListChunks(ctx, ino)
	if len(chunks) != 2 {
		t.Fatalf("after truncate, chunks = %+v", chunks)
	}
	in, _ := m.GetInode(ctx, ino)
	if in.Size != 12 {
		t.Fatalf("meta size = %d, want 12", in.Size)
	}
}

// TestTruncateGrowUpdatesSize is the simpler grow case.
func TestTruncateGrowUpdatesSize(t *testing.T) {
	w, m, _, _, ino := setupWriter(t, 10)
	ctx := context.Background()
	if err := w.Truncate(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if w.Size() != 100 {
		t.Fatalf("Writer.Size = %d", w.Size())
	}
	in, _ := m.GetInode(ctx, ino)
	if in.Size != 100 {
		t.Fatalf("meta size = %d, want 100", in.Size)
	}
}
