package chunk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"telfs/internal/crypto"
	"telfs/internal/meta"
)

// fakeUploader records uploads to a map and can be configured to fail.
// The Writer now drives uploads concurrently (4 in-flight by default),
// so every shared field needs its own synchronization.
type fakeUploader struct {
	mu      sync.Mutex // guards uploads
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
		var idx int32
		if err := parseIdxSuffix(filename, &idx); err == nil && idx >= f.failFromIdx {
			return 0, f.failErr
		}
	}
	id := int(f.nextID.Add(1))
	f.mu.Lock()
	f.uploads[id] = byteBlob{name: filename, data: data}
	f.mu.Unlock()
	return id, nil
}

// get returns a shallow snapshot suitable for inspection.
func (f *fakeUploader) get(id int) (byteBlob, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.uploads[id]
	return b, ok
}

func (f *fakeUploader) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.uploads)
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
	blob, ok := f.uploader.get(int(msgID))
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
	w, err := NewWriter(ctx, m, cache, up, nil, ino, chunkSize, 0)
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

// TestAsyncUploadPipeline verifies that the async dispatch + buffered-
// write pattern works end-to-end: many small sequential writes pile
// bytes well beyond the dirty cap, eager flush kicks in, every chunk
// lands in chunk_map, and the final size is right.
//
// This is the regression test for the "PC hangs on 8 GB copy" bug —
// before the async pipeline, eager flush held the per-handle mutex
// through the full network round-trip, blocking new FUSE writes and
// wedging the kernel's writeback queue. The fact that this test
// completes promptly (and doesn't deadlock) is the property we care
// about; the upload-concurrency tunable is a separate dimension (see
// DefaultUploadConcurrency).
func TestAsyncUploadPipeline(t *testing.T) {
	w, m, _, _, ino := setupWriter(t, 16) // 16-byte chunks
	ctx := context.Background()

	// Write 32 chunks worth (512 B) — past the dirty cap so eager
	// dispatch runs at least a few times during the write loop.
	w.dirtyCap = 32
	const total = 32 * 16
	for i := 0; i < total; i++ {
		buf := []byte{byte(i & 0xff)}
		if _, err := w.WriteAt(ctx, buf, int64(i)); err != nil {
			t.Fatalf("WriteAt %d: %v", i, err)
		}
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	chunks, _ := m.ListChunks(ctx, ino)
	if len(chunks) != 32 {
		t.Errorf("chunk_map count = %d, want 32", len(chunks))
	}
	in, _ := m.GetInode(ctx, ino)
	if in.Size != total {
		t.Errorf("file size = %d, want %d", in.Size, total)
	}
}


// TestEagerFlushCorrectness simulates a cp-like sequential write pattern
// at sufficient scale to trigger eager-flush. Reads back via Reader and
// compares to the original source. If TelFS's eager-flush corrupts
// data, this fails deterministically.
//
// Setup: chunkSize=64 bytes, dirtyCap=256 bytes (4 chunks). Source is
// 1024 bytes (16 chunks). 8-byte writes (mimics cp's 128 KiB writes
// vs 4 MiB chunks). Eager flush fires after 4 chunks accumulate; each
// new chunk after that triggers one flush.
func TestEagerFlushCorrectness(t *testing.T) {
	w, _, _, _, ino := setupWriter(t, 64)
	ctx := context.Background()
	w.dirtyCap = 256

	src := make([]byte, 1024)
	for i := range src {
		src[i] = byte(i & 0xff) // 0,1,...,255,0,1,...
	}
	// Write in 8-byte chunks, sequential.
	for off := 0; off < len(src); off += 8 {
		if _, err := w.WriteAt(ctx, src[off:off+8], int64(off)); err != nil {
			t.Fatalf("WriteAt off=%d: %v", off, err)
		}
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Read back via Reader.
	r := NewReader(w.meta, w.cache, w.chunkSz)
	dest := make([]byte, len(src))
	n, err := r.ReadAt(ctx, ino, dest, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != len(src) {
		t.Errorf("read %d bytes, want %d", n, len(src))
	}
	if !bytes.Equal(src, dest) {
		// Find first mismatch chunk
		for i := 0; i < len(src); i += 64 {
			if !bytes.Equal(src[i:i+64], dest[i:i+64]) {
				t.Errorf("first chunk mismatch at offset %d (chunk %d)", i, i/64)
				t.Errorf("  want first 16 bytes: %v", src[i:i+16])
				t.Errorf("  got  first 16 bytes: %v", dest[i:i+16])
				return
			}
		}
	}
}

// TestEagerFlushRealScale uses production chunk size (4 MiB) and a
// proportional dirty cap to verify the eager-flush path works at real
// scale. Writes 16 MiB (4 chunks) with 128 KiB write granularity
// (matching cp's typical block size); eager flush fires twice.
func TestEagerFlushRealScale(t *testing.T) {
	w, _, _, _, ino := setupWriter(t, 4<<20)
	ctx := context.Background()
	w.dirtyCap = 8 << 20

	src := make([]byte, 16<<20)
	for i := range src {
		src[i] = byte(i & 0xff)
	}
	const writeSz = 128 << 10
	for off := 0; off < len(src); off += writeSz {
		if _, err := w.WriteAt(ctx, src[off:off+writeSz], int64(off)); err != nil {
			t.Fatalf("WriteAt off=%d: %v", off, err)
		}
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	r := NewReader(w.meta, w.cache, w.chunkSz)
	dest := make([]byte, len(src))
	n, err := r.ReadAt(ctx, ino, dest, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != len(src) {
		t.Errorf("read %d, want %d", n, len(src))
	}
	if !bytes.Equal(src, dest) {
		for i := 0; i < len(src); i++ {
			if src[i] != dest[i] {
				t.Fatalf("first mismatch at byte %d: want %d got %d (chunk %d offset %d)",
					i, src[i], dest[i], i/(4<<20), i%(4<<20))
			}
		}
	}
}

// TestDedupReusesIdenticalContent verifies the content-addressed
// dedup path: writing the same plaintext to two different inodes
// triggers exactly ONE channel upload; the second write reuses the
// first chunk's tg_message_id via chunk_blob.
func TestDedupReusesIdenticalContent(t *testing.T) {
	m := newTestMeta(t)
	ctx := context.Background()
	ino1, _ := m.CreateChild(ctx, meta.RootIno, "a", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	ino2, _ := m.CreateChild(ctx, meta.RootIno, "b", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	up := newFakeUploader()
	cache := newTestCache(t, writerFetcher{uploader: up})

	payload := []byte("the same content twice")

	// Write to ino1 → first upload.
	w1, err := NewWriter(ctx, m, cache, up, nil, ino1, 1024, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w1.WriteAt(ctx, payload, 0); err != nil {
		t.Fatal(err)
	}
	if err := w1.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	w1.Close()
	if got := up.count(); got != 1 {
		t.Fatalf("after first write: %d uploads, want 1", got)
	}

	// Write IDENTICAL content to ino2 → must reuse, no new upload.
	w2, err := NewWriter(ctx, m, cache, up, nil, ino2, 1024, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w2.WriteAt(ctx, payload, 0); err != nil {
		t.Fatal(err)
	}
	if err := w2.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	w2.Close()
	if got := up.count(); got != 1 {
		t.Fatalf("after dedup'd write: %d uploads, want 1 (reuse)", got)
	}

	// Both inodes' chunk_map rows should point at the same msg id.
	c1, _ := m.GetChunk(ctx, ino1, 0)
	c2, _ := m.GetChunk(ctx, ino2, 0)
	if c1.TGMessageID != c2.TGMessageID {
		t.Fatalf("dedup didn't share msg id: %d vs %d", c1.TGMessageID, c2.TGMessageID)
	}
	if c1.Size != int32(len(payload)) || c2.Size != int32(len(payload)) {
		t.Fatalf("dedup'd row size wrong: %d/%d", c1.Size, c2.Size)
	}
}

// TestDedupSkippedWhenEncrypted: with a non-Noop cipher, dedup is
// disabled — identical writes upload twice (the existing pre-v0.15
// behavior, preserved so encrypted FSes don't accidentally cross-
// reference ciphertexts under different (ino, idx) AAD).
func TestDedupSkippedWhenEncrypted(t *testing.T) {
	m := newTestMeta(t)
	ctx := context.Background()
	ino1, _ := m.CreateChild(ctx, meta.RootIno, "a", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	ino2, _ := m.CreateChild(ctx, meta.RootIno, "b", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	up := newFakeUploader()
	cache := newTestCache(t, writerFetcher{uploader: up})

	key := make([]byte, 32) // all-zero key is fine for the test
	cipher, err := crypto.NewAESGCM(key)
	if err != nil {
		t.Fatal(err)
	}

	payload := []byte("encrypted content")
	for _, ino := range []int64{ino1, ino2} {
		w, err := NewWriter(ctx, m, cache, up, cipher, ino, 1024, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.WriteAt(ctx, payload, 0); err != nil {
			t.Fatal(err)
		}
		if err := w.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		w.Close()
	}
	if got := up.count(); got != 2 {
		t.Fatalf("encrypted FS: %d uploads, want 2 (dedup must be off)", got)
	}
	blobs, _ := m.CountChunkBlobs(ctx)
	if blobs != 0 {
		t.Fatalf("encrypted FS indexed %d blobs, want 0", blobs)
	}
}
