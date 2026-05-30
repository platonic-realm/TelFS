package chunk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"telfs/internal/compress"
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
	// Tests use artificially small chunk sizes (10, 16, 64 bytes) to
	// exercise multi-chunk paths without 4 MiB allocations. Stamp the
	// per-inode chunk_size at create so NewWriter picks it up; without
	// this, multi-tier promotion would bump w.chunkSz to the small
	// tier (4 MiB) and collapse the test payload into one chunk.
	ino, err := m.CreateChild(ctx, meta.RootIno, "f", meta.Inode{
		Kind: meta.KindFile, Mode: 0o100644, ChunkSize: chunkSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	up := newFakeUploader()
	cache := newTestCache(t, writerFetcher{uploader: up})
	w, err := NewWriter(ctx, m, cache, up, nil, nil, nil, nil, ino, chunkSize, 0)
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
	if err := w.Sync(ctx); err != nil {
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
	if err := w.Sync(ctx); err != nil {
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
	if err := w.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	// Overwrite bytes 2..5 with 'B'.
	if _, err := w.WriteAt(ctx, []byte("BBBB"), 2); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	// Read it back via Reader.
	r := NewReader(w.meta, w.cache, nil, w.chunkSz)
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

	err := w.Sync(ctx)
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
	if err := w.Sync(ctx); err != nil {
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
	if err := w.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	// Prime the cache by reading through Reader.
	r := NewReader(w.meta, w.cache, nil, w.chunkSz)
	dest := make([]byte, 10)
	_, _ = r.ReadAt(ctx, w.ino, dest, 0)
	if string(dest) != "OLDOLDOLDX" {
		t.Fatalf("primed read = %q", dest)
	}
	// Overwrite via Writer.
	if _, err := w.WriteAt(ctx, []byte("NEW"), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(ctx); err != nil {
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
	if err := w.Sync(ctx); err != nil {
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
	if err := w.Sync(ctx); err != nil {
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
	if err := w.Sync(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Read back via Reader.
	r := NewReader(w.meta, w.cache, nil, w.chunkSz)
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
	if err := w.Sync(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	r := NewReader(w.meta, w.cache, nil, w.chunkSz)
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
	w1, err := NewWriter(ctx, m, cache, up, nil, nil, nil, nil, ino1, 1024, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w1.WriteAt(ctx, payload, 0); err != nil {
		t.Fatal(err)
	}
	if err := w1.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	w1.Close()
	if got := up.count(); got != 1 {
		t.Fatalf("after first write: %d uploads, want 1", got)
	}

	// Write IDENTICAL content to ino2 → must reuse, no new upload.
	w2, err := NewWriter(ctx, m, cache, up, nil, nil, nil, nil, ino2, 1024, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w2.WriteAt(ctx, payload, 0); err != nil {
		t.Fatal(err)
	}
	if err := w2.Sync(ctx); err != nil {
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
		w, err := NewWriter(ctx, m, cache, up, cipher, nil, nil, nil, ino, 1024, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.WriteAt(ctx, payload, 0); err != nil {
			t.Fatal(err)
		}
		if err := w.Sync(ctx); err != nil {
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

// TestDedupWithConvergentCipher exercises the full v3 path: two
// inodes write identical plaintext through an AESGCMConvergent
// cipher, and dedup MUST take effect (only one upload). Then both
// inodes' chunks must read back via the SAME underlying channel
// message under the SAME cipher — proving Open is mode-correct
// (nil AAD, ignores ino/idx).
func TestDedupWithConvergentCipher(t *testing.T) {
	m := newTestMeta(t)
	ctx := context.Background()
	ino1, _ := m.CreateChild(ctx, meta.RootIno, "a", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	ino2, _ := m.CreateChild(ctx, meta.RootIno, "b", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	up := newFakeUploader()
	cache := newTestCache(t, writerFetcher{uploader: up})

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i ^ 0x5A)
	}
	cipher, err := crypto.NewAESGCMConvergent(key)
	if err != nil {
		t.Fatal(err)
	}

	payload := []byte("identical plaintext under convergent v3 — dedup must collapse")

	// Write payload to ino1 via convergent cipher.
	w1, err := NewWriter(ctx, m, cache, up, cipher, nil, nil, nil, ino1, 1024, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w1.WriteAt(ctx, payload, 0); err != nil {
		t.Fatal(err)
	}
	if err := w1.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	w1.Close()
	if got := up.count(); got != 1 {
		t.Fatalf("after first encrypted write: %d uploads, want 1", got)
	}

	// Write IDENTICAL payload to ino2 — dedup MUST kick in (zero new
	// uploads) because the convergent cipher produces the same
	// ciphertext bytes.
	w2, err := NewWriter(ctx, m, cache, up, cipher, nil, nil, nil, ino2, 1024, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w2.WriteAt(ctx, payload, 0); err != nil {
		t.Fatal(err)
	}
	if err := w2.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	w2.Close()
	if got := up.count(); got != 1 {
		t.Fatalf("after dedup'd v3 write: %d uploads, want 1 (reuse)", got)
	}

	// Both inodes should point at the same tg_message_id.
	c1, _ := m.GetChunk(ctx, ino1, 0)
	c2, _ := m.GetChunk(ctx, ino2, 0)
	if c1.TGMessageID != c2.TGMessageID {
		t.Fatalf("v3 dedup didn't share msg id: %d vs %d", c1.TGMessageID, c2.TGMessageID)
	}

	// CRITICAL: the on-channel ciphertext must decrypt cleanly under
	// BOTH (ino1, idx=0) and (ino2, idx=0) — that's the property
	// that breaks if Open mistakenly uses (ino, idx) as AAD.
	blob, ok := up.get(int(c1.TGMessageID))
	if !ok {
		t.Fatal("uploaded blob missing")
	}
	for _, slot := range []struct {
		ino int64
		idx int32
	}{{ino1, 0}, {ino2, 0}} {
		pt, err := cipher.Open(slot.ino, slot.idx, blob.data)
		if err != nil {
			t.Fatalf("Open(%d, %d) failed on shared v3 ciphertext: %v — dedup'd reads are broken", slot.ino, slot.idx, err)
		}
		if !bytes.Equal(pt, payload) {
			t.Fatalf("Open(%d, %d) returned wrong plaintext", slot.ino, slot.idx)
		}
	}
}

// slowUploader gates each upload on a channel so tests can hold a
// chunk mid-flight and observe pre-completion state.
type slowUploader struct {
	mu      sync.Mutex
	gate    chan struct{} // close to release ALL uploads
	uploads map[int]byteBlob
	nextID  atomic.Int64
}

func newSlowUploader() *slowUploader {
	return &slowUploader{
		gate:    make(chan struct{}),
		uploads: make(map[int]byteBlob),
	}
}

func (s *slowUploader) release() { close(s.gate) }

func (s *slowUploader) get(id int) (byteBlob, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.uploads[id]
	return b, ok
}

func (s *slowUploader) UploadDocument(ctx context.Context, r io.Reader, filename, _ string) (int, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	select {
	case <-s.gate:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	id := int(s.nextID.Add(1))
	s.mu.Lock()
	s.uploads[id] = byteBlob{name: filename, data: data}
	s.mu.Unlock()
	return id, nil
}

type slowFetcher struct{ up *slowUploader }

func (f slowFetcher) Fetch(_ context.Context, _ Key, msgID int64) ([]byte, error) {
	blob, ok := f.up.get(int(msgID))
	if !ok {
		return nil, errors.New("no such msg")
	}
	out := make([]byte, len(blob.data))
	copy(out, blob.data)
	return out, nil
}

// TestFlushIsAsync proves the core async-close property: Flush returns
// before background uploads land, and the per-ino Pending barrier
// holds a count for the in-flight upload so readers will wait.
func TestFlushIsAsync(t *testing.T) {
	m := newTestMeta(t)
	ctx := context.Background()
	ino, err := m.CreateChild(ctx, meta.RootIno, "f", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	if err != nil {
		t.Fatal(err)
	}
	up := newSlowUploader()
	cache := newTestCache(t, slowFetcher{up: up})
	pool := NewUploadPool(4)
	w, err := NewWriter(ctx, m, cache, up, nil, nil, pool, nil, ino, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteAt(ctx, []byte("hello"), 0); err != nil {
		t.Fatal(err)
	}
	flushDone := make(chan error, 1)
	go func() { flushDone <- w.Flush(ctx) }()
	select {
	case err := <-flushDone:
		if err != nil {
			t.Fatalf("Flush returned err=%v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Flush blocked on the slow upload — async semantics broken")
	}
	// Upload hasn't landed: chunk_map still empty, but size persisted.
	chunks, _ := m.ListChunks(ctx, ino)
	if len(chunks) != 0 {
		t.Fatalf("chunks landed before upload completed: %+v", chunks)
	}
	in, _ := m.GetInode(ctx, ino)
	if in.Size != 5 {
		t.Fatalf("size = %d, want 5 (Flush must persist size synchronously)", in.Size)
	}
	pool.Pending.mu.Lock()
	count := pool.Pending.counts[ino]
	pool.Pending.mu.Unlock()
	if count != 1 {
		t.Fatalf("Pending barrier count for ino %d = %d, want 1", ino, count)
	}
	up.release()
	drainCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if !pool.Drain(drainCtx) {
		t.Fatal("pool drain timed out")
	}
	chunks, _ = m.ListChunks(ctx, ino)
	if len(chunks) != 1 {
		t.Fatalf("after drain, chunks = %+v", chunks)
	}
}

// TestReadAfterAsyncCloseWaits is the read-side correctness check:
// once Flush returns, a subsequent ReadAt that goes through the same
// Pending barrier blocks until the upload lands and then sees the
// new bytes.
func TestReadAfterAsyncCloseWaits(t *testing.T) {
	m := newTestMeta(t)
	ctx := context.Background()
	ino, err := m.CreateChild(ctx, meta.RootIno, "f", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	if err != nil {
		t.Fatal(err)
	}
	up := newSlowUploader()
	cache := newTestCache(t, slowFetcher{up: up})
	pool := NewUploadPool(4)
	w, err := NewWriter(ctx, m, cache, up, nil, nil, pool, nil, ino, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteAt(ctx, []byte("hello"), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	r := NewReader(m, cache, pool.Pending, 10)
	type readResult struct {
		n   int
		err error
		buf []byte
	}
	readDone := make(chan readResult, 1)
	go func() {
		buf := make([]byte, 5)
		n, err := r.ReadAt(ctx, ino, buf, 0)
		readDone <- readResult{n, err, buf}
	}()
	select {
	case res := <-readDone:
		t.Fatalf("ReadAt returned before upload landed: n=%d buf=%q — Pending barrier didn't engage", res.n, res.buf)
	case <-time.After(50 * time.Millisecond):
	}
	up.release()
	res := <-readDone
	if res.err != nil || res.n != 5 || string(res.buf) != "hello" {
		t.Fatalf("ReadAt after barrier: n=%d err=%v buf=%q", res.n, res.err, res.buf)
	}
	drainCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if !pool.Drain(drainCtx) {
		t.Fatal("pool drain timed out")
	}
}

// TestPoolGlobalConcurrency proves the upload sem is shared across
// writers — the load-bearing fix that async close depends on. With
// concurrency=1 the second writer's dispatch must wait until the
// first writer's in-flight upload completes.
func TestPoolGlobalConcurrency(t *testing.T) {
	m := newTestMeta(t)
	ctx := context.Background()
	ino1, _ := m.CreateChild(ctx, meta.RootIno, "a", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	ino2, _ := m.CreateChild(ctx, meta.RootIno, "b", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	up := newSlowUploader()
	cache := newTestCache(t, slowFetcher{up: up})
	pool := NewUploadPool(1) // only one upload at a time globally
	w1, err := NewWriter(ctx, m, cache, up, nil, nil, pool, nil, ino1, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	w2, err := NewWriter(ctx, m, cache, up, nil, nil, pool, nil, ino2, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w1.WriteAt(ctx, []byte("aaaaa"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := w2.WriteAt(ctx, []byte("bbbbb"), 0); err != nil {
		t.Fatal(err)
	}
	// Flush w1 — sem grabbed by w1's upload (which is stuck on gate).
	if err := w1.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// w2.Flush must now block on dispatch's pool.Reserve.
	flush2Done := make(chan error, 1)
	go func() { flush2Done <- w2.Flush(ctx) }()
	select {
	case <-flush2Done:
		t.Fatal("w2.Flush returned with only 1 sem slot and 1 in-flight upload — sem isn't actually global")
	case <-time.After(50 * time.Millisecond):
	}
	up.release()
	if err := <-flush2Done; err != nil {
		t.Fatalf("w2.Flush after release: %v", err)
	}
	drainCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if !pool.Drain(drainCtx) {
		t.Fatal("pool drain timed out")
	}
}

// TestCompressionRoundTrip exercises the full plaintext → compress →
// encrypt → upload → fetch → decrypt → decompress → plaintext pipeline.
// The on-wire bytes (what the fakeUploader saw) must DIFFER from the
// plaintext — otherwise compression isn't engaged — while the reader
// returns the original payload byte-perfect.
func TestCompressionRoundTrip(t *testing.T) {
	codec, err := compress.New(compress.NameZstd)
	if err != nil {
		t.Fatal(err)
	}
	m := newTestMeta(t)
	ctx := context.Background()
	ino, err := m.CreateChild(ctx, meta.RootIno, "f", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	if err != nil {
		t.Fatal(err)
	}
	up := newFakeUploader()
	// Cache must know the codec too, otherwise it can't decompress
	// what the writer compressed.
	dir := t.TempDir()
	cache, err := NewCache(dir, 1<<20, writerFetcher{uploader: up}, nil, codec)
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWriter(ctx, m, cache, up, nil, codec, nil, nil, ino, 4096, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Highly compressible payload so size diff is unambiguous.
	payload := bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog "), 64)
	if _, err := w.WriteAt(ctx, payload, 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	chunks, _ := m.ListChunks(ctx, ino)
	if len(chunks) != 1 {
		t.Fatalf("want 1 chunk, got %d", len(chunks))
	}
	uploaded, _ := up.get(int(chunks[0].TGMessageID))
	if bytes.Equal(uploaded.data, payload) {
		t.Fatal("on-wire bytes equal plaintext — codec wasn't engaged")
	}
	if len(uploaded.data) >= len(payload)/2 {
		t.Fatalf("on-wire %d B vs plaintext %d B — zstd should compress this >2x", len(uploaded.data), len(payload))
	}
	// Reader path — must round-trip to plaintext.
	r := NewReader(m, cache, nil, 4096)
	dest := make([]byte, len(payload))
	n, err := r.ReadAt(ctx, ino, dest, 0)
	if err != nil || n != len(payload) {
		t.Fatalf("ReadAt: n=%d err=%v", n, err)
	}
	if !bytes.Equal(dest, payload) {
		t.Fatal("read-back differs from original plaintext")
	}
}

// TestPackerRoutesSmallFiles: writes a small file (below the packer
// threshold). Verifies that (a) the chunk lands in chunk_map with
// non-zero pack_offset/pack_size, (b) the on-channel blob contains
// the encrypted wire at that offset, (c) Reader can round-trip.
func TestPackerRoutesSmallFiles(t *testing.T) {
	m := newTestMeta(t)
	ctx := context.Background()
	ino, err := m.CreateChild(ctx, meta.RootIno, "f", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	if err != nil {
		t.Fatal(err)
	}
	up := newFakeUploader()
	cache := newTestCache(t, writerFetcher{uploader: up})
	// Aggressive window so test doesn't wait long.
	packer := NewPacker(m, up, 1024, 10000, 50*time.Millisecond)
	defer packer.Close()
	w, err := NewWriter(ctx, m, cache, up, nil, nil, nil, packer, ino, 4096, 0)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("hello packer world")
	if _, err := w.WriteAt(ctx, payload, 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(ctx); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	chunks, _ := m.ListChunks(ctx, ino)
	if len(chunks) != 1 {
		t.Fatalf("want 1 chunk, got %d", len(chunks))
	}
	c := chunks[0]
	if !c.IsPacked() {
		t.Fatalf("chunk not packed: %+v", c)
	}
	if c.Size != int32(len(payload)) {
		t.Fatalf("Size=%d want %d", c.Size, len(payload))
	}
	if c.PackSize == 0 {
		t.Fatal("PackSize is 0 — pack route didn't run")
	}
	// Reader round-trip — the slice + decrypt + decompress path.
	r := NewReader(m, cache, nil, 4096)
	dest := make([]byte, len(payload))
	n, err := r.ReadAt(ctx, ino, dest, 0)
	if err != nil || n != len(payload) || !bytes.Equal(dest, payload) {
		t.Fatalf("read-back: n=%d err=%v got=%q want=%q", n, err, dest, payload)
	}
}

// TestPackerAggregatesMultiple: write three small files; observe
// they share a single TG message (one blob upload), with distinct
// pack_offsets.
func TestPackerAggregatesMultiple(t *testing.T) {
	m := newTestMeta(t)
	ctx := context.Background()
	up := newFakeUploader()
	cache := newTestCache(t, writerFetcher{uploader: up})
	// Wider window so all three submits land in the same batch.
	packer := NewPacker(m, up, 1024, 1<<20, 100*time.Millisecond)
	defer packer.Close()

	const N = 3
	inos := make([]int64, N)
	for i := 0; i < N; i++ {
		ino, err := m.CreateChild(ctx, meta.RootIno, fmt.Sprintf("f%d", i), meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
		if err != nil {
			t.Fatal(err)
		}
		inos[i] = ino
	}
	// Submit all three before the window elapses. Each Sync blocks on
	// its own per-writer wg, so we issue them in goroutines.
	var wg sync.WaitGroup
	for i, ino := range inos {
		wg.Add(1)
		go func(i int, ino int64) {
			defer wg.Done()
			w, err := NewWriter(ctx, m, cache, up, nil, nil, nil, packer, ino, 4096, 0)
			if err != nil {
				t.Error(err)
				return
			}
			payload := []byte(fmt.Sprintf("file-%d-content", i))
			if _, err := w.WriteAt(ctx, payload, 0); err != nil {
				t.Error(err)
				return
			}
			if err := w.Sync(ctx); err != nil {
				t.Error(err)
				return
			}
		}(i, ino)
	}
	wg.Wait()

	// All three chunks must share one TG message id (the packed blob).
	seen := map[int64]struct{}{}
	for _, ino := range inos {
		chunks, _ := m.ListChunks(ctx, ino)
		if len(chunks) != 1 {
			t.Fatalf("ino %d has %d chunks", ino, len(chunks))
		}
		c := chunks[0]
		if !c.IsPacked() {
			t.Fatalf("ino %d not packed: %+v", ino, c)
		}
		seen[c.TGMessageID] = struct{}{}
	}
	if len(seen) != 1 {
		t.Fatalf("packer didn't aggregate: chunks land in %d distinct TG messages", len(seen))
	}
}

// TestPackerLargeBypasses: a file larger than the packer's threshold
// should go straight to direct upload, not through the pack route.
func TestPackerLargeBypasses(t *testing.T) {
	m := newTestMeta(t)
	ctx := context.Background()
	ino, err := m.CreateChild(ctx, meta.RootIno, "big", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	if err != nil {
		t.Fatal(err)
	}
	up := newFakeUploader()
	cache := newTestCache(t, writerFetcher{uploader: up})
	packer := NewPacker(m, up, 100, 10000, 50*time.Millisecond)
	defer packer.Close()
	w, err := NewWriter(ctx, m, cache, up, nil, nil, nil, packer, ino, 4096, 0)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{'A'}, 500) // way above threshold=100
	if _, err := w.WriteAt(ctx, payload, 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	chunks, _ := m.ListChunks(ctx, ino)
	if len(chunks) != 1 {
		t.Fatalf("want 1 chunk")
	}
	if chunks[0].IsPacked() {
		t.Fatalf("large chunk wrongly packed: %+v", chunks[0])
	}
}

// TestPackerCloseDrainsBuffer is the advisor's discriminator for the
// "Close cancels its own upload ctx" bug. Submit one chunk, Close
// before the window fires, assert the chunk_map row landed and the
// reader can round-trip the data. The bug version of Close cancelled
// the upload ctx before the final flush ran, so the row never landed
// and every buffered small file evaporated at unmount.
func TestPackerCloseDrainsBuffer(t *testing.T) {
	m := newTestMeta(t)
	ctx := context.Background()
	ino, err := m.CreateChild(ctx, meta.RootIno, "f", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	if err != nil {
		t.Fatal(err)
	}
	up := newFakeUploader()
	cache := newTestCache(t, writerFetcher{uploader: up})
	// Window is long so the only flush opportunity is Close's drain.
	packer := NewPacker(m, up, 1024, 1<<20, 30*time.Second)
	w, err := NewWriter(ctx, m, cache, up, nil, nil, nil, packer, ino, 4096, 0)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("drained-on-close")
	if _, err := w.WriteAt(ctx, payload, 0); err != nil {
		t.Fatal(err)
	}
	// Issue Sync in a goroutine — it'll block on the packer's result
	// chan, which only fires after Close's drain.
	syncDone := make(chan error, 1)
	go func() { syncDone <- w.Sync(ctx) }()
	// Give the writer a moment to enqueue in the packer.
	time.Sleep(50 * time.Millisecond)
	packer.Close()
	if err := <-syncDone; err != nil {
		t.Fatalf("Sync after Close: %v", err)
	}
	chunks, _ := m.ListChunks(ctx, ino)
	if len(chunks) != 1 {
		t.Fatalf("chunk_map row missing after Close-drain: %+v", chunks)
	}
	r := NewReader(m, cache, nil, 4096)
	dest := make([]byte, len(payload))
	n, err := r.ReadAt(ctx, ino, dest, 0)
	if err != nil || n != len(payload) || !bytes.Equal(dest, payload) {
		t.Fatalf("read-back: n=%d err=%v got=%q want=%q", n, err, dest, payload)
	}
}

// TestDedupOverwritePreservesUnpackedFields is the advisor's
// discriminator for the "ReuseChunkByHash leaves stale pack
// fields" bug. Setup: write a small file via the packer, then
// overwrite it with a NON-packed chunk whose content hash matches
// an existing direct-uploaded chunk so the dedup path fires. The
// chunk_map row must end up with pack_offset/pack_size = NULL so
// Reader doesn't slice the wrong message.
func TestDedupOverwritePreservesUnpackedFields(t *testing.T) {
	m := newTestMeta(t)
	ctx := context.Background()
	up := newFakeUploader()
	cache := newTestCache(t, writerFetcher{uploader: up})

	// Step 1: create a "source" file with the content X (large enough
	// to bypass packing). This populates chunk_blob with hash(X).
	srcIno, _ := m.CreateChild(ctx, meta.RootIno, "src", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	packer := NewPacker(m, up, 256, 1<<20, 50*time.Millisecond)
	defer packer.Close()
	wSrc, err := NewWriter(ctx, m, cache, up, nil, nil, nil, packer, srcIno, 4096, 0)
	if err != nil {
		t.Fatal(err)
	}
	X := bytes.Repeat([]byte("X"), 1024)
	if _, err := wSrc.WriteAt(ctx, X, 0); err != nil {
		t.Fatal(err)
	}
	if err := wSrc.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Step 2: a small file packed via the packer.
	tgtIno, _ := m.CreateChild(ctx, meta.RootIno, "tgt", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	wTgt, err := NewWriter(ctx, m, cache, up, nil, nil, nil, packer, tgtIno, 4096, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wTgt.WriteAt(ctx, []byte("small content"), 0); err != nil {
		t.Fatal(err)
	}
	if err := wTgt.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	chunks, _ := m.ListChunks(ctx, tgtIno)
	if !chunks[0].IsPacked() {
		t.Fatalf("setup: target wasn't packed: %+v", chunks)
	}

	// Step 3: overwrite the target with content X. The chunk is now
	// 1024 bytes — bigger than the threshold (256) — so it skips the
	// pack route and hits dedup, which reuses the source's TG message.
	wTgt2, err := NewWriter(ctx, m, cache, up, nil, nil, nil, packer, tgtIno, 4096, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := wTgt2.Truncate(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := wTgt2.WriteAt(ctx, X, 0); err != nil {
		t.Fatal(err)
	}
	if err := wTgt2.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	chunks, _ = m.ListChunks(ctx, tgtIno)
	if len(chunks) != 1 {
		t.Fatalf("after overwrite: chunks = %+v", chunks)
	}
	if chunks[0].IsPacked() {
		t.Fatalf("overwrite-dedup left stale pack fields: %+v", chunks[0])
	}
	// And the read-back is the new content X.
	r := NewReader(m, cache, nil, 4096)
	dest := make([]byte, len(X))
	n, err := r.ReadAt(ctx, tgtIno, dest, 0)
	if err != nil || n != len(X) || !bytes.Equal(dest, X) {
		t.Fatalf("read-back after overwrite: n=%d err=%v match=%v", n, err, bytes.Equal(dest, X))
	}
}

// TestPackRouteReleasesPoolSemEarly verifies that pack-routed
// uploadOne calls ReleaseSem() BEFORE waiting on the packer's result
// chan, so a single packer batch can aggregate more than
// DefaultUploadConcurrency wires. Without the early release, the
// pool sem caps the batch at the number of concurrent pool slots.
func TestPackRouteReleasesPoolSemEarly(t *testing.T) {
	m := newTestMeta(t)
	ctx := context.Background()
	up := newSlowUploader()
	cache := newTestCache(t, slowFetcher{up: up})
	// Pool with only 1 slot — without the early release, only one
	// pack-route writer can dispatch at a time, capping the batch.
	pool := NewUploadPool(1)
	// Packer with a 100ms window so the test runs fast.
	packer := NewPacker(m, up, 1024, 1<<20, 100*time.Millisecond)
	defer packer.Close()

	const N = 8 // way over the pool's capacity
	inos := make([]int64, N)
	for i := range inos {
		ino, _ := m.CreateChild(ctx, meta.RootIno, fmt.Sprintf("p%d", i), meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
		inos[i] = ino
	}
	var wg sync.WaitGroup
	for i, ino := range inos {
		wg.Add(1)
		go func(i int, ino int64) {
			defer wg.Done()
			w, err := NewWriter(ctx, m, cache, up, nil, nil, pool, packer, ino, 4096, 0)
			if err != nil {
				t.Error(err)
				return
			}
			if _, err := w.WriteAt(ctx, []byte(fmt.Sprintf("file %d", i)), 0); err != nil {
				t.Error(err)
				return
			}
			// Just Flush (async) — no Sync — the result chan fires
			// when the packer flushes. Without ReleaseSem, only 1
			// writer at a time dispatches, so 8 of them would take
			// >= 8 windows (800ms+) to all reach Submit.
			if err := w.Flush(ctx); err != nil {
				t.Error(err)
				return
			}
		}(i, ino)
	}
	wg.Wait()
	// Release the slow uploader so the packer's flush completes.
	up.release()
	// Drain so chunk_map rows land before assertions.
	drainCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if !pool.Drain(drainCtx) {
		t.Fatal("pool drain timed out")
	}
	// Assert all chunks landed in ONE blob (or close to it). Without
	// the early release, batches would each contain ~1 chunk.
	seen := map[int64]int{}
	for _, ino := range inos {
		chunks, _ := m.ListChunks(ctx, ino)
		if len(chunks) != 1 || !chunks[0].IsPacked() {
			t.Fatalf("ino %d not packed: %+v", ino, chunks)
		}
		seen[chunks[0].TGMessageID]++
	}
	if len(seen) > 2 {
		t.Fatalf("packer made too many batches (%d) — early sem release didn't kick in: %v", len(seen), seen)
	}
}

// rangedFetcher implements both Fetcher and RangedFetcher against the
// fakeUploader's recorded blobs. Used to exercise the cache's
// preference for ranged fetches on packed chunks.
type rangedFetcher struct {
	up         *fakeUploader
	rangeCalls atomic.Int64
	fullCalls  atomic.Int64
}

func (f *rangedFetcher) Fetch(_ context.Context, _ Key, msgID int64) ([]byte, error) {
	f.fullCalls.Add(1)
	blob, ok := f.up.get(int(msgID))
	if !ok {
		return nil, errors.New("no such msg")
	}
	out := make([]byte, len(blob.data))
	copy(out, blob.data)
	return out, nil
}

func (f *rangedFetcher) FetchRange(_ context.Context, _ Key, msgID int64, offset, length int64) ([]byte, error) {
	f.rangeCalls.Add(1)
	blob, ok := f.up.get(int(msgID))
	if !ok {
		return nil, errors.New("no such msg")
	}
	if offset+length > int64(len(blob.data)) {
		return nil, errors.New("range past EOF")
	}
	out := make([]byte, length)
	copy(out, blob.data[offset:offset+length])
	return out, nil
}

// TestPackedChunkUsesRangedFetcher: a packed chunk should be fetched
// via FetchRange (not full-blob Fetch), and the read-back bytes must
// match what was written. Unpacked chunks should still use Fetch.
func TestPackedChunkUsesRangedFetcher(t *testing.T) {
	m := newTestMeta(t)
	ctx := context.Background()
	up := newFakeUploader()
	rf := &rangedFetcher{up: up}
	cache, err := NewCache(t.TempDir(), 1<<20, rf, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Aggressive window so the test runs fast.
	packer := NewPacker(m, up, 1024, 1<<20, 50*time.Millisecond)
	defer packer.Close()

	const N = 3
	inos := make([]int64, N)
	payloads := make([][]byte, N)
	for i := range inos {
		ino, _ := m.CreateChild(ctx, meta.RootIno, fmt.Sprintf("p%d", i), meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
		inos[i] = ino
		payloads[i] = []byte(fmt.Sprintf("packed-file-%d-content", i))
	}
	var wg sync.WaitGroup
	for i, ino := range inos {
		wg.Add(1)
		go func(i int, ino int64) {
			defer wg.Done()
			w, err := NewWriter(ctx, m, cache, up, nil, nil, nil, packer, ino, 4096, 0)
			if err != nil {
				t.Error(err)
				return
			}
			if _, err := w.WriteAt(ctx, payloads[i], 0); err != nil {
				t.Error(err)
				return
			}
			if err := w.Sync(ctx); err != nil {
				t.Error(err)
				return
			}
		}(i, ino)
	}
	wg.Wait()

	// Sanity: all three packed into the same blob (or close to it).
	r := NewReader(m, cache, nil, 4096)
	for i, ino := range inos {
		chunks, _ := m.ListChunks(ctx, ino)
		if len(chunks) != 1 || !chunks[0].IsPacked() {
			t.Fatalf("ino %d not packed: %+v", ino, chunks)
		}
		dest := make([]byte, len(payloads[i]))
		n, err := r.ReadAt(ctx, ino, dest, 0)
		if err != nil || n != len(payloads[i]) || !bytes.Equal(dest, payloads[i]) {
			t.Fatalf("read-back %d: n=%d err=%v match=%v", i, n, err, bytes.Equal(dest, payloads[i]))
		}
	}
	if got := rf.rangeCalls.Load(); got != int64(N) {
		t.Fatalf("FetchRange called %d times, want %d (one per packed chunk read)", got, N)
	}
	if got := rf.fullCalls.Load(); got != 0 {
		t.Fatalf("Fetch called %d times for packed chunks (want 0 — RangedFetcher should be preferred)", got)
	}
}

// TestUnpackedChunkUsesFullFetch keeps the inverse honest: when
// chunk_map has no pack fields, we should use plain Fetch (not
// FetchRange), so legacy chunks keep their existing path.
func TestUnpackedChunkUsesFullFetch(t *testing.T) {
	m := newTestMeta(t)
	ctx := context.Background()
	ino, _ := m.CreateChild(ctx, meta.RootIno, "big", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	up := newFakeUploader()
	rf := &rangedFetcher{up: up}
	cache, err := NewCache(t.TempDir(), 1<<20, rf, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Large chunk (above packer threshold), so it doesn't pack.
	w, err := NewWriter(ctx, m, cache, up, nil, nil, nil, nil, ino, 4096, 0)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("A"), 4000) // above DefaultPackThreshold? No, 256 KB. Just no packer wired anyway.
	if _, err := w.WriteAt(ctx, payload, 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	r := NewReader(m, cache, nil, 4096)
	dest := make([]byte, len(payload))
	n, err := r.ReadAt(ctx, ino, dest, 0)
	if err != nil || n != len(payload) || !bytes.Equal(dest, payload) {
		t.Fatalf("read-back: n=%d err=%v match=%v", n, err, bytes.Equal(dest, payload))
	}
	if got := rf.fullCalls.Load(); got != 1 {
		t.Fatalf("Fetch called %d times, want 1", got)
	}
	if got := rf.rangeCalls.Load(); got != 0 {
		t.Fatalf("FetchRange called %d times for unpacked chunk (want 0)", got)
	}
}

// TestPackerCapsFlushBatch is the regression guard for the
// unbounded-batch bug that turned 173 recovered wires into one
// 689 MiB blob: flush must take at most ~target bytes of wires
// per call, with the rest staying in pending and re-signaling
// wake.
func TestPackerCapsFlushBatch(t *testing.T) {
	m := newTestMeta(t)
	ctx := context.Background()
	up := newFakeUploader()
	cache := newTestCache(t, writerFetcher{uploader: up})
	// Target is small (100 bytes) so we can produce many "wires"
	// well under DefaultPackThreshold and reason about batching.
	packer := NewPacker(m, up, 1024, 100, 50*time.Millisecond)
	defer packer.Close()

	const N = 10
	const wireSize = 40 // each wire is 40 B; target=100 → ~2-3 wires per blob
	inos := make([]int64, N)
	for i := range inos {
		ino, _ := m.CreateChild(ctx, meta.RootIno, fmt.Sprintf("p%d", i), meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
		inos[i] = ino
	}
	// Submit all wires in a tight loop BEFORE the window fires, so a
	// buggy flush would coalesce them into one blob.
	var wg sync.WaitGroup
	for i, ino := range inos {
		wg.Add(1)
		go func(i int, ino int64) {
			defer wg.Done()
			w, err := NewWriter(ctx, m, cache, up, nil, nil, nil, packer, ino, 4096, 0)
			if err != nil {
				t.Error(err)
				return
			}
			payload := bytes.Repeat([]byte{'x'}, wireSize)
			if _, err := w.WriteAt(ctx, payload, 0); err != nil {
				t.Error(err)
				return
			}
			if err := w.Sync(ctx); err != nil {
				t.Error(err)
				return
			}
		}(i, ino)
	}
	wg.Wait()

	// Count distinct blob msg_ids. With cap working, we should see
	// multiple blobs (~N×wireSize / target = 4-ish). With the bug,
	// all 10 land in one blob.
	seen := map[int64]int{}
	for _, ino := range inos {
		chunks, _ := m.ListChunks(ctx, ino)
		if len(chunks) != 1 || !chunks[0].IsPacked() {
			t.Fatalf("ino %d not packed: %+v", ino, chunks)
		}
		seen[chunks[0].TGMessageID]++
	}
	if len(seen) < 2 {
		t.Fatalf("packer fused all %d wires into one blob (saw %d distinct msg_ids); flush cap not respected", N, len(seen))
	}
	// And no blob should exceed target by more than one wire's worth
	// (the "always take at least 1" allowance).
	for msgID, count := range seen {
		blob, _ := up.get(int(msgID))
		if int64(len(blob.data)) > 100+wireSize {
			t.Fatalf("blob %d is %d bytes (target=%d, max-overhead=%d) — cap broken",
				msgID, len(blob.data), 100, 100+wireSize)
		}
		_ = count
	}
}

// TestTierPromotion exercises the multi-tier chunk-size picker:
// a fresh writer with the FS-default 4 MiB chunkSz, writing past
// the mid threshold (64 MiB), must promote to 16 MiB chunks
// BEFORE first dispatch — and the inode row + dispatched chunks
// must reflect the new size.
func TestTierPromotion(t *testing.T) {
	m := newTestMeta(t)
	ctx := context.Background()
	// Create inode at FS-default (4 MiB) — the tier-small floor.
	ino, _ := m.CreateChild(ctx, meta.RootIno, "big", meta.Inode{
		Kind: meta.KindFile, Mode: 0o100644, ChunkSize: TierSmallChunkSize,
	})
	up := newFakeUploader()
	cache := newTestCache(t, writerFetcher{uploader: up})
	w, err := NewWriter(ctx, m, cache, up, nil, nil, nil, nil, ino, TierSmallChunkSize, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	// Write 80 MiB — above TierMidThreshold (64 MiB) → mid tier.
	payload := bytes.Repeat([]byte{'A'}, 80<<20)
	if _, err := w.WriteAt(ctx, payload, 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	in, _ := m.GetInode(ctx, ino)
	if in.ChunkSize != TierMidChunkSize {
		t.Fatalf("inode chunk_size = %d, want mid tier %d", in.ChunkSize, TierMidChunkSize)
	}
	chunks, _ := m.ListChunks(ctx, ino)
	wantChunks := int((80 << 20) / TierMidChunkSize)
	if (80<<20)%TierMidChunkSize != 0 {
		wantChunks++
	}
	if len(chunks) != wantChunks {
		t.Fatalf("chunk count = %d, want %d (80 MiB at mid tier)", len(chunks), wantChunks)
	}
	// Verify reader round-trips through the new tier.
	r := NewReader(m, cache, nil, TierSmallChunkSize) // FS default fallback
	got := make([]byte, len(payload))
	n, err := r.ReadAt(ctx, ino, got, 0)
	if err != nil || n != len(payload) || !bytes.Equal(got, payload) {
		t.Fatalf("read-back: n=%d err=%v equal=%v", n, err, bytes.Equal(got, payload))
	}
}

// TestTierNoDowngradeAfterDispatch: once chunks have been dispatched
// at one tier, subsequent writes (even very large) must not promote.
// The reader's offset math depends on a stable per-inode chunk_size.
func TestTierNoDowngradeAfterDispatch(t *testing.T) {
	m := newTestMeta(t)
	ctx := context.Background()
	ino, _ := m.CreateChild(ctx, meta.RootIno, "grow", meta.Inode{
		Kind: meta.KindFile, Mode: 0o100644, ChunkSize: TierSmallChunkSize,
	})
	up := newFakeUploader()
	cache := newTestCache(t, writerFetcher{uploader: up})
	w, err := NewWriter(ctx, m, cache, up, nil, nil, nil, nil, ino, TierSmallChunkSize, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	// First Sync at 5 MiB — well under mid threshold, no promotion.
	small := bytes.Repeat([]byte{'a'}, 5<<20)
	if _, err := w.WriteAt(ctx, small, 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	in, _ := m.GetInode(ctx, ino)
	if in.ChunkSize != TierSmallChunkSize {
		t.Fatalf("after small sync, chunk_size = %d, want small %d", in.ChunkSize, TierSmallChunkSize)
	}
	// Now write a LOT more — past mid threshold. But chunks are
	// already dispatched, so we must NOT promote.
	more := bytes.Repeat([]byte{'b'}, 100<<20)
	if _, err := w.WriteAt(ctx, more, int64(len(small))); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	in, _ = m.GetInode(ctx, ino)
	if in.ChunkSize != TierSmallChunkSize {
		t.Fatalf("after large append, chunk_size silently changed to %d (must stay locked at small after dispatch)", in.ChunkSize)
	}
}
