package chunk

import (
	"bytes"
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"

	"telfs/internal/meta"
)

// fakeFetcher serves predictable byte patterns keyed by message id. It
// also counts calls so tests can confirm the cache is actually caching.
type fakeFetcher struct {
	contents map[int64][]byte
	calls    atomic.Int64
}

func (f *fakeFetcher) Fetch(_ context.Context, _ Key, msgID int64) ([]byte, error) {
	f.calls.Add(1)
	return f.contents[msgID], nil
}

// seedFile inserts a file inode under root with chunk_map entries
// pointing at the byte slices in `chunks` (each becomes one chunk
// keyed by ascending msg id, starting at base).
func seedFile(t *testing.T, m *meta.Store, name string, chunks [][]byte, baseMsgID int64) (int64, *fakeFetcher) {
	t.Helper()
	ctx := context.Background()
	ino, err := m.CreateChild(ctx, meta.RootIno, name, meta.Inode{
		Kind: meta.KindFile, Mode: 0o100644,
	})
	if err != nil {
		t.Fatalf("CreateChild: %v", err)
	}
	ff := &fakeFetcher{contents: make(map[int64][]byte)}
	for i, payload := range chunks {
		msgID := baseMsgID + int64(i)
		ff.contents[msgID] = payload
		if err := m.PutChunk(ctx, meta.Chunk{
			Ino: ino, Idx: int32(i), TGMessageID: msgID, Size: int32(len(payload)),
		}); err != nil {
			t.Fatalf("PutChunk %d: %v", i, err)
		}
	}
	// Set file size to total.
	var total int64
	for _, c := range chunks {
		total += int64(len(c))
	}
	if err := m.SetSize(ctx, ino, total); err != nil {
		t.Fatal(err)
	}
	return ino, ff
}

func newTestMeta(t *testing.T) *meta.Store {
	t.Helper()
	s, err := meta.Open(filepath.Join(t.TempDir(), "meta.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func newTestCache(t *testing.T, fetcher Fetcher) *Cache {
	t.Helper()
	c, err := NewCache(filepath.Join(t.TempDir(), "cache"), 100<<10, fetcher)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestReadAtCrossChunkBoundary is the discriminator the advisor flagged:
// reading bytes that straddle the boundary between chunk 0 and chunk 1
// must stitch them back together correctly.
func TestReadAtCrossChunkBoundary(t *testing.T) {
	m := newTestMeta(t)
	// chunkSize 10 -> two chunks: "AAAAAAAAAA" (10 bytes) + "BBBBBBB" (7 bytes).
	chunks := [][]byte{
		[]byte("AAAAAAAAAA"),
		[]byte("BBBBBBB"),
	}
	ino, ff := seedFile(t, m, "x", chunks, 1000)
	cache := newTestCache(t, ff)
	r := NewReader(m, cache, 10)

	// Read 5 bytes starting at offset 8 — spans chunk 0 (8..10) + chunk 1 (0..3).
	got := make([]byte, 5)
	n, err := r.ReadAt(context.Background(), ino, got, 8)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != 5 {
		t.Fatalf("n = %d, want 5", n)
	}
	want := []byte("AABBB")
	if !bytes.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestReadAtPastEOFReturnsShort(t *testing.T) {
	m := newTestMeta(t)
	ino, ff := seedFile(t, m, "x", [][]byte{[]byte("Hello")}, 1)
	cache := newTestCache(t, ff)
	r := NewReader(m, cache, 10)

	// Ask for 20 bytes; file only has 5.
	dest := make([]byte, 20)
	n, err := r.ReadAt(context.Background(), ino, dest, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != 5 {
		t.Fatalf("n = %d, want 5", n)
	}
	if !bytes.Equal(dest[:5], []byte("Hello")) {
		t.Fatalf("got %q", dest[:5])
	}
}

func TestReadAtCachesAfterFirstFetch(t *testing.T) {
	m := newTestMeta(t)
	ino, ff := seedFile(t, m, "x", [][]byte{[]byte("HelloWorld")}, 1)
	cache := newTestCache(t, ff)
	r := NewReader(m, cache, 10)

	dest := make([]byte, 10)
	for i := 0; i < 3; i++ {
		if _, err := r.ReadAt(context.Background(), ino, dest, 0); err != nil {
			t.Fatalf("ReadAt iter %d: %v", i, err)
		}
	}
	if ff.calls.Load() != 1 {
		t.Fatalf("fetcher called %d times, want 1 (cache miss + 2 hits)", ff.calls.Load())
	}
}

func TestReadAtFullChunkAligned(t *testing.T) {
	m := newTestMeta(t)
	chunks := [][]byte{
		[]byte("AAAAAAAAAA"),
		[]byte("BBBBBBBBBB"),
		[]byte("CCCCC"),
	}
	ino, ff := seedFile(t, m, "x", chunks, 100)
	cache := newTestCache(t, ff)
	r := NewReader(m, cache, 10)

	dest := make([]byte, 25)
	n, err := r.ReadAt(context.Background(), ino, dest, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 25 {
		t.Fatalf("n = %d, want 25", n)
	}
	want := []byte("AAAAAAAAAABBBBBBBBBBCCCCC")
	if !bytes.Equal(dest, want) {
		t.Fatalf("got %q, want %q", dest, want)
	}
}

func TestCacheInvalidateRemovesEntry(t *testing.T) {
	m := newTestMeta(t)
	ino, ff := seedFile(t, m, "x", [][]byte{[]byte("HelloWorld")}, 1)
	cache := newTestCache(t, ff)
	r := NewReader(m, cache, 10)

	dest := make([]byte, 10)
	_, _ = r.ReadAt(context.Background(), ino, dest, 0)
	if !cache.Invalidate(Key{Ino: ino, Idx: 0}) {
		t.Fatalf("Invalidate returned false; entry should have existed")
	}
	// Next read re-fetches.
	_, _ = r.ReadAt(context.Background(), ino, dest, 0)
	if ff.calls.Load() != 2 {
		t.Fatalf("fetcher calls = %d, want 2", ff.calls.Load())
	}
}
