package chunk

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"telfs/internal/meta"
)

// blobFakeFetcher serves ranged reads out of an in-memory blob and
// records every call. Implements both Fetcher (whole message) and
// RangedFetcher.
type blobFakeFetcher struct {
	mu    sync.Mutex
	blob  []byte
	calls []struct{ off, length int64 }
}

func (f *blobFakeFetcher) Fetch(_ context.Context, _ Key, _ int64) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, struct{ off, length int64 }{0, int64(len(f.blob))})
	return append([]byte(nil), f.blob...), nil
}

func (f *blobFakeFetcher) FetchRange(_ context.Context, _ Key, _ int64, off, length int64) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, struct{ off, length int64 }{off, length})
	if off < 0 || off+length > int64(len(f.blob)) {
		return nil, fmt.Errorf("range [%d:%d) out of blob (%d bytes)", off, off+length, len(f.blob))
	}
	return append([]byte(nil), f.blob[off:off+length]...), nil
}

func (f *blobFakeFetcher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// buildPackedBlob creates n extents of extSize bytes back-to-back
// (NoopCipher + compress.None: wire == plaintext), registers a
// chunk_map row per extent under a fresh inode, and returns the blob
// plus the rows.
func buildPackedBlob(t *testing.T, m *meta.Store, msgID int64, n int, extSize int) ([]byte, []meta.Chunk) {
	t.Helper()
	ctx := context.Background()
	var blob bytes.Buffer
	var rows []meta.Chunk
	for i := 0; i < n; i++ {
		ino, err := m.CreateChild(ctx, meta.RootIno, fmt.Sprintf("f%d-%d", msgID, i), meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
		if err != nil {
			t.Fatal(err)
		}
		payload := bytes.Repeat([]byte{byte('a' + i%26)}, extSize)
		row := meta.Chunk{
			Ino: ino, Idx: 0, TGMessageID: msgID,
			Size:       int32(extSize),
			PackOffset: int64(blob.Len()),
			PackSize:   int32(extSize),
		}
		if err := m.PutChunk(ctx, row); err != nil {
			t.Fatal(err)
		}
		blob.Write(payload)
		rows = append(rows, row)
	}
	return blob.Bytes(), rows
}

// waitCached polls until every key is in the cache or the deadline
// passes (the prefetcher works in a background goroutine).
func waitCached(t *testing.T, c *Cache, keys []Key, deadline time.Duration) bool {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		all := true
		for _, k := range keys {
			if !c.Has(k) {
				all = false
				break
			}
		}
		if all {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// TestBlobPrefetchWarmsNeighbors is the core scenario: one Note on
// the first extent of a cold blob warms every sibling in the budget
// window with a single ranged download, and subsequent reads of the
// siblings are pure cache hits (no further fetcher calls).
func TestBlobPrefetchWarmsNeighbors(t *testing.T) {
	m := newTestMeta(t)
	blob, rows := buildPackedBlob(t, m, 500, 8, 128)
	f := &blobFakeFetcher{blob: blob}
	cache := newTestCache(t, f)
	p := NewBlobPrefetcher(m, cache, f, nil, nil, 1<<20)
	defer p.Close()

	p.Note(500, rows[0].PackOffset)

	keys := make([]Key, len(rows))
	for i, r := range rows {
		keys[i] = Key{Ino: r.Ino, Idx: r.Idx}
	}
	if !waitCached(t, cache, keys, 2*time.Second) {
		t.Fatalf("prefetch did not warm all %d extents", len(rows))
	}
	if got := f.callCount(); got != 1 {
		t.Fatalf("fetcher calls = %d, want 1 (single span download)", got)
	}
	// Reading a warmed sibling must be a hit with correct content.
	data, err := cache.Get(context.Background(), keys[5], 500, rows[5].PackOffset, rows[5].PackSize)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, bytes.Repeat([]byte{byte('a' + 5)}, 128)) {
		t.Fatal("warmed extent content mismatch")
	}
	if got := f.callCount(); got != 1 {
		t.Fatalf("fetcher calls after warmed read = %d, want still 1", got)
	}
}

// TestBlobPrefetchBudget: extents whose END crosses the budget
// boundary stay cold; the span only covers what fits.
func TestBlobPrefetchBudget(t *testing.T) {
	m := newTestMeta(t)
	blob, rows := buildPackedBlob(t, m, 501, 6, 100)
	f := &blobFakeFetcher{blob: blob}
	cache := newTestCache(t, f)
	// Budget of 350 bytes → extents at offsets 0,100,200 fit
	// ([0,300) ends ≤ 350); the extent at 300 ends at 400 > 350.
	p := NewBlobPrefetcher(m, cache, f, nil, nil, 350)
	defer p.Close()

	p.Note(501, 0)

	warm := []Key{
		{Ino: rows[0].Ino}, {Ino: rows[1].Ino}, {Ino: rows[2].Ino},
	}
	if !waitCached(t, cache, warm, 2*time.Second) {
		t.Fatal("in-budget extents not warmed")
	}
	for _, r := range rows[3:] {
		if cache.Has(Key{Ino: r.Ino, Idx: r.Idx}) {
			t.Fatalf("extent at offset %d beyond budget was warmed", r.PackOffset)
		}
	}
}

// TestBlobPrefetchDedupe: repeated Notes into the same budget bucket
// coalesce to one span fetch.
func TestBlobPrefetchDedupe(t *testing.T) {
	m := newTestMeta(t)
	blob, rows := buildPackedBlob(t, m, 502, 4, 64)
	f := &blobFakeFetcher{blob: blob}
	cache := newTestCache(t, f)
	p := NewBlobPrefetcher(m, cache, f, nil, nil, 1<<20)
	defer p.Close()

	for i := 0; i < 10; i++ {
		p.Note(502, rows[i%len(rows)].PackOffset)
	}
	keys := make([]Key, len(rows))
	for i, r := range rows {
		keys[i] = Key{Ino: r.Ino, Idx: r.Idx}
	}
	if !waitCached(t, cache, keys, 2*time.Second) {
		t.Fatal("extents not warmed")
	}
	// Give any (buggy) duplicate spans a moment to fire, then count.
	time.Sleep(50 * time.Millisecond)
	if got := f.callCount(); got != 1 {
		t.Fatalf("fetcher calls = %d, want 1 (bucket dedupe)", got)
	}
}

// TestBlobPrefetchChains is the regression for the bucket-alignment
// bug: a miss BEYOND what an earlier span covered must trigger a new
// span starting at that miss (the walk's future), not be dedupe-
// suppressed and not re-fetch the walk's past.
func TestBlobPrefetchChains(t *testing.T) {
	m := newTestMeta(t)
	blob, rows := buildPackedBlob(t, m, 505, 10, 100)
	f := &blobFakeFetcher{blob: blob}
	cache := newTestCache(t, f)
	p := NewBlobPrefetcher(m, cache, f, nil, nil, 350)
	defer p.Close()

	// Span 1 from offset 0 covers extents 0..2 ([0,300); extent 3
	// ends at 400 > 350).
	p.Note(505, 0)
	if !waitCached(t, cache, []Key{{Ino: rows[0].Ino}, {Ino: rows[1].Ino}, {Ino: rows[2].Ino}}, 2*time.Second) {
		t.Fatal("span 1 extents not warmed")
	}
	// A miss at extent 5 (offset 500) is past span 1's coverage —
	// must chain span 2 covering extents 5..7 ([500,800)).
	p.Note(505, rows[5].PackOffset)
	if !waitCached(t, cache, []Key{{Ino: rows[5].Ino}, {Ino: rows[6].Ino}, {Ino: rows[7].Ino}}, 2*time.Second) {
		t.Fatal("chained span 2 extents not warmed")
	}
	if got := f.callCount(); got != 2 {
		t.Fatalf("fetcher calls = %d, want 2 (one per chained span)", got)
	}
	// The gap (extents 3,4) and the tail (8,9) stay cold.
	for _, i := range []int{3, 4, 8, 9} {
		if cache.Has(Key{Ino: rows[i].Ino}) {
			t.Fatalf("extent %d outside both spans was warmed", i)
		}
	}
	// Spans must start AT the miss: no fetched range may begin
	// before its triggering offset.
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls[0].off != 0 || f.calls[1].off != rows[5].PackOffset {
		t.Fatalf("span starts = %d,%d; want 0,%d (forward from miss)", f.calls[0].off, f.calls[1].off, rows[5].PackOffset)
	}
}

// TestBlobPrefetchSingleExtentSkips: a blob region with only one
// uncached extent isn't worth a span — the foreground read handles
// it. No fetcher call may happen.
func TestBlobPrefetchSingleExtentSkips(t *testing.T) {
	m := newTestMeta(t)
	blob, rows := buildPackedBlob(t, m, 503, 1, 64)
	f := &blobFakeFetcher{blob: blob}
	cache := newTestCache(t, f)
	p := NewBlobPrefetcher(m, cache, f, nil, nil, 1<<20)
	defer p.Close()

	p.Note(503, rows[0].PackOffset)
	time.Sleep(100 * time.Millisecond)
	if got := f.callCount(); got != 0 {
		t.Fatalf("fetcher calls = %d, want 0 (single extent skipped)", got)
	}
}

// TestReaderTriggersBlobPrefetch: an end-to-end read through the
// Reader on a cold packed extent both returns the right bytes AND
// warms the blob neighbors.
func TestReaderTriggersBlobPrefetch(t *testing.T) {
	m := newTestMeta(t)
	blob, rows := buildPackedBlob(t, m, 504, 6, 256)
	f := &blobFakeFetcher{blob: blob}
	cache := newTestCache(t, f)
	p := NewBlobPrefetcher(m, cache, f, nil, nil, 1<<20)
	defer p.Close()

	// Give the first extent's inode a real size so ReadAt has bounds.
	ctx := context.Background()
	if err := m.SetSize(ctx, rows[0].Ino, 256); err != nil {
		t.Fatal(err)
	}
	r := NewReader(m, cache, nil, 4<<20)
	r.SetBlobPrefetcher(p)

	buf := make([]byte, 256)
	n, err := r.ReadAt(ctx, rows[0].Ino, buf, 0)
	if err != nil || n != 256 {
		t.Fatalf("ReadAt: n=%d err=%v", n, err)
	}
	keys := make([]Key, len(rows))
	for i, row := range rows {
		keys[i] = Key{Ino: row.Ino, Idx: row.Idx}
	}
	if !waitCached(t, cache, keys, 2*time.Second) {
		t.Fatal("reader-triggered prefetch did not warm blob neighbors")
	}
}
