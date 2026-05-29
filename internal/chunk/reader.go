package chunk

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"telfs/internal/meta"
)

// PrefetchWindow is how many chunks past the current ReadAt cursor
// the Reader speculatively fetches. Sequential reads (cp, md5sum, cat,
// video playback) benefit because the round trip for chunk N+k happens
// in the background while the kernel is still consuming chunk N. The
// window matches DefaultUploadConcurrency on the write side — the
// same 4 in-flight gotd transfers a typical residential uplink can
// sustain.
const PrefetchWindow = 4

// PrefetchConcurrency caps simultaneous prefetch RPCs. Independent of
// PrefetchWindow because windows can pile up if a Read sweeps fast
// through many small reads (e.g., kernel readahead chunked into
// 128 KiB requests) — we want at most this many gotd downloads in
// flight regardless.
const PrefetchConcurrency = 4

// Reader orchestrates random-offset reads of a TelFS file by walking
// chunk_map, consulting the disk LRU cache, and pulling missing chunks
// from Telegram via the Cache's Fetcher.
//
// Reader runs a small prefetch pool so sequential reads don't pay the
// full network round-trip per chunk. Each ReadAt call schedules
// background fetches for the next PrefetchWindow chunks before doing
// its own (potentially slow) cache.Get; by the time the kernel asks
// for chunk N+1, it should already be in the LRU.
type Reader struct {
	meta      *meta.Store
	cache     *Cache
	chunkSize int64

	// Prefetch state. prefetchSem caps in-flight fetches; inflight is
	// a per-Key dedup so two consecutive ReadAt calls don't both kick
	// off the same fetch. Both protect against pathological wasted
	// network calls, not against correctness — the cache already
	// dedups concurrent inserts for the same key.
	prefetchSem chan struct{}
	inflight    sync.Map // Key → struct{}
}

// NewReader wires up the read path. chunkSize is normally ChunkSize but
// is configurable for tests.
func NewReader(m *meta.Store, c *Cache, chunkSize int64) *Reader {
	if chunkSize <= 0 {
		chunkSize = ChunkSize
	}
	return &Reader{
		meta:        m,
		cache:       c,
		chunkSize:   chunkSize,
		prefetchSem: make(chan struct{}, PrefetchConcurrency),
	}
}

// ReadAt fills dest with bytes from ino starting at off. Returns the
// number of bytes actually written into dest; if the request runs past
// EOF, the short return is the truncated portion (no error).
//
// Missing chunks (chunk_map entry not present at idx) are treated as
// EOF — when a write extends a file we record chunk entries as we go, so
// a gap means the file ends there.
func (r *Reader) ReadAt(ctx context.Context, ino int64, dest []byte, off int64) (int, error) {
	if len(dest) == 0 {
		return 0, nil
	}
	startIdx := int32(off / r.chunkSize)
	endIdx := int32((off + int64(len(dest)) - 1) / r.chunkSize)

	written := 0
	cur := off
	end := off + int64(len(dest))

	for idx := startIdx; idx <= endIdx; idx++ {
		// Kick off read-ahead BEFORE we block on the current chunk —
		// the network round-trip for chunks idx+1..idx+W can overlap
		// with this chunk's fetch.
		r.scheduleAhead(ino, idx+1, PrefetchWindow)

		c, err := r.meta.GetChunk(ctx, ino, idx)
		if errors.Is(err, meta.ErrNotFound) {
			// Past EOF — return what we have so far.
			return written, nil
		}
		if err != nil {
			return written, fmt.Errorf("chunk lookup %d/%d: %w", ino, idx, err)
		}
		data, err := r.cache.Get(ctx, Key{Ino: ino, Idx: idx}, c.TGMessageID)
		if err != nil {
			return written, err
		}
		chunkStart := int64(idx) * r.chunkSize
		relStart := cur - chunkStart
		// relEnd is bounded by both the requested range AND the actual
		// chunk size (the last chunk is short).
		relEnd := relStart + (end - cur)
		if relEnd > int64(len(data)) {
			relEnd = int64(len(data))
		}
		if relStart >= relEnd {
			break // nothing more to read from this chunk
		}
		n := copy(dest[written:], data[relStart:relEnd])
		written += n
		cur += int64(n)
		// If we got less than the full requested range from this chunk
		// (because the chunk was short — last chunk of file), we're at
		// EOF regardless of dest's remaining space.
		if int64(len(data)) < r.chunkSize && relEnd == int64(len(data)) {
			break
		}
	}
	return written, nil
}

// scheduleAhead fires off background prefetches for chunks
// [startIdx, startIdx+count). Skips chunks that are already cached or
// already being fetched. Uses Background ctx so the prefetch survives
// the originating ReadAt's return — the next ReadAt for that idx will
// see a warm cache.
func (r *Reader) scheduleAhead(ino int64, startIdx int32, count int) {
	for offset := 0; offset < count; offset++ {
		idx := startIdx + int32(offset)
		key := Key{Ino: ino, Idx: idx}
		if r.cache.Has(key) {
			continue
		}
		// Dedup: a previous ReadAt may already have a prefetch in flight
		// for this key.
		if _, loaded := r.inflight.LoadOrStore(key, struct{}{}); loaded {
			continue
		}
		go r.prefetchOne(ino, idx, key)
	}
}

// prefetchOne acquires a sem slot then warms the cache for a single
// chunk. Errors are silent — prefetch is best-effort; the caller's own
// ReadAt will surface a real error when it asks for the chunk later.
func (r *Reader) prefetchOne(ino int64, idx int32, key Key) {
	defer r.inflight.Delete(key)
	// Bounded concurrency. If the queue is full, this prefetch waits;
	// that's the backpressure we want — sequential reads will catch up.
	ctx := context.Background()
	r.prefetchSem <- struct{}{}
	defer func() { <-r.prefetchSem }()

	c, err := r.meta.GetChunk(ctx, ino, idx)
	if err != nil {
		// Past EOF or missing — nothing to warm.
		return
	}
	// cache.Get does the fetch + decrypt + disk persist on miss.
	_, _ = r.cache.Get(ctx, key, c.TGMessageID)
}
