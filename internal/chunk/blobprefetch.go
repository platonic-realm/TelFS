package chunk

// Packed-blob locality prefetch.
//
// Small files are packed many-to-a-message (see Packer), in write
// order — which for tar extraction and directory walks is also the
// order they'll be READ. A cold read of one packed extent therefore
// strongly predicts reads of the extents packed right after it. The
// per-file read path can't exploit that (each tiny file is a single
// extent; there is nothing "ahead" within the file to prefetch), so a
// cold `make menuconfig` walk over ~1,500 Kconfig files paid ~1,500
// serial ranged round trips.
//
// BlobPrefetcher turns a packed-extent cache miss into ONE ranged
// download covering the next run of uncached extents in the same
// blob (bounded by a byte budget), decodes each extent, and lands
// them in the cache as if individually fetched. The reader's own
// fetch for the missed extent proceeds untouched in the foreground —
// prefetch strictly adds background warmth, so a wrong guess costs
// bandwidth, never latency.

import (
	"context"
	"log"
	"sync"

	"telfs/internal/compress"
	"telfs/internal/crypto"
	"telfs/internal/meta"
	"telfs/internal/metrics"
)

// DefaultBlobPrefetchBudget caps the bytes one span download may
// cover. 4 MiB rides the ranged fetcher's internal parallelism (4 ×
// 1 MiB parts ≈ one round-trip burst) and typically spans hundreds of
// packed small files, while keeping the cost of a wrong locality
// guess (a genuinely random single-file access) to one burst.
const DefaultBlobPrefetchBudget int64 = 4 << 20

// blobPrefetchMaxExtents bounds the chunk_map rows examined (and
// decoded) per span, independent of the byte budget — a blob packed
// from very tiny files could otherwise put thousands of rows in one
// budget window.
const blobPrefetchMaxExtents = 2048

// blobSpan is one prefetch request: warm the blob FORWARD from the
// missed extent's offset. Forward, not bucket-aligned: a first
// version aligned the fetch to budget-sized buckets, which mostly
// downloaded the extents BEFORE the miss (the walk's past, not its
// future) and then dedup-suppressed every later miss in the bucket.
type blobSpan struct {
	msgID  int64
	offset int64
}

// interval is a half-open covered byte range [start, end) of a blob.
type interval struct{ start, end int64 }

// BlobPrefetcher warms cache neighborhoods around packed-extent
// misses. One worker goroutine; a bounded queue that DROPS new spans
// when full (prefetch is advisory — backpressure here would leak into
// the foreground read path that calls Note).
type BlobPrefetcher struct {
	meta    *meta.Store
	cache   *Cache
	fetcher RangedFetcher
	wc      WireCodec
	budget  int64

	queue  chan blobSpan
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// covered tracks, per blob message, which byte ranges a span has
	// already fetched (or has queued). A miss inside a covered range
	// is dropped — either its extent is warming right now, or it was
	// warmed and later evicted, in which case the bounded interval
	// list (oldest falls off at cap) lets the region re-qualify
	// eventually. Misses BEYOND the covered ranges chain a new span,
	// which is what keeps a long directory walk continuously warmed.
	mu      sync.Mutex
	covered map[int64][]interval
}

// NewBlobPrefetcher constructs and starts the prefetcher. budget <= 0
// falls back to DefaultBlobPrefetchBudget. cipher/codec may be nil
// (plaintext / no compression), mirroring NewCache's defaults —
// decode here MUST match what the cache's own miss path would do.
func NewBlobPrefetcher(m *meta.Store, c *Cache, f RangedFetcher, cipher crypto.Cipher, codec compress.Codec, budget int64) *BlobPrefetcher {
	return NewBlobPrefetcherWire(m, c, f, NewFormat1Codec(cipher, codec), budget)
}

// NewBlobPrefetcherWire is NewBlobPrefetcher with an explicit wire codec.
func NewBlobPrefetcherWire(m *meta.Store, c *Cache, f RangedFetcher, wc WireCodec, budget int64) *BlobPrefetcher {
	if budget <= 0 {
		budget = DefaultBlobPrefetchBudget
	}
	if wc == nil {
		wc = NewFormat1Codec(nil, nil)
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &BlobPrefetcher{
		meta:    m,
		cache:   c,
		fetcher: f,
		wc:      wc,
		budget:  budget,
		queue:   make(chan blobSpan, 32),
		ctx:     ctx,
		cancel:  cancel,
		covered: make(map[int64][]interval),
	}
	p.wg.Add(1)
	go p.worker()
	return p
}

// Close stops the worker and waits for any in-flight span download to
// unwind. Safe to call once; Note after Close is a no-op (queue send
// races the drain, worst case the span is silently dropped).
func (p *BlobPrefetcher) Close() {
	p.cancel()
	p.wg.Wait()
}

// Note reports a cache miss on a packed extent. Non-blocking: the
// span is enqueued for the background worker, or dropped if the miss
// falls inside an already-covered range or the queue is full.
func (p *BlobPrefetcher) Note(tgMessageID, packOffset int64) {
	prov := interval{start: packOffset, end: packOffset + p.budget}
	p.mu.Lock()
	for _, iv := range p.covered[tgMessageID] {
		if packOffset >= iv.start && packOffset < iv.end {
			p.mu.Unlock()
			return
		}
	}
	// Provisionally cover [offset, offset+budget) so the flood of
	// misses right behind this one (a walk hitting the cold region)
	// coalesces into this single span. The worker replaces the
	// provisional interval with what it actually fetched.
	p.addCoveredLocked(tgMessageID, prov)
	p.mu.Unlock()
	select {
	case p.queue <- blobSpan{msgID: tgMessageID, offset: packOffset}:
	default:
		// Queue full — drop and un-cover so the region stays
		// retryable on the next miss.
		p.mu.Lock()
		p.removeCoveredLocked(tgMessageID, prov)
		p.mu.Unlock()
	}
}

// addCoveredLocked appends an interval, bounding both the per-blob
// list (oldest falls off) and the tracked-blob count. Caller holds mu.
func (p *BlobPrefetcher) addCoveredLocked(msgID int64, iv interval) {
	ivs := append(p.covered[msgID], iv)
	if len(ivs) > 16 {
		ivs = ivs[len(ivs)-16:]
	}
	if len(p.covered) > 64 {
		// Cheap global bound: forget everything but this blob. Worst
		// case some future misses re-fetch already-warm regions and
		// find every extent cached.
		p.covered = make(map[int64][]interval)
	}
	p.covered[msgID] = ivs
}

// removeCoveredLocked drops an exact interval (provisional undo).
// Caller holds mu.
func (p *BlobPrefetcher) removeCoveredLocked(msgID int64, iv interval) {
	ivs := p.covered[msgID]
	for i, have := range ivs {
		if have == iv {
			p.covered[msgID] = append(ivs[:i], ivs[i+1:]...)
			return
		}
	}
}

// replaceCovered swaps the provisional interval for what the span
// actually covered (or removes it on failure, end <= start).
func (p *BlobPrefetcher) replaceCovered(msgID int64, prov, actual interval) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.removeCoveredLocked(msgID, prov)
	if actual.end > actual.start {
		p.addCoveredLocked(msgID, actual)
	}
}

func (p *BlobPrefetcher) worker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case span := <-p.queue:
			p.fetchSpan(span)
		}
	}
}

// fetchSpan downloads one contiguous range of the blob, FORWARD from
// the missed extent, covering the uncached extents within the byte
// budget, and lands each in the cache.
func (p *BlobPrefetcher) fetchSpan(span blobSpan) {
	prov := interval{start: span.offset, end: span.offset + p.budget}
	rows, err := p.meta.PackedExtentsFrom(p.ctx, span.msgID, span.offset, blobPrefetchMaxExtents)
	if err != nil {
		if p.ctx.Err() == nil {
			log.Printf("[blob-prefetch] extents msg=%d: %v", span.msgID, err)
		}
		p.replaceCovered(span.msgID, prov, interval{})
		return
	}
	// Keep extents that end inside the budget window and aren't
	// cached yet. The range we download runs from the first such
	// extent to the last — cached extents in the middle are fetched
	// over (they're part of the contiguous range) but not re-decoded.
	limit := span.offset + p.budget
	var todo []meta.Chunk
	// examinedEnd is the end of the last extent this span looked at
	// inside the budget window. The row query stops at
	// blobPrefetchMaxExtents, so on a blob of tiny extents it can lie
	// far before `limit`: nothing past it was examined.
	examinedEnd := span.offset
	for _, c := range rows {
		if c.PackSize <= 0 || c.PackOffset+int64(c.PackSize) > limit {
			continue
		}
		if e := c.PackOffset + int64(c.PackSize); e > examinedEnd {
			examinedEnd = e
		}
		if p.cache.Has(Key{Ino: c.Ino, Idx: c.Idx}) {
			continue
		}
		todo = append(todo, c)
	}
	// A single uncached extent isn't worth a span: the reader's own
	// foreground fetch handles the one that triggered us, and there
	// is no neighbor to warm. Mark covered ONLY what was examined —
	// every extent in it is either cached or being fetched right now.
	// Keeping the provisional [offset, offset+budget) instead dropped
	// every later miss in that window although no span ever looked at
	// it (the stall of the 1M test's name-order reads: one download
	// per file once a walk stepped back one extent).
	if len(todo) < 2 {
		p.replaceCovered(span.msgID, prov, interval{start: span.offset, end: examinedEnd})
		return
	}
	start := todo[0].PackOffset
	end := todo[len(todo)-1].PackOffset + int64(todo[len(todo)-1].PackSize)
	raw, err := p.fetcher.FetchRange(p.ctx, Key{Ino: todo[0].Ino, Idx: todo[0].Idx}, span.msgID, start, end-start)
	if err != nil {
		if p.ctx.Err() == nil {
			log.Printf("[blob-prefetch] span msg=%d [%d:%d): %v", span.msgID, start, end, err)
		}
		p.replaceCovered(span.msgID, prov, interval{})
		return
	}
	if int64(len(raw)) < end-start {
		log.Printf("[blob-prefetch] span msg=%d short read: got %d want %d", span.msgID, len(raw), end-start)
		p.replaceCovered(span.msgID, prov, interval{})
		return
	}
	// Record what this span ACTUALLY covered so the dedupe window
	// matches reality: misses past `end` chain the next span.
	p.replaceCovered(span.msgID, prov, interval{start: span.offset, end: end})
	metrics.BlobPrefetchSpans.Inc()
	metrics.BytesDownloaded.Add(int64(len(raw)))
	warmed := 0
	for _, c := range todo {
		if p.ctx.Err() != nil {
			return
		}
		key := Key{Ino: c.Ino, Idx: c.Idx}
		if p.cache.Has(key) {
			continue // a foreground Get landed it while we downloaded
		}
		rel := c.PackOffset - start
		wire := raw[rel : rel+int64(c.PackSize)]
		data, err := p.wc.Decode(c.Ino, c.Idx, c.Hash, wire)
		if err != nil {
			log.Printf("[blob-prefetch] decode %d/%d: %v", c.Ino, c.Idx, err)
			continue
		}
		if err := p.cache.PutDecoded(key, data); err != nil {
			log.Printf("[blob-prefetch] cache put %d/%d: %v", c.Ino, c.Idx, err)
			continue
		}
		warmed++
	}
	metrics.BlobPrefetchExtents.Add(int64(warmed))
}
