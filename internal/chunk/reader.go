package chunk

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"telfs/internal/compress"
	"telfs/internal/crypto"
	"telfs/internal/meta"
)

// DefaultPrefetchWindow is the speculative-read window: how many
// chunks past the current ReadAt cursor the Reader schedules
// background fetches for. Each scheduled chunk is fetched through
// the cache miss path (which itself parallelises 4 ways inside one
// DownloadDocumentRange), so window × concurrency is the upper
// bound on in-flight prefetch RPCs.
//
// 8 picks a sweet spot for Telegram's per-RPC-throttled download
// model: each upload.getFile call takes ~3-5s on a typical
// connection regardless of how many you have in flight (TG throttles
// per-stream, not per-user, up to its concurrency limit). So more
// parallelism = more aggregate throughput, up to the point where
// TG starts rejecting. 8 chunks scheduled × 4 prefetchers = 32 RPC
// slots; combined with foreground that's well inside what TG
// tolerates.
const DefaultPrefetchWindow = 8

// SeekPrefetchWindow is the larger window used for the FIRST read
// after a detected seek. mpv-style players need 5-15 chunks
// downloaded post-seek (keyframe search + buffer fill), and serving
// them sequentially at per-RPC latency adds up to minutes.
// Scheduling them as one prefetch burst lets the parallel
// fetcher work them down to ~one chunk's wall-clock.
const SeekPrefetchWindow = 16

// DefaultPrefetchConcurrency caps simultaneous prefetchOne
// goroutines, not RPCs — each one can fire 4 concurrent
// UploadGetFile requests through the parallel ranged fetcher.
const DefaultPrefetchConcurrency = 4

// PrefetchWindow / PrefetchConcurrency are kept as legacy aliases
// for tests and existing callers that referenced the constants.
// New code should use Reader.SetPrefetch or the Default* constants.
const PrefetchWindow = DefaultPrefetchWindow
const PrefetchConcurrency = DefaultPrefetchConcurrency

// Reader orchestrates random-offset reads of a TelFS file by walking
// chunk_map, consulting the disk LRU cache, and pulling missing chunks
// from Telegram via the Cache's Fetcher.
//
// Reader runs a small prefetch pool so sequential reads don't pay the
// full network round-trip per chunk. Each ReadAt call schedules
// background fetches for the next PrefetchWindow chunks before doing
// its own (potentially slow) cache.Get; by the time the kernel asks
// for chunk N+1, it should already be in the LRU.
//
// pending is the per-ino read-after-write barrier owned by the upload
// pool. ReadAt blocks on it before consulting chunk_map so a read
// issued just after the writer's async close() still sees the data.
// May be nil for read-only mounts that have no writer side at all.
type Reader struct {
	meta      *meta.Store
	cache     *Cache
	pending   *PendingInodes
	chunkSize int64

	// pendingDir + cipher + codec power the read-from-staging
	// fallback (see ReadAt). When a chunk_map row is missing but
	// the inode's size says the file has data, we look for the
	// pre-dispatch wire under <pendingDir>/<ino>-<idx>-*.bin and
	// decode it inline. Zero values disable the fallback (no
	// pending dir, no cipher, no codec).
	pendingDir   string
	pendingIndex *PendingIndex // optional in-process accelerator over the dir scan
	cipher       crypto.Cipher
	codec        compress.Codec

	// Prefetch state. prefetchSem caps in-flight fetches; inflight is
	// a per-Key dedup so two consecutive ReadAt calls don't both kick
	// off the same fetch. Both protect against pathological wasted
	// network calls, not against correctness — the cache already
	// dedups concurrent inserts for the same key.
	prefetchSem    chan struct{}
	inflight       sync.Map // Key → struct{}
	prefetchWindow int      // chunks ahead of cursor to warm

	// prefetchGroups is a per-inode cancellable context for in-
	// flight prefetches. A seek (handled by the file-handle's
	// no-prefetch read path) cancels the previous prefetches for
	// that inode so they don't keep eating bandwidth the foreground
	// chunk needs. Sequential reads keep the same context alive.
	// nil entries are treated as "no active prefetch group" — the
	// new prefetch creates one.
	prefetchGroups sync.Map // ino → *prefetchGroup
}

// prefetchGroup is the per-inode prefetch context that the seek
// path cancels. New reads on the same inode that detect a seek
// pull the current entry, call cancel, and let scheduleAhead
// install a fresh one. Sequential reads reuse the existing group
// so prefetches in flight continue.
type prefetchGroup struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// NewReader wires up the read path. chunkSize is normally ChunkSize but
// is configurable for tests. pending may be nil for read-only mounts.
func NewReader(m *meta.Store, c *Cache, pending *PendingInodes, chunkSize int64) *Reader {
	if chunkSize <= 0 {
		chunkSize = ChunkSize
	}
	return &Reader{
		meta:           m,
		cache:          c,
		pending:        pending,
		chunkSize:      chunkSize,
		prefetchSem:    make(chan struct{}, DefaultPrefetchConcurrency),
		prefetchWindow: DefaultPrefetchWindow,
	}
}

// SetPrefetch overrides the read-ahead window and concurrency at
// runtime. Window > 0 changes how many chunks past the read cursor
// get speculatively scheduled; Concurrency > 0 resizes the
// prefetchOne semaphore (capping simultaneous prefetch goroutines).
// Either value may be 0/negative to leave that side unchanged.
//
// Safe to call before any ReadAt has run. Calling after concurrent
// reads have already started will not break anything in flight
// (the old sem keeps draining), but the new cap only applies to
// fresh prefetches.
func (r *Reader) SetPrefetch(window, concurrency int) {
	if window > 0 {
		r.prefetchWindow = window
	}
	if concurrency > 0 {
		r.prefetchSem = make(chan struct{}, concurrency)
	}
}

// SetPendingFallback wires up the staging-area read fallback. After
// this call, ReadAt will try <pendingDir>/<ino>-<idx>-*.bin on
// chunk_map miss, decode the wire through cipher + codec, and serve
// it directly — so freshly written files are readable even before
// their chunk_map row lands. Empty pendingDir disables.
//
// Cipher and codec MUST match what the Writer used to encode the
// wires (the FS-wide settings stored in meta_kv). The daemon's main
// loop is the right place to call this, after it has resolved
// crypto + compression and ensured the pending dir exists.
func (r *Reader) SetPendingFallback(pendingDir string, cipher crypto.Cipher, codec compress.Codec) {
	r.pendingDir = pendingDir
	r.cipher = cipher
	r.codec = codec
}

// SetPendingIndex installs an in-process accelerator over the disk
// scan: tryPendingFallback consults the index first (O(1) map
// lookup) and only falls back to filepath.Glob when the index
// reports a miss. Optional — index nil disables the acceleration
// and the existing Glob fallback handles everything.
func (r *Reader) SetPendingIndex(idx *PendingIndex) {
	r.pendingIndex = idx
}

// ReadAt fills dest with bytes from ino starting at off. Returns the
// number of bytes actually written into dest; if the request runs past
// EOF, the short return is the truncated portion (no error).
//
// Missing chunks (chunk_map entry not present at idx) are treated as
// EOF — when a write extends a file we record chunk entries as we go,
// so a gap means the file ends there.
//
// There is NO unconditional pending.Wait up here, even when a writer
// recently dispatched chunks for this ino. The barrier still exists,
// but we only fall back to it when both chunk_map AND the on-disk
// staging dir come up empty. Reads of freshly-written files (whose
// plaintext wires are already on disk under cache/pending/) skip the
// network round-trip entirely.
func (r *Reader) ReadAt(ctx context.Context, ino int64, dest []byte, off int64) (int, error) {
	if len(dest) == 0 {
		return 0, nil
	}
	in, err := r.meta.GetInode(ctx, ino)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("read get inode %d: %w", ino, err)
	}
	chunkSize := in.ChunkSize
	if chunkSize <= 0 {
		chunkSize = r.chunkSize
	}
	return r.readAt(ctx, ino, dest, off, chunkSize, in.Size, in.Sparse, r.prefetchWindow)
}

// ReadAtKnown is the fast-path entry point used when the caller (a
// file handle) already knows the inode's chunkSize + size + sparse
// flag. Skips the per-call GetInode SQL row. The caller is
// responsible for the freshness of size — a handle that has been
// written through should fall back to ReadAt instead.
//
// Negative or zero chunkSize falls through to ReadAt; a hinted size
// of 0 is treated as "unknown / unbounded" (the chunk_map miss path
// will surface a genuine missing chunk as data lost, which is the
// correct behavior — readers past true EOF wouldn't ask via this
// path anyway because the kernel knows the file size from Getattr).
func (r *Reader) ReadAtKnown(ctx context.Context, ino int64, dest []byte, off int64, chunkSize, size int64, sparse bool) (int, error) {
	if chunkSize <= 0 {
		return r.ReadAt(ctx, ino, dest, off)
	}
	return r.readAt(ctx, ino, dest, off, chunkSize, size, sparse, r.prefetchWindow)
}

// ReadAtKnownSeek is the seek path: a non-sequential jump just
// happened, the prior prefetches were targeting the OLD position
// (now wasted bandwidth), and the player is about to demand a
// burst of forward reads from the NEW position (keyframe search +
// buffer fill — typically 5-15 chunks for a video player).
//
// We cancel the stale prefetches as a group AND schedule a LARGER
// window (SeekPrefetchWindow) for the new burst so the forward
// reads land in the cache instead of forcing serial fetches that
// add up to minutes of latency on per-RPC-throttled networks.
func (r *Reader) ReadAtKnownSeek(ctx context.Context, ino int64, dest []byte, off int64, chunkSize, size int64, sparse bool) (int, error) {
	r.cancelPrefetchesForIno(ino)
	if chunkSize <= 0 {
		in, err := r.meta.GetInode(ctx, ino)
		if err != nil {
			if errors.Is(err, meta.ErrNotFound) {
				return 0, nil
			}
			return 0, fmt.Errorf("read get inode %d: %w", ino, err)
		}
		cs := in.ChunkSize
		if cs <= 0 {
			cs = r.chunkSize
		}
		return r.readAt(ctx, ino, dest, off, cs, in.Size, in.Sparse, SeekPrefetchWindow)
	}
	return r.readAt(ctx, ino, dest, off, chunkSize, size, sparse, SeekPrefetchWindow)
}

// ReadAtKnownNoPrefetch is the same as ReadAtKnown but with the
// prefetcher disabled for THIS read. Kept for callers that have
// explicit reasons to suppress prefetch (read-only mounts probing
// metadata, debug tools); the file-handle's seek path uses
// ReadAtKnownSeek instead, which DOES prefetch but at a larger
// window.
func (r *Reader) ReadAtKnownNoPrefetch(ctx context.Context, ino int64, dest []byte, off int64, chunkSize, size int64, sparse bool) (int, error) {
	r.cancelPrefetchesForIno(ino)
	if chunkSize <= 0 {
		in, err := r.meta.GetInode(ctx, ino)
		if err != nil {
			if errors.Is(err, meta.ErrNotFound) {
				return 0, nil
			}
			return 0, fmt.Errorf("read get inode %d: %w", ino, err)
		}
		cs := in.ChunkSize
		if cs <= 0 {
			cs = r.chunkSize
		}
		return r.readAt(ctx, ino, dest, off, cs, in.Size, in.Sparse, 0)
	}
	return r.readAt(ctx, ino, dest, off, chunkSize, size, sparse, 0)
}

// cancelPrefetchesForIno cancels the prefetchGroup associated with
// ino, if any. The cancelled goroutines drop their reservation and
// exit; new prefetches scheduled by subsequent reads install a fresh
// group. Safe to call concurrently; idempotent for an unknown ino.
func (r *Reader) cancelPrefetchesForIno(ino int64) {
	if g, ok := r.prefetchGroups.LoadAndDelete(ino); ok {
		if pg, ok := g.(*prefetchGroup); ok && pg.cancel != nil {
			pg.cancel()
		}
	}
}

// prefetchCtxFor returns the cancellable context for prefetches on
// ino. Creates a fresh group on first call (or after cancellation).
// All prefetches scheduled within the same read share one context;
// a seek calls cancelPrefetchesForIno to drop them as a unit.
func (r *Reader) prefetchCtxFor(ino int64) context.Context {
	if g, ok := r.prefetchGroups.Load(ino); ok {
		if pg, ok := g.(*prefetchGroup); ok {
			return pg.ctx
		}
	}
	pg := &prefetchGroup{}
	pg.ctx, pg.cancel = context.WithCancel(context.Background())
	if actual, loaded := r.prefetchGroups.LoadOrStore(ino, pg); loaded {
		// Lost the race; cancel the throwaway and use the winner.
		pg.cancel()
		if existing, ok := actual.(*prefetchGroup); ok {
			return existing.ctx
		}
	}
	return pg.ctx
}

func (r *Reader) readAt(ctx context.Context, ino int64, dest []byte, off int64, chunkSize, inSize int64, sparse bool, prefetchWindow int) (int, error) {
	if len(dest) == 0 {
		return 0, nil
	}
	startIdx := int32(off / chunkSize)
	endIdx := int32((off + int64(len(dest)) - 1) / chunkSize)

	// Bulk metadata fetch: one SELECT for the foreground range +
	// prefetch window instead of one per chunk. The Reader's hot
	// loop now consults a map; misses (chunks not in chunk_map)
	// fall through to the staging fallback / barrier path inside
	// fetchOne, exactly as before. Stale rows on a concurrent
	// delete surface as a cache.Get error downstream — same outcome
	// as today's per-call GetChunk.
	rangeCount := int(endIdx-startIdx) + 1 + prefetchWindow
	chunkBatch, _ := r.meta.GetChunksRange(ctx, ino, startIdx, rangeCount)
	chunkMap := make(map[int32]meta.Chunk, len(chunkBatch))
	for _, c := range chunkBatch {
		chunkMap[c.Idx] = c
	}

	written := 0
	cur := off
	end := off + int64(len(dest))

	for idx := startIdx; idx <= endIdx; idx++ {
		// Kick off read-ahead BEFORE we block on the current chunk —
		// the network round-trip for chunks idx+1..idx+W can overlap
		// with this chunk's fetch.
		if prefetchWindow > 0 {
			r.scheduleAheadFromBatch(ino, idx+1, prefetchWindow, chunkMap)
		}

		data, err := r.fetchOne(ctx, ino, idx, chunkSize, inSize, sparse, chunkMap)
		if err != nil {
			return written, err
		}
		chunkStart := int64(idx) * chunkSize
		// logicalLen is how many bytes this chunk logically holds.
		// Non-sparse: exactly the fetched data (data == nil ⇒ EOF).
		// Sparse: the extent [chunkStart, min(chunkStart+chunkSize,
		// size)) — a nil data is a hole (all zeros) and short data is
		// zero-padded, because holes and short boundary chunks are
		// legitimate in a sparse file.
		var logicalLen int64
		if sparse {
			if chunkStart >= inSize {
				return written, nil // true EOF
			}
			logicalLen = inSize - chunkStart
			if logicalLen > chunkSize {
				logicalLen = chunkSize
			}
			// data longer than the logical extent (stale row past a
			// shrink) is clamped by the relEnd bound below and never
			// escapes to the caller.
		} else {
			if data == nil {
				// fetchOne returns nil + nil error when the loop has
				// walked past EOF (idx >= ceil(in.Size / chunkSize)).
				return written, nil
			}
			logicalLen = int64(len(data))
		}
		relStart := cur - chunkStart
		// relEnd is bounded by both the requested range AND the
		// chunk's logical length (the last chunk is short).
		relEnd := relStart + (end - cur)
		if relEnd > logicalLen {
			relEnd = logicalLen
		}
		if relStart >= relEnd {
			break // nothing more to read from this chunk
		}
		// Copy the real bytes that exist, zero-fill the rest of the
		// span (hole tail). dest is a reused kernel buffer, so the
		// zeros must be written explicitly.
		n := 0
		if relStart < int64(len(data)) {
			realEnd := relEnd
			if realEnd > int64(len(data)) {
				realEnd = int64(len(data))
			}
			n = copy(dest[written:], data[relStart:realEnd])
		}
		span := int(relEnd - relStart)
		for i := written + n; i < written+span; i++ {
			dest[i] = 0
		}
		written += span
		cur += int64(span)
		// If we got less than the full requested range from this chunk
		// (because the chunk was short — last chunk of file), we're at
		// EOF regardless of dest's remaining space.
		if logicalLen < chunkSize && relEnd == logicalLen {
			break
		}
	}
	return written, nil
}

// fetchOne returns the plaintext bytes for chunk idx of ino, walking
// the three-tier read path:
//
//  1. chunk_map row + cache.Get (hot path — file is fully uploaded).
//  2. Staging-dir fallback (the wire is on local disk but the
//     packer hasn't committed the chunk_map row yet — typical for
//     freshly-written files mid-flush). Decode the wire inline.
//  3. R-A-W barrier wait + re-check both above (covers the brief
//     packer.flush() window where the wire was deleted before the
//     row landed, or where the inode genuinely had work in flight
//     that wasn't on disk yet).
//
// Returns (data, nil) on success; (nil, nil) when idx is past EOF
// (a normal short-read termination signal — caller's loop bails);
// (nil, err) for genuine failures (data lost, decrypt mismatch,
// network error on cache.Get, etc).
//
// Splitting this out keeps ReadAt's loop body readable: at the call
// site you have "fetch this chunk or terminate" semantics, and the
// three-tier dance lives in one named function with a single happy
// path.
func (r *Reader) fetchOne(ctx context.Context, ino int64, idx int32, chunkSize, inSize int64, sparse bool, batch map[int32]meta.Chunk) ([]byte, error) {
	chunkStart := int64(idx) * chunkSize

	// Tier 1: chunk_map. Prefer the pre-fetched batch (one SELECT
	// for the whole ReadAt) — only fall through to a per-idx
	// GetChunk if the batch didn't include this idx (which can
	// happen when the foreground range was much larger than the
	// initial pre-fetch, or when the row was committed by the
	// packer between the batch fetch and now).
	if c, ok := batch[idx]; ok {
		return r.cache.Get(ctx, Key{Ino: ino, Idx: idx}, c.TGMessageID, c.PackOffset, c.PackSize)
	}
	c, err := r.meta.GetChunk(ctx, ino, idx)
	if err == nil {
		return r.cache.Get(ctx, Key{Ino: ino, Idx: idx}, c.TGMessageID, c.PackOffset, c.PackSize)
	}
	if !errors.Is(err, meta.ErrNotFound) {
		return nil, fmt.Errorf("chunk lookup %d/%d: %w", ino, idx, err)
	}
	// Row not committed. Could be EOF, could be pending.
	if chunkStart >= inSize {
		return nil, nil
	}
	// Tier 2: staging dir. Avoids the R-A-W barrier on the common
	// case where the writer just dispatched but the packer hasn't
	// flushed yet.
	pdata, found, perr := r.tryPendingFallback(ino, idx)
	if perr != nil {
		return nil, perr
	}
	if found {
		return pdata, nil
	}
	// Tier 3: barrier. If pending is non-nil AND it has work for
	// this ino, wait for the current upload batch to drain, then
	// retry chunk_map (the row should land) and the staging dir
	// (the wire may have just been deleted by a successful flush).
	if r.pending != nil {
		if err := r.pending.Wait(ctx, ino); err != nil {
			return nil, err
		}
		c, err := r.meta.GetChunk(ctx, ino, idx)
		if err == nil {
			return r.cache.Get(ctx, Key{Ino: ino, Idx: idx}, c.TGMessageID, c.PackOffset, c.PackSize)
		}
		if !errors.Is(err, meta.ErrNotFound) {
			return nil, fmt.Errorf("chunk lookup %d/%d: %w", ino, idx, err)
		}
		pdata, found, perr := r.tryPendingFallback(ino, idx)
		if perr != nil {
			return nil, perr
		}
		if found {
			return pdata, nil
		}
	}
	// chunk_map empty, staging empty, no pending upload to wait for.
	// For a SPARSE file this is a legitimate hole — the caller
	// zero-fills the extent. For everything else, inode.size says
	// there should be bytes here but every source has come up empty —
	// surface EIO so the caller knows the data is genuinely gone
	// instead of returning silent zero-fill that the kernel would
	// happily pad with NULs.
	if sparse {
		return nil, nil
	}
	return nil, fmt.Errorf("chunk %d/%d: missing from chunk_map AND staging (data lost)", ino, idx)
}

// tryPendingFallback looks under the staging dir for a wire
// matching (ino, idx) and, if found, decrypts + decompresses it.
// Returns (plaintext, true, nil) on success, (nil, false, nil) when
// no matching wire exists, or (nil, false, err) on read/decode
// failure.
//
// The filename pattern is <ino>-<idx>-<plaintextSize>.bin (set by
// PersistPending). We glob by (ino, idx) because the plaintext size
// suffix is informational only — the wire file's bytes hold the
// encoded chunk, not the inode size. Multiple matching files would
// only happen on a race we don't have today; we use the first.
func (r *Reader) tryPendingFallback(ino int64, idx int32) ([]byte, bool, error) {
	if r.pendingDir == "" {
		return nil, false, nil
	}
	// Fast path: the in-process index knows the path. One map
	// lookup + one ReadFile instead of a directory scan.
	var path string
	if p, ok := r.pendingIndex.Lookup(ino, idx); ok {
		path = p
	} else {
		pattern := filepath.Join(r.pendingDir, fmt.Sprintf("%d-%d-*.bin", ino, idx))
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, false, fmt.Errorf("pending glob %s: %w", pattern, err)
		}
		if len(matches) == 0 {
			return nil, false, nil
		}
		path = matches[0]
	}
	wire, err := os.ReadFile(path)
	if err != nil {
		// File could have been deleted by the packer between Glob
		// and ReadFile (race with a completing flush). Treat as
		// "not found" so the caller can fall through and retry the
		// chunk_map lookup — the next round will see the now-
		// committed row.
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("read pending %s: %w", path, err)
	}
	// Reverse the writer pipeline: decrypt → decompress.
	plaintext := wire
	if r.cipher != nil {
		dec, derr := r.cipher.Open(ino, idx, plaintext)
		if derr != nil {
			return nil, false, fmt.Errorf("decrypt pending %d/%d: %w", ino, idx, derr)
		}
		plaintext = dec
	}
	if r.codec != nil {
		dec, derr := r.codec.Decompress(plaintext)
		if derr != nil {
			return nil, false, fmt.Errorf("decompress pending %d/%d: %w", ino, idx, derr)
		}
		plaintext = dec
	}
	return plaintext, true, nil
}

// scheduleAheadFromBatch fires off background prefetches for chunks
// [startIdx, startIdx+count). Skips chunks already cached or in
// flight, and skips chunks NOT present in the batch (those are past
// EOF or not yet committed — fetching them would just hit
// ErrNotFound). All prefetches scheduled in the same window share a
// per-inode cancellable context so a subsequent seek-read can
// cancel them as a group via cancelPrefetchesForIno.
func (r *Reader) scheduleAheadFromBatch(ino int64, startIdx int32, count int, batch map[int32]meta.Chunk) {
	pctx := r.prefetchCtxFor(ino)
	for offset := 0; offset < count; offset++ {
		idx := startIdx + int32(offset)
		key := Key{Ino: ino, Idx: idx}
		if r.cache.Has(key) {
			continue
		}
		c, ok := batch[idx]
		if !ok {
			continue
		}
		if _, loaded := r.inflight.LoadOrStore(key, struct{}{}); loaded {
			continue
		}
		go r.prefetchOne(pctx, ino, key, c)
	}
}

// prefetchOne acquires a sem slot then warms the cache for a single
// chunk. Takes a pre-resolved Chunk (from the batch) so no SQL round
// trip is needed inside the prefetcher. Errors are silent — prefetch
// is best-effort; the caller's own ReadAt will surface a real error
// when it asks for the chunk later. The pctx is the per-inode
// prefetch context; a seek cancels it and this goroutine exits as
// soon as cache.Get notices.
func (r *Reader) prefetchOne(pctx context.Context, ino int64, key Key, c meta.Chunk) {
	defer r.inflight.Delete(key)
	// Bail before consuming a sem slot if we were already cancelled.
	select {
	case <-pctx.Done():
		return
	default:
	}
	// Bounded concurrency. If the queue is full, this prefetch waits;
	// that's the backpressure we want — sequential reads will catch up.
	select {
	case r.prefetchSem <- struct{}{}:
	case <-pctx.Done():
		return
	}
	defer func() { <-r.prefetchSem }()
	_, _ = r.cache.Get(pctx, key, c.TGMessageID, c.PackOffset, c.PackSize)
}
