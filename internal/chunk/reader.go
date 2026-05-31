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
	pendingDir string
	cipher     crypto.Cipher
	codec      compress.Codec

	// Prefetch state. prefetchSem caps in-flight fetches; inflight is
	// a per-Key dedup so two consecutive ReadAt calls don't both kick
	// off the same fetch. Both protect against pathological wasted
	// network calls, not against correctness — the cache already
	// dedups concurrent inserts for the same key.
	prefetchSem chan struct{}
	inflight    sync.Map // Key → struct{}
}

// NewReader wires up the read path. chunkSize is normally ChunkSize but
// is configurable for tests. pending may be nil for read-only mounts.
func NewReader(m *meta.Store, c *Cache, pending *PendingInodes, chunkSize int64) *Reader {
	if chunkSize <= 0 {
		chunkSize = ChunkSize
	}
	return &Reader{
		meta:        m,
		cache:       c,
		pending:     pending,
		chunkSize:   chunkSize,
		prefetchSem: make(chan struct{}, PrefetchConcurrency),
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
	// Per-inode chunk size: each file picks its tier at first
	// dispatch; the value is stored on the inode row. Look it up
	// once per ReadAt and use that for idx math (instead of the
	// reader's FS-default chunkSize). One extra GetInode per
	// ReadAt — comparable to the per-chunk GetChunk cost already
	// in the loop below.
	//
	// Note: there is NO unconditional pending.Wait up here, even
	// when a writer recently dispatched chunks for this ino. The
	// barrier still exists, but we only fall back to it when both
	// chunk_map AND the on-disk staging dir come up empty. That way
	// reads of freshly-written files (whose plaintext wires are
	// already on disk under cache/pending/) skip the network round-
	// trip entirely — they're served from the staging dir at memory
	// speed. The barrier matters for the corner case where the
	// packer's flush has DELETED the staged wire but PutChunk
	// hasn't committed the row yet (a very brief window inside
	// flush()); ReadFromStaging-or-wait sequencing handles that
	// without paying the barrier cost on the common path.
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
	startIdx := int32(off / chunkSize)
	endIdx := int32((off + int64(len(dest)) - 1) / chunkSize)

	written := 0
	cur := off
	end := off + int64(len(dest))

	for idx := startIdx; idx <= endIdx; idx++ {
		// Kick off read-ahead BEFORE we block on the current chunk —
		// the network round-trip for chunks idx+1..idx+W can overlap
		// with this chunk's fetch.
		r.scheduleAhead(ino, idx+1, PrefetchWindow)

		data, err := r.fetchOne(ctx, ino, idx, chunkSize, in.Size)
		if err != nil {
			return written, err
		}
		if data == nil {
			// fetchOne returns nil + nil error when the loop has
			// walked past EOF (idx >= ceil(in.Size / chunkSize)).
			return written, nil
		}
		chunkStart := int64(idx) * chunkSize
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
		if int64(len(data)) < chunkSize && relEnd == int64(len(data)) {
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
func (r *Reader) fetchOne(ctx context.Context, ino int64, idx int32, chunkSize, inSize int64) ([]byte, error) {
	chunkStart := int64(idx) * chunkSize

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
	// chunk_map empty, staging empty, no pending upload to wait
	// for. inode.size says there should be bytes here, but every
	// source has come up empty — surface EIO so the caller knows
	// the data is genuinely gone instead of returning silent zero-
	// fill that the kernel would happily pad with NULs.
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
	pattern := filepath.Join(r.pendingDir, fmt.Sprintf("%d-%d-*.bin", ino, idx))
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, false, fmt.Errorf("pending glob %s: %w", pattern, err)
	}
	if len(matches) == 0 {
		return nil, false, nil
	}
	wire, err := os.ReadFile(matches[0])
	if err != nil {
		// File could have been deleted by the packer between Glob
		// and ReadFile (race with a completing flush). Treat as
		// "not found" so the caller can fall through and retry the
		// chunk_map lookup — the next round will see the now-
		// committed row.
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("read pending %s: %w", matches[0], err)
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
	_, _ = r.cache.Get(ctx, key, c.TGMessageID, c.PackOffset, c.PackSize)
}
