package chunk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"telfs/internal/compress"
	"telfs/internal/crypto"
	"telfs/internal/meta"
	"telfs/internal/metrics"
)

// Uploader is the upload side of a chunk pipeline — what Writer uses to
// push a chunk's bytes to the backing channel. *tg.Session satisfies it
// (via UploadDocument), as does any test fake.
type Uploader interface {
	UploadDocument(ctx context.Context, r io.Reader, filename, caption string) (int, error)
}

// DefaultDirtyCapBytes is the per-Writer cap on accumulated dirty bytes
// before WriteAt starts dispatching the oldest dirty chunks for async
// upload. Big enough to soak the kernel's writeback for typical large-
// file copies without forcing FUSE to round-trip on every chunk
// boundary; small enough that one rogue handle can't OOM the daemon.
const DefaultDirtyCapBytes int64 = 256 << 20

// DefaultUploadConcurrency is how many chunks may be uploading at the
// same time per Writer. Each Telegram upload is itself parallelized
// internally by gotd (multiple SaveFilePart calls); 4 simultaneous
// chunks is enough to keep a typical residential uplink saturated
// without thrashing FLOOD_WAIT.
//
// (Until v0.7 this was pinned at 1 because concurrent UploadDocument
// calls on the same *tg.Session appeared to crash the daemon and
// corrupt data. The real bug was in extractNewMessageID — see
// internal/tg/session.go — not in concurrency itself. With that
// fixed, parallelism is safe again.)
const DefaultUploadConcurrency = 4

// Writer owns the dirty-chunk buffer for a single open file handle. It
// is NOT safe to share across goroutines — go-fuse may call WriteAt
// concurrently for one handle, so all entry points acquire mu.
//
// Lifecycle (mirrors a FUSE handle's open/write/flush/release):
//   - New: allocate, recover current file size from meta, spawn nothing.
//   - WriteAt: copy bytes into dirty chunks; eagerly DISPATCH (not
//     synchronously upload) the oldest dirty chunks when dirtyBytes >
//     dirtyCap. Dispatch acquires an upload-pool slot, so if
//     concurrency is saturated the caller blocks — honest backpressure
//     to the kernel.
//   - Truncate: drop dirty chunks past size; trim or delete last chunk.
//   - Flush (FUSE close): dispatch every remaining dirty chunk and
//     persist the final logical size to meta, then RETURN — does not
//     wait for the network round-trips. Subsequent reads of the same
//     ino block on the pool's per-ino pending barrier until uploads
//     land, so read-after-write stays coherent. fsync still synchronous.
//   - Sync (FUSE fsync): dispatch + WAIT this writer's uploadWg +
//     SetSize. Surfaces any upload error from THIS handle.
//   - Detach (FUSE release): drop in-memory dirty state. Does NOT cancel
//     in-flight uploads — they ride pool.UploadCtx, which survives until
//     Backend.Shutdown.
//
// Lock discipline: w.mu protects every map/slice field below. The
// network round-trip itself happens with mu released, so FUSE write
// goroutines can keep filling new dirty chunks while old ones upload
// in the background.
type Writer struct {
	meta     *meta.Store
	cache    *Cache
	uploader Uploader
	wc       WireCodec // never nil; format 1 or 2
	pool     *UploadPool
	packer   *Packer // nil disables packing; small chunks upload directly
	chunkSz  int64
	dirtyCap int64
	ino      int64

	mu         sync.Mutex
	dirty      map[int32]*dirtyChunk
	dirtyBytes int64
	dirtyOrder []int32 // FIFO insertion order — eager-dispatch victims pop from front
	size       int64
	closed     bool
	// sparse mirrors inodes.sparse: the file contains holes (chunk
	// slots with no data that read as zeros). Set when a write skips
	// past unwritten space across a chunk boundary, or by truncate-
	// grow. Once set it's persisted via meta.SetInodeSparse and never
	// cleared.
	sparse bool
	// hasDispatched is true once at least one chunk has been
	// dispatched (or the inode already had chunks at NewWriter
	// time). Once set, chunkSz is locked — promoting would
	// invalidate the (ino, idx) → file-offset arithmetic for
	// rows that are already on the channel.
	hasDispatched bool

	// Per-handle upload bookkeeping. uploading tracks chunks whose data
	// is no longer in `dirty` but whose upload hasn't finished — both
	// for loadForWrite's "wait for in-flight" path and so Sync (fsync)
	// can block on THIS handle's outstanding uploads via uploadWg.
	// uploadErr is the first sticky error from any background upload;
	// WriteAt and Sync surface it. Flush clears it (retry slate). After
	// Detach the writer is no longer mutated; failed uploads log to
	// stderr (via recordError) and drop their dc.
	uploadWg  sync.WaitGroup
	uploading map[int32]chan struct{}
	uploadErr error
}

type dirtyChunk struct {
	data []byte // 0..chunkSz bytes; len(data) == effective size of this chunk
}

// NewWriter constructs a Writer for an open file handle. chunkSize and
// dirtyCap may be zero, in which case the package defaults apply. If
// cipher is nil, NoopCipher is used (plaintext on the wire). If codec
// is nil, compress.None is used (no compression). pool may also be
// nil — in that case a single-handle pool is created on the fly,
// useful for transient writers (truncate-without-handle, tests).
//
// New blocks on the pool's per-ino Pending barrier before reading the
// initial size from meta, so a freshly-opened writer sees the size that
// previously-detached writers committed via SetSize even if their
// chunk uploads are still in flight.
func NewWriter(ctx context.Context, m *meta.Store, c *Cache, u Uploader, cipher crypto.Cipher, codec compress.Codec, pool *UploadPool, packer *Packer, ino int64, chunkSize, dirtyCap int64) (*Writer, error) {
	return NewWriterWire(ctx, m, c, u, NewFormat1Codec(cipher, codec), pool, packer, ino, chunkSize, dirtyCap)
}

// NewWriterWire is NewWriter with an explicit wire codec (format 2).
func NewWriterWire(ctx context.Context, m *meta.Store, c *Cache, u Uploader, wc WireCodec, pool *UploadPool, packer *Packer, ino int64, chunkSize, dirtyCap int64) (*Writer, error) {
	if chunkSize <= 0 {
		chunkSize = ChunkSize
	}
	if dirtyCap <= 0 {
		dirtyCap = DefaultDirtyCapBytes
	}
	if wc == nil {
		wc = NewFormat1Codec(nil, nil)
	}
	if pool == nil {
		pool = NewUploadPool(DefaultUploadConcurrency)
	}
	if err := pool.Pending.Wait(ctx, ino); err != nil {
		return nil, err
	}
	in, err := m.GetInode(ctx, ino)
	if err != nil {
		return nil, fmt.Errorf("writer: get inode %d: %w", ino, err)
	}
	// Per-inode chunk size takes precedence: it's the per-file tier
	// (multi-tier sizing) and the authoritative value the reader
	// uses too. The FS-default `chunkSize` parameter is only the
	// fallback when an inode predates the column (shouldn't happen
	// after the schema migration, but cheap to keep).
	if in.ChunkSize > 0 {
		chunkSize = in.ChunkSize
	}
	// hasChunksAlready: if the inode already has at least one
	// chunk_map row, we cannot promote chunk_size — the (ino, idx) →
	// offset arithmetic for existing rows depends on it. Marking
	// dispatched=true achieves the same lock without a separate flag.
	chunks, err := m.ListChunks(ctx, ino)
	if err != nil {
		return nil, fmt.Errorf("writer: list chunks %d: %w", ino, err)
	}
	hasDispatched := len(chunks) > 0
	return &Writer{
		meta:          m,
		cache:         c,
		uploader:      u,
		wc:            wc,
		pool:          pool,
		packer:        packer,
		chunkSz:       chunkSize,
		dirtyCap:      dirtyCap,
		ino:           ino,
		dirty:         make(map[int32]*dirtyChunk),
		size:          in.Size,
		sparse:        in.Sparse,
		uploading:     make(map[int32]chan struct{}),
		hasDispatched: hasDispatched,
	}, nil
}

// markSparseLocked persists the sparse flag (once) and records it on
// the writer. Caller holds w.mu. From this point missing chunks of
// this inode read as zeros instead of data-lost EIO.
func (w *Writer) markSparseLocked(ctx context.Context) error {
	if w.sparse {
		return nil
	}
	if err := w.meta.SetInodeSparse(ctx, w.ino); err != nil {
		return fmt.Errorf("mark sparse %d: %w", w.ino, err)
	}
	w.sparse = true
	return nil
}

// Size returns the current logical file size including unflushed writes.
func (w *Writer) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

// WriteAt copies src into the file starting at offset off. Returns the
// number of bytes written (always len(src) on success; partial returns
// only happen on error).
func (w *Writer) WriteAt(ctx context.Context, src []byte, off int64) (int, error) {
	if len(src) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, fmt.Errorf("writer: negative offset %d", off)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, fmt.Errorf("writer: handle closed")
	}
	// NOTE: we DO NOT surface w.uploadErr here. Background upload
	// failures that have been restored-to-dirty will retry on the
	// next dispatch (eager below, or explicit Flush). Returning the
	// error here would make tar see EIO mid-extract even though the
	// chunk is queued for an imminent retry. Truly-lost chunks
	// (writer detached, can't restore) still set uploadErr via
	// recordLost, so Flush/Sync surface them at the durability
	// checkpoint where it actually matters.

	// Writing past EOF. Two shapes:
	//
	//   - The gap stays inside the chunk that currently ends the file:
	//     zero-fill it in the dirty buffer (bounded by one chunk).
	//
	//   - The gap crosses a chunk boundary: DO NOT materialize the
	//     intermediate chunks. The old behavior allocated and uploaded
	//     every zero chunk in the gap — a `dd seek=100G` materialized
	//     ~100 GB in RAM (OOM-killing the daemon) and then would have
	//     shipped it all to Telegram. Instead, mark the inode sparse:
	//     the reader zero-fills missing chunks and zero-pads short
	//     ones for sparse files, so POSIX hole semantics hold with no
	//     RAM, no upload, and no chunk_map rows.
	if off > w.size {
		holeStartIdx := w.size / w.chunkSz
		targetIdx := off / w.chunkSz
		if holeStartIdx == targetIdx {
			if err := w.materializeZeros(ctx, w.size, off); err != nil {
				return 0, err
			}
		} else if err := w.markSparseLocked(ctx); err != nil {
			return 0, err
		}
	}

	startIdx := int32(off / w.chunkSz)
	endIdx := int32((off + int64(len(src)) - 1) / w.chunkSz)
	written := 0
	cur := off

	for idx := startIdx; idx <= endIdx; idx++ {
		dc, err := w.loadForWrite(ctx, idx)
		if err != nil {
			return written, err
		}
		chunkStart := int64(idx) * w.chunkSz
		relOff := cur - chunkStart // 0..chunkSz-1
		// How many bytes of src apply to this chunk?
		maxInChunk := w.chunkSz - relOff
		remaining := int64(len(src) - written)
		toWrite := maxInChunk
		if remaining < toWrite {
			toWrite = remaining
		}
		// Grow chunk buffer if write extends past current chunk size.
		needed := relOff + toWrite
		w.growChunk(dc, needed)
		n := copy(dc.data[relOff:relOff+toWrite], src[written:written+int(toWrite)])
		written += n
		cur += int64(n)
		// Update file size.
		if cur > w.size {
			w.size = cur
		}
	}

	// Multi-tier: promote chunk_size to a bigger tier if the file
	// has grown past a tier threshold and we haven't dispatched
	// yet. Runs BEFORE eager-dispatch so the first dispatch sees
	// the final layout.
	if err := w.maybePromoteLocked(ctx); err != nil {
		return written, err
	}

	// Eager DISPATCH if we're over the cap. Drains oldest dirty chunks
	// (by insertion order) until back under the cap. Dispatching does
	// not block on the network — it queues for the pool — BUT it does
	// block on the pool's concurrency semaphore when the pool is
	// saturated. That semaphore wait IS the backpressure that prevents
	// the kernel from piling up unbounded dirty pages, and now that
	// the sem is backend-global, it bounds aggregate Telegram
	// concurrency across every open handle on the mount.
	for w.dirtyBytes > w.dirtyCap && len(w.dirtyOrder) > 0 {
		victim := w.dirtyOrder[0]
		if err := w.dispatchFlushLocked(ctx, victim); err != nil {
			return written, err
		}
		// Don't surface w.uploadErr here either — the dispatch path
		// removes the chunk from dirty up-front, so this loop's
		// termination is bounded by the dirty queue draining, not
		// by an error escape. Background failures (which restore
		// the chunk later) are handled by the next dispatch cycle.
	}

	return written, nil
}

// loadForWrite returns (and registers as dirty) the chunk at idx,
// preloading existing bytes via the cache when this is the first
// dirty touch.
//
// If an in-flight upload exists for idx (we've popped it from `dirty`
// but the network round-trip hasn't completed yet), this blocks until
// that upload finishes so the read-side starts from a consistent
// chunk_map row.
func (w *Writer) loadForWrite(ctx context.Context, idx int32) (*dirtyChunk, error) {
	for {
		if dc, ok := w.dirty[idx]; ok {
			return dc, nil
		}
		ch, uploading := w.uploading[idx]
		if !uploading {
			break
		}
		// Wait for the in-flight upload to commit (or fail) before we
		// preload from meta. Drop the lock during the wait.
		w.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			w.mu.Lock()
			return nil, ctx.Err()
		}
		w.mu.Lock()
		if err := w.uploadErr; err != nil {
			return nil, err
		}
		// Loop: the chunk may now have a fresh chunk_map row, or it
		// may have been re-dirtied by another writer in the meantime.
	}
	var data []byte
	// Try to pull existing chunk content (cache hit or TG download).
	c, err := w.meta.GetChunk(ctx, w.ino, idx)
	if err == nil {
		existing, fetchErr := w.cache.GetChunk(ctx, Key{Ino: w.ino, Idx: idx}, c)
		if fetchErr != nil {
			return nil, fmt.Errorf("preload chunk %d: %w", idx, fetchErr)
		}
		data = make([]byte, len(existing))
		copy(data, existing)
	} else if !errors.Is(err, meta.ErrNotFound) {
		return nil, fmt.Errorf("lookup chunk %d: %w", idx, err)
	}
	// data may be nil (new chunk) or the existing bytes.
	dc := &dirtyChunk{data: data}
	w.dirty[idx] = dc
	w.dirtyOrder = append(w.dirtyOrder, idx)
	w.dirtyBytes += int64(len(data))
	return dc, nil
}

// growChunk extends dc.data to at least `need` bytes, zero-filling.
// Caller holds w.mu.
func (w *Writer) growChunk(dc *dirtyChunk, need int64) {
	if int64(len(dc.data)) >= need {
		return
	}
	delta := need - int64(len(dc.data))
	dc.data = append(dc.data, make([]byte, delta)...)
	w.dirtyBytes += delta
}

// maybePromoteLocked promotes w.chunkSz to a higher tier if (a) no
// chunks have been dispatched yet AND (b) the dirty buffer has
// crossed a tier threshold. Re-chunks dirty into the new layout in
// place. Persists the new size to meta so the reader can find
// chunks correctly. Caller holds w.mu.
//
// Why "before dispatch only": once any chunk_map row exists at the
// old chunkSz, changing the per-inode chunk_size would silently
// mis-map every existing row (offset = idx * chunk_size). Locking
// at first dispatch is the simplest safe rule; growing past tier
// thresholds AFTER first dispatch just leaves you stuck at the
// originally-chosen tier (acceptable for fresh cp; mid-edit growth
// is an unusual workload).
func (w *Writer) maybePromoteLocked(ctx context.Context) error {
	if w.hasDispatched {
		return nil
	}
	// Sparse files never promote: the re-chunk below materializes one
	// contiguous buffer of w.size bytes, which for a sparse file is
	// mostly holes (a 100 GB preallocation would OOM), and re-chunking
	// would turn those holes into real zero chunks. The default tier
	// is fine — holes cost no chunk_map rows at any tier.
	if w.sparse {
		return nil
	}
	// Don't auto-promote artificially small chunk sizes — those are
	// either tests exercising multi-chunk paths with toy sizes, or
	// user overrides from `telfs init --chunk-size N` that should
	// be honored. Production filesystems start at TierSmallChunkSize
	// (4 MiB) by default.
	if w.chunkSz < TierSmallChunkSize {
		return nil
	}
	target := PickTierChunkSize(w.size)
	if target <= w.chunkSz {
		return nil
	}
	// Materialize one contiguous buffer of size w.size, then re-split.
	// At first-dispatch time w.dirtyBytes is bounded by the dirtyCap
	// (256 MiB by default), so the temporary allocation is bounded
	// too — same order of magnitude as the existing dirty footprint.
	buf := make([]byte, w.size)
	for idx, dc := range w.dirty {
		off := int64(idx) * w.chunkSz
		if off >= int64(len(buf)) {
			continue
		}
		end := off + int64(len(dc.data))
		if end > int64(len(buf)) {
			end = int64(len(buf))
		}
		copy(buf[off:end], dc.data[:end-off])
	}
	// Replace dirty layout with target-sized chunks.
	w.dirty = make(map[int32]*dirtyChunk)
	w.dirtyOrder = w.dirtyOrder[:0]
	w.dirtyBytes = 0
	w.chunkSz = target
	for off := int64(0); off < w.size; off += target {
		end := off + target
		if end > w.size {
			end = w.size
		}
		idx := int32(off / target)
		data := make([]byte, end-off)
		copy(data, buf[off:end])
		w.dirty[idx] = &dirtyChunk{data: data}
		w.dirtyOrder = append(w.dirtyOrder, idx)
		w.dirtyBytes += int64(len(data))
	}
	// Persist the promoted size so subsequent Open + Reader see the
	// right value. SetInodeChunkSize refuses if chunk_map already
	// has rows for this ino, which our hasDispatched guard above
	// already ensures isn't the case.
	if err := w.meta.SetInodeChunkSize(ctx, w.ino, target); err != nil {
		return fmt.Errorf("promote chunk_size to %d: %w", target, err)
	}
	return nil
}

// materializeZeros zero-fills the region [from, to). Caller holds w.mu.
// to may extend past the current last chunk; intermediate chunks are
// created as needed.
func (w *Writer) materializeZeros(ctx context.Context, from, to int64) error {
	if from >= to {
		return nil
	}
	startIdx := int32(from / w.chunkSz)
	endIdx := int32((to - 1) / w.chunkSz)
	for idx := startIdx; idx <= endIdx; idx++ {
		dc, err := w.loadForWrite(ctx, idx)
		if err != nil {
			return err
		}
		chunkStart := int64(idx) * w.chunkSz
		regStart := from - chunkStart
		if regStart < 0 {
			regStart = 0
		}
		regEnd := to - chunkStart
		if regEnd > w.chunkSz {
			regEnd = w.chunkSz
		}
		w.growChunk(dc, regEnd)
		// growChunk's appended bytes are already zero, so no explicit
		// zeroing needed unless we're filling INSIDE existing data.
		if int64(len(dc.data)) > regStart {
			zeroEnd := regEnd
			if zeroEnd > int64(len(dc.data)) {
				zeroEnd = int64(len(dc.data))
			}
			for i := regStart; i < zeroEnd; i++ {
				dc.data[i] = 0
			}
		}
	}
	if to > w.size {
		w.size = to
	}
	return nil
}

// Truncate sets the file's logical size to n. If n shrinks the file:
// dirty chunks past n are dropped; meta chunks past n are deleted; the
// last surviving chunk is trimmed if needed (including the PERSISTED
// copy — otherwise a later re-grow would resurrect the old bytes past
// n instead of reading zeros). If n grows the file, the recorded size
// is bumped and the inode is marked sparse so the new region reads as
// zeros.
func (w *Writer) Truncate(ctx context.Context, n int64) error {
	if n < 0 {
		return fmt.Errorf("writer: negative truncate size %d", n)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return fmt.Errorf("writer: handle closed")
	}

	if n > w.size {
		// Grow: bump size and mark sparse — the region [old size, n)
		// has no chunks, and short boundary chunks must zero-pad on
		// read. Without the flag the reader would report the missing
		// region as data-lost EIO.
		if err := w.markSparseLocked(ctx); err != nil {
			return err
		}
		w.size = n
		// Persist size eagerly so cold readers see the new size before any
		// write actually fills the hole.
		return w.meta.SetSize(ctx, w.ino, n)
	}

	// Shrink. First wait out any in-flight uploads for chunks at/past
	// the truncation point: their PutChunk (which runs on Background
	// ctx) would otherwise re-insert rows AFTER our DeleteChunksAbove,
	// leaving stale chunks that a later re-grow would resurrect.
	lastIdx := int32((n - 1) / w.chunkSz)
	if n == 0 {
		lastIdx = -1
	}
	for {
		var waitCh chan struct{}
		for idx, ch := range w.uploading {
			if idx >= lastIdx { // boundary chunk included — we may trim it below
				waitCh = ch
				break
			}
		}
		if waitCh == nil {
			break
		}
		w.mu.Unlock()
		select {
		case <-waitCh:
		case <-ctx.Done():
			w.mu.Lock()
			return ctx.Err()
		}
		w.mu.Lock()
		if w.closed {
			return fmt.Errorf("writer: handle closed")
		}
	}

	// Drop dirty chunks fully past n; trim the dirty boundary chunk.
	for idx, dc := range w.dirty {
		if idx > lastIdx {
			w.dirtyBytes -= int64(len(dc.data))
			delete(w.dirty, idx)
			// Remove from dirtyOrder.
			for i, o := range w.dirtyOrder {
				if o == idx {
					w.dirtyOrder = append(w.dirtyOrder[:i], w.dirtyOrder[i+1:]...)
					break
				}
			}
		} else if idx == lastIdx {
			// Trim the boundary chunk.
			rel := n - int64(idx)*w.chunkSz
			if int64(len(dc.data)) > rel {
				w.dirtyBytes -= int64(len(dc.data)) - rel
				dc.data = dc.data[:rel]
			}
		}
	}
	// If the boundary chunk is NOT dirty but exists persisted with
	// bytes past n, load + trim + re-dirty it so the next flush
	// replaces the stored copy. Skipping this used to leave the full
	// old chunk on the channel: reads clamped at size so it stayed
	// invisible — until a truncate-grow re-exposed the stale bytes
	// where POSIX promises zeros.
	if lastIdx >= 0 {
		rel := n - int64(lastIdx)*w.chunkSz
		if rel > 0 {
			if _, isDirty := w.dirty[lastIdx]; !isDirty {
				if c, err := w.meta.GetChunk(ctx, w.ino, lastIdx); err == nil && int64(c.Size) > rel {
					dc, err := w.loadForWrite(ctx, lastIdx)
					if err != nil {
						return fmt.Errorf("truncate: load boundary chunk %d: %w", lastIdx, err)
					}
					if int64(len(dc.data)) > rel {
						w.dirtyBytes -= int64(len(dc.data)) - rel
						dc.data = dc.data[:rel]
					}
				} else if err != nil && !errors.Is(err, meta.ErrNotFound) {
					return fmt.Errorf("truncate: probe boundary chunk %d: %w", lastIdx, err)
				}
			}
		}
	}
	// Drop persisted chunks past the new size.
	if _, err := w.meta.DeleteChunksAbove(ctx, w.ino, lastIdx+1); err != nil {
		return err
	}
	w.size = n
	return w.meta.SetSize(ctx, w.ino, n)
}

// Flush dispatches every remaining dirty chunk for upload and persists
// the final logical size to meta, then returns WITHOUT waiting for the
// network round-trips. Read-after-write coherence rides on the pool's
// per-ino Pending barrier: any reader opening this ino after Flush
// returned blocks at chunk-map lookup until the dispatched uploads
// land.
//
// FUSE invokes Flush at close(2) of each fd; with this contract close()
// returns roughly at the speed of "fill the dispatch sem", which for
// many-small-files workloads (cp -r, tar -x, git clone) collapses N
// serialized round trips into ~N/DefaultUploadConcurrency pipelined
// ones.
//
// Tradeoff: an upload that fails AFTER Flush returned cannot surface
// back to the original close() — it only shows up in stderr (via
// recordError) and as the next read EIO. Apps that need close-implies-
// durable must call fsync(), which routes to Sync below.
//
// Flush is also the retry primitive: it clears any sticky error from
// the previous batch before dispatching, so a workflow of "write,
// fsync (fails), write more, fsync" retries cleanly.
func (w *Writer) Flush(ctx context.Context) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return fmt.Errorf("writer: handle closed")
	}
	prevErr := w.uploadErr
	w.uploadErr = nil // retry-with-clean-slate
	// Multi-tier: if we still haven't dispatched, this is our last
	// chance to promote chunk_size from the observed total file
	// size. For files that never crossed dirtyCap (most small/mid
	// files) this is the ONLY promotion site.
	if err := w.maybePromoteLocked(ctx); err != nil {
		w.mu.Unlock()
		return err
	}
	toFlush := make([]int32, len(w.dirtyOrder))
	copy(toFlush, w.dirtyOrder)
	for _, idx := range toFlush {
		if err := w.dispatchFlushLocked(ctx, idx); err != nil {
			w.mu.Unlock()
			return err
		}
	}
	finalSize := w.size
	w.mu.Unlock()
	if err := w.meta.SetSize(ctx, w.ino, finalSize); err != nil {
		return err
	}
	// If there was a sticky error from before this Flush (e.g. an
	// earlier WriteAt's dispatch failed and we never got a chance to
	// surface it), bubble it up here.
	return prevErr
}

// Sync is the fsync(2) path: dispatch + WAIT this handle's uploads +
// SetSize. Use when the caller needs close-implies-durable semantics
// that Flush no longer provides.
//
// Since the recordTransient/recordLost split, uploadErr is no longer
// the durability signal — transient failures only log and restore-
// to-dirty, they don't poison uploadErr. So Sync's durability check
// is: after waiting for all in-flight uploads to complete, is the
// dirty map empty? If non-empty, at least one upload failed and was
// restored — return an error so the caller (fsync) knows the data
// isn't yet on the channel. Caller can retry Sync after the
// transient condition clears.
func (w *Writer) Sync(ctx context.Context) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return fmt.Errorf("writer: handle closed")
	}
	w.uploadErr = nil
	if err := w.maybePromoteLocked(ctx); err != nil {
		w.mu.Unlock()
		return err
	}
	toFlush := make([]int32, len(w.dirtyOrder))
	copy(toFlush, w.dirtyOrder)
	for _, idx := range toFlush {
		if err := w.dispatchFlushLocked(ctx, idx); err != nil {
			w.mu.Unlock()
			return err
		}
	}
	w.mu.Unlock()
	// Wait for THIS handle's outstanding uploads (per-handle wg, not
	// the pool's global wg — fsync shouldn't block on every other
	// handle's pending uploads).
	w.uploadWg.Wait()
	w.mu.Lock()
	stickyErr := w.uploadErr
	restoredCount := len(w.dirty)
	finalSize := w.size
	w.mu.Unlock()
	if stickyErr != nil {
		return stickyErr
	}
	if restoredCount > 0 {
		// At least one upload failed and was restored to dirty for a
		// future retry. fsync semantics require us to surface that;
		// the caller can retry Sync once the transient condition
		// clears.
		return fmt.Errorf("sync: %d chunk(s) failed to upload and were restored for retry", restoredCount)
	}
	return w.meta.SetSize(ctx, w.ino, finalSize)
}

// dispatchFlushLocked pops the dirty chunk at idx, marks it as in-flight,
// acquires staging-byte allowance (BLOCKING only past the pool's
// staged cap — flow control, not network wait), and launches a
// goroutine that does the encrypt + stage + upload + chunk_map update.
//
// The caller holds w.mu on entry; this method releases the mutex during
// the staging-gate wait (so other writers can keep filling new chunks)
// and reacquires it before returning.
//
// Returns the error from AcquireStaged if the upload ctx or caller ctx
// canceled while we waited for allowance — in which case nothing was
// dispatched and dc stays available to retry via a future Flush.
func (w *Writer) dispatchFlushLocked(ctx context.Context, idx int32) error {
	dc, ok := w.dirty[idx]
	if !ok {
		// Already dispatched (could happen if dispatchFlushLocked is
		// called twice for the same idx after a release/reacquire gap).
		// Remove from order if it's still there and bail.
		w.removeFromOrderLocked(idx)
		return nil
	}
	// Lock chunkSz: from this point forward the writer can't promote
	// to a bigger tier because chunk_map will have a row at the
	// current chunkSz, and changing it would mis-map this row.
	w.hasDispatched = true
	delete(w.dirty, idx)
	w.removeFromOrderLocked(idx)
	w.dirtyBytes -= int64(len(dc.data))
	doneCh := make(chan struct{})
	w.uploading[idx] = doneCh
	w.uploadWg.Add(1)
	w.mu.Unlock()
	// Staging gate, NOT the upload sem: dispatch only waits for RAM
	// allowance (DefaultStagedCapBytes of in-flight chunk data), never
	// for the network. The upload goroutine stages the wire to
	// pending/ first and only then queues for a concurrency slot
	// (Reservation.AcquireSem inside uploadOne), so close(2) of a
	// burst that fits the gate returns at local speed while uploads
	// drain behind it. A sustained over-cap writer still blocks here —
	// that's the flow control that keeps a 100 GB dd honest.
	stagedN, err := w.pool.AcquireStaged(ctx, int64(len(dc.data)))
	if err != nil {
		// Couldn't get allowance (ctx canceled / shutdown) — undo the
		// bookkeeping and put dc back where dispatchFlushLocked found
		// it so the next Flush can retry.
		w.mu.Lock()
		w.uploadWg.Done()
		delete(w.uploading, idx)
		close(doneCh)
		w.dirty[idx] = dc
		w.dirtyOrder = append([]int32{idx}, w.dirtyOrder...)
		w.dirtyBytes += int64(len(dc.data))
		return err
	}
	// Register arms the R-A-W barrier + drain wg synchronously —
	// before this dispatch returns — so readers and unmount can never
	// miss the in-flight upload.
	res := w.pool.Register(w.ino)
	go func() {
		defer w.pool.ReleaseStaged(stagedN)
		w.uploadOne(w.pool.UploadCtx(), idx, dc, doneCh, res)
	}()
	w.mu.Lock()
	return nil
}

// removeFromOrderLocked drops idx from w.dirtyOrder if present.
func (w *Writer) removeFromOrderLocked(idx int32) {
	for i, o := range w.dirtyOrder {
		if o == idx {
			w.dirtyOrder = append(w.dirtyOrder[:i], w.dirtyOrder[i+1:]...)
			return
		}
	}
}

// uploadOne runs in its own goroutine and performs the encrypt → upload
// → chunk_map → cache-invalidate sequence with the lock RELEASED for
// the duration of the network round-trip.
//
// On any failure the chunk is RESTORED to the dirty set so a subsequent
// Flush can retry it, unless a fresh write to the same idx has already
// arrived (in which case the new dirty entry supersedes ours), or the
// writer has been Detached (no map left to restore into — the failed
// chunk is lost, the error logs to stderr).
func (w *Writer) uploadOne(ctx context.Context, idx int32, dc *dirtyChunk, doneCh chan struct{}, res *Reservation) {
	// Defers unwind LIFO: panic guard runs LAST (outermost — must catch
	// even the inner defers' panics). Then restore-on-failure runs
	// first on normal return (so waiters see the resurrected dirty
	// entry), then delete uploading[idx], then close doneCh (which
	// wakes loadForWrite), then Release the pool reservation
	// (decrements pool wg + per-ino Pending so reader-side barriers
	// unblock), then per-writer wg.Done. Pack route may call
	// res.ReleaseSem() early to free the upload concurrency slot
	// while keeping Pending+wg held until the packer's PutChunk lands.
	defer w.uploadWg.Done()
	defer res.Release()
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[writer] uploadOne PANIC ino=%d idx=%d: %v\n", w.ino, idx, r)
			// A panic from within the upload pipeline is not safely
			// retryable — the chunk is in unknown state. Promote
			// straight to LOST so any concurrent Flush sees it.
			w.recordLost(fmt.Errorf("uploadOne panic ino=%d idx=%d: %v", w.ino, idx, r))
		}
	}()
	defer close(doneCh)
	defer func() {
		w.mu.Lock()
		delete(w.uploading, idx)
		w.mu.Unlock()
	}()
	var restore bool
	var lastErr error // captured by the error sites; used by the defer to call recordLost on drop-on-floor
	// pendingPath is the on-disk staging file for this chunk's wire,
	// set by whichever route (pack or direct) staged it. Shared with
	// the restore defer: a failed upload whose wire is staged is NOT
	// lost — reads serve it via the staging fallback and the next
	// mount re-submits it.
	var pendingPath string
	defer func() {
		if !restore {
			return
		}
		w.mu.Lock()
		if w.closed {
			// Writer already detached — no dirty map to put dc back
			// into and no Flush will retry in this session.
			w.mu.Unlock()
			if lastErr == nil {
				return
			}
			if pendingPath != "" {
				// The wire survives on disk: readable now via the
				// staging fallback, re-submitted on next mount by
				// recoverPending. Recoverable, not lost.
				fmt.Fprintf(os.Stderr, "[writer] upload failed after handle close (ino=%d idx=%d): %v — wire staged at %s for recovery\n",
					w.ino, idx, lastErr, pendingPath)
				return
			}
			// No staging (PendingDir unset) — genuinely lost. Promote
			// to a LOST chunk: uploadErr gets the sticky failure so
			// any concurrent Flush surfaces it, and the daemon log
			// records the data-loss event explicitly.
			w.recordLost(lastErr)
			return
		}
		if _, hasNew := w.dirty[idx]; !hasNew {
			w.dirty[idx] = dc
			// Prepend so a retry Flush re-attempts the failed chunk first.
			w.dirtyOrder = append([]int32{idx}, w.dirtyOrder...)
			w.dirtyBytes += int64(len(dc.data))
		}
		w.mu.Unlock()
	}()

	// Content-addressed dedup. The codec decides: format 1 hashes
	// (sha256) only when the cipher is deterministic (plaintext /
	// aes-gcm-v3); format 2 always hashes (HMAC under the FS hash key —
	// the hash is also the wire's AAD). The reuse path inserts the
	// chunk_map row in the same transaction as the aliveness check, so
	// GC can never delete the shared message between the check and our
	// reference. As in v0.22 every chunk is looked up, but only direct
	// uploads are INDEXED (RecordChunkBlob below), so a small chunk only
	// dedups against a direct one; indexing packed files is P3.
	hash := w.wc.Hash(dc.data)
	packRoute := w.packer != nil && int32(len(dc.data)) < w.packer.Threshold()
	if hash != nil {
		reused, _, _, derr := w.meta.ReuseChunkByHash(context.Background(), w.ino, idx, hash)
		if derr != nil {
			restore = true
			lastErr = fmt.Errorf("dedup lookup %d: %w", idx, derr)
			w.recordTransient(lastErr)
			return
		}
		if reused {
			w.cache.Invalidate(Key{Ino: w.ino, Idx: idx})
			metrics.DedupHits.Inc()
			return
		}
	}

	// Pipeline: plaintext → compress → encrypt → wire (inside the
	// codec). Compress before encrypt because ciphertext is high-
	// entropy and would defeat the compressor.
	w.mu.Lock()
	fileSize := w.size
	w.mu.Unlock()
	wire, codecUsed, err := w.wc.Encode(w.ino, idx, dc.data, hash)
	if err != nil {
		restore = true
		lastErr = err
		w.recordTransient(lastErr)
		return
	}
	// chunk_map stores the hash only on format 2 (it is the AAD there);
	// format 1 rows stay exactly as v0.22 wrote them.
	var rowHash []byte
	if w.wc.Format() == 2 {
		rowHash = hash
	}
	// Route: pack small chunks, direct-upload everything else. Packing
	// trades per-file dedup (skipped for packed chunks in v1) for a
	// massive throughput win on many-tiny-files workloads, so the
	// dedup decision above is BYPASSED whenever the chunk would
	// qualify for packing. Larger chunks still flow through the
	// dedup + direct-upload path unchanged.
	if packRoute {
		// Persist wire to disk BEFORE submitting to packer so a
		// hard daemon kill (or panic) between here and the
		// packer's chunk_map write doesn't lose the bytes. The
		// pending dir lives under the cache root; restart scans
		// it and re-submits via Backend.RecoverPending.
		if dir := w.pool.PendingDir; dir != "" {
			p, perr := PersistPendingHashed(dir, w.ino, idx, int32(len(dc.data)), rowHash, wire)
			if perr != nil {
				restore = true
				lastErr = fmt.Errorf("persist pending chunk %d: %w", idx, perr)
				w.recordTransient(lastErr)
				return
			}
			pendingPath = p
			// Drop a previous generation of this slot (a retry whose
			// wire length differed writes a different filename) so
			// recovery never sees two competing files — same contract
			// as the direct route below.
			if old, ok := w.pool.PendingIndex.Lookup(w.ino, idx); ok && old != p {
				_ = DeletePending(old)
			}
			w.pool.PendingIndex.RecordPersist(w.ino, idx, p)
		}
		// The pack route takes NO upload sem slot: the packer runs its
		// own serialized flusher, so a slot would neither bound extra
		// network concurrency nor help aggregation (holding one used
		// to cap per-batch aggregation at DefaultUploadConcurrency).
		// Pending + wg (via Register) stay held until res.Release()
		// at the bottom of this function, so the reader-side R-A-W
		// barrier remains honored.
		resultCh := w.packer.SubmitFrom(w.ino, idx, int32(len(dc.data)), wire, pendingPath, rowHash, codecUsed, fileSize)
		select {
		case packRes := <-resultCh:
			if packRes.err != nil {
				restore = true
				lastErr = fmt.Errorf("pack chunk %d: %w", idx, packRes.err)
				w.recordTransient(lastErr)
				return
			}
			// Packer already wrote the chunk_map row with pack_offset
			// + pack_size. We just need to invalidate the local cache
			// so the next read fetches the new bytes.
			w.cache.Invalidate(Key{Ino: w.ino, Idx: idx})
			return
		case <-ctx.Done():
			restore = true
			lastErr = fmt.Errorf("pack chunk %d: %w", idx, ctx.Err())
			w.recordTransient(lastErr)
			return
		}
	}

	// Stage the wire on disk BEFORE the network round-trip — same
	// contract the pack route has always had. Until this existed,
	// direct-route chunks lived only in RAM: a daemon crash mid-
	// upload, or a retry ladder exhausted after FUSE Release (when
	// there's no dirty map left to restore into), silently lost the
	// bytes even though close() had already returned success. With
	// the wire staged, (a) reads serve it via the staging fallback,
	// (b) the next daemon startup re-submits it via recoverPending,
	// and (c) recordLost stops being a data-loss event.
	if dir := w.pool.PendingDir; dir != "" {
		p, perr := PersistPendingHashed(dir, w.ino, idx, int32(len(dc.data)), rowHash, wire)
		if perr != nil {
			restore = true
			lastErr = fmt.Errorf("persist pending chunk %d: %w", idx, perr)
			w.recordTransient(lastErr)
			return
		}
		pendingPath = p
		// A previous attempt at this slot may have staged a wire of a
		// different length (different filename). Drop it so recovery
		// never sees two generations of the same slot.
		if old, ok := w.pool.PendingIndex.Lookup(w.ino, idx); ok && old != p {
			_ = DeletePending(old)
		}
		w.pool.PendingIndex.RecordPersist(w.ino, idx, p)
	}

	// Queue for an upload concurrency slot ONLY now that the wire is
	// staged on disk: while we wait, reads are served via the staging
	// fallback and a crash/shutdown loses nothing (recoverPending
	// re-submits on next mount). A canceled wait therefore restores
	// to dirty with pendingPath set — the recoverable shape.
	if err := res.AcquireSem(ctx); err != nil {
		restore = true
		lastErr = fmt.Errorf("upload slot chunk %d: %w", idx, err)
		w.recordTransient(lastErr)
		return
	}
	name, caption, err := w.wc.ChunkDoc(ctx, ChunkDocInfo{Ino: w.ino, Idx: idx, Hash: rowHash, Len: int64(len(dc.data)), Codec: codecUsed, FileSize: fileSize})
	if err != nil {
		restore = true
		lastErr = fmt.Errorf("caption chunk %d: %w", idx, err)
		w.recordTransient(lastErr)
		return
	}
	msgID, err := uploadWithRetry(ctx, w.uploader, wire, name, caption)
	if err != nil {
		restore = true
		metrics.UploadErrors.Inc()
		lastErr = fmt.Errorf("upload chunk %d: %w", idx, err)
		w.recordTransient(lastErr)
		return
	}
	metrics.ChunksUploaded.Inc()
	metrics.BytesUploaded.Add(int64(len(wire)))
	// PutChunk uses Background ctx, not the upload worker's ctx —
	// Close() cancels uploadCtx to abort in-flight gotd round-trips,
	// but once an upload has SUCCEEDED we want its chunk_map row to
	// land regardless. Without this, a sibling chunk's failure (which
	// calls uploadCancel via recordError on Close) would race the
	// PutChunk of an already-successful upload and orphan it on the
	// channel.
	if err := w.meta.PutChunk(context.Background(), meta.Chunk{
		Ino:         w.ino,
		Idx:         idx,
		TGMessageID: int64(msgID),
		Size:        int32(len(dc.data)),
		Hash:        rowHash,
	}); err != nil {
		// FK violation = the inode was unlinked while this upload was
		// in flight. The row isn't wanted anymore; retrying would fail
		// forever. Drop the chunk (message becomes gc fodder) and the
		// staged wire (it would re-upload on next mount otherwise).
		if meta.IsFKViolation(err) {
			if pendingPath != "" {
				_ = DeletePending(pendingPath)
				w.pool.PendingIndex.UnrecordPersist(w.ino, idx)
			}
			fmt.Fprintf(os.Stderr, "[writer] dropped orphan chunk ino=%d/%d (inode gone, msg %d kept for gc)\n", w.ino, idx, msgID)
			return
		}
		// At this point the chunk IS on the channel (TG message exists)
		// but chunk_map didn't get the row. Treat as failure and restore
		// to dirty for retry — the next Flush will re-upload and the
		// orphan TG message becomes garbage for `telfs gc`.
		restore = true
		lastErr = fmt.Errorf("chunk_map %d: %w", idx, err)
		w.recordTransient(lastErr)
		return
	}
	// chunk_map row landed — the staged wire is superseded. Leaving it
	// would re-upload on the next startup and orphan the just-landed
	// message.
	if pendingPath != "" {
		_ = DeletePending(pendingPath)
		w.pool.PendingIndex.UnrecordPersist(w.ino, idx)
	}
	if hash != nil {
		// Best-effort index update. A failure here doesn't lose data —
		// the chunk is on the channel, chunk_map points at it — it just
		// means future identical writes won't dedup. Surface via stderr
		// rather than restoring the upload (which would re-upload and
		// orphan the just-landed message).
		if err := w.meta.RecordChunkBlob(context.Background(), hash, int64(msgID), int32(len(dc.data))); err != nil {
			fmt.Fprintf(os.Stderr, "[writer] record blob index ino=%d idx=%d: %v\n", w.ino, idx, err)
		}
	}
	w.cache.Invalidate(Key{Ino: w.ino, Idx: idx})
}

// dedupSafe marks a format-1 cipher as deterministic (NoopCipher,
// AESGCMConvergent): only then may format 1 deduplicate, because a
// reused message must decrypt the same for every slot. Format 1's codec
// checks it; the random-nonce AESGCM deliberately lacks the method, so
// it cannot be flipped into dedup-eligible state by accident. Format 2
// needs no such marker: its chunks are bound to their content hash, not
// their slot.
type dedupSafe interface {
	Deterministic() bool
}

// recordTransient logs a transient upload failure but does NOT set
// uploadErr. The chunk is restored to dirty by the calling site's
// restore-on-failure defer, so the next dispatch (eager from
// WriteAt or explicit from Flush) will retry it — there's no point
// surfacing the failure to userspace as long as the data is still
// queued for retry.
//
// Old shape (recordError, first-wins) used to ALSO set uploadErr,
// which made the next WriteAt return EIO to tar even though the
// chunk was about to retry successfully. Tar then aborted mid-
// extract on transient TG hiccups despite eventual consistency.
//
// Stderr line stays so live mount logs still surface the underlying
// cause (FLOOD_WAIT past middleware cap, RPC timeout, network
// glitch).
func (w *Writer) recordTransient(err error) {
	fmt.Fprintf(os.Stderr, "[writer] transient upload error (ino=%d, restored for retry): %v\n", w.ino, err)
}

// recordLost is called when a chunk truly cannot be retried — the
// writer has been Detached, so there's no dirty map to restore into
// and no future Flush will fire. The chunk's bytes are now orphans
// on Telegram (if the upload had succeeded but PutChunk hadn't yet
// run) or simply lost. Sets uploadErr so any concurrent Flush/Sync
// (rare) sees it.
func (w *Writer) recordLost(err error) {
	w.mu.Lock()
	first := w.uploadErr == nil
	if first {
		w.uploadErr = err
	}
	w.mu.Unlock()
	if first {
		fmt.Fprintf(os.Stderr, "[writer] LOST chunk (ino=%d, writer detached, no retry): %v\n", w.ino, err)
	}
}

// Close releases the writer's in-memory state. Does NOT cancel or wait
// on in-flight uploads — those ride pool.UploadCtx, which survives the
// handle and is drained at Backend.Shutdown. The pool's per-ino
// Pending barrier keeps subsequent readers correct.
//
// FUSE Release calls this after Flush. Detach is the recommended
// name elsewhere; Close stays as the method name so the
// io.Closer-shaped tests and existing call sites don't need changes.
func (w *Writer) Close() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	w.dirty = nil
	w.dirtyOrder = nil
	w.dirtyBytes = 0
	w.uploading = nil
	w.mu.Unlock()
}
