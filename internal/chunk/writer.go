package chunk

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"telfs/internal/crypto"
	"telfs/internal/meta"
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
//     dirtyCap. Dispatch acquires an upload-semaphore slot, so if
//     concurrency is saturated the caller blocks — honest backpressure
//     to the kernel.
//   - Truncate: drop dirty chunks past size; trim or delete last chunk.
//   - Flush: dispatch every remaining dirty chunk, then WAIT for all
//     in-flight uploads to finish. Surfaces the first sticky error.
//     Updates meta.inodes.size to the final logical size as the LAST
//     step.
//   - Close: cancels the worker context (in-flight gotd uploads
//     unwind), drains the workgroup, drops in-memory state.
//
// Lock discipline: w.mu protects every map/slice field below. The
// network round-trip itself happens with mu released, so FUSE write
// goroutines can keep filling new dirty chunks while old ones upload
// in the background.
type Writer struct {
	meta     *meta.Store
	cache    *Cache
	uploader Uploader
	cipher   crypto.Cipher // never nil; defaults to NoopCipher
	chunkSz  int64
	dirtyCap int64
	ino      int64

	mu         sync.Mutex
	dirty      map[int32]*dirtyChunk
	dirtyBytes int64
	dirtyOrder []int32 // FIFO insertion order — eager-dispatch victims pop from front
	size       int64
	closed     bool

	// Async upload pipeline. uploadSem caps in-flight concurrency.
	// uploading tracks chunks whose data is no longer in `dirty` but
	// whose upload hasn't finished; loadForWrite waits on the done
	// channel before treating the chunk as missing from meta.
	// uploadErr is the first sticky error from any background upload;
	// once set, subsequent WriteAt / Flush return it.
	uploadCtx    context.Context
	uploadCancel context.CancelFunc
	uploadSem    chan struct{}
	uploadWg     sync.WaitGroup
	uploading    map[int32]chan struct{}
	uploadErr    error
}

type dirtyChunk struct {
	data []byte // 0..chunkSz bytes; len(data) == effective size of this chunk
}

// NewWriter constructs a Writer for an open file handle. chunkSize and
// dirtyCap may be zero, in which case the package defaults apply. If
// cipher is nil, NoopCipher is used (plaintext on the wire).
func NewWriter(ctx context.Context, m *meta.Store, c *Cache, u Uploader, cipher crypto.Cipher, ino int64, chunkSize, dirtyCap int64) (*Writer, error) {
	if chunkSize <= 0 {
		chunkSize = ChunkSize
	}
	if dirtyCap <= 0 {
		dirtyCap = DefaultDirtyCapBytes
	}
	if cipher == nil {
		cipher = crypto.NoopCipher{}
	}
	in, err := m.GetInode(ctx, ino)
	if err != nil {
		return nil, fmt.Errorf("writer: get inode %d: %w", ino, err)
	}
	uploadCtx, uploadCancel := context.WithCancel(context.Background())
	return &Writer{
		meta:         m,
		cache:        c,
		uploader:     u,
		cipher:       cipher,
		chunkSz:      chunkSize,
		dirtyCap:     dirtyCap,
		ino:          ino,
		dirty:        make(map[int32]*dirtyChunk),
		size:         in.Size,
		uploadCtx:    uploadCtx,
		uploadCancel: uploadCancel,
		uploadSem:    make(chan struct{}, DefaultUploadConcurrency),
		uploading:    make(map[int32]chan struct{}),
	}, nil
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
	if err := w.uploadErr; err != nil {
		return 0, err
	}

	// Materialize any sparse region between size and off as zeros. This
	// is wasteful for true sparse files but correct (POSIX reads of holes
	// return zero). M6 may add real sparse handling.
	if off > w.size {
		if err := w.materializeZeros(ctx, w.size, off); err != nil {
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

	// Eager DISPATCH if we're over the cap. Drains oldest dirty chunks
	// (by insertion order) until back under the cap. Dispatching does
	// not block on the network — it queues for the worker pool — BUT
	// it does block on the upload semaphore when the pool is saturated.
	// That semaphore wait IS the backpressure that prevents the kernel
	// from piling up unbounded dirty pages.
	for w.dirtyBytes > w.dirtyCap && len(w.dirtyOrder) > 0 {
		victim := w.dirtyOrder[0]
		w.dispatchFlushLocked(victim)
		// dispatchFlushLocked releases + reacquires w.mu; re-check
		// the dirtyErr each iteration so a fast-failed background
		// upload surfaces here.
		if err := w.uploadErr; err != nil {
			return written, err
		}
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
		existing, fetchErr := w.cache.Get(ctx, Key{Ino: w.ino, Idx: idx}, c.TGMessageID)
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
// last surviving chunk is trimmed if needed. If n grows the file, just
// updates the recorded size (reads of the new region return zero until
// it's actually written).
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
		// Grow: just bump size. Reads past current data return zeros via
		// chunk.Reader's "missing chunk = EOF" semantics; full sparse
		// support is deferred.
		w.size = n
		// Persist size eagerly so cold readers see the new size before any
		// write actually fills the hole.
		return w.meta.SetSize(ctx, w.ino, n)
	}

	// Shrink. Drop dirty chunks fully past n.
	lastIdx := int32((n - 1) / w.chunkSz)
	if n == 0 {
		lastIdx = -1
	}
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
	// Drop persisted chunks past the new size.
	if _, err := w.meta.DeleteChunksAbove(ctx, w.ino, lastIdx+1); err != nil {
		return err
	}
	w.size = n
	return w.meta.SetSize(ctx, w.ino, n)
}

// Flush dispatches every remaining dirty chunk for upload, waits for
// all in-flight uploads to complete, surfaces the first error from
// THIS batch (if any), and writes the final logical size to meta.
//
// Flush is the retry primitive: it clears any sticky error from a
// previous failed Flush before dispatching. A failed Flush leaves the
// failed chunks back in `dirty`; the next Flush retries them. WriteAt
// reads the sticky error so the kernel sees EIO immediately on the
// post-failure write — only Flush wipes the slate.
func (w *Writer) Flush(ctx context.Context) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return fmt.Errorf("writer: handle closed")
	}
	w.uploadErr = nil // retry-with-clean-slate
	// Snapshot the dirty list once — restore-on-failure puts failed
	// chunks back into dirtyOrder, and we MUST NOT re-dispatch them in
	// the same Flush call or we'd loop forever on persistent failures.
	// The next Flush picks them up.
	toFlush := make([]int32, len(w.dirtyOrder))
	copy(toFlush, w.dirtyOrder)
	for _, idx := range toFlush {
		w.dispatchFlushLocked(idx)
	}
	w.mu.Unlock()
	// Wait for every dispatched upload to finish.
	w.uploadWg.Wait()
	w.mu.Lock()
	if err := w.uploadErr; err != nil {
		w.mu.Unlock()
		return err
	}
	finalSize := w.size
	w.mu.Unlock()
	return w.meta.SetSize(ctx, w.ino, finalSize)
}

// dispatchFlushLocked pops the dirty chunk at idx, marks it as in-flight,
// acquires a semaphore slot (BLOCKING for backpressure), and launches a
// goroutine that does the encrypt + upload + chunk_map update.
//
// The caller holds w.mu on entry; this method releases the mutex during
// the semaphore acquire (so other writers can keep filling new chunks)
// and reacquires it before returning.
func (w *Writer) dispatchFlushLocked(idx int32) {
	dc, ok := w.dirty[idx]
	if !ok {
		// Already dispatched (could happen if dispatchFlushLocked is
		// called twice for the same idx after a release/reacquire gap).
		// Remove from order if it's still there and bail.
		w.removeFromOrderLocked(idx)
		return
	}
	delete(w.dirty, idx)
	w.removeFromOrderLocked(idx)
	w.dirtyBytes -= int64(len(dc.data))
	doneCh := make(chan struct{})
	w.uploading[idx] = doneCh
	w.uploadWg.Add(1)
	// Drop the lock to acquire the sem slot — this is the backpressure
	// point. New writers calling WriteAt can hold the lock in the
	// meantime; they'll see this chunk in w.uploading so loadForWrite
	// for the same idx will wait correctly.
	w.mu.Unlock()
	w.uploadSem <- struct{}{}
	go w.uploadOne(w.uploadCtx, idx, dc, doneCh)
	w.mu.Lock()
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
// arrived (in which case the new dirty entry supersedes ours).
func (w *Writer) uploadOne(ctx context.Context, idx int32, dc *dirtyChunk, doneCh chan struct{}) {
	// Defers unwind LIFO: panic guard runs LAST (outermost — must catch
	// even the inner defers' panics). Then restore-on-failure runs
	// first on normal return (so waiters see the resurrected dirty
	// entry), then delete uploading[idx], then close doneCh (which
	// wakes loadForWrite), then release the sem slot, then wg.Done.
	defer w.uploadWg.Done()
	defer func() { <-w.uploadSem }()
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[writer] uploadOne PANIC ino=%d idx=%d: %v\n", w.ino, idx, r)
			w.recordError(fmt.Errorf("uploadOne panic ino=%d idx=%d: %v", w.ino, idx, r))
		}
	}()
	defer close(doneCh)
	defer func() {
		w.mu.Lock()
		delete(w.uploading, idx)
		w.mu.Unlock()
	}()
	var restore bool
	defer func() {
		if !restore {
			return
		}
		w.mu.Lock()
		if _, hasNew := w.dirty[idx]; !hasNew {
			w.dirty[idx] = dc
			// Prepend so a retry Flush re-attempts the failed chunk first.
			w.dirtyOrder = append([]int32{idx}, w.dirtyOrder...)
			w.dirtyBytes += int64(len(dc.data))
		}
		w.mu.Unlock()
	}()

	// Content-addressed dedup (plaintext FSes only): hash the chunk
	// bytes and ask meta whether an existing channel message holds the
	// same content. The reuse path inserts the chunk_map row in the
	// same transaction as the aliveness check, so GC can never delete
	// the shared message between the check and our reference.
	//
	// Why plaintext only: encrypted chunks use random per-chunk nonces
	// and (ino, idx)-bound AAD, so the same plaintext yields a
	// different ciphertext for every slot. Sharing the ciphertext
	// across slots would require either convergent encryption or
	// rebinding the AAD — both are wire-format changes that need their
	// own opt-in. Encrypted FSes get the existing upload-every-time
	// behavior.
	if w.dedupEnabled() {
		sum := sha256.Sum256(dc.data)
		reused, blobMsgID, _, derr := w.meta.ReuseChunkByHash(context.Background(), w.ino, idx, sum[:])
		if derr != nil {
			restore = true
			w.recordError(fmt.Errorf("dedup lookup %d: %w", idx, derr))
			return
		}
		if reused {
			w.cache.Invalidate(Key{Ino: w.ino, Idx: idx})
			_ = blobMsgID
			return
		}
	}

	wire, err := w.cipher.Seal(w.ino, idx, dc.data)
	if err != nil {
		restore = true
		w.recordError(fmt.Errorf("encrypt chunk %d: %w", idx, err))
		return
	}
	name := fmt.Sprintf("ino%d-idx%d", w.ino, idx)
	msgID, err := w.uploader.UploadDocument(ctx, bytes.NewReader(wire), name, "")
	if err != nil {
		restore = true
		w.recordError(fmt.Errorf("upload chunk %d: %w", idx, err))
		return
	}
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
	}); err != nil {
		// At this point the chunk IS on the channel (TG message exists)
		// but chunk_map didn't get the row. Treat as failure and restore
		// to dirty for retry — the next Flush will re-upload and the
		// orphan TG message becomes garbage for `telfs gc`.
		restore = true
		w.recordError(fmt.Errorf("chunk_map %d: %w", idx, err))
		return
	}
	if w.dedupEnabled() {
		// Best-effort index update. A failure here doesn't lose data —
		// the chunk is on the channel, chunk_map points at it — it just
		// means future identical writes won't dedup. Surface via stderr
		// rather than restoring the upload (which would re-upload and
		// orphan the just-landed message).
		sum := sha256.Sum256(dc.data)
		if err := w.meta.RecordChunkBlob(context.Background(), sum[:], int64(msgID), int32(len(dc.data))); err != nil {
			fmt.Fprintf(os.Stderr, "[writer] record blob index ino=%d idx=%d: %v\n", w.ino, idx, err)
		}
	}
	w.cache.Invalidate(Key{Ino: w.ino, Idx: idx})
}

// dedupEnabled reports whether this writer should attempt content-
// addressed reuse for outgoing chunks. True for any cipher that
// declares itself deterministic — NoopCipher (plaintext FS) and
// AESGCMConvergent (aes-gcm-v3 FS) qualify; the random-nonce AESGCM
// used by v1/v2 does not. The marker interface lives in
// internal/crypto so a future cipher type can opt in by implementing
// it; the writer doesn't have to learn new concrete types.
//
// Why an interface rather than a Cipher-level method: a missing
// Deterministic() on the random-nonce AESGCM is a compile-checked
// guarantee that it cannot be silently flipped into dedup-eligible
// state. Adding a method that returns false would put the safety in
// a runtime bool.
type dedupSafe interface {
	Deterministic() bool
}

func (w *Writer) dedupEnabled() bool {
	d, ok := w.cipher.(dedupSafe)
	return ok && d.Deterministic()
}

// recordError sets the sticky uploadErr (first-wins). It does NOT
// cancel the worker context — sibling uploads that are already
// past their network round-trip should still get their chunk_map
// rows in; the sticky error stops the NEXT dispatch but doesn't
// abort what's already committed. Close() is the only place that
// cancels uploadCtx.
//
// The error is also printed to stderr at first occurrence so live
// mount logs surface the underlying cause (FLOOD_WAIT, RPC timeout,
// network glitch) rather than just "I/O error" via the kernel.
func (w *Writer) recordError(err error) {
	w.mu.Lock()
	first := w.uploadErr == nil
	if first {
		w.uploadErr = err
	}
	w.mu.Unlock()
	if first {
		fmt.Fprintf(os.Stderr, "[writer] upload error (ino=%d): %v\n", w.ino, err)
	}
}

// Close releases the writer's in-memory state. Cancels any in-flight
// uploads and drains them so no goroutine survives this method's
// return. It does NOT flush — the caller (FUSE Release) is expected
// to call Flush first if durability matters.
func (w *Writer) Close() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	w.uploadCancel()
	w.mu.Unlock()
	w.uploadWg.Wait()
	w.mu.Lock()
	w.dirty = nil
	w.dirtyOrder = nil
	w.dirtyBytes = 0
	w.uploading = nil
	w.mu.Unlock()
}
