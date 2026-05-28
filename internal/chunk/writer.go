package chunk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
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
// before WriteAt starts eagerly flushing the oldest dirty chunks. Keeps
// memory usage bounded for streaming workloads ("cat big > mnt/copy")
// without forcing a flush on every chunk boundary.
const DefaultDirtyCapBytes int64 = 64 << 20

// Writer owns the dirty-chunk buffer for a single open file handle. It
// is NOT safe to share across goroutines — go-fuse may call WriteAt
// concurrently for one handle, so all entry points acquire mu.
//
// Lifecycle (mirrors a FUSE handle's open/write/flush/release):
//   - New: allocate, recover current file size from meta.
//   - WriteAt: copy bytes into dirty chunks; may auto-Flush oldest chunks
//     when dirtyBytes > dirtyCap.
//   - Truncate: drop dirty chunks past size; trim or delete last chunk.
//   - Flush: upload every dirty chunk in idx order; on any failure, stop
//     and return — already-committed chunks stay committed, the rest
//     remain dirty for a retry. Updates meta.inodes.size to the final
//     logical size as the LAST step (so a partial flush doesn't shrink
//     the apparent file size).
//   - Close: idempotent; drops in-memory state. Caller is responsible
//     for invoking Flush first if they care about durability.
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
	dirtyOrder []int32 // FIFO insertion order — eager-flush victims pop from front
	size       int64
	closed     bool
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
	return &Writer{
		meta:     m,
		cache:    c,
		uploader: u,
		cipher:   cipher,
		chunkSz:  chunkSize,
		dirtyCap: dirtyCap,
		ino:      ino,
		dirty:    make(map[int32]*dirtyChunk),
		size:     in.Size,
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

	// Eager flush if we're over the cap. Flush oldest dirty chunks (by
	// insertion order) until back under the cap.
	for w.dirtyBytes > w.dirtyCap && len(w.dirtyOrder) > 0 {
		victim := w.dirtyOrder[0]
		if err := w.flushChunkLocked(ctx, victim); err != nil {
			return written, fmt.Errorf("eager flush chunk %d: %w", victim, err)
		}
	}

	return written, nil
}

// loadForWrite returns (and registers as dirty) the chunk at idx,
// preloading existing bytes via the cache when this is the first
// dirty touch.
func (w *Writer) loadForWrite(ctx context.Context, idx int32) (*dirtyChunk, error) {
	if dc, ok := w.dirty[idx]; ok {
		return dc, nil
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

// Flush uploads every dirty chunk in ascending idx order. On any
// per-chunk failure, returns immediately; earlier chunks are already
// committed (chunk_map + size update for them stays), later chunks
// remain dirty for the next Flush. The file's size in meta is updated
// at the end so a partial flush doesn't shrink the visible file.
func (w *Writer) Flush(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return fmt.Errorf("writer: handle closed")
	}
	return w.flushAllLocked(ctx)
}

func (w *Writer) flushAllLocked(ctx context.Context) error {
	// Snapshot the dirty idxes in ascending order so the order is
	// deterministic regardless of insertion order.
	idxs := make([]int32, 0, len(w.dirty))
	for k := range w.dirty {
		idxs = append(idxs, k)
	}
	// Insertion sort suffices.
	for i := 1; i < len(idxs); i++ {
		for j := i; j > 0 && idxs[j-1] > idxs[j]; j-- {
			idxs[j-1], idxs[j] = idxs[j], idxs[j-1]
		}
	}
	for _, idx := range idxs {
		if err := w.flushChunkLocked(ctx, idx); err != nil {
			return err
		}
	}
	// All dirty chunks committed — write the final size to meta.
	return w.meta.SetSize(ctx, w.ino, w.size)
}

// flushChunkLocked uploads a single dirty chunk and removes it from the
// dirty set. Caller must hold w.mu.
func (w *Writer) flushChunkLocked(ctx context.Context, idx int32) error {
	dc, ok := w.dirty[idx]
	if !ok {
		return nil
	}
	// Encrypt before upload. With NoopCipher the bytes pass through.
	wire, err := w.cipher.Seal(w.ino, idx, dc.data)
	if err != nil {
		return fmt.Errorf("encrypt chunk %d: %w", idx, err)
	}
	name := fmt.Sprintf("ino%d-idx%d", w.ino, idx)
	msgID, err := w.uploader.UploadDocument(ctx, bytes.NewReader(wire), name, "")
	if err != nil {
		return fmt.Errorf("upload chunk %d: %w", idx, err)
	}
	if err := w.meta.PutChunk(ctx, meta.Chunk{
		Ino:         w.ino,
		Idx:         idx,
		TGMessageID: int64(msgID),
		Size:        int32(len(dc.data)),
	}); err != nil {
		return fmt.Errorf("chunk_map update %d: %w", idx, err)
	}
	// Invalidate the LRU read cache for this chunk so a concurrent reader
	// on another handle picks up the new bytes on next access.
	w.cache.Invalidate(Key{Ino: w.ino, Idx: idx})
	// Remove from dirty bookkeeping.
	w.dirtyBytes -= int64(len(dc.data))
	delete(w.dirty, idx)
	for i, o := range w.dirtyOrder {
		if o == idx {
			w.dirtyOrder = append(w.dirtyOrder[:i], w.dirtyOrder[i+1:]...)
			break
		}
	}
	return nil
}

// Close releases the writer's in-memory state. It does NOT flush; the
// caller (FUSE Release) is expected to call Flush first if durability
// matters.
func (w *Writer) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	w.dirty = nil
	w.dirtyOrder = nil
	w.dirtyBytes = 0
}
