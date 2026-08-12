package fs

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"telfs/internal/chunk"
)

// seekThresholdBytes is the gap (between previous read end and new
// read start) that we treat as a non-sequential seek. Below the
// threshold, prefetch stays on; at or above it, the handle calls
// Reader.ReadAtKnownNoPrefetch so the prior in-flight prefetches
// get cancelled (freeing bandwidth for the seek's foreground chunk)
// and no new prefetches get scheduled for this read.
//
// 16 MiB = four 4 MiB chunks of slack. Catches movie scrubbing
// (seeks of seconds-to-minutes worth of stream) without flagging
// the kernel's natural readahead-driven jitter (which stays
// within one or two chunks of the previous read).
const seekThresholdBytes int64 = 16 << 20

// fileHandle is the per-Open state for a regular file. It implements
// the go-fuse v2 FileHandle interfaces for Read, Write, Flush, Release,
// Fsync, and Getattr.
//
// A handle owns a chunk.Writer that buffers dirty chunks until Flush.
// Reads still go through node.backend.Reader (the read path), but if
// there are dirty bytes for the chunk being read, Writer's snapshot
// takes precedence (TODO M6 — for now reads from the same handle see
// their own writes only after Flush; concurrent processes get a
// last-flush-wins view).
type fileHandle struct {
	node *Node

	mu     sync.Mutex
	writer *chunk.Writer

	// metaCached holds the inode's invariant-during-read fields:
	// chunkSize (per-inode tier, set at first dispatch), size (file
	// length at Open time), and the sparse flag. Reader.ReadAt
	// fetches these per call by default; populating them on the
	// handle skips that SQL row when we can guarantee they're
	// current.
	//
	// dirty trips on the first Write (or truncate) through this
	// handle. While dirty, the cached values are potentially stale
	// (the writer may have extended the file or marked it sparse),
	// so Read falls back to the GetInode-per-call path. Pure-read
	// workloads (cp, md5sum, ffmpeg streaming, gcc reads) leave
	// dirty=false and get the SQL row eliminated.
	//
	// dirty is atomic because Read intentionally does NOT take h.mu
	// (reads must parallelize), while Write/Setattr set it under mu.
	chunkSize int64
	size      int64
	sparse    bool
	dirty     atomic.Bool

	// lastReadEnd is the byte offset immediately past the previous
	// Read's range, atomically updated. The next Read compares its
	// own offset to this value to detect a non-sequential seek
	// (player scrubbing a movie, jump in an editor). On seek the
	// handle calls Reader.ReadAtKnownNoPrefetch, which cancels the
	// stale prefetches and avoids scheduling new ones — both of
	// which would otherwise compete with the foreground chunk for
	// bandwidth. Initialized to -1 so the first Read always
	// prefetches normally.
	lastReadEnd atomic.Int64
}

// Implements the go-fuse v2 file-handle interfaces.
var (
	_ gofuse.FileReader   = (*fileHandle)(nil)
	_ gofuse.FileWriter   = (*fileHandle)(nil)
	_ gofuse.FileFlusher  = (*fileHandle)(nil)
	_ gofuse.FileReleaser = (*fileHandle)(nil)
	_ gofuse.FileFsyncer  = (*fileHandle)(nil)
)

// Read services a FUSE read by delegating to chunk.Reader. Dirty writes
// from the SAME handle are NOT visible to subsequent reads until Flush;
// this matches typical filesystem semantics where reads serve persisted
// data and writers explicitly flush to make their data durable.
func (h *fileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	// Seek detection: compare this read's start to the previous
	// read's end. A big gap (forward or backward) means the player /
	// editor scrubbed — the prefetcher's prior queue is now junk and
	// should be cancelled. -1 means "never read" (first Read after
	// Open), which we treat as sequential so initial reads prefetch.
	prevEnd := h.lastReadEnd.Load()
	seek := false
	if prevEnd >= 0 {
		gap := off - prevEnd
		if gap < 0 {
			gap = -gap
		}
		seek = gap > seekThresholdBytes
	}

	// Fast path: use cached chunkSize + size from Open time, skipping
	// Reader's per-call GetInode SQL row. Only safe when this handle
	// hasn't been written to — a dirty handle's size may have moved
	// past the cached value, and the fall-back GetInode preserves
	// correctness in that case.
	var (
		n   int
		err error
	)
	clean := h.chunkSize > 0 && !h.dirty.Load()
	switch {
	case seek && clean:
		// Seek path: cancel the stale prefetches from the old
		// position and fire a LARGER prefetch burst at the new
		// position. Critical for video players — mpv after a seek
		// reads 5-15 chunks looking for the next keyframe + filling
		// its buffer; without a wide prefetch each one pays the
		// full per-RPC latency serially.
		n, err = h.node.backend.Reader.ReadAtKnownSeek(ctx, h.node.ino, dest, off, h.chunkSize, h.size, h.sparse)
	case seek:
		// Dirty / cold handle on a seek: same idea but no chunkSize
		// hint to pass — Reader does its own GetInode.
		n, err = h.node.backend.Reader.ReadAtKnownSeek(ctx, h.node.ino, dest, off, 0, 0, false)
	case clean:
		n, err = h.node.backend.Reader.ReadAtKnown(ctx, h.node.ino, dest, off, h.chunkSize, h.size, h.sparse)
	default:
		n, err = h.node.backend.Reader.ReadAt(ctx, h.node.ino, dest, off)
	}
	if err != nil {
		if interrupted(ctx, err) {
			return nil, syscall.EINTR
		}
		log.Printf("fs.handle.Read(ino=%d, off=%d, len=%d): %v", h.node.ino, off, len(dest), err)
		return nil, syscall.EIO
	}
	// Record the END of this read so the next call can detect a seek.
	// Use the requested length, not n, so a short-at-EOF read still
	// counts as sequential continuation when the player then reads
	// the next file region.
	h.lastReadEnd.Store(off + int64(len(dest)))
	return fuse.ReadResultData(dest[:n]), 0
}

// Write copies data into the file at off via the chunk.Writer.
func (h *fileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.writer == nil {
		// Read-only handle — the kernel enforces the access mode, so
		// this only fires on a protocol violation.
		return 0, syscall.EBADF
	}
	n, err := h.writer.WriteAt(ctx, data, off)
	if err != nil {
		if interrupted(ctx, err) {
			return uint32(n), syscall.EINTR
		}
		log.Printf("fs.handle.Write(ino=%d, off=%d, len=%d): wrote %d, err=%v", h.node.ino, off, len(data), n, err)
		return uint32(n), syscall.EIO
	}
	// Trip the dirty flag so subsequent Reads on this handle fall
	// back to the GetInode-per-call path (the cached size is
	// potentially stale after a write extended the file).
	h.dirty.Store(true)
	return uint32(n), 0
}

// markDirty invalidates the handle's cached inode hints (size,
// chunkSize, sparse) — subsequent Reads re-fetch the inode. Called by
// Setattr after a truncate through this handle.
func (h *fileHandle) markDirty() {
	h.dirty.Store(true)
}

// Flush uploads all dirty chunks and updates chunk_map. FUSE invokes
// Flush at close(2) of each fd that referenced this file; we want
// writes to be durable when close returns.
func (h *fileHandle) Flush(ctx context.Context) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.writer == nil {
		return 0 // read-only handle: nothing buffered
	}
	// WithoutCancel: FLUSH is issued from close(2), which userspace
	// CANNOT retry — the kernel doesn't restart an interrupted FLUSH,
	// and the fd is gone either way, so aborting on a signal would
	// strand dirty chunks behind a spurious EINTR from close(). The
	// flush work is local (stage to pending/ + dispatch within the
	// staging allowance) and must simply complete; a gate wait during
	// unmount is still unblocked by the pool's own shutdown ctx.
	if err := h.writer.Flush(context.WithoutCancel(ctx)); err != nil {
		if interrupted(ctx, err) {
			return syscall.EINTR // pool shutdown mid-teardown
		}
		log.Printf("fs.handle.Flush(ino=%d): %v", h.node.ino, err)
		return syscall.EIO
	}
	return 0
}

// Fsync is the durability checkpoint: dispatch + WAIT for the round
// trips before returning. Flush above is async by design so close(2)
// returns fast; apps that need close-implies-durable must fsync, and
// this path honors that.
func (h *fileHandle) Fsync(ctx context.Context, _ uint32) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.writer == nil {
		return 0 // read-only handle: nothing to make durable
	}
	if err := h.writer.Sync(ctx); err != nil {
		if interrupted(ctx, err) {
			return syscall.EINTR
		}
		log.Printf("fs.handle.Fsync(ino=%d): %v", h.node.ino, err)
		return syscall.EIO
	}
	return 0
}

// Release tears down the in-memory state. By the time FUSE calls
// Release, all flushes that callers cared about should have happened
// via Flush.
func (h *fileHandle) Release(_ context.Context) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.writer != nil {
		h.writer.Close()
	}
	return 0
}
