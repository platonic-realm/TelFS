package fs

import (
	"context"
	"sync"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"telfs/internal/chunk"
)

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
	n, err := h.node.backend.Reader.ReadAt(ctx, h.node.ino, dest, off)
	if err != nil {
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:n]), 0
}

// Write copies data into the file at off via the chunk.Writer.
func (h *fileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	h.mu.Lock()
	defer h.mu.Unlock()
	n, err := h.writer.WriteAt(ctx, data, off)
	if err != nil {
		return uint32(n), syscall.EIO
	}
	return uint32(n), 0
}

// Flush uploads all dirty chunks and updates chunk_map. FUSE invokes
// Flush at close(2) of each fd that referenced this file; we want
// writes to be durable when close returns.
func (h *fileHandle) Flush(ctx context.Context) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.writer.Flush(ctx); err != nil {
		return syscall.EIO
	}
	return 0
}

// Fsync also forces a flush; we don't distinguish between data-only and
// metadata-only fsync because both target the same backing path.
func (h *fileHandle) Fsync(ctx context.Context, _ uint32) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.writer.Flush(ctx); err != nil {
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
	h.writer.Close()
	return 0
}
