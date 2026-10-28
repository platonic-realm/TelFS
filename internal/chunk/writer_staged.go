package chunk

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"telfs/internal/meta"
	"telfs/internal/metrics"
)

// queueMode reports whether this writer stages through the disk-backed
// queue (spec v1, row 9): the pool has a staging directory and a gate.
func (w *Writer) queueMode() bool {
	return w.pool != nil && w.pool.Disk != nil && w.pool.PendingDir != ""
}

// uploadOneStaged is uploadOne in queue mode. Stage 1 runs here, in the
// dispatch goroutine, while the chunk still holds its in-memory allowance:
// hash, dedup, encode, reserve disk bytes, write the wire to the staging
// directory. A failure in stage 1 puts the chunk back into the dirty set
// (its data is still in memory), exactly as uploadOne does. Once the wire
// is on disk the goroutine returns — its memory and allowance go with it —
// and stage 2 runs from the queue (direct) or the packer (small chunks),
// both reading the wire back from disk and retrying failures from it for
// as long as the daemon runs. finish (once) clears the in-flight state,
// wakes loadForWrite and releases the reservation.
func (w *Writer) uploadOneStaged(ctx context.Context, idx int32, dc *dirtyChunk, doneCh chan struct{}, res *Reservation) {
	var once sync.Once
	finish := func() {
		once.Do(func() {
			w.mu.Lock()
			delete(w.uploading, idx)
			w.mu.Unlock()
			close(doneCh)
			res.Release()
			w.uploadWg.Done()
		})
	}
	staged := false
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[writer] staged upload PANIC ino=%d idx=%d: %v\n", w.ino, idx, r)
			w.recordLost(fmt.Errorf("staged upload panic ino=%d idx=%d: %v", w.ino, idx, r))
			if !staged {
				finish()
			}
		}
	}()
	// restore: the chunk never reached the staging directory.
	restore := func(err error) {
		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			w.recordLost(err) // detached and never staged: genuinely lost
			finish()
			return
		}
		if _, hasNew := w.dirty[idx]; !hasNew {
			w.dirty[idx] = dc
			w.dirtyOrder = append([]int32{idx}, w.dirtyOrder...)
			w.dirtyBytes += int64(len(dc.data))
		}
		if errors.Is(err, ErrStagingFull) && w.uploadErr == nil {
			w.uploadErr = err // sticky: WriteAt/Flush report ENOSPC
		}
		w.mu.Unlock()
		w.recordTransient(err)
		finish()
	}

	hash := w.wc.Hash(dc.data)
	packRoute := w.packer != nil && int32(len(dc.data)) < w.packer.Threshold()
	key := Key{Ino: w.ino, Idx: idx}
	if hash != nil {
		reused, _, _, derr := w.meta.ReuseChunkByHash(context.Background(), w.ino, idx, hash)
		if derr != nil {
			restore(fmt.Errorf("dedup lookup %d: %w", idx, derr))
			return
		}
		if reused {
			w.cache.Invalidate(key)
			metrics.DedupHits.Inc()
			finish()
			return
		}
	}
	w.mu.Lock()
	fileSize := w.size
	w.mu.Unlock()
	wire, codecUsed, err := w.wc.Encode(w.ino, idx, dc.data, hash)
	if err != nil {
		restore(err)
		return
	}
	var rowHash []byte
	if w.wc.Format() == 2 {
		rowHash = hash
	}
	plainLen := int32(len(dc.data))
	wireLen := int64(len(wire))
	// Reserve the disk bytes BEFORE writing: above the cap this blocks
	// (holding the in-memory allowance, so the writer's dispatch blocks
	// once that is full too — the backpressure that slows a 400 GB rsync
	// to upload speed instead of filling the disk).
	if err := w.pool.Disk.Acquire(ctx, wireLen); err != nil {
		restore(fmt.Errorf("stage chunk %d: %w", idx, err))
		return
	}
	path, perr := PersistPendingHashed(w.pool.PendingDir, w.ino, idx, plainLen, rowHash, wire)
	if perr != nil {
		w.pool.Disk.Release(wireLen)
		restore(fmt.Errorf("persist pending chunk %d: %w", idx, perr))
		return
	}
	if old, ok := w.pool.PendingIndex.Lookup(w.ino, idx); ok && old != path {
		w.pool.Disk.Remove(old) // a previous generation of this slot
	}
	w.pool.PendingIndex.RecordPersist(w.ino, idx, path)
	staged = true // stage 2 owns finish from here on

	if packRoute {
		w.packer.SubmitStaged(w.ino, idx, plainLen, wireLen, path, rowHash, codecUsed, fileSize, func(packResult) {
			w.cache.Invalidate(key)
			finish()
		})
		return
	}
	info := ChunkDocInfo{Ino: w.ino, Idx: idx, Hash: rowHash, Len: int64(plainLen), Codec: codecUsed, FileSize: fileSize}
	w.pool.EnqueueStaged(func(ctx context.Context) bool {
		return w.uploadStagedDirect(ctx, path, plainLen, rowHash, hash, info, key, finish)
	})
}

// uploadStagedDirect is one attempt of a staged direct upload (a queue job):
// true = finished (uploaded, or dropped because the file is gone), false =
// retry later from the staged wire.
func (w *Writer) uploadStagedDirect(ctx context.Context, path string, plainLen int32, rowHash, hash []byte, info ChunkDocInfo, key Key, finish func()) bool {
	unrecord := func() {
		if p, ok := w.pool.PendingIndex.Lookup(key.Ino, key.Idx); ok && p == path {
			w.pool.PendingIndex.UnrecordPersist(key.Ino, key.Idx)
		}
	}
	wire, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		unrecord() // removed under us: the inode was unlinked, or a newer generation replaced it
		finish()
		return true
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "[writer] staged chunk ino=%d idx=%d: read %s: %v — will retry\n", key.Ino, key.Idx, path, err)
		return false
	}
	name, caption, err := w.wc.ChunkDoc(ctx, info)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[writer] staged chunk ino=%d idx=%d: caption: %v — will retry\n", key.Ino, key.Idx, err)
		return false
	}
	msgID, err := uploadWithRetry(ctx, w.uploader, wire, name, caption)
	if err != nil {
		metrics.UploadErrors.Inc()
		if ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "[writer] staged chunk ino=%d idx=%d: upload: %v — will retry from the staged copy\n", key.Ino, key.Idx, err)
		}
		return false
	}
	metrics.ChunksUploaded.Inc()
	metrics.BytesUploaded.Add(int64(len(wire)))
	if err := w.meta.PutChunk(context.Background(), meta.Chunk{
		Ino: key.Ino, Idx: key.Idx, TGMessageID: int64(msgID), Size: plainLen, Hash: rowHash,
	}); err != nil {
		if meta.IsFKViolation(err) {
			w.pool.Disk.Remove(path)
			unrecord()
			fmt.Fprintf(os.Stderr, "[writer] dropped orphan chunk ino=%d/%d (inode gone, msg %d kept for gc)\n", key.Ino, key.Idx, msgID)
			finish()
			return true
		}
		// On the channel but without a row: upload again later (this
		// message becomes gc fodder).
		fmt.Fprintf(os.Stderr, "[writer] staged chunk ino=%d idx=%d: chunk_map: %v — will retry\n", key.Ino, key.Idx, err)
		return false
	}
	w.pool.Disk.Remove(path)
	unrecord()
	if hash != nil {
		if err := w.meta.RecordChunkBlob(context.Background(), hash, int64(msgID), plainLen); err != nil {
			fmt.Fprintf(os.Stderr, "[writer] record blob index ino=%d idx=%d: %v\n", key.Ino, key.Idx, err)
		}
	}
	w.cache.Invalidate(key)
	finish()
	return true
}
