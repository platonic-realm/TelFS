package chunk

import (
	"context"
	"errors"
	"fmt"

	"telfs/internal/meta"
)

// Reader orchestrates random-offset reads of a TelFS file by walking
// chunk_map, consulting the disk LRU cache, and pulling missing chunks
// from Telegram via the Cache's Fetcher.
type Reader struct {
	meta      *meta.Store
	cache     *Cache
	chunkSize int64
}

// NewReader wires up the read path. chunkSize is normally ChunkSize but
// is configurable for tests.
func NewReader(m *meta.Store, c *Cache, chunkSize int64) *Reader {
	if chunkSize <= 0 {
		chunkSize = ChunkSize
	}
	return &Reader{meta: m, cache: c, chunkSize: chunkSize}
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
