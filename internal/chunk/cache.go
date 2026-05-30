package chunk

import (
	"container/list"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"telfs/internal/compress"
	"telfs/internal/crypto"
	"telfs/internal/metrics"
)

// ChunkSize is TelFS's fixed chunk size in bytes (4 MiB). Every file is
// split into chunks of this size; only the last chunk may be smaller.
const ChunkSize int64 = 4 << 20

// DefaultCacheCapBytes is the default LRU cache capacity in bytes.
const DefaultCacheCapBytes int64 = 1 << 30 // 1 GiB

// Key identifies a chunk slot. Two files never collide because Ino is
// globally unique within the SQLite metadata store.
type Key struct {
	Ino int64
	Idx int32
}

func (k Key) fileName() string {
	return fmt.Sprintf("%d-%d.bin", k.Ino, k.Idx)
}

// Fetcher is the upstream from which the Cache pulls chunks on miss.
// Implementations are typically backed by tg.Session in production and a
// fake in tests.
type Fetcher interface {
	Fetch(ctx context.Context, key Key, tgMessageID int64) ([]byte, error)
}

type entry struct {
	key  Key
	size int64
	el   *list.Element // self-reference into the LRU list
}

// Cache is a disk-backed LRU of chunk payloads. Chunk data lives on disk
// at <dir>/<ino>-<idx>.bin; in-memory state is just the key/size/order
// metadata. Eviction removes the on-disk file too.
//
// Safe for concurrent use. The LRU mutation is serialized by mu; the disk
// I/O happens outside the lock so a slow fetch doesn't block readers.
//
// The cache holds PLAINTEXT bytes on disk and in returns from Get. On a
// miss, the upstream Fetcher returns ciphertext; Cache decrypts and
// decompresses (in that order — compression happens before encryption
// on the write side) before persisting, so cache hits skip both costs.
type Cache struct {
	dir       string
	capBytes  int64
	fetcher   Fetcher
	cipher    crypto.Cipher    // never nil; defaults to NoopCipher
	codec     compress.Codec   // never nil; defaults to compress.None

	mu        sync.Mutex
	entries   map[Key]*entry
	order     *list.List // front = MRU, back = LRU
	totalSize int64
}

// NewCache creates (or reopens) a Cache rooted at dir. The directory is
// created if missing. On startup we scan the directory and adopt any
// `<ino>-<idx>.bin` files we find as cache entries — they become hits
// on next access, so the cache survives daemon restarts. The LRU
// order of inherited files is "uniform LRU" (all at the back),
// because we can't recover the original access order; chunks fetched
// after startup move to MRU normally.
//
// cipher and codec may be nil — defaults are NoopCipher (raw bytes
// on the wire) and compress.None (no compression). When set, the
// upstream Fetcher returns ciphertext; the Cache decrypts then
// decompresses before persisting plaintext, so cache hits skip both
// costs.
func NewCache(dir string, capBytes int64, fetcher Fetcher, cipher crypto.Cipher, codec compress.Codec) (*Cache, error) {
	if capBytes <= 0 {
		capBytes = DefaultCacheCapBytes
	}
	if cipher == nil {
		cipher = crypto.NoopCipher{}
	}
	if codec == nil {
		codec = compress.None{}
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("chunk: mkdir cache %s: %w", dir, err)
	}
	c := &Cache{
		dir:      dir,
		capBytes: capBytes,
		fetcher:  fetcher,
		cipher:   cipher,
		codec:    codec,
		entries:  make(map[Key]*entry),
		order:    list.New(),
	}
	c.adoptExisting()
	return c, nil
}

// adoptExisting walks the cache directory and registers any
// `<ino>-<idx>.bin` files as cache entries. Files with unparseable
// names or zero size are skipped (a zero-byte file usually means a
// previous crash mid-write). If the inherited footprint exceeds
// capBytes, we evict immediately to bring it back in line.
func (c *Cache) adoptExisting() {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		var ino int64
		var idx int32
		if _, err := fmt.Sscanf(e.Name(), "%d-%d.bin", &ino, &idx); err != nil {
			continue
		}
		info, err := e.Info()
		if err != nil || info.Size() == 0 {
			continue
		}
		key := Key{Ino: ino, Idx: idx}
		ent := &entry{key: key, size: info.Size()}
		ent.el = c.order.PushBack(ent) // back = LRU since we don't know real recency
		c.entries[key] = ent
		c.totalSize += info.Size()
	}
	// If the inherited footprint already exceeds the cap, evict.
	c.mu.Lock()
	c.evictLocked()
	c.mu.Unlock()
}

// Dir returns the on-disk cache directory.
func (c *Cache) Dir() string { return c.dir }

// CapBytes returns the configured cap.
func (c *Cache) CapBytes() int64 { return c.capBytes }

// SizeBytes returns the current footprint on disk (sum of LRU entry
// sizes). Used by the metrics endpoint as a gauge.
func (c *Cache) SizeBytes() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.totalSize
}

// Get returns the plaintext bytes for the chunk identified by key.
// On a cache hit, the file is read from disk and the LRU position is
// bumped. On miss, we call the Fetcher, write the result to disk,
// and insert into the LRU (evicting older entries if needed).
//
// packOffset / packSize: when packSize > 0, the chunk is an EXTENT
// into a packed blob — the fetcher returns the full blob and Get
// slices [packOffset : packOffset+packSize] before decryption.
// packOffset==0 && packSize==0 means "unpacked" — Get uses the full
// fetched bytes (the historical behavior).
func (c *Cache) Get(ctx context.Context, key Key, tgMessageID int64, packOffset int64, packSize int32) ([]byte, error) {
	// Hit path: bump LRU under lock; then read off disk outside lock.
	c.mu.Lock()
	if e, ok := c.entries[key]; ok {
		c.order.MoveToFront(e.el)
		c.mu.Unlock()
		metrics.CacheHits.Inc()
		return os.ReadFile(filepath.Join(c.dir, key.fileName()))
	}
	c.mu.Unlock()
	metrics.CacheMisses.Inc()

	// Miss path: fetch outside the lock. Fetcher returns the bytes
	// stored in TG (ciphertext if encryption is enabled, plaintext
	// otherwise). Inverse of the write-side pipeline:
	//
	//   wire ── decrypt ──▶ compressed ── decompress ──▶ plaintext
	//
	// We persist plaintext so cache hits skip both costs.
	fetched, err := c.fetcher.Fetch(ctx, key, tgMessageID)
	if err != nil {
		return nil, fmt.Errorf("fetch chunk %d/%d (msg=%d): %w", key.Ino, key.Idx, tgMessageID, err)
	}
	metrics.ChunksDownloaded.Inc()
	metrics.BytesDownloaded.Add(int64(len(fetched)))
	// Slice if packed: the fetched blob holds many chunks back-to-back.
	// packSize > 0 marks this chunk as a sub-extent. packOffset == 0 is
	// valid (start-of-blob); packSize is the load-bearing signal.
	var ciphertext []byte
	if packSize > 0 {
		end := packOffset + int64(packSize)
		if end > int64(len(fetched)) {
			return nil, fmt.Errorf("packed extent out of range for chunk %d/%d: blob=%d bytes, want [%d:%d]",
				key.Ino, key.Idx, len(fetched), packOffset, end)
		}
		ciphertext = fetched[packOffset:end]
	} else {
		ciphertext = fetched
	}
	compressed, err := c.cipher.Open(key.Ino, key.Idx, ciphertext)
	if err != nil {
		return nil, fmt.Errorf("decrypt chunk %d/%d: %w", key.Ino, key.Idx, err)
	}
	data, err := c.codec.Decompress(compressed)
	if err != nil {
		return nil, fmt.Errorf("decompress chunk %d/%d: %w", key.Ino, key.Idx, err)
	}
	// Persist to disk, then insert into the LRU.
	path := filepath.Join(c.dir, key.fileName())
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return nil, fmt.Errorf("cache write %s: %w", path, err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// A concurrent miss could have inserted the same key; in that case
	// just drop our duplicate write attempt and use what's there.
	if existing, ok := c.entries[key]; ok {
		c.order.MoveToFront(existing.el)
		return data, nil
	}
	e := &entry{key: key, size: int64(len(data))}
	e.el = c.order.PushFront(e)
	c.entries[key] = e
	c.totalSize += e.size
	c.evictLocked()
	return data, nil
}

// Has reports whether a chunk is already present in the LRU.
// Used by the prefetcher to skip work for chunks the cache already
// has — saves both a Telegram round trip and the disk write inside
// Get's miss path.
func (c *Cache) Has(key Key) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.entries[key]
	return ok
}

// Invalidate removes a chunk from the cache (both in-memory and on
// disk). Used after a chunk overwrite so subsequent reads don't return
// stale data. Returns true if the chunk was actually in the cache.
func (c *Cache) Invalidate(key Key) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok {
		return false
	}
	c.order.Remove(e.el)
	delete(c.entries, key)
	c.totalSize -= e.size
	_ = os.Remove(filepath.Join(c.dir, key.fileName()))
	return true
}

// evictLocked drops the oldest entries until totalSize <= capBytes.
// Caller must hold c.mu.
func (c *Cache) evictLocked() {
	for c.totalSize > c.capBytes && c.order.Len() > 0 {
		oldest := c.order.Back()
		e := oldest.Value.(*entry)
		c.order.Remove(oldest)
		delete(c.entries, e.key)
		c.totalSize -= e.size
		_ = os.Remove(filepath.Join(c.dir, e.key.fileName()))
	}
}
