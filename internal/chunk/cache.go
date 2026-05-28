package chunk

import (
	"container/list"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
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
type Cache struct {
	dir      string
	capBytes int64
	fetcher  Fetcher

	mu        sync.Mutex
	entries   map[Key]*entry
	order     *list.List // front = MRU, back = LRU
	totalSize int64
}

// NewCache creates (or reopens) a Cache rooted at dir. The directory is
// created if missing. On startup we start with an empty in-memory LRU;
// any existing files on disk are NOT indexed in v1 (they'll be silently
// overwritten on re-fetch, or evicted naturally as new chunks are added).
//
// TODO(M6): scan dir on startup and seed the LRU from existing files so
// chunk caches survive daemon restarts.
func NewCache(dir string, capBytes int64, fetcher Fetcher) (*Cache, error) {
	if capBytes <= 0 {
		capBytes = DefaultCacheCapBytes
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("chunk: mkdir cache %s: %w", dir, err)
	}
	return &Cache{
		dir:      dir,
		capBytes: capBytes,
		fetcher:  fetcher,
		entries:  make(map[Key]*entry),
		order:    list.New(),
	}, nil
}

// Dir returns the on-disk cache directory.
func (c *Cache) Dir() string { return c.dir }

// CapBytes returns the configured cap.
func (c *Cache) CapBytes() int64 { return c.capBytes }

// Get returns the bytes for the chunk identified by key. On a cache hit,
// the file is read from disk and the LRU position is bumped. On miss, we
// call the Fetcher, write the result to disk, and insert into the LRU
// (evicting older entries if needed).
func (c *Cache) Get(ctx context.Context, key Key, tgMessageID int64) ([]byte, error) {
	// Hit path: bump LRU under lock; then read off disk outside lock.
	c.mu.Lock()
	if e, ok := c.entries[key]; ok {
		c.order.MoveToFront(e.el)
		c.mu.Unlock()
		return os.ReadFile(filepath.Join(c.dir, key.fileName()))
	}
	c.mu.Unlock()

	// Miss path: fetch outside the lock.
	data, err := c.fetcher.Fetch(ctx, key, tgMessageID)
	if err != nil {
		return nil, fmt.Errorf("fetch chunk %d/%d (msg=%d): %w", key.Ino, key.Idx, tgMessageID, err)
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
