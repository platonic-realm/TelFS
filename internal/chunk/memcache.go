package chunk

import (
	"container/list"
	"sync"
)

// DefaultMemCacheCapBytes is the in-RAM hot-tier cache size. 128 MiB
// is 32 chunks at the default 4 MiB tier — enough to hold a small
// working set (a build's includes, a video's recent key frames)
// across re-reads without paying os.ReadFile on every hit.
const DefaultMemCacheCapBytes int64 = 128 << 20 // 128 MiB

// MemCache is a small in-memory LRU sitting in front of the disk
// LRU cache. Disk cache hits still pay an os.ReadFile per chunk,
// which adds up when the same chunks are touched in tight loops
// (a build re-reading the same headers, ffmpeg re-seeking within a
// keyframe). The mem tier intercepts those hits with a map lookup +
// slice return.
//
// Concurrency: a single mu serializes the LRU mutation; readers
// can copy the byte slice (it's plaintext — the same buffer the
// outer Cache.Get would have returned).
//
// Eviction is strict: PushFront a new entry, then drop from the
// back while totalSize > capBytes. No size = 0 entries.
type MemCache struct {
	mu        sync.Mutex
	capBytes  int64
	entries   map[Key]*memEntry
	order     *list.List // front = MRU, back = LRU
	totalSize int64
}

type memEntry struct {
	key  Key
	data []byte
	el   *list.Element
}

// NewMemCache constructs a MemCache with capBytes. Zero/negative
// capBytes falls back to DefaultMemCacheCapBytes.
func NewMemCache(capBytes int64) *MemCache {
	if capBytes <= 0 {
		capBytes = DefaultMemCacheCapBytes
	}
	return &MemCache{
		capBytes: capBytes,
		entries:  make(map[Key]*memEntry),
		order:    list.New(),
	}
}

// CapBytes returns the configured cap.
func (m *MemCache) CapBytes() int64 {
	if m == nil {
		return 0
	}
	return m.capBytes
}

// SizeBytes returns the current footprint.
func (m *MemCache) SizeBytes() int64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.totalSize
}

// Get returns the cached bytes and bumps the LRU position. The
// returned slice is the cache's own backing buffer — the caller
// MUST NOT mutate it. (Cache consumers don't; they slice for
// FUSE Read's copy into the kernel buffer.)
func (m *MemCache) Get(key Key) ([]byte, bool) {
	if m == nil {
		return nil, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[key]
	if !ok {
		return nil, false
	}
	m.order.MoveToFront(e.el)
	return e.data, true
}

// Put inserts (or refreshes) an entry. Evicts oldest entries until
// total size <= cap. Caller must not mutate the slice afterwards
// (the cache stores it by reference — copying for every Put would
// double allocation cost for large chunks).
func (m *MemCache) Put(key Key, data []byte) {
	if m == nil || len(data) == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.entries[key]; ok {
		m.totalSize -= int64(len(e.data))
		e.data = data
		m.totalSize += int64(len(data))
		m.order.MoveToFront(e.el)
	} else {
		e := &memEntry{key: key, data: data}
		e.el = m.order.PushFront(e)
		m.entries[key] = e
		m.totalSize += int64(len(data))
	}
	for m.totalSize > m.capBytes && m.order.Len() > 0 {
		oldest := m.order.Back()
		ev := oldest.Value.(*memEntry)
		m.order.Remove(oldest)
		delete(m.entries, ev.key)
		m.totalSize -= int64(len(ev.data))
	}
}

// Invalidate drops the entry for key, if present. Returns true if
// something was removed. Called by Cache.Invalidate so a chunk
// overwrite doesn't leave stale plaintext in the mem tier.
func (m *MemCache) Invalidate(key Key) bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[key]
	if !ok {
		return false
	}
	m.order.Remove(e.el)
	delete(m.entries, key)
	m.totalSize -= int64(len(e.data))
	return true
}
