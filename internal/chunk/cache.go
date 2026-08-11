package chunk

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

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
//
// Fetch downloads the entire message. Most callers should prefer
// RangedFetcher when one is available — for packed chunks where many
// rows share a TG message, fetching just the relevant extent avoids
// downloading the entire blob on every cache miss.
type Fetcher interface {
	Fetch(ctx context.Context, key Key, tgMessageID int64) ([]byte, error)
}

// RangedFetcher is the optional ranged variant. Cache type-asserts
// for this on every miss; if absent, falls back to whole-message
// Fetch + client-side slice. Implementations include tg.TGFetcher;
// fake fetchers in tests may omit it.
type RangedFetcher interface {
	FetchRange(ctx context.Context, key Key, tgMessageID int64, offset, length int64) ([]byte, error)
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
	dir      string
	capBytes int64
	fetcher  Fetcher
	cipher   crypto.Cipher  // never nil; defaults to NoopCipher
	codec    compress.Codec // never nil; defaults to compress.None

	mu        sync.Mutex
	entries   map[Key]*entry
	order     *list.List // front = MRU, back = LRU
	totalSize int64

	// misses merges concurrent Get calls for the same Key onto a
	// single miss-path execution. Without it, a FUSE Read goroutine
	// and the Reader's prefetcher can race for the same chunk —
	// each downloads + decrypts + decompresses + writes-to-disk in
	// parallel before one of them wins the LRU insert race. The
	// in-flight check at LRU-insert time only deduped the LRU
	// entry; the network and CPU work was wasted.
	misses singleflight.Group

	// mem is an optional in-RAM hot tier sitting in front of disk.
	// When set, Cache.Get checks RAM first and only falls through
	// to os.ReadFile on miss; successful disk reads populate RAM
	// so the next access pays one map lookup, no syscall. Nil
	// disables the tier; disk-only behavior is preserved.
	mem *MemCache
}

// SetMemCache installs the optional in-RAM hot tier. Pass nil to
// disable. Idempotent; safe to call before or after concurrent
// reads have started (the swap is a single pointer write — Go's
// memory model makes the pointer load safe even without atomics
// because callers that race a SetMemCache will either see the
// old (nil/old MemCache) or new value, both of which are safe).
func (c *Cache) SetMemCache(m *MemCache) {
	c.mem = m
}

// MemCache returns the installed in-RAM tier, or nil if disabled.
// Used by metrics so the /cache JSON can report size + cap.
func (c *Cache) MemCache() *MemCache {
	return c.mem
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
	// Tier 0: in-RAM hot cache, if enabled. One map lookup; no
	// syscall on hit. Skipped if no mem tier is installed.
	if data, ok := c.mem.Get(key); ok {
		metrics.CacheHits.Inc()
		metrics.MemCacheHits.Inc()
		return data, nil
	}
	// The retry loop covers two races, both of which used to surface
	// as spurious EIO to userspace:
	//   1. Hit-path ENOENT: the LRU said hit but the file vanished
	//      between the bump and the ReadFile (Invalidate or eviction
	//      racing us) — fall through to the miss path and refetch.
	//   2. Singleflight cancellation poisoning: our Do call can
	//      piggyback on a fetch started by a PREFETCH goroutine whose
	//      per-inode context a seek then cancels. The shared result is
	//      "context canceled" even though OUR ctx is alive — retry
	//      with our own ctx driving a fresh fetch.
	for attempt := 0; ; attempt++ {
		// Hit path: bump LRU under lock; then read off disk outside lock.
		c.mu.Lock()
		if e, ok := c.entries[key]; ok {
			c.order.MoveToFront(e.el)
			c.mu.Unlock()
			data, err := os.ReadFile(filepath.Join(c.dir, key.fileName()))
			if err == nil {
				metrics.CacheHits.Inc()
				// Promote into the mem tier so the next access is a map
				// lookup, not a syscall. The mem cache evicts itself once
				// it crosses its own cap.
				c.mem.Put(key, data)
				return data, nil
			}
			if !os.IsNotExist(err) {
				return nil, err
			}
			// Stale LRU entry — drop it and treat as a miss.
			c.mu.Lock()
			if e2, ok := c.entries[key]; ok {
				c.order.Remove(e2.el)
				delete(c.entries, key)
				c.totalSize -= e2.size
			}
			c.mu.Unlock()
		} else {
			c.mu.Unlock()
		}
		metrics.CacheMisses.Inc()

		// Miss path. Funnel through singleflight so two callers asking
		// for the same key share one fetch + decrypt + decompress + disk
		// write. singleflight keys are strings; the chunk Key has a fixed
		// integer shape, so a short binary-ish encoding is fine.
		sfKey := fmt.Sprintf("%d/%d", key.Ino, key.Idx)
		v, err, shared := c.misses.Do(sfKey, func() (any, error) {
			// Recheck the cache once we've serialized: a parallel miss may
			// have populated it while we were queued.
			c.mu.Lock()
			if e, ok := c.entries[key]; ok {
				c.order.MoveToFront(e.el)
				c.mu.Unlock()
				data, rerr := os.ReadFile(filepath.Join(c.dir, key.fileName()))
				if rerr == nil {
					return data, nil
				}
				if !os.IsNotExist(rerr) {
					return nil, rerr
				}
				// Stale entry here too — fall through to a real fetch.
				c.mu.Lock()
				if e2, ok := c.entries[key]; ok {
					c.order.Remove(e2.el)
					delete(c.entries, key)
					c.totalSize -= e2.size
				}
				c.mu.Unlock()
				return c.fetchAndStore(ctx, key, tgMessageID, packOffset, packSize)
			}
			c.mu.Unlock()
			return c.fetchAndStore(ctx, key, tgMessageID, packOffset, packSize)
		})
		if err != nil {
			if errors.Is(err, context.Canceled) && ctx.Err() == nil && attempt < 3 {
				continue // foreign cancellation — retry with our live ctx
			}
			// A dying MTProto engine fails every in-flight download at
			// once ("connection dead" / "engine was closed") and then
			// reconnects within a few seconds. Absorb it: pause briefly
			// so the reconnect can land, then refetch. Without this,
			// each transport blip during a sustained cold-read pass
			// surfaced as EIO for whichever file was mid-fetch —
			// observed twice in 20 min during the 6 GB verify run.
			if isTransientConnErr(err) && ctx.Err() == nil && attempt < 3 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(2 * time.Second):
				}
				continue
			}
			return nil, err
		}
		if shared {
			metrics.CacheMergedMisses.Inc()
		}
		data, ok := v.([]byte)
		if !ok {
			return nil, fmt.Errorf("cache.Get singleflight returned %T, want []byte", v)
		}
		return data, nil
	}
}

// fetchAndStore is the actual miss path: download (range or whole),
// slice if packed, decrypt, decompress, persist to disk, insert into
// the LRU. Split out of Get so singleflight can wrap it cleanly.
func (c *Cache) fetchAndStore(ctx context.Context, key Key, tgMessageID int64, packOffset int64, packSize int32) ([]byte, error) {
	// Fetch. For packed chunks we prefer a RangedFetcher (one
	// upload.getFile per ~1 MiB) so we don't download the entire
	// shared blob just to access one extent.
	var fetched []byte
	if packSize > 0 {
		if rf, ok := c.fetcher.(RangedFetcher); ok {
			b, err := rf.FetchRange(ctx, key, tgMessageID, packOffset, int64(packSize))
			if err != nil {
				return nil, fmt.Errorf("ranged fetch chunk %d/%d (msg=%d): %w", key.Ino, key.Idx, tgMessageID, err)
			}
			fetched = b
		} else {
			b, err := c.fetcher.Fetch(ctx, key, tgMessageID)
			if err != nil {
				return nil, fmt.Errorf("fetch chunk %d/%d (msg=%d): %w", key.Ino, key.Idx, tgMessageID, err)
			}
			fetched = b
		}
	} else {
		b, err := c.fetcher.Fetch(ctx, key, tgMessageID)
		if err != nil {
			return nil, fmt.Errorf("fetch chunk %d/%d (msg=%d): %w", key.Ino, key.Idx, tgMessageID, err)
		}
		fetched = b
	}
	metrics.ChunksDownloaded.Inc()
	metrics.BytesDownloaded.Add(int64(len(fetched)))

	// Slice if packed. When we used the ranged fetcher above,
	// `fetched` already contains exactly the extent's bytes (offset 0
	// in the buffer), so packBase stays 0.
	var packBase int64
	if packSize > 0 {
		if _, ranged := c.fetcher.(RangedFetcher); !ranged {
			packBase = packOffset
		}
	}
	var ciphertext []byte
	if packSize > 0 {
		sliceStart := packBase
		sliceEnd := packBase + int64(packSize)
		if sliceEnd > int64(len(fetched)) {
			return nil, fmt.Errorf("packed extent out of range for chunk %d/%d: buf=%d bytes, want [%d:%d]",
				key.Ino, key.Idx, len(fetched), sliceStart, sliceEnd)
		}
		ciphertext = fetched[sliceStart:sliceEnd]
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
	if existing, ok := c.entries[key]; ok {
		c.order.MoveToFront(existing.el)
		return data, nil
	}
	e := &entry{key: key, size: int64(len(data))}
	e.el = c.order.PushFront(e)
	c.entries[key] = e
	c.totalSize += e.size
	c.evictLocked()
	// Populate the mem tier with the freshly-decoded plaintext so
	// the immediate next read for this key is a one-lookup hit.
	c.mem.Put(key, data)
	return data, nil
}

// isTransientConnErr recognizes fetch failures caused by an MTProto
// engine death / reconnect window — conditions that heal within
// seconds and deserve one in-place retry instead of an EIO to the
// reading application. String-matched because the errors arrive
// wrapped through gotd's layers without stable sentinel types.
func isTransientConnErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	for _, marker := range []string{
		"connection dead",
		"engine was closed",
		"engine forcibly closed",
		"session not connected",
	} {
		if strings.Contains(s, marker) {
			return true
		}
	}
	return false
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

// Invalidate removes a chunk from the cache (RAM tier + LRU + on
// disk). Used after a chunk overwrite so subsequent reads don't
// return stale data. Returns true if the chunk was actually in the
// disk LRU.
func (c *Cache) Invalidate(key Key) bool {
	c.mem.Invalidate(key)
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
