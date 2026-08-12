// Package metrics is the daemon-side Prometheus exposition. It serves
// /metrics over the mount daemon's --metrics-addr listener (off by
// default — opt-in keeps the surface area of "telfs mount" small).
//
// All metric names use the telfs_ prefix and conform to Prometheus
// best practices (counters end in _total, gauges are bare nouns).
// The text format is hand-rolled so no external client_golang dep is
// pulled in for what is, in the end, a few dozen integer reads.
package metrics

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
)

// Counter is a monotonically increasing integer. Safe for concurrent
// use. Wrapper exists so callers don't have to know the atomic op.
type Counter struct {
	v atomic.Int64
}

// Inc bumps the counter by one.
func (c *Counter) Inc() { c.v.Add(1) }

// Add bumps the counter by n. Negative n is allowed but discouraged —
// counter semantics are monotone in Prometheus.
func (c *Counter) Add(n int64) { c.v.Add(n) }

// Load returns the current value.
func (c *Counter) Load() int64 { return c.v.Load() }

// GaugeFunc is the closure form of a gauge — the value is read on
// every scrape, so callers don't need to remember to Set. Used for
// quantities like "current pending uploads" that are derived from
// live state rather than tracked separately.
type GaugeFunc func() int64

// Registry holds the metric set served at /metrics. Counter and
// gauge sets are registered via Add* helpers; the default singleton
// is metrics.Default.
type Registry struct {
	mu       sync.RWMutex
	counters map[string]*counterEntry
	gauges   map[string]*gaugeEntry
}

type counterEntry struct {
	help    string
	counter *Counter
}

type gaugeEntry struct {
	help string
	fn   GaugeFunc
}

// NewRegistry constructs an empty registry. Tests prefer their own;
// production wiring uses metrics.Default.
func NewRegistry() *Registry {
	return &Registry{
		counters: make(map[string]*counterEntry),
		gauges:   make(map[string]*gaugeEntry),
	}
}

// AddCounter registers a Counter under the given name and HELP text.
// Returns the Counter so the caller can hold a reference and Inc/Add
// without going through the registry on every event. Re-registering
// the same name returns the existing Counter — idempotent on init.
func (r *Registry) AddCounter(name, help string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.counters[name]; ok {
		return existing.counter
	}
	c := &Counter{}
	r.counters[name] = &counterEntry{help: help, counter: c}
	return c
}

// AddGauge registers a GaugeFunc under the given name. Subsequent
// registrations of the same name overwrite — useful when the
// daemon rebuilds its pool/cache references during recovery.
func (r *Registry) AddGauge(name, help string, fn GaugeFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gauges[name] = &gaugeEntry{help: help, fn: fn}
}

// WriteProm renders the registry in Prometheus text exposition
// format. Sorts by name so scrapes are stable for diffing.
func (r *Registry) WriteProm(w io.Writer) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.counters)+len(r.gauges))
	for n := range r.counters {
		names = append(names, n)
	}
	for n := range r.gauges {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		if c, ok := r.counters[name]; ok {
			if _, err := fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, c.help, name, name, c.counter.Load()); err != nil {
				return err
			}
		}
		if g, ok := r.gauges[name]; ok {
			if _, err := fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %d\n", name, g.help, name, name, g.fn()); err != nil {
				return err
			}
		}
	}
	return nil
}

// Default is the process-wide registry. Subpackages use these
// counters directly so we don't have to thread a *Registry through
// every constructor.
var Default = NewRegistry()

// Exported counters, registered eagerly at package init so consumers
// can take their addresses without first calling AddCounter from a
// startup hook. The TYPE / HELP strings here are what the scrape
// endpoint exposes.
var (
	ChunksUploaded    = Default.AddCounter("telfs_chunks_uploaded_total", "Chunks successfully uploaded to the channel.")
	ChunksDownloaded  = Default.AddCounter("telfs_chunks_downloaded_total", "Chunks fetched from the channel on cache miss.")
	DedupHits         = Default.AddCounter("telfs_dedup_hits_total", "Plaintext-hash dedup hits avoided one upload.")
	CacheHits         = Default.AddCounter("telfs_cache_hits_total", "Local LRU cache hits.")
	CacheMisses       = Default.AddCounter("telfs_cache_misses_total", "Local LRU cache misses (forced a fetch).")
	CacheMergedMisses = Default.AddCounter("telfs_cache_merged_misses_total", "Cache misses that piggy-backed on an already-in-flight fetch for the same chunk (singleflight wins).")
	MemCacheHits      = Default.AddCounter("telfs_memcache_hits_total", "In-RAM hot-tier cache hits — served without an os.ReadFile.")
	FloodWaits        = Default.AddCounter("telfs_flood_waits_total", "FLOOD_WAIT responses observed and slept through.")
	UploadErrors      = Default.AddCounter("telfs_upload_errors_total", "Chunk uploads that returned a non-FLOOD_WAIT error.")
	BytesUploaded     = Default.AddCounter("telfs_bytes_uploaded_total", "Bytes (post-compression, post-encryption) sent to the channel.")
	BytesDownloaded   = Default.AddCounter("telfs_bytes_downloaded_total", "Bytes (post-compression, post-encryption) fetched from the channel.")

	BlobPrefetchSpans   = Default.AddCounter("telfs_blob_prefetch_spans_total", "Packed-blob locality spans fetched (one ranged download warming many neighbor extents).")
	BlobPrefetchExtents = Default.AddCounter("telfs_blob_prefetch_extents_total", "Neighbor extents decoded and cached by blob-locality prefetch.")
)
