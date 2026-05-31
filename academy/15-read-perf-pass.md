# 15 · The read-performance pass

Chapter 14 closed the bugs that made reads return wrong bytes or
hang. This chapter is about making the reads *fast*.

The reading order matters: you'll get more out of this chapter if
you've read the previous one and at least skimmed chapter 6
(chunk pipeline) so the words `Cache`, `Reader`, `Fetcher`, and
`packed extent` are concrete to you.

## What we mean by "read perf"

Three workloads stand in for the rest:

1. **Sequential streaming of a large file** — `dd if=/mnt/foo
   of=/dev/null bs=1M` on a 500 MiB file that's not in the cache.
2. **Random reads across many small files** — `find linux | shuf
   | head -1000 | xargs cat > /dev/null`. Each file is one
   packed-extent fetch.
3. **Re-reads of hot chunks** — a build that re-reads the same
   headers, ffmpeg replaying a few seconds.

Before this pass, all three were bound by something other than the
network's true ceiling: serial ranged downloads, per-miss
`channels.getMessages` round trips, kernel-side 128 KiB
fragmentation. After the pass, they're bound by bandwidth (1+2) or
by the speed of a map lookup (3).

## Nine knobs we changed

Each one was small in isolation. They compound.

### 1. Singleflight on cache miss

`internal/chunk/cache.go`. Before: foreground Read and the
prefetcher could BOTH miss the same key concurrently — both
downloaded, decrypted, decompressed, wrote to disk, then raced for
the LRU slot. Now `Cache.Get` funnels misses through
`golang.org/x/sync/singleflight` keyed by `(ino, idx)`. The second
caller awaits the first's result. New counter:
`telfs_cache_merged_misses_total` shows how often this fires.

### 2. Parallel ranged fetch

`internal/tg/session.go`. `streamDocumentRange` used to loop
`upload.getFile` requests serially, 1 MiB at a time. Now it
pre-plans the parts (each still ≤ 1 MiB and never crossing a 1 MiB
boundary — see chapter 14's LIMIT_INVALID story) and fans up to
4 workers through an `errgroup`. In-order stitching happens AFTER
all workers complete, so the writer sees the parts in offset order
even though the network sees them concurrently.

The single-part case (a small slice that fits in one MiB block)
short-circuits to the serial path so we don't pay errgroup overhead
for a one-RPC read.

### 3. Document / FileReference cache

`Session.fetchDocument` used to issue one `channels.getMessages`
per cache miss to recover the `FileReference` token, even though
Telegram guarantees the token stays valid for ~30 minutes. Now a
`sync.Map` keyed by msgID memoises `*tg.Document` for 20 minutes
(headroom under the guarantee). On `FILE_REFERENCE_EXPIRED` the
entry is invalidated and the existing one-retry path refreshes.

Result: in a random-read workload, `channels.getMessages` drops to
one call per blob per 20 minutes instead of one call per chunk
read.

### 4. Bulk chunk metadata fetch

New helper `meta.GetChunksRange(ino, fromIdx, count)`. Replaces
the old per-chunk `GetChunk` calls inside `Reader.ReadAt`: at the
top of the call we now fetch the metadata for the whole
[startIdx, endIdx + prefetchWindow] range in **one SELECT** and
pass the resulting map into both `fetchOne` and
`scheduleAheadFromBatch`. For a sequential read of N chunks,
this collapses N+W lookups into one.

### 5. In-process pending-fallback index

`internal/chunk/persist.go` grew a `PendingIndex` —
`map[Key]string` populated by `PersistPending` and
`recoverPending`, drained by `DeletePending` and `DropInode`. The
Reader consults the index before falling back to a directory scan.
With 30 000 wires staged during a kernel-untar, the previous
`filepath.Glob` was ~10 ms per Reader fallback; the index makes it
a microsecond.

The disk is still the source of truth — a stale index entry just
falls through to the existing `ErrNotExist` path.

### 6. FUSE `MaxReadAhead` and `MaxBackground`

`internal/fs/mount.go`. Default `MaxReadAhead` on Linux is 128 KiB.
For a 4 MiB chunk that means the kernel issues 32 separate FUSE
read requests per chunk, each going through `Node.Read →
Reader.ReadAt` and being recoalesced. Setting it to
`chunk.ChunkSize` lets the kernel ask for whole chunks at once,
which also gives our prefetcher meaningful work to do (the
prefetcher schedules ahead by *chunks*, not by bytes). We raised
`MaxBackground` to 64 so concurrent reads and background
prefetches don't queue at the kernel-side request slot.

### 7. Prefetch knobs configurable

`internal/chunk/reader.go`. `PrefetchWindow` and
`PrefetchConcurrency` are no longer compile-time constants — they
live on the `Reader` and can be tuned at mount via
`--prefetch-window` / `--prefetch-concurrency`. We tried 8 / 4 and
the bandwidth contention on movie seeks was too high (see "Movie
scrubbing" below). The committed defaults are **4 / 2**: four
chunks scheduled ahead, two prefetcher goroutines at a time.

### 8. In-RAM hot-tier cache

New file `internal/chunk/memcache.go` and the
`Cache.SetMemCache(*MemCache)` integration. A small LRU in front
of the disk LRU: hits return the in-memory plaintext directly,
skipping `os.ReadFile`. Default cap 128 MiB (32 chunks at the
default tier); `--mem-cache-cap` to tune; 0 disables. Eviction
into disk happens passively — when the disk-LRU code already
holds the plaintext for the chunk, the RAM tier's eviction just
drops it.

New counter `telfs_memcache_hits_total` distinguishes RAM hits
from disk hits.

### 9. Cache inode metadata on the file handle

`internal/fs/handle.go`. `Reader.ReadAt` used to call
`meta.GetInode` per ReadAt just to recover `chunkSize` and the
file size. The file handle now caches both at Open time and calls
the new `Reader.ReadAtKnown` (which skips the SQL row). A `dirty`
flag trips on the first Write through the handle so subsequent
Reads fall back to the GetInode-per-call path — keeps writes
correct without giving up the read win.

## Movie scrubbing — a real-world failure mode

After landing items 1-9 above, a user reported video playback was
fine on startup but **seeking to the middle of a movie took
minutes** to resume. The story is worth telling because the fix
is half "tune the knobs" and half "rethink the prefetch lifecycle."

### What was happening

VLC seeks to byte X. The kernel issues one Read of up to
`MaxReadAhead = 4 MiB` at offset X. We:

1. Schedule a prefetch window of 8 chunks (the over-aggressive
   default we tried first).
2. Spawn 4 prefetcher goroutines (the other over-aggressive
   default).
3. Each prefetcher calls `cache.Get` → `fetcher.Fetch` →
   `Session.DownloadDocument` → `streamDocument` with gotd's
   `Parallel(4 threads)`.

Total in-flight: 1 foreground × 4 + 4 prefetchers × 4 = **20 gotd
`UploadGetFile` RPCs**, sharing the same socket. The foreground
chunk — the one VLC is sitting at, waiting on — got 1/5 of the
network. On a 5 Mbps connection a 4 MiB chunk in this regime takes
~30 seconds, and if the user scrubs again the queue grows and the
foreground is starved further.

Worse, the prefetcher ran under `context.Background()` so **prior
prefetches never cancelled**. Scrubbing once piled up junk; scrubbing
again piled up more junk on top.

### The fix

Three changes:

- **Drop the defaults** from 8 / 4 to **4 / 2**. Less prefetch
  pressure on the foreground.
- **Per-inode cancellable prefetch context.** The `Reader` now
  holds a `sync.Map[ino]*prefetchGroup`. When a Read schedules
  prefetches it gets (or creates) the inode's group context and
  passes that into each `prefetchOne`. A seek calls
  `cancelPrefetchesForIno(ino)` — the prior group context is
  cancelled, the in-flight prefetchers' `cache.Get` sees the
  cancellation, the workers exit promptly.
- **Seek detection on the file handle.** `fileHandle.lastReadEnd`
  is an `atomic.Int64`, updated to `off + len(dest)` after every
  Read. The next Read compares its own `off` to `lastReadEnd`; if
  the gap exceeds 16 MiB it routes through
  `Reader.ReadAtKnownNoPrefetch` instead of the regular fast path.
  That method cancels the prior prefetches AND passes
  `prefetchWindow=0` so no new prefetches are scheduled for this
  read.

After the seek lands, the next 2-3 sequential reads still don't
prefetch (the gap is small but the *velocity* signal hasn't been
re-established) — once the player resumes streaming, the natural
sequential pattern brings prefetch back online.

### Why 16 MiB

It's four chunks of slack. Catches scrubbing (which jumps seconds
to minutes worth of stream) without flagging the kernel's natural
readahead jitter (which stays within a chunk or two of the
previous Read).

### Why not give the foreground higher priority instead

We considered making the foreground use 8 gotd threads and prefetch
use 2, so the foreground dominates by default. It would help, but
it doesn't solve the "scrubs accumulate stale work" problem.
Cancellation does, and it falls out cleanly from the per-inode
group context.

## What's reading like now

For sequential streams: bandwidth-bound, with prefetch warming the
next 4 chunks while you consume the current one. The disk LRU
+ RAM hot tier hold the most recently played material.

For random reads: each is one packed extent fetch + a cached
FileReference + a single-SELECT metadata lookup + a single
`upload.getFile` (small extents fit in one MiB block). The 1 MiB
boundary clamp from chapter 14 keeps these legal; the parallel
fetcher kicks in only when the extent crosses a boundary.

For re-reads: RAM hot tier returns in microseconds. Disk LRU when
the RAM tier evicted. Network only when both miss.

For seeks during streaming: the previous prefetch wave gets
cancelled, the foreground chunk gets full bandwidth, the player
resumes within one chunk's worth of network time.

## What we did NOT do

- **No partial-chunk fetches.** A 16 KB Read still pulls the whole
  4 MiB chunk because the cache's storage unit is a chunk. Doing
  partial fetches would mean redesigning the cache around byte
  ranges; the wins didn't justify the complexity given that the
  kernel's `MaxReadAhead = chunkSize` already coalesces small
  reads.
- **No restructure of `cache.Get`'s packed-extent slicing.** The
  existing code is correct; items 1+2 give it everything it needs.
- **No removal of the disk-tier LRU.** The RAM tier (item 8) sits
  on top; the disk LRU is the durable survival-across-restart
  layer.
- **No switch to `gotd` async download builder.** The synchronous
  parallel mode is enough at this layer; we added our own parallel
  fetcher (item 2) instead.

## File map

| File | What landed here |
|---|---|
| `internal/chunk/cache.go` | singleflight, RAM tier hook, populate-on-disk-hit |
| `internal/chunk/memcache.go` | new — RAM LRU implementation |
| `internal/chunk/reader.go` | `ReadAtKnown`, `ReadAtKnownNoPrefetch`, per-inode prefetch group, bulk metadata fetch |
| `internal/chunk/persist.go` | `PendingIndex` |
| `internal/chunk/packer.go` | `SetPendingIndex` + UnrecordPersist on success/FK paths |
| `internal/chunk/writer.go` | RecordPersist after PersistPending |
| `internal/chunk/pool.go` | `PendingIndex` field on UploadPool |
| `internal/tg/session.go` | document cache, parallel ranged fetch, planRangeParts helper |
| `internal/tg/tracker.go` | (carried from chapter 14) tx/rx split that powers the network-volume visibility |
| `internal/meta/chunk.go` | `GetChunksRange` |
| `internal/fs/handle.go` | cached chunkSize/size, seek detection, `lastReadEnd` |
| `internal/fs/fs.go` | populate handle cache at Open, init `lastReadEnd` |
| `internal/fs/mount.go` | `MaxReadAhead` + `MaxBackground` |
| `cmd/telfs/main.go` | `--prefetch-window`, `--prefetch-concurrency`, `--mem-cache-cap`, wiring |
| `internal/metrics/metrics.go` | `CacheMergedMisses`, `MemCacheHits` counters |

## Lessons that generalise

1. **Prefetch is a tax on the foreground.** It only pays off when
   it warms data you were going to need anyway. As soon as the
   user's intent changes — a seek, a directory jump — the
   prefetched work becomes a cost with no benefit. Cancellation
   matters more than bigger windows.
2. **Convergent defaults beat clever ones.** We tried 8 / 4 because
   "downloads are unlimited now." That was the wrong frame: the
   bottleneck wasn't a rate limiter, it was the user's bandwidth.
   The right frame was "what fraction of bandwidth do we want the
   foreground chunk to have" — 1/3 (with 2 prefetchers) felt
   better than 1/5 (with 4).
3. **Singleflight is a great defense against parallel work duplication.**
   Even when the work is "correct" — both callers would get the
   right answer — duplicating it wastes capacity. Add a
   singleflight any time you have a function that takes a key and
   does network I/O.
4. **Caches benefit from a memory tier even if you already have
   disk.** The syscall cost is real on hot loops. 128 MiB of RAM
   buys back syscall latency for cents of memory.
5. **Sequential reads aren't synonymous with "previously-known
   future reads."** A movie player scrubs. A code editor jumps.
   Detecting the velocity of a stream is what tells prefetch to
   ramp up; detecting a discontinuity is what tells it to ramp
   down. Both signals are cheap to compute on a file handle.
