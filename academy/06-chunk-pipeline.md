# 06 — The chunk pipeline

This is the longest chapter because the chunk pipeline is the single
most complex component. It also has the highest density of
"why-not-X" decisions, because every part of it touches latency,
throughput, durability, or concurrency in ways the rest of the codebase
relies on.

## The cast

| Type | Lives in | Role |
|---|---|---|
| `Writer` | `internal/chunk/writer.go` | Per-open-fd buffer + dispatch. Owns the dirty map. |
| `Reader` | `internal/chunk/reader.go` | Stateless servicing of ReadAt. Hits the cache first. |
| `Cache` | `internal/chunk/cache.go` | LRU on-disk cache, content-addressed by (msg_id, pack_offset, pack_size). |
| `UploadPool` | `internal/chunk/pool.go` | Backend-global concurrency cap + per-ino R-A-W barrier. |
| `Packer` | `internal/chunk/packer.go` | Aggregates sub-256 KB chunks into shared messages. |
| `Reservation` | `internal/chunk/pool.go` | Token an upload holds; releases on done. |
| `Pending` | `internal/chunk/pool.go` | Per-ino "uploads in flight" tracker. |
| `Uploader` | `internal/chunk/writer.go` (interface) | What WriteAt calls to push bytes. `*tg.Session` implements it. |
| `Fetcher` | `internal/chunk/cache.go` (interface) | What ReadAt calls to download. `*TGFetcher` implements it. |

Notice the consistent shape: the package depends on **interfaces** for
its external collaborators. That makes the test story tractable —
`writer_test.go` swaps in a `fakeUploader` that doesn't know what
Telegram is.

## What a "chunk" is

The default chunk size is 4 MiB. Older filesystems still use 4 MiB; new
ones promote to larger sizes via the multi-tier mechanism (chapter 07).
A chunk is a contiguous slice `[idx*chunkSize, (idx+1)*chunkSize)` of a
file's logical address space.

`chunk_map` maps (ino, idx) to a Telegram message ID:

```
chunks for file ino=42 (size = 9 MiB, chunkSize = 4 MiB):
  idx 0  →  msg 12345  size=4194304  pack_offset=NULL
  idx 1  →  msg 12346  size=4194304  pack_offset=NULL
  idx 2  →  msg 12347  size=1048576  pack_offset=NULL
```

For a packed chunk, the row carries `pack_offset` + `pack_size`:

```
idx 5  →  msg 99999  size=2048  pack_offset=1024  pack_size=2096
```

That row says: "to get this chunk, download message 99999, then take
bytes [1024, 1024+2096). The original chunk's plaintext size is 2048
bytes." The extra 48 bytes are AES-GCM tag + IV. (`pack_size` is the
on-the-wire length, including encryption overhead; `size` is the
plaintext length.)

## The Writer

`internal/chunk/writer.go`. The struct (abridged):

```go
type Writer struct {
    meta     *meta.Store
    cache    *Cache
    uploader Uploader
    cipher   crypto.Cipher
    codec    compress.Codec
    pool     *UploadPool
    packer   *Packer
    ino      int64
    chunkSz  int64
    dirtyCap int64

    mu         sync.Mutex
    dirty      map[int32]*dirtyChunk
    dirtyOrder []int32            // insertion order, for eager dispatch
    dirtyBytes int64
    size       int64
    closed     bool
    hasDispatched bool

    uploadWg  sync.WaitGroup
    uploading map[int32]chan struct{}
    uploadErr error
}
```

A handful of points to highlight:

### `dirty` keyed by `idx`, ordered by `dirtyOrder`

We need O(1) lookup by index (Write at offset X has to find chunk
`X / chunkSz`) and we need insertion-ordered iteration (eager
dispatch removes the *oldest* dirty chunk first). A bare `map` doesn't
give ordering; a bare slice doesn't give O(1) lookup. So we have both,
kept in sync.

### `uploading` lets the writer wait for in-flight uploads

Look at `loadForWrite`. When you `WriteAt(off, data)` to a chunk that's
already been dispatched (popped from `dirty`, in-flight goroutine has
the bytes), we need to wait until the upload finishes before reloading
the chunk's pre-write contents. Otherwise we'd read stale bytes from
the cache (which doesn't have the new chunk yet) and the write would
clobber the pending upload's view.

The `chan struct{}` per in-flight idx is closed when the goroutine
completes. `loadForWrite` blocks on it. This is a tiny piece of code
but it is essential to correctness — without it, fast read-modify-write
patterns over the same offset would lose data.

### `dirtyCap` and eager dispatch

If we let `dirty` grow without bound, a multi-GB write would buffer the
entire file in RAM. We don't want that. So `WriteAt` checks
`dirtyBytes > dirtyCap` (default ~32 MiB) and, if true, dispatches the
oldest dirty chunks. Dispatch is *async* — it queues an upload in a
goroutine; it does **not** block on the network.

But it *does* block on `pool.Reserve`. The pool's concurrency cap (4
parallel uploads, by default) is the backpressure mechanism. If 4
uploads are in flight, the 5th's `Reserve` blocks. That's how a
slow Telegram throttles the kernel's writes without us losing data.

### `chunkSize` is locked once dispatched

`w.hasDispatched` flips to `true` the first time we dispatch a chunk.
After that, the chunk size cannot be promoted to a higher tier — if
we did, future chunks would be a different size, and a Read that
spans tiers would see misaligned data. The promotion logic (chapter
07) only fires before the first dispatch.

## WriteAt, line by line

```go
func (w *Writer) WriteAt(ctx context.Context, src []byte, off int64) (int, error) {
    if len(src) == 0 { return 0, nil }
    if off < 0 { return 0, fmt.Errorf("writer: negative offset %d", off) }

    w.mu.Lock()
    defer w.mu.Unlock()
    if w.closed {
        return 0, fmt.Errorf("writer: handle closed")
    }
    // Note: we deliberately do NOT check w.uploadErr here. See below.

    // Hole-fill: writing past the current end zero-fills the gap.
    if off > w.size {
        if err := w.materializeZeros(ctx, w.size, off); err != nil {
            return 0, err
        }
    }

    startIdx := int32(off / w.chunkSz)
    endIdx := int32((off + int64(len(src)) - 1) / w.chunkSz)
    written := 0
    cur := off

    for idx := startIdx; idx <= endIdx; idx++ {
        dc, err := w.loadForWrite(ctx, idx)
        if err != nil { return written, err }
        chunkStart := int64(idx) * w.chunkSz
        relOff := cur - chunkStart
        maxInChunk := w.chunkSz - relOff
        remaining := int64(len(src) - written)
        toWrite := maxInChunk
        if remaining < toWrite { toWrite = remaining }
        needed := relOff + toWrite
        w.growChunk(dc, needed)
        n := copy(dc.data[relOff:relOff+toWrite], src[written:written+int(toWrite)])
        written += n
        cur += int64(n)
        if cur > w.size { w.size = cur }
    }

    // Multi-tier promotion runs BEFORE eager-dispatch so the first
    // dispatched chunk uses the final layout.
    if err := w.maybePromoteLocked(ctx); err != nil { return written, err }

    // Eager dispatch when over cap.
    for w.dirtyBytes > w.dirtyCap && len(w.dirtyOrder) > 0 {
        victim := w.dirtyOrder[0]
        if err := w.dispatchFlushLocked(ctx, victim); err != nil {
            return written, err
        }
    }

    return written, nil
}
```

The thing to internalize: **WriteAt is fast.** It does memory copies,
maybe a couple of small synchronous reads (loadForWrite preloading
existing bytes), and queues uploads. It does NOT wait on Telegram.
The latency the caller sees is essentially memcpy + map maintenance.

## The `uploadErr` decoupling

WriteAt does **not** check `w.uploadErr`. This wasn't always true.
Earlier code did:

```go
// OLD
if err := w.uploadErr; err != nil {
    return 0, err
}
```

The problem: when a background upload failed (e.g., a single FLOOD_WAIT
exceeded the middleware's cap), the writer set `uploadErr`. The very
next `WriteAt` returned that error to userspace. tar saw EIO on a
random byte and aborted the file. Yet the chunk was already restored
to dirty for retry on the next Flush — so by the time tar bailed, the
chunk was *about to retry successfully*.

The fix split `recordError` into two functions:

- `recordTransient(err)` — just logs to stderr. Used when the chunk
  goes back to dirty.
- `recordLost(err)` — sets `uploadErr` and logs LOST. Used only when
  the writer is already closed and the chunk has nowhere to restore.

WriteAt no longer reads `uploadErr` ever. Flush still returns it
(close-time durability check); Sync waits for in-flight uploads and
returns a non-nil error if the dirty map isn't empty post-wait
(fsync-style durability).

> See commit `003150a` for the full story.

## The dispatch pipeline

`dispatchFlushLocked` pops a chunk from `dirty`, removes it from
`dirtyOrder`, increments `uploadWg`, registers the `uploading[idx]`
channel, releases the lock, and spawns `uploadOne` in a goroutine.

`uploadOne` runs through several stages, each of which can fail:

```
load dc (already in hand)
        │
        ▼
    dedup?  (only for plaintext + convergent-encrypted FSes)
        │
        ├── reuse hit → write chunk_map row pointing at existing msg, return
        ▼
    compress (codec.Compress)
        │
        ▼
    encrypt (cipher.Seal)
        │
        ▼
    pack?  (if size < packer.Threshold)
        │
        ├── persist wire to disk
        ├── submit to packer
        ├── wait for packer result
        ▼
    direct upload (uploader.UploadDocument with retry)
        │
        ▼
    PutChunk (chunk_map row landed)
        │
        ▼
    cache.Invalidate
```

Each stage that fails calls `recordTransient(err)` (or `recordLost` if
the writer is detached), sets `restore = true`, and returns. The
restore defer puts `dc` back in `dirty` for the next dispatch to retry.

The `uploadWithRetry` wrapper around `UploadDocument` (and around the
packer's flush) covers transient network errors: 5 attempts, 500 ms →
8 s backoff. ctx cancellation is the only non-retriable signal (that's
daemon shutdown). Real persistent failures get to `recordTransient`
after ~30 s of trying.

> See `internal/chunk/upload_retry.go` and commit `8011e3b`.

## The Reader

Much simpler than the Writer:

```go
func (r *Reader) ReadAt(ctx context.Context, ino int64, dest []byte, off int64) (int, error) {
    // Per-ino "is there an in-flight upload for this idx?" barrier.
    if r.pending != nil {
        if err := r.pending.Wait(ctx, ino); err != nil { return 0, err }
    }

    chunks, err := r.meta.ListChunks(ctx, ino)
    if err != nil { return 0, err }

    in, err := r.meta.GetInode(ctx, ino)
    if err != nil { return 0, err }
    if off >= in.Size { return 0, io.EOF }

    end := off + int64(len(dest))
    if end > in.Size { end = in.Size }

    // For each chunk that this read touches, hit the cache (which falls
    // through to TG download on miss), copy out the relevant bytes.
    var n int
    cur := off
    for _, c := range chunks {
        chunkStart := int64(c.Idx) * r.chunkSz
        chunkEnd := chunkStart + int64(c.Size)
        if chunkEnd <= cur || chunkStart >= end { continue }

        bytes, err := r.cache.Get(ctx, Key{Ino: ino, Idx: c.Idx}, c.TGMessageID,
            c.PackOffset, c.PackSize)
        if err != nil { return n, err }

        relStart := cur - chunkStart
        if relStart < 0 { relStart = 0 }
        relEnd := end - chunkStart
        if relEnd > int64(c.Size) { relEnd = int64(c.Size) }
        copied := copy(dest[n:], bytes[relStart:relEnd])
        n += copied
        cur += int64(copied)
        if cur >= end { break }
    }
    return n, nil
}
```

The Reader does no retry of its own — the cache fetcher does, and if
that ultimately fails, EIO is the right answer. There's no "restore
for retry on next read" pattern because reads don't have side-effects
to roll back.

### The pending barrier

`r.pending.Wait(ctx, ino)` waits if any writer has an in-flight upload
for this inode. Without it, you'd see this race:

1. Writer: `WriteAt`, fill chunk 5 with "NEW", dispatch.
2. (goroutine begins upload, but hasn't written chunk_map row yet)
3. Reader: `ReadAt` for byte 5 * chunkSize.
4. Reader: `ListChunks` doesn't see chunk 5's new row yet.
5. Reader: returns 0 / wrong data / falls through to download stale.

The Pending tracker holds a per-ino atomic counter + a chan. Writers
increment on dispatch and decrement on completion; readers `Wait`
blocks while the counter is non-zero. This is the **read-after-write
consistency barrier** the FS contract requires.

## The Cache

`internal/chunk/cache.go`. An LRU on-disk cache keyed by what the
**channel** holds, not by what the **inode** wants. Specifically:

```go
type Key struct {
    Ino int64
    Idx int32
}
```

Wait, that's keyed by (ino, idx)? Yes — the `Key` type the Writer
passes to `Invalidate`. But the **cache file path** on disk is derived
from the chunk's content (the message_id + pack_offset + pack_size):

```go
func cachePath(dir string, msgID int64, packOffset, packSize int64) string {
    if packSize > 0 {
        return filepath.Join(dir, fmt.Sprintf("msg-%d-%d-%d.chunk",
            msgID, packOffset, packSize))
    }
    return filepath.Join(dir, fmt.Sprintf("msg-%d.chunk", msgID))
}
```

This means: if two inodes' chunks point at the same TG message (dedup
hit), they share the same cache file. The cache doesn't know or care.
The Invalidate call by `Key{ino, idx}` doesn't actually delete the
cache file — it bumps the LRU eviction priority so future evictions
prefer this entry. The cache file remains correct because the bytes
haven't changed.

The cache size cap (default 1 GiB) is enforced by sweeping the
cache directory on LRU evict.

### The Fetcher interface

```go
type Fetcher interface {
    Fetch(ctx context.Context, key Key, msgID int64) ([]byte, error)
}
```

For packed chunks, `TGFetcher` issues a **ranged download** — it asks
Telegram for bytes `[pack_offset, pack_offset + pack_size)` rather
than the entire blob. This is critical: a packed message can be 32
MiB; downloading the whole thing to read a 4 KiB chunk would tank
ratios on small-file reads.

The ranged-download code is in `internal/tg/file.go`. It's worth
reading because the gotd API for it is mildly unergonomic (you have to
loop, calling `upload.GetFile` with explicit offset/limit, until you
have the bytes).

## The UploadPool

`internal/chunk/pool.go`. Three jobs:

1. **Backend-global concurrency cap.** `DefaultUploadConcurrency = 4`
   means at most 4 chunk uploads in flight across the entire mount.
   The cap was tuned via measurement: 4 is enough to fill a typical
   home connection's upload bandwidth; more just queues at Telegram
   and increases FLOOD_WAIT pressure.
2. **Per-ino R-A-W barrier.** The `Pending` type tracks "uploads in
   flight per ino" so the Reader can wait. Implemented as
   `map[int64]int` + cond var.
3. **Long-lived UploadCtx.** Uploads ride this context, not the
   user's. So a `close(fd)` doesn't cancel an upload mid-flight — it
   just detaches the writer. The pool's ctx only cancels at unmount
   via `pool.Shutdown()`.

The `Reservation` returned by `pool.Reserve` is the token. It carries
a method `ReleaseSem()` that releases the concurrency slot early —
the packer uses this so its workers don't hold a slot while waiting
on the packer's serialized flusher.

## Pending wires on disk

`internal/chunk/persist.go`. Before submitting to the packer (or
direct upload), the writer **persists the wire bytes to disk** in
`<cache>/pending/<ino>-<idx>-<plaintextSize>.bin`. On success
(chunk_map row landed), the file is deleted. On daemon crash, the
file stays.

At next startup, `recoverPending` scans the directory and re-submits
each wire. The chunk gets a fresh message ID; the orphan from the
previous attempt becomes `telfs gc` fodder.

This is the durability guarantee: "the writer told you bytes were
written" survives a SIGKILL between dispatch and PutChunk. Without
persist-pending, a kill at the wrong moment could lose those bytes
even though the upload to TG had succeeded — the message would exist
but no chunk_map row would point at it.

> See commit `f24d39e`.

## Async close: why Flush returns before uploads land

Look at `Flush`:

```go
func (w *Writer) Flush(ctx context.Context) error {
    w.mu.Lock()
    if w.closed {
        w.mu.Unlock()
        return fmt.Errorf("writer: handle closed")
    }
    prevErr := w.uploadErr
    w.uploadErr = nil
    if err := w.maybePromoteLocked(ctx); err != nil { ... }

    toFlush := make([]int32, len(w.dirtyOrder))
    copy(toFlush, w.dirtyOrder)
    for _, idx := range toFlush {
        if err := w.dispatchFlushLocked(ctx, idx); err != nil { ... }
    }
    finalSize := w.size
    w.mu.Unlock()
    if err := w.meta.SetSize(ctx, w.ino, finalSize); err != nil {
        return err
    }
    return prevErr
}
```

Flush dispatches every remaining dirty chunk but **does not wait** for
them. SetSize persists the final size synchronously. Then Flush
returns.

Why?

- POSIX `close(2)` doesn't require durability. fsync does.
- Returning fast from close means tar's per-file overhead is minimal.
- Async uploads still ride `pool.UploadCtx`, which outlives any single
  fileHandle. They'll complete (or be retried by recoverPending).

The cost: a process that crashes between Flush returning and the async
uploads landing will lose those uploads' chunk_map rows. The repair
pass at next startup truncates affected files to their consistent
prefix. The user sees a shorter file, not corrupt data.

Sync (fsync) is the durability-guaranteed path. It waits for the
per-handle WaitGroup, checks `w.dirty` is empty post-wait, and only
then SetSize and return success. If any chunks failed and were
restored to dirty, Sync returns "N chunks failed to upload" and the
caller can retry.

## What this chapter omits

- The packer's flush + ranged download details — chapter 07.
- The multi-tier sizing rules — chapter 07.
- The encryption format on the wire — chapter 10.

## Next chapter

[07 — Packer and dedup](07-packer-and-dedup.md) — the throughput story.
