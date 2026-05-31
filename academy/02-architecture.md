# 02 — Architecture

This chapter is the diagram. Every other chapter assumes you have it in
your head.

## The layered view

```
                   ┌────────────────────────────────────┐
                   │     userspace process (tar, vim)   │
                   └────────────────────────────────────┘
                                    │  POSIX syscalls
                                    ▼
                   ┌────────────────────────────────────┐
                   │      Linux kernel / FUSE driver    │
                   └────────────────────────────────────┘
                                    │  FUSE op stream over /dev/fuse
                                    ▼
        ┌──────────────────────────────────────────────────────┐
        │  internal/fs   — go-fuse Node + fileHandle            │
        │   Lookup, Readdir, Create, Write, Flush, Sync, …      │
        └──────────────────────────────────────────────────────┘
            │              │              │              │
            ▼              ▼              ▼              ▼
        ┌────────┐    ┌─────────┐    ┌─────────┐    ┌──────────┐
        │ chunk  │    │  chunk  │    │  meta   │    │  trash   │
        │ Reader │    │ Writer  │    │ Store   │    │ rename   │
        └────────┘    └─────────┘    └─────────┘    └──────────┘
             │             │              │
             ▼             ▼              │
        ┌─────────┐  ┌─────────┐          │
        │  Cache  │  │  Pool   │          │
        │  (LRU)  │  │  +      │          │
        │  on     │  │ Packer  │          │
        │  disk   │  │         │          │
        └─────────┘  └─────────┘          │
             │             │              │
             ▼             ▼              ▼
        ┌──────────────────────────────────────────────────────┐
        │  internal/tg.Session — gotd MTProto wrapper           │
        │   + RateLimiter + FloodWaiter + Tracking middleware   │
        └──────────────────────────────────────────────────────┘
                                    │
                                    ▼
                          (Telegram channel)

                  ┌─────────────────────────────────┐
                  │  internal/snapshot.Manager       │
                  │  + Poster — back up the meta DB  │
                  │  to the channel periodically     │
                  └─────────────────────────────────┘
                       ▲                      │
                       │                      ▼
                  meta_kv                 channel (snapshot
                                          message + journal-op
                                          messages)
```

A few things to internalize:

- **`internal/fs` is the only thing the kernel talks to.** Every other
  package is downstream. Going up from `internal/chunk` or
  `internal/meta` to userspace happens only through a return value from
  a FUSE handler.
- **`internal/tg` is the only thing Telegram talks to.** Snapshot,
  chunk Writer, chunk Reader — they all reach the channel by holding a
  `*tg.Session` and calling its methods. They don't open MTProto
  connections themselves.
- **The metadata store is on the local disk.** The channel is durable
  storage; the local DB is a fast cache of the channel's state. A
  freshly cloned host bootstraps the DB by reading the latest snapshot
  + journal-ops from the channel.

## The write path, end to end

A `write(fd, buf, n)` from userspace flows like this:

1. Kernel forwards the call as a FUSE WRITE op.
2. `(*fileHandle).Write` (in `internal/fs/handle.go`) takes the handle
   lock and calls `chunk.Writer.WriteAt`.
3. `WriteAt` (in `internal/chunk/writer.go`) figures out which
   chunks the offset/length touches, loads each into the dirty map
   (preloading existing bytes via the cache + reader if needed), copies
   the new bytes in, and updates `w.size`. **It returns success here.**
   No network IO has happened.
4. If the dirty map crosses a configurable cap (`DefaultDirtyCapBytes`,
   ~32 MiB), `WriteAt` eagerly *dispatches* the oldest dirty chunks for
   upload. Dispatching = compress → encrypt → submit to packer or
   pool, in a fresh goroutine. `WriteAt` does **not** wait for the
   upload to finish.
5. When userspace calls `close(fd)`, the kernel sends a FUSE FLUSH then
   FUSE RELEASE.
6. `(*fileHandle).Flush` calls `Writer.Flush`, which dispatches every
   remaining dirty chunk **and** writes the final `inode.size` via
   `meta.Store.SetSize`. It returns success even though uploads may
   still be in flight (see "async close" in
   [chapter 06](06-chunk-pipeline.md)).
7. `Release` calls `Writer.Close`, which nils out the dirty map. The
   in-flight upload goroutines outlive this — they ride
   `pool.UploadCtx()`, which only cancels when the FUSE backend shuts
   down at unmount.
8. Each upload goroutine, when it eventually lands, calls
   `meta.Store.PutChunk` to persist the new `chunk_map` row. That row
   is what makes the upload visible to subsequent reads.

The reason there are so many steps for a write is that the chain is the
single most contested code path in the project. Every step has rejected
alternatives; chapter 06 walks them.

## The read path, end to end

`read(fd, buf, n)` is shorter because there's no upload side:

1. Kernel forwards as FUSE READ.
2. `(*fileHandle).Read` calls `chunk.Reader.ReadAt`.
3. `ReadAt` (in `internal/chunk/reader.go`) figures out which
   `chunk_map` rows the offset/length touch.
4. For each chunk, the Reader hits the on-disk LRU cache first
   (`internal/chunk/cache.go`). Cache hit → done.
5. Cache miss → the cache's `Fetcher` (concretely `chunk.TGFetcher`)
   downloads the message from Telegram, optionally decrypts +
   decompresses, and writes it back to the cache.
6. Per-inode "pending" barrier: if a writer is still flushing the chunk
   we're trying to read, `pool.Pending.Wait(ino)` blocks until the
   flush completes. Otherwise the read could see stale data.

If you remember nothing else: **the cache is content-addressed by the
channel message ID, not by inode/offset.** That's how the dedup story
in chapter 07 falls out for free.

## Where the meta store fits

`meta.Store` is a *SQLite handle around a single file* (and a WAL).
It's used by:

- Every FUSE handler (lookup, readdir, getattr, create, …).
- The chunk Writer (`PutChunk`, `SetSize`, `ListChunks`).
- The chunk Reader (`GetChunk` for the message-ID lookup).
- The snapshot Manager (`Take(...)` reads the whole DB; `PutKV`
  records the latest snapshot message-id).
- The journal Poster (`PendingJournal`, `BatchMarkJournalPosted`).
- The cleanup / GC commands.

The fact that every component shares this single SQLite file is why
chapter 05 is so long. WAL gives us many concurrent readers + one
writer, but "one writer" plus a tar extract producing 1500 ops/sec is
the contention story that motivated `_txlock=immediate`.

## Where the channel format lives

Everything we upload to the channel falls into one of three categories:

- **Chunks**: file contents. The message is an `InputMediaUploadedDocument`
  whose document body is one of:
    - The chunk's bytes verbatim (legacy, plaintext FS).
    - The chunk's bytes after compression + encryption (modern path).
    - A **packed blob** containing many small chunks concatenated; each
      chunk's row in `chunk_map` carries a `pack_offset` + `pack_size`
      so the Reader knows the byte range to extract.
- **Snapshots**: gzipped SQLite DB optionally wrapped in an encryption
  envelope. Sent as `InputMediaUploadedDocument` with a magic filename
  prefix so recovery can find them via `messages.search`.
- **Journal-op batches**: a CBOR-ish concatenation of recent meta-ops
  (Create/Unlink/Setattr/...) so a freshly-cloned host can replay
  what's happened since the last full snapshot.

The full byte-level format is in chapter 10. For now, just know that
**no other shape ever hits the channel**. There's no chat, no metadata
message, no out-of-band signaling — the channel is exclusively a blob
store, and the daemon's reconciliation logic is what turns it back
into a filesystem.

## The supervisor and the web

`internal/web` is a separate concern: it's a single-binary HTTP UI
(spawned via `telfs web`) that wraps the CLI for human friendliness.
It spawns + supervises `telfs mount` subprocesses, exposes the
`/state`, `/telegram`, `/mount` tabs, and so on. The daemon and the
web server are **independent processes**: the web reaches into the
daemon's debug HTTP endpoint (`/telegram`) to surface real-time RPC
state, and reads `meta_kv` directly via the same SQLite DB.

The web doesn't insert itself into the data path. Everything it does
could be done by the CLI, by hand, with `sqlite3` + `curl`. Chapter 12
walks the design choices.

## Next chapter

[03 — Tour of the repo](03-tour-of-the-repo.md) walks every directory so
you know which file owns which concept.
