# 07 — Packer and dedup

This chapter is about throughput. The pipeline in chapter 06 explains
how one chunk gets to Telegram. This chapter explains why we can do
ninety thousand of them without dying.

## The cost of "one message per chunk"

A naive design uploads one chunk per Telegram message:

```
chunk 0 → MessagesSendMediaRequest → message_id 12345
chunk 1 → MessagesSendMediaRequest → message_id 12346
chunk 2 → MessagesSendMediaRequest → message_id 12347
…
```

For 1 GiB written as 4 MiB chunks: 256 messages. Each message is at
least one RPC (and several `UploadSaveBigFilePart` calls if the chunk
is >128 KiB). Throughput is dominated by the per-RPC latency and the
account's per-method rate budget.

For 1 GiB written as 90 k Linux kernel files of average 11 KiB: 90 k
messages, each a single small RPC. Telegram's per-user budget for
`MessagesSendMedia` is on the order of 30/sec sustained. That's a
50-minute wall-clock floor for 90 k messages just from rate-limiting,
not counting bandwidth, and you'd hit FLOOD_WAITs every few seconds.

That's the problem the packer solves.

## The packer's contract

`internal/chunk/packer.go`. The shape:

- Submit a chunk under some plaintext size threshold (default 256 KB).
- The packer buffers it with siblings.
- When buffered bytes cross a target (default 32 MiB) **or** a window
  timer expires (default 1 s), the packer concatenates the buffered
  wires into one blob, uploads it as a single Telegram message, and
  writes one `chunk_map` row per contributing chunk, each pointing at
  the new message ID with a `(pack_offset, pack_size)` byte range.

The result: 90 k Linux files → ~30 packed messages → ~30 RPCs (plus the
`UploadSaveBigFilePart` parts inside each, but those are 128 KiB and go
to a different account-budget bucket). Throughput goes from "Telegram's
rate-limit floor" to "your upload bandwidth ceiling."

## The data flow

```
Writer.dispatchFlushLocked
    │
    ▼
uploadOne (encrypts, compresses)
    │
    ▼ if size < threshold
PersistPending → durable bytes on disk
    │
    ▼
packer.Submit(ino, idx, plaintextSize, wire, pendingPath)
    │ returns a result chan
    ▼
[packer's loop, separate goroutine]
    │
    ├── buffer the wire (in memory + already on disk)
    ├── if buffer over target, wake flusher
    ├── if window timer fires, wake flusher
    ▼
flush():
    │
    ├── take up to target bytes worth (batch the head)
    ├── concatenate wires → blob
    ├── uploadWithRetry(blob)
    │     │
    │     ▼ result: msgID
    ├── per chunk in batch:
    │     ├── PutChunk with (msg=msgID, pack_offset, pack_size)
    │     ├── DeletePending(pendingPath)
    │     └── result chan ← packResult{msgID, packOff}
    ▼
[uploadOne resumes]
    │
    ├── cache.Invalidate
    └── return (success → restore not set)
```

## Why a separate goroutine, not synchronous batching

An alternative: every `WriteAt` accumulates locally, and at Flush time
the writer packages all dirty chunks into one blob. Simpler in some
ways, but:

- It only batches *within a single file*. Many small files extracted
  by tar (each with its own writer + Flush) would still be one
  message per file.
- It introduces a long synchronous wait at Flush.

The current packer batches **across files**. Every writer dispatches
into the same packer. The flusher picks them up in insertion order.
That's how a tar of 90 k tiny files turns into 30 messages — they all
go into the shared buffer.

## The window: 1 second of patience

A purely size-triggered flush would never drain on a slow workload.
Imagine writing 10 small files per second; the buffer never reaches 32
MiB. So we also fire on a 1-second window timer.

1 second is the latency tradeoff. Larger window = better packing
ratio = fewer messages = better throughput. Smaller window = better
end-to-end latency for a single small write. Empirically, 1 second is
the sweet spot — interactive use doesn't notice it (you can't see a
1-second async upload), and bulk writes still pack ~30 chunks per
batch.

You'll see this in the daemon log as "the packer drains in batches of
N chunks per ~1s tick" pattern during a tar extract.

> See commit `4c1b4b9` for the rationale on raising the window from
> 100 ms to 1 s. The shorter window was producing way too many small
> packed messages, defeating the optimization's purpose.

## Why persist-pending on disk before submit

If the daemon dies after `Submit` but before the packer's flush
completes, the wire bytes are in memory only — gone forever. By
writing them to `<cache>/pending/<ino>-<idx>-<size>.bin` first,
crash recovery can re-submit them on next startup.

This is a tradeoff: every chunk now writes twice (once to pending,
once to TG). On SSD it's invisible; on a slow disk it's measurable. We
accept the cost; the bytes-must-survive-crash guarantee is more
important than a 5% throughput hit.

The pending file is deleted only AFTER `PutChunk` succeeds. So the
sequence "wire on TG, wire on disk, chunk_map row absent" can only
happen mid-flight; at any quiescent point the disk state is consistent
with the channel state.

## Flush bounding: the 689 MiB bug

The flush loop originally drained **all** buffered chunks into one
blob:

```go
// OLD
for _, r := range p.pending {
    batch = append(batch, r)
}
p.pending = nil
```

A burst submit + recovery re-submit (`recoverPending` feeding 173 wires
in tight succession) once produced a 689 MiB blob. The upload took
~10 minutes, hit a FLOOD_WAIT mid-upload, and was retried by gotd's
internal logic — which restarted the upload from byte 0 each time. The
result was a wedged daemon.

The fix bounds each flush at `p.target`:

```go
for i, r := range p.pending {
    w := int64(len(r.wire))
    if i > 0 && batchBytes + w > p.target {
        break
    }
    batch = append(batch, r)
    batchBytes += w
    taken = i + 1
}
p.pending = p.pending[taken:]
```

And re-signals `wake` if `len(p.pending) > 0` so the loop runs again
without waiting another window tick. So a burst of 200 small chunks
turns into 7 batches of ~30 each, each well under target, each
finishing in seconds.

> Commit `02b2ff3` landed it. The comment in `flush()` still calls out
> the 689 MiB war story explicitly.

## Reading a packed chunk

The Reader path for a packed chunk:

1. `meta.GetChunk(ino, idx)` returns a `Chunk` with non-nil
   `pack_offset` and `pack_size`.
2. `cache.Get(Key, msgID, packOffset, packSize)` hits the on-disk
   cache. The cache filename includes the (msgID, offset, size) tuple,
   so two reads of the same packed slice share the file.
3. On cache miss, `TGFetcher.Fetch(key, msgID, offset, size)` issues a
   ranged download: `upload.GetFile` with explicit `offset` and
   `limit`, looping until the byte range is in hand.

The ranged-download piece is critical. If a packed message is 32 MiB
and you want to read a 4 KiB chunk, downloading the whole 32 MiB is
catastrophic for small-read latency. The ranged API gives us exactly
the bytes we need.

Telegram's `upload.GetFile` requires the offset to be a multiple of 4
KiB and the limit to be 1 MiB or a multiple thereof (the rules are
documented in the MTProto layer reference). The reader code handles
the alignment math; if the packed chunk starts mid-4KiB, we download
the surrounding aligned range and slice it.

## Content-addressed dedup

Dedup is a separate concept layered on top of the pipeline. When the
writer is about to upload a chunk:

1. Compute `sha256(plaintext)`.
2. `meta.ReuseChunkByHash(ino, idx, hash)` runs a transaction that:
   - Looks up the hash in `chunk_blob`.
   - If a row exists, verifies the referenced `tg_message_id` is still
     alive (some chunk_map row points at it).
   - If alive, inserts a chunk_map row pointing at the same message.
   - Returns `(reused=true, msgID, size)`.
3. If reused, the writer skips the upload entirely.

The dedup index is `chunk_blob` (hash → msg_id, size). It grows
monotonically; `telfs gc` prunes entries whose msg_id no longer
appears in chunk_map.

### Why content-addressed not offset-addressed

A naive scheme would key dedup by `(file_path, offset, size)`. That's
useless — every file has a unique path, so the cache never hits.

Content-addressing means: "if you write the same bytes twice (even
into different files), the second write costs zero TG round trips."
A copy of a 100 MiB file is free. A `git clone` of a repo with
duplicated assets is free. A tar of identical kernel sources from a
previous tar is free.

### Why plaintext-only by default

Dedup is keyed by `sha256(plaintext)`. The Writer's encryption step
uses a random nonce by default — so the same plaintext encrypted
twice produces different ciphertexts on the wire. We can't dedup
those without breaking encryption semantics.

The `aes-gcm-v3` cipher mode (chapter 10) uses **convergent
encryption** — deterministic nonce per chunk — so two identical
plaintexts produce identical ciphertexts. Dedup works on v3 encrypted
FSes. The tradeoff is documented in chapter 10.

For `aes-gcm-v1` and `aes-gcm-v2`, the writer disables dedup
(`dedupEnabled()` returns false). For plaintext and v3 FSes, dedup
runs.

### The aliveness gate

The chunk_blob index can outlive its referenced message — `telfs gc`
might have reaped the message between the time the blob was indexed
and now. So `ReuseChunkByHash` checks:

```sql
SELECT 1 FROM chunk_map WHERE tg_message_id = ? LIMIT 1
```

If no row references the message anymore, we treat it as a miss and
let the upload proceed normally. The stale `chunk_blob` row will be
pruned by the next `telfs gc` (via `PruneStaleChunkBlobs`). We don't
bother deleting it inline.

> See commit `5447e52` for the dedup landing.

## Multi-tier chunk size

Linux filesystems traditionally pick a single block size at mkfs and
stick with it. TelFS does something different: chunk size is **per
inode**, set at file create, and promoted to a larger tier at first
dispatch if the file has grown past a threshold.

The tiers (in `internal/chunk/tier.go`):

| Tier | Size | When it kicks in |
|---|---|---|
| `small` | 4 MiB | Default for any new file |
| `mid` | 16 MiB | File grew past 64 MiB before first dispatch |
| `big` | 32 MiB | File grew past 256 MiB before first dispatch |
| `very_big` | 64 MiB | File grew past 1 GiB before first dispatch |

The tier locks at first dispatch. After that, every subsequent chunk
for the file uses the same size. Promotion only fires while
`hasDispatched` is still false.

### Why tier instead of one size

A single chunk size optimal for 1 GiB files (say, 16 MiB) is wrong for
3 KiB files: every tiny file would have one nearly-empty 16 MiB chunk,
wasting cache space. A single chunk size optimal for tiny files
(say, 256 KB) is wrong for 1 GiB files: 4000 chunks per file, 4000
chunk_map rows, 4000 RPC overheads on read.

Per-file tiers let the writer pick after observing how big the file
actually wants to be. The kernel's writeback often gives us the full
file's worth of dirty pages before forcing flush — so by the time the
first dispatch fires, we've seen the final size and can promote
accordingly.

### Tier promotion runs before eager dispatch

In `WriteAt`:

```go
if err := w.maybePromoteLocked(ctx); err != nil { return written, err }

// Eager DISPATCH if we're over the cap.
for w.dirtyBytes > w.dirtyCap && len(w.dirtyOrder) > 0 { ... }
```

The order matters. Promote first; the first dispatch sees the promoted
chunk size. If you dispatched first and promoted second, the first
dispatched chunk would be 4 MiB and every subsequent chunk would be
16 MiB — half-sized chunks at the head of the file, full-sized at the
tail. Reads spanning the boundary would have to know about the
discontinuity.

The current code makes promotion a no-op if `hasDispatched == true`,
guaranteeing all chunks in a file are the same size.

> See commit `1998c04`.

## The threshold-vs-tier interaction

The packer's threshold is 256 KB. That's much smaller than even the
small tier (4 MiB). So a 4 MiB chunk doesn't go through the packer; it
gets direct-uploaded.

The packer is exclusively about tiny files. A 1 MiB file = one 1 MiB
chunk = one direct upload. A 10 KiB file = one 10 KiB chunk =
packed. The threshold separates "you're worth a TG message of your
own" from "let's share."

We did experiment with packing full-size chunks too (raised threshold
to 8 MiB). It produced 689 MiB blobs and the 5-minute TG upload
timeout fired. Reverted in commit `02b2ff3`. The pack-route is
strictly for small chunks.

## Next chapter

[08 — FUSE handlers](08-fuse-handlers.md) — the layer that turns the
chunk + meta machinery into a POSIX filesystem.
