# TelFS recovery model

This doc describes how TelFS survives the three classes of failure that
matter for a personal cloud-backed filesystem.

## Failure classes

| Class | Example | Recovery mechanism |
|---|---|---|
| **Process crash** | OOM-kill, kernel panic mid-write | local SQLite WAL + journal replay |
| **Local-disk loss** | dev box wiped, laptop stolen | channel snapshot + meta-op replay |
| **Partial network failure** | upload mid-flight, FLOOD_WAIT | idempotent ops, journal pending-bit |

## 1. Process crash

SQLite is opened in WAL mode. Every metadata mutation is a single transaction
that writes:
- the actual change to `inodes` / `dirents` / `chunk_map` / etc.;
- a row in `journal` describing the same mutation as a JSON op.

On startup:
1. SQLite recovers the WAL automatically.
2. TelFS scans `journal` for rows where `posted_at IS NULL` (not yet posted
   to the channel as a `meta-op`) and posts them now, in order. Posting is
   idempotent: each row has a `seq` and re-posting the same `seq` is a no-op
   on the channel side because we de-dup by `seq` during recovery.

Chunk uploads use a two-phase commit:
1. Upload chunk to channel → receive `message_id`.
2. Insert `chunk_map` row + journal entry in one SQLite transaction.

If we crash between (1) and (2), the chunk is orphaned in the channel but no
metadata claims it; a later GC pass identifies and deletes such orphans by
diffing `chunk_map` against a channel scan.

## 2. Local-disk loss

Cold mount path (no local DB, or DB marked unclean by a fingerprint check):

1. Resolve the configured channel via `internal/tg`.
2. Scan history newest-first via `messages.getHistory`, parsing each message
   header.
3. Find the most recent `snapshot` message → download → gunzip → restore as
   `.telfs/db.sqlite`.
4. Continue scanning forward from `snapshot.seq` and replay every
   `meta-op` with `seq > snapshot.seq` against the restored DB.
5. Reconcile chunk visibility: every `chunk_map` row's `tg_message_id` must
   still exist in the channel. Missing ones mark the file as truncated at
   the lost chunk boundary and a `meta-op` is appended noting the loss.

Recovery time is dominated by the snapshot cadence: with the default cadence
(every 500 ops or 10 minutes), worst-case replay is ~500 meta-ops, which is
seconds.

## 3. Partial network failure

`internal/tg` retries with exponential backoff on transient errors, and
honors the explicit sleep returned in `FLOOD_WAIT_<n>` errors from gotd.

In-flight chunk uploads have a deadline; on timeout the local staging
buffer is preserved (still dirty in cache) so a later flush retries.

`meta-op` posts are non-blocking from the FUSE write path: they go into the
local journal first and a background goroutine drains them to the channel.
If the network is down for an extended period, mutations accumulate in the
journal — reads/writes continue against the local DB unimpaired. When
connectivity returns, the journal drains and a snapshot is forced if its
size threshold has been crossed.

## What we don't (yet) defend against

- **Channel deletion**: if the channel is deleted server-side, all snapshots
  and meta-ops go with it. Mitigation: don't delete the channel. (A future
  feature could mirror to a second channel.)
- **Concurrent mounts**: TelFS v1 assumes a single mounter per channel. Two
  concurrent mounters will race and likely corrupt each other's state. The
  on-disk schema reserves `meta_kv['owner']` and `meta_kv['lock_ts']` keys
  for a future lock-message-in-channel mechanism.
