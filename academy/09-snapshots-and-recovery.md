# 09 — Snapshots and recovery

> **Since v1 (October 2026) the cadence below is format 1's.** A format-2
> filesystem journals every change, chunk placements included, takes a
> whole-DB snapshot only every 500,000 journal ops or 24 h, and is found
> through a pinned anchor instead of a history scan: see
> [chapter 18](18-v1-backup-service.md), section 6.

The local SQLite DB is fast and authoritative while the daemon is
running. But it's on **one machine**. If that machine dies, every
chunk on the channel becomes an orphan — we can see the bytes, but
not the file tree that knew about them.

The fix: periodically back up the DB itself to the channel. A fresh
host with only the channel + credentials can pull the latest
snapshot, restore it locally, and resume.

This chapter walks the snapshot model and the journal that complements
it.

## The two-piece recovery model

There are two kinds of channel-side state used for recovery:

| Artifact | Frequency | Coverage | Cost |
|---|---|---|---|
| **Full snapshot** | Every 5 min | All metadata as of `seq = N` | Gzipped DB, typically a few MB |
| **Journal-op batch** | Every ~5 s | Mutations since last snapshot | One message per ~1000 ops |

To recover, a new host:

1. Finds the most recent snapshot message in the channel.
2. Downloads it, decompresses, and writes the bytes back as
   `db.sqlite`.
3. Replays every journal-op message with `seq > snapshot.JournalSeq`.

That gives you metadata that's at most 5 seconds stale (the journal
batch interval).

## Why a journal alongside snapshots

Without the journal, recovery is bounded by the snapshot cadence. 5
minutes between snapshots = 5 minutes of mutations potentially lost
on a crash. For a filesystem with active workloads, that's too much.

The journal closes the gap. Every meta mutation that goes through the
FS layer (Create, Unlink, Rename, Setattr, ...) appends a row to the
local `journal` table inside the same tx. The poster periodically
batches recent rows into a channel message. Recovery applies snapshot
+ replay-from-journal, narrowing the loss window to seconds.

## The snapshot itself

`internal/snapshot/snapshot.go`:

```go
type Snapshot struct {
    Bytes      []byte
    JournalSeq int64
    FSUUID     string
}

func Take(ctx context.Context, m *meta.Store) (*Snapshot, error) {
    seq, err := m.LastJournalSeq(ctx)
    if err != nil { return nil, err }

    fsUUID, err := m.FSUUID(ctx)
    if err != nil { return nil, err }

    // VACUUM INTO writes a clean copy of the DB to a temp file.
    tmp, err := os.CreateTemp("", "telfs-snap-*.sqlite")
    if err != nil { return nil, err }
    defer os.Remove(tmp.Name())
    tmp.Close()

    if _, err := m.DB().ExecContext(ctx,
        fmt.Sprintf("VACUUM INTO '%s'", tmp.Name())); err != nil {
        return nil, fmt.Errorf("vacuum into: %w", err)
    }

    raw, err := os.ReadFile(tmp.Name())
    if err != nil { return nil, err }

    var gz bytes.Buffer
    gw := gzip.NewWriter(&gz)
    if _, err := gw.Write(raw); err != nil { return nil, err }
    if err := gw.Close(); err != nil { return nil, err }

    return &Snapshot{
        Bytes:      gz.Bytes(),
        JournalSeq: seq,
        FSUUID:     fsUUID,
    }, nil
}
```

`VACUUM INTO` is the right tool for "a clean consistent copy of the
DB." It:

- Writes pages in PRIMARY KEY order, defragging.
- Doesn't include the WAL — the copy is a single SQLite file, not
  a triple.
- Holds a read lock for the duration but doesn't block writes (in WAL
  mode). Writes that happen during VACUUM appear in the next snapshot.

The output is gzipped because SQLite pages are very compressible
(lots of padding to page boundaries). The gzip stage shaves the typical
snapshot size by 70–80%.

## The cadence loop

`Manager.Run`:

```go
func (m *Manager) Run(ctx context.Context) error {
    if m.Interval <= 0 { m.Interval = DefaultInterval }
    if err := m.Once(ctx); err != nil {
        m.Logger.Printf("[snapshot] initial snapshot failed: %v", err)
    }
    t := time.NewTicker(m.Interval)
    defer t.Stop()
    for {
        select {
        case <-ctx.Done(): return nil
        case <-t.C:
            if err := m.Once(ctx); err != nil {
                m.Logger.Printf("[snapshot] periodic snapshot failed: %v", err)
            }
        }
    }
}
```

5-minute ticker. Initial snapshot at start (because a freshly recovered
DB is "stale" relative to the channel — re-uploading establishes a
baseline). The loop logs failures but doesn't surface them; the next
tick is the recovery mechanism.

## Skipping no-op snapshots

`Once` doesn't unconditionally upload. It checks:

```go
currentSeq, _ := m.Meta.LastJournalSeq(ctx)
prevSeq, hadPrev, _ := loadCurrentJournalSeq(ctx, m.Meta)
if hadPrev && currentSeq == prevSeq {
    m.Logger.Printf("[snapshot] skipped: no new journal entries since seq %d", currentSeq)
    return nil
}
```

If the journal hasn't advanced since the last snapshot, nothing has
changed. A new snapshot would be byte-identical to the one already on
the channel — pointless upload + delete cycle that burns rate budget.

The cost of checking is cheap (one `meta_kv` read + a query against
sqlite_sequence). The savings on an idle FS are real: ~50% of the
daemon's RPCs were no-op snapshots on a quiet system.

> See commit `1528391`.

## Retention pruning

After a successful upload, prune to keep the newest 12 snapshots:

```go
func (m *Manager) pruneRetention(ctx context.Context, fsUUID string, justUploaded int) error {
    keep := m.retention()  // default 12
    snaps, err := m.Session.ListSnapshots(ctx, fsUUID, keep+64)
    if err != nil { return err }
    if len(snaps) <= keep { return nil }
    toDelete := make([]int, 0, len(snaps)-keep)
    for _, s := range snaps[keep:] {
        if s.MessageID == justUploaded { continue }
        toDelete = append(toDelete, s.MessageID)
    }
    if len(toDelete) == 0 { return nil }
    return m.Session.DeleteChannelMessages(ctx, toDelete...)
}
```

12 snapshots × 5-minute cadence = ~1 hour of recoverable history. That
covers "I deleted the wrong thing 30 minutes ago" without paying
unbounded channel storage. The retention is configurable (it's a
field on `Manager`); commercial users with stricter compliance can
raise it.

The "list with cap = keep + 64" pattern is defensive: if a chatty
channel has pushed many other messages between snapshots, the search
might not return *every* old snapshot in one call. We delete what we
see; the rest are cleaned up by `telfs gc` or the next prune.

## The journal poster

`internal/snapshot/journal_poster.go`. The structure mirrors the
snapshot manager: a `Run` with a ticker (5 s), draining the local
`journal` table to the channel.

```go
func (p *Poster) drainOnce(ctx context.Context) error {
    fsUUID, _ := p.Meta.FSUUID(ctx)
    pending, err := p.Meta.PendingJournal(ctx)
    if err != nil { return err }
    if len(pending) == 0 { return nil }

    const batchSize = 1000
    for start := 0; start < len(pending); start += batchSize {
        end := start + batchSize
        if end > len(pending) { end = len(pending) }
        batch := pending[start:end]
        entries := make([]tg.JournalBatchEntry, len(batch))
        for i, e := range batch {
            entries[i] = tg.JournalBatchEntry{Seq: e.Seq, Op: e.OpJSON}
        }
        if _, err := p.Session.UploadJournalOpBatch(ctx, entries,
            time.Now().Unix(), fsUUID); err != nil { return ... }
        seqs := make([]int64, len(batch))
        for i, e := range batch { seqs[i] = e.Seq }
        if err := p.Meta.BatchMarkJournalPosted(ctx, seqs,
            time.Now().Unix()); err != nil { return ... }
    }
    return nil
}
```

The batching turns "post 1000 small messages" into "post one message
containing 1000 packed ops." A burst of mutations from a tar extract
might produce 50 k journal ops; that's 50 channel messages, not 50
thousand.

The `BatchMarkJournalPosted` you saw in chapter 05 is the local-side
counterpart: one UPDATE for 1000 seqs instead of 1000 sequential
UPDATEs.

> See commits `d86dd39` (the batch upload story) and `bc8175a` (the
> batched-mark-posted story).

## The journal_op format on the channel

A `UploadJournalOpBatch` message contains:

- A magic prefix identifying it as a journal batch.
- An `fs_uuid` so a channel hosting multiple FSes can be partitioned.
- A timestamp.
- A list of `(seq, op_json)` entries.

`op_json` is the same per-op record we store in the local `journal`
table. The replay code (`meta.Store.ReplayOp`) understands this shape
and applies it as if the original FUSE op had just happened.

## Cold recovery

When you `telfs mount` and there's no local DB:

```go
// cmd/telfs/main.go (abridged)
if !fileExists(cfg.DBPath()) {
    if err := tryRecover(signalCtx, cfg); err != nil {
        return fmt.Errorf("cold recovery: %w", err)
    }
}
```

`tryRecover`:

1. Connects to Telegram with the existing session.
2. Calls `session.FindLatestSnapshot(fsUUID)` which walks recent
   channel messages looking for the snapshot magic + matching
   `fs_uuid`.
3. Downloads it.
4. Decompresses, writes as the new `db.sqlite`.
5. Calls `session.ReplayJournalSince(fsUUID, snapshot.JournalSeq)`,
   which fetches all subsequent journal-op batches and applies each
   op via `meta.ReplayOp`.

After recovery, the daemon's `meta_kv.snap_msg_id` and
`snap_journal_seq` are populated; the next snapshot tick re-baselines
the now-restored DB onto the channel.

The `fs_uuid` poison check is important: it's stored in `meta_kv` at
first mount and embedded in every snapshot/journal-op message. If
someone runs `telfs mount --recover` against a channel that's hosted
*a different* TelFS before, the fs_uuid won't match and recovery
refuses to apply. Otherwise you'd silently merge two filesystems'
metadata.

## What's lost on a hard kill

The recovery model leaves a small window:

- Mutations applied to the DB but not yet in the journal table → can't
  happen (we append to journal in the same tx as the mutation).
- Mutations in the journal table but not yet posted to the channel →
  lost (the journal poster runs every 5 s).
- Chunk uploads that succeeded but `chunk_map` row hasn't landed yet
  → the pending wire is on disk; `recoverPending` will re-submit on
  next start.

So worst case: ~5 s of metadata mutations and any chunk uploads that
were happening at the moment of the kill. The repair pass at startup
(chapter 05) cleans up partial chunk_map state. Metadata not in the
journal is gone — the kernel told the FS "this happened" but the
channel doesn't know yet.

For absolute durability, an app calls `fsync()`, which our
`Sync` implements as "wait for all in-flight uploads + verify dirty
empty + SetSize." That doesn't cover the journal, though. The next
journal tick is what makes a mutation channel-durable. We could shrink
the journal tick to 1 s for tighter durability at the cost of more
messages; 5 s was our balance point.

## The `telfs snapshot` command

```
telfs snapshot list                  # list channel snapshots, newest first
telfs snapshot restore [--yes] <id>  # roll back local DB to that snapshot
```

`restore` is rarely the right tool — usually you want to fix forward,
not roll back. It exists for "I deleted everything 30 minutes ago and
the trash GC already ran." See chapter 11 for the safer-by-default
trash mechanism.

## Next chapter

[10 — Crypto and compression](10-crypto-and-compression.md) — what
exactly we hand to the channel.
