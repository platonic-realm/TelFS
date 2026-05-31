# 05 — The metadata store

Of all the subsystems in TelFS, this one cost the most engineering
hours per line of code. SQLite is famously easy to "get working" and
famously hard to "get right under sustained concurrent write load."
Most of this chapter is the journey to that conclusion.

## Why SQLite

We needed a local store that:

- **Is fast** — every FUSE `getattr` traverses it, and a `ls` of a
  10k-file directory is one query.
- **Is durable** — a SIGKILL must leave the DB in a consistent state.
- **Survives concurrent reads + writes** without external coordination.
- **Ships as a single file** so `telfs profile export` is `tar czf`.
- **Is pure Go** so we can build a single static binary with no cgo.

That's a tight constraint set. SQLite checks every box if we use the
right driver and the right pragmas. We use
`modernc.org/sqlite`, a pure-Go transpilation of SQLite's C source.
It's slower than `mattn/go-sqlite3` by ~20%, but the no-cgo property is
non-negotiable for our deployment story.

## The schema

Defined as one string literal in `internal/meta/store.go`. The tables:

```sql
CREATE TABLE inodes (
  ino            INTEGER PRIMARY KEY AUTOINCREMENT,
  kind           TEXT    NOT NULL,        -- 'file' | 'dir' | 'symlink'
  mode           INTEGER NOT NULL,
  uid, gid       INTEGER NOT NULL DEFAULT 0,
  size           INTEGER NOT NULL DEFAULT 0,
  nlink          INTEGER NOT NULL DEFAULT 1,
  mtime, ctime   INTEGER NOT NULL DEFAULT 0,
  symlink_target TEXT,
  chunk_size     INTEGER NOT NULL DEFAULT 4194304  -- per-file tier
);

CREATE TABLE dirents (
  parent_ino INTEGER NOT NULL,
  name       TEXT    NOT NULL,
  child_ino  INTEGER NOT NULL,
  PRIMARY KEY (parent_ino, name),
  FOREIGN KEY (parent_ino) REFERENCES inodes(ino) ON DELETE CASCADE,
  FOREIGN KEY (child_ino)  REFERENCES inodes(ino) ON DELETE CASCADE
);

CREATE INDEX idx_dirents_child ON dirents(child_ino);

CREATE TABLE chunk_map (
  ino           INTEGER NOT NULL,
  idx           INTEGER NOT NULL,
  tg_message_id INTEGER NOT NULL,
  size          INTEGER NOT NULL,
  pack_offset   INTEGER,          -- for packed chunks (NULL otherwise)
  pack_size     INTEGER,
  PRIMARY KEY (ino, idx)
);

CREATE TABLE xattrs (
  ino   INTEGER NOT NULL,
  name  TEXT    NOT NULL,
  value BLOB    NOT NULL,
  PRIMARY KEY (ino, name),
  FOREIGN KEY (ino) REFERENCES inodes(ino) ON DELETE CASCADE
);

CREATE TABLE journal (
  seq       INTEGER PRIMARY KEY AUTOINCREMENT,
  op_json   BLOB    NOT NULL,
  posted_at INTEGER
);

CREATE TABLE meta_kv (
  key   TEXT PRIMARY KEY,
  value BLOB NOT NULL
);

CREATE TABLE chunk_blob (
  hash          BLOB    PRIMARY KEY,
  tg_message_id INTEGER NOT NULL,
  size          INTEGER NOT NULL
);

CREATE INDEX idx_chunk_blob_msgid ON chunk_blob(tg_message_id);
CREATE INDEX idx_chunk_map_msgid  ON chunk_map(tg_message_id);
```

### Why these tables and not others

Several alternatives we considered and rejected:

- **One big "files" table with a JSON column for chunks.** Rejected:
  every chunk_map mutation would be a JSON parse/re-encode/string
  compare. With 100k chunks that's painful, and the SQL got worse not
  better.
- **Separate `files` and `dirs` tables.** Rejected: hardlinks. A POSIX
  hardlink is one inode with multiple dirents. Splitting kind-by-table
  would require synthesizing the relationship in the application; the
  current schema models it directly.
- **`chunk_map` without an explicit `idx` column** (just an ordering
  flag). Rejected: tar's random-write patterns and `truncate` both
  need O(1) lookup of "which chunk holds offset X." That's
  `idx = offset / chunk_size`, and the PRIMARY KEY makes the lookup an
  index seek.
- **A `dirents.parent_ino` foreign key with `ON DELETE RESTRICT`
  instead of CASCADE.** Rejected: `rm -rf` of a populated subtree would
  require explicit recursive deletes from the application. CASCADE
  pushes the recursion into SQLite, which knows how to do it
  efficiently.

### Why two indexes on `chunk_map.tg_message_id`

- `idx_chunk_map_msgid` makes "find every chunk that references this
  message ID" an index seek. The GC and dedup paths use it.
- The implicit `PRIMARY KEY (ino, idx)` is what every chunk-fetch lookup
  uses.

We pay write amplification on every chunk_map insert (two indexes
maintained) for read efficiency. The math worked out: writes are
batched, reads are constant-time, and the index sizes are small.

## The pragmas, justified

From `internal/meta/store.go`:

```go
dsn := path + "?_pragma=journal_mode(WAL)" +
              "&_pragma=foreign_keys(1)" +
              "&_pragma=synchronous(NORMAL)" +
              "&_pragma=busy_timeout(60000)" +
              "&_txlock=immediate"
```

Each pragma justifies its existence:

- **`journal_mode=WAL`** — write-ahead logging. Lets many readers run
  concurrently with one writer. The alternative (`DELETE`, the SQLite
  default) serializes everything. With our workload that would mean
  every `getattr` waits for every `mkdir`. Untenable.

- **`foreign_keys=1`** — turns on FK enforcement. Required for the
  `ON DELETE CASCADE` on `dirents` and `chunk_map`. Without this
  pragma, SQLite parses the FK and ignores it (legacy compatibility).

- **`synchronous=NORMAL`** — fsync on transaction commit, but skip the
  WAL header fsync. `FULL` would be safer (zero loss on power cut);
  `NORMAL` is the WAL-recommended setting and is enough because the
  WAL itself is the durability record. We accept a few ms of loss on
  power cut (the most-recent transactions might roll back). The
  channel-side journal poster recovers anyway.

- **`busy_timeout=60000`** — when a writer can't acquire the write lock
  because another writer holds it, the driver internally retries with
  a backoff for up to 60 seconds. Without this, you get
  `SQLITE_BUSY (5)` immediately on contention. With it, you only get
  busy-style errors after 60 s — which is approximately never. **But**:
  this does not apply to all busy-class errors. See the war story
  below.

- **`_txlock=immediate`** — this is the one that took the longest to
  land. Every transaction begins with `BEGIN IMMEDIATE` instead of the
  default `BEGIN DEFERRED`. It's worth its own section.

## The `_txlock=immediate` story

By default, a SQLite transaction starts with `BEGIN DEFERRED`. That
means:

1. The transaction acquires a read lock on first read.
2. It acquires the write lock only when it first writes.
3. Between (1) and (2), another writer can sneak in and modify pages
   we read.
4. When we try to write, SQLite detects the snapshot is stale and
   returns `SQLITE_BUSY_SNAPSHOT (517)`.

The application must then re-try the entire transaction from scratch.
That's fine — `WithTx` retries. But:

> **Critical fact**: `busy_timeout` does **not** apply to
> `SQLITE_BUSY_SNAPSHOT`. It fires once, immediately, no retry.

We learned that the hard way. The symptom was tar exiting with
"Cannot open: Input/output error" on a handful of files during a
massive extract. The daemon log showed `insert inode: database is
locked (517)` and the app-level retry exhausted its 5 attempts because
each was hitting 517 immediately (no SQLite-internal wait).

`BEGIN IMMEDIATE` acquires the write lock at the *start* of the
transaction. Now:

1. We hold the write lock from BEGIN.
2. Concurrent writers wait at BEGIN (which `busy_timeout` **does**
   handle — returns `SQLITE_BUSY (5)`).
3. Our snapshot is current at write-lock-time; nothing can invalidate
   it.
4. `SQLITE_BUSY_SNAPSHOT (517)` cannot occur.

The cost is a small loss of concurrency: a read-only transaction that
went through `WithTx` would now take the write lock too. But almost
nothing in the codebase uses `WithTx` for pure reads. The few mixed
read+write paths (notably `ReuseChunkByHash`, which does a SELECT +
INSERT) always benefit from immediate locking anyway.

> See commit `ff7670c` for the full story and the test that
> demonstrates the failure mode.

## `WithTx` — the transaction wrapper

`internal/meta/store.go`:

```go
func (s *Store) WithTx(ctx context.Context, fn func(*sql.Tx) error) (err error) {
    const txMaxAttempts = 10
    backoff := 10 * time.Millisecond
    for attempt := 1; ; attempt++ {
        err = s.withTxOnce(ctx, fn)
        if err == nil {
            return nil
        }
        if !isBusyErr(err) || attempt >= txMaxAttempts {
            return err
        }
        log.Printf("meta.WithTx: SQLITE_BUSY on attempt %d/%d, retrying after %s: %v",
            attempt, txMaxAttempts, backoff, err)
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-time.After(backoff):
        }
        backoff *= 2
        if backoff > 2*time.Second {
            backoff = 2 * time.Second
        }
    }
}
```

10 attempts, exponential backoff up to 2 s, total ~10 s of patience.
Each attempt logs (so you can tell from the daemon log whether
contention is firing). With `_txlock=immediate` we should almost never
see retries — but the safety net is cheap.

`isBusyErr` is a string match for "SQLITE_BUSY" or "database is
locked." We can't use error codes because `modernc.org/sqlite` doesn't
expose them in a stable API. The substring approach is annoying but
the strings are stable across driver versions.

```go
func isBusyErr(err error) bool {
    if err == nil { return false }
    msg := err.Error()
    return strings.Contains(msg, "SQLITE_BUSY") ||
        strings.Contains(msg, "database is locked")
}
```

The callback contract: `fn` must be **idempotent across retries**. The
transaction is rolled back on error, so any work fn did is invisible to
SQLite. fn must not call out to other side-effectful code that can't
unwind (e.g., posting a journal message to Telegram inside the tx —
don't do that).

## `execWithRetry` — for single-statement writes

Not every write goes through `WithTx`. A single-statement UPDATE
doesn't need transactional semantics; just an atomic exec. But single
statements can hit SQLITE_BUSY too. The wrapper:

```go
func (s *Store) execWithRetry(ctx context.Context, query string, args ...any) (sql.Result, error) {
    const maxAttempts = 5
    backoff := 10 * time.Millisecond
    for attempt := 1; ; attempt++ {
        res, err := s.db.ExecContext(ctx, query, args...)
        if err == nil { return res, nil }
        if !isBusyErr(err) || attempt >= maxAttempts { return nil, err }
        log.Printf("meta.execWithRetry: SQLITE_BUSY on attempt %d/%d, retrying after %s: %v",
            attempt, maxAttempts, backoff, err)
        select {
        case <-ctx.Done(): return nil, ctx.Err()
        case <-time.After(backoff):
        }
        backoff *= 2
        if backoff > 500*time.Millisecond { backoff = 500 * time.Millisecond }
    }
}
```

Same shape as `WithTx`. Every hot-path single-statement write —
`PutChunk`, `MarkJournalPosted`, `SetAttrs`, `PutKV`, `SetXattr`,
`AppendJournal`, etc. — routes through it. The init/migration
statements stay on raw `ExecContext` because they run once at open
and can't lose races.

> **War story.** Originally only `WithTx` retried. `PutChunk` was
> direct `ExecContext`. Under load, ~1% of `PutChunk` calls hit
> SQLITE_BUSY, surfaced to the writer's uploadOne, which set
> `uploadErr`, which the next `WriteAt` returned to userspace as EIO.
> 17 k tar files failed. The fix was wholesale: introduce
> `execWithRetry` and route every operational single-statement through
> it. Commit `1814127`.

## MaxOpenConns: 1 → 8

For most of the project's life, `db.SetMaxOpenConns(1)`. A single
connection. Every read, write, and transaction went through it. The
comment said `TODO(M3): re-evaluate this cap.`

That TODO was a real bug. With one connection, every concurrent FUSE
op queues behind whatever the connection is currently doing. A heavy
write (a 1000-row batched journal update) blocks every lookup, readdir,
getattr — for hundreds of ms. Under tar load, those queued ops hit
their own kernel-side deadlines and surfaced as EIO.

The fix was `db.SetMaxOpenConns(8)`. WAL allows many concurrent
readers + one writer; with 8 connections the readers actually parallelize
instead of serializing through one bottleneck. Writers still serialize
at the SQLite level — `_txlock=immediate` makes that contention show up
as plain `BUSY (5)`, which `busy_timeout` handles internally without
the app seeing anything.

8 is not magic. It's "enough to absorb the kernel FUSE op concurrency
without burning RAM." Real measurement on the tar workload showed
diminishing returns past 4; 8 leaves headroom for the snapshot
manager + journal poster + chunk pipeline all wanting reads at once.

## The journal batching story

The journal poster periodically drains rows from the `journal` table
to Telegram and marks them posted. Originally:

```go
// snapshot/journal_poster.go, OLD code
for _, e := range batch {
    if err := p.Meta.MarkJournalPosted(ctx, e.Seq, now); err != nil { ... }
}
```

One `UPDATE` per seq. With a 1000-op batch, that's 1000 sequential
UPDATEs on a shared connection. Each one is fast — but the loop holds
the connection for ~hundreds of ms. Every FUSE op queued behind it
waited.

The fix:

```go
// meta/journal.go
func (s *Store) BatchMarkJournalPosted(ctx context.Context, seqs []int64, postedAt int64) error {
    if len(seqs) == 0 { return nil }
    const chunkSz = 800  // under SQLite's 999-variable cap
    for start := 0; start < len(seqs); start += chunkSz {
        end := start + chunkSz
        if end > len(seqs) { end = len(seqs) }
        chunk := seqs[start:end]
        placeholders := strings.Repeat("?,", len(chunk))
        placeholders = placeholders[:len(placeholders)-1]
        query := "UPDATE journal SET posted_at = ? WHERE seq IN (" + placeholders + ")"
        args := make([]any, 0, 1+len(chunk))
        args = append(args, postedAt)
        for _, s := range chunk { args = append(args, s) }
        if _, err := s.execWithRetry(ctx, query, args...); err != nil { ... }
    }
    return nil
}
```

One UPDATE per chunk of 800 seqs. A 1000-op batch is now 2 UPDATEs,
each a few ms. The poster is back to invisible. Commit `bc8175a`
landed it.

## The repair pass

`internal/meta/repair.go`. Every mount-start, the daemon runs
`RepairPartialFiles`. The query that finds candidates:

```sql
SELECT ino FROM chunk_map
GROUP BY ino
HAVING MAX(idx) + 1 != COUNT(*)
ORDER BY ino
```

This finds inodes where the chunk map has gaps — chunks 0,1,3,4 but
missing 2. The cause: a daemon crash (or kill -9) between PersistPending
and PutChunk. The pending wires get re-submitted by `recoverPending`,
but the rows that *did* land aren't contiguous yet.

The repair truncates the file's `inode.size` to the size covered by
the contiguous prefix (chunks 0,1) and DELETES the unreachable tail
(chunks 3,4 — their TG messages become orphans for `telfs gc`).

This is the strictly safer recovery: rather than report "EIO trying to
read bytes at the gap," we report a shorter file. The user can detect
truncation; they cannot detect EIO mid-read silently.

The exact same pass runs via the `/state` page's "Run repair" button.

## Test discipline

`internal/meta` has heavy test coverage (~50% of the package size is
tests). The patterns worth knowing:

- Every test opens a fresh `Store` in a `t.TempDir()`. No shared
  state between tests.
- The race detector is on for every CI run (`go test -race`). Any
  shared map without a mutex would fail immediately.
- The slow tests (those that exercise BatchMarkJournalPosted with
  1500 seqs, or run repair across 100 inodes) are still under a
  second. Tests that take longer get refactored.

## Next chapter

[06 — The chunk pipeline](06-chunk-pipeline.md) — where bytes actually
travel.
