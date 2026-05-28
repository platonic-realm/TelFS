# TelFS architecture

## Goal

Expose a private Telegram channel as a mountable POSIX filesystem. The
user sees a normal directory tree; under the hood, file contents are
chunked and stored as messages in the channel, and the directory tree
is kept in a local SQLite database that is periodically backed up to
the same channel.

## High-level layers

```
┌──────────────────────────────────────────────────────────────┐
│                       Kernel / FUSE                          │
└──────────────────────────────────────────────────────────────┘
                              ▲
                              │ syscalls (open, read, write, …)
                              ▼
┌──────────────────────────────────────────────────────────────┐
│  internal/fs  — go-fuse Node/handle  (single Node type for   │
│   files, dirs, symlinks; dispatch on meta.Kind)              │
└──────────────────────────────────────────────────────────────┘
        │                          │                  │
        ▼                          ▼                  ▼
┌────────────────┐        ┌────────────────┐   ┌──────────────┐
│ internal/meta  │        │ internal/chunk │   │  internal/tg │
│  SQLite + WAL  │        │  LRU on disk + │   │  gotd/td     │
│  inodes        │        │  Reader (paged │   │  Session     │
│  dirents       │        │   chunk reads) │   │  (long-lived │
│  chunk_map     │        │  Writer (dirty │   │   MTProto)   │
│  xattrs        │        │   chunks +     │   │  FLOOD_WAIT  │
│  journal*      │        │   flush)       │   │   middleware │
│  meta_kv       │        └────────────────┘   └──────────────┘
└────────────────┘                 │                  ▲
        │                          └──────────────────┘
        ▼                              upload / download
┌──────────────────┐
│ internal/snapshot│
│  VACUUM INTO →   │
│  gzip → upload   │
│  Manager (5-min  │
│   cadence + ^C)  │
└──────────────────┘
```

`*journal` is declared in the schema (M2) and reserved for the future
meta-op posting mechanism — M5 ships snapshots only and leaves the
journal unused.

## Modules

| Module | Responsibility |
|---|---|
| `cmd/telfs` | CLI entrypoint: `login`, `channel {list,set,info,ping}`, `mount`, `gc`, `debug seed-file`. |
| `internal/fs` | go-fuse `Node` + `fileHandle`. Single Node type dispatches on inode kind. `Backend` bundles meta + chunk read/write + read-only flag. |
| `internal/meta` | SQLite schema (six tables) + CRUD. Owns the authoritative metadata. `fs_uuid` bootstrapped on first Open. |
| `internal/chunk` | Fixed 4 MiB chunker. `Cache` is the disk-backed LRU read path. `Reader` stitches chunks for arbitrary `ReadAt`. `Writer` buffers dirty chunks per-handle and flushes on close. |
| `internal/tg` | gotd wrapper. `Client.RunSession` holds one MTProto connection for the daemon's lifetime. `Session` exposes channel ops, document upload/download, snapshot helpers, history walk. Uses `gotd/contrib/middleware/floodwait` so FLOOD_WAIT_N retries are transparent. |
| `internal/snapshot` | `Take` runs SQLite `VACUUM INTO` to produce a read-consistent copy, then gzips. `Restore` is the inverse. `Manager.Run` is the cadence goroutine (5-min ticker). |
| `internal/crypto` | `Cipher` interface; `NoopCipher` is the v1 implementation. M7 will plug in AES-256-GCM here without rewriting the chunk pipeline. |
| `internal/config` | TOML config + env overrides; data-dir resolution. |

## Storage model (SQLite)

```sql
CREATE TABLE inodes (
  ino            INTEGER PRIMARY KEY AUTOINCREMENT,
  kind           TEXT    NOT NULL,            -- 'file' | 'dir' | 'symlink'
  mode           INTEGER NOT NULL,
  uid            INTEGER NOT NULL DEFAULT 0,
  gid            INTEGER NOT NULL DEFAULT 0,
  size           INTEGER NOT NULL DEFAULT 0,
  nlink          INTEGER NOT NULL DEFAULT 1,
  mtime          INTEGER NOT NULL DEFAULT 0,
  ctime          INTEGER NOT NULL DEFAULT 0,
  symlink_target TEXT
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
  PRIMARY KEY (ino, idx),
  FOREIGN KEY (ino) REFERENCES inodes(ino) ON DELETE CASCADE
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
  posted_at INTEGER                              -- nullable; unused in M5
);

CREATE TABLE meta_kv (
  key   TEXT PRIMARY KEY,                       -- 'fs_uuid', 'snap_msg_id', …
  value BLOB NOT NULL
);
```

Foreign-key cascades from `inodes(ino)` mean a single
`DELETE FROM inodes WHERE ino = ?` cleans up that inode's chunk_map
rows, xattrs, and any remaining hardlink dirents. We never delete
directly out of chunk_map (except by `DeleteChunksAbove` for
truncate).

`nlink` is the POSIX hardlink refcount for regular files; TelFS does
NOT maintain the `2 + nsubdirs` convention for directories.

## Channel message format

| Caption | Body | Identifies |
|---|---|---|
| `""` (empty) | document (gzipped is fine; we don't compress chunks) | a chunk |
| `{"k":"snap","seq":N,"ts":T,"fs_uuid":U}` | document (gzipped SQLite copy) | a snapshot |
| other text | text-only message | ignored (test pings, manual posts) |

`telfs gc` uses this classification to identify orphans without
ambiguity.

## Critical paths

### Read

`Node.Read` → `chunk.Reader.ReadAt(ino, dest, off)`:

1. Map `[off, off+len)` to a contiguous range of chunk indices.
2. For each idx:
   - Look up `chunk_map` for `(ino, idx)`.
   - If hit: ask `chunk.Cache.Get(key, msg_id)`. On cache hit, read
     from disk; on miss, the Cache's Fetcher (`tg.Session.DownloadDocument`)
     pulls the document, writes to cache, returns bytes.
   - If `chunk_map` miss: treat as EOF (file ends here).
3. Slice the relevant range and copy into `dest`.

### Write

`Node.Open` returns a `fileHandle` that owns a `chunk.Writer`.
`Write` → `Writer.WriteAt`:

1. Identify affected chunks.
2. For each, load existing bytes into a dirty buffer on first touch
   (via `Cache.Get`; on miss, download).
3. Apply the write to the dirty buffer; extend if growing past chunk
   size.
4. If total dirty bytes > 64 MiB, eagerly flush the oldest dirty
   chunk inside `WriteAt` (eager flush is the "streaming write
   doesn't OOM the daemon" knob).

`Flush` (called by FUSE on close / `fsync`):

1. Walk dirty chunks in ascending idx order.
2. For each: upload via Session, update `chunk_map`, invalidate the
   read cache for that key.
3. After ALL chunks land: bump `inodes.size` to the new logical size.
4. On any per-chunk failure: stop immediately. Earlier chunks are
   committed; later chunks remain dirty for the next Flush. The
   user's `Flush` call returns EIO; size update doesn't fire (so
   readers never see a half-written file as "smaller than it is").

### Mount lifecycle

```
   cmdMount(signalCtx, args)
       │
       ▼
   cfg.Load + RequireChannel
       │
       ▼  (db.sqlite missing?)
   tryRecover(signalCtx, cfg)             ← transient RunSession,
       │   FindLatestSnapshot → Download    finds the most-recent
       │   → snapshot.Restore → dbPath      k:"snap" message, gunzips
       ▼
   meta.Open(dbPath)
       │
       ▼
   client.RunSession(Background, fn) ─────┐  gotd ctx is independent of
       │                                  │  signalCtx so the final
       ▼                                  │  snapshot can post AFTER
   fn(sessCtx, sess):                     │  the user signal arrives.
     chunk.Cache + Reader, fs.Mount       │
     go snapMgr.Run(snapCtx)              │  cadence ticker
       │                                  │
       ▼                                  │
     <-signalCtx.Done()                   │  user ^C
       │                                  │
       ▼                                  │
     stopSnap();  <-snapDone              │  stop periodic ticker
       │                                  │
       ▼                                  │
     snapMgr.Once(WithTimeout(60s))       │  final snapshot — gotd alive
       │                                  │
       ▼                                  │
     server.Unmount(); server.Wait()      │  drain FUSE
       │                                  │
       └───── return nil ─────────────────┘  gotd shuts down cleanly
```

The teardown order matters: tearing down the gotd session before the
FUSE server drains would crash in-flight Reads with "client already
closed"; running the final snapshot inside `Manager.Run`'s
`ctx.Done()` branch was an earlier attempt that fired during gotd
teardown and failed with "engine closed".

## Concurrency

- One `go-fuse` server goroutine per VFS call (library-managed).
- One gotd connection for the daemon's lifetime; all session ops
  share it. The FLOOD_WAIT middleware serializes retries
  transparently.
- One snapshot goroutine (`snapshot.Manager.Run`).
- `chunk.Cache` is mutex-guarded; disk I/O happens outside the lock.
- `chunk.Writer` is per-handle, lock-guarded. Two handles writing
  the same file race at flush time (last-flush-wins).

## What's deliberately not in v1

- **Encryption.** Interface is in place (`internal/crypto.Cipher`);
  M7 will plug in AES-256-GCM. Today's bytes go to Telegram in the
  clear.
- **Inline TG deletes.** Chunk overwrites and unlinks do NOT delete
  the backing channel messages; old chunks become orphans, reclaimed
  by `telfs gc`. This trade keeps every "delete" code path local
  and side-effect-free.
- **Multi-mounter coordination.** Two concurrent mounts of the same
  channel will race. `meta_kv['lock_ts']` is reserved for a future
  lock-message-in-channel mechanism.
- **Meta-op posting.** M5 ships snapshots only. The journal table is
  schema-allocated but unused. Recovery window = the snapshot
  cadence interval (~5 min).
- **Tail-of-channel polling.** No "watch the channel for external
  mutations" — TelFS assumes it's the only writer.
