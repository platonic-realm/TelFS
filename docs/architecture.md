# TelFS architecture

## Goal

Expose a private Telegram channel as a mountable POSIX filesystem. The user
sees a normal directory tree; under the hood, file contents are chunked and
stored as messages in the channel, and the directory tree is kept in a local
SQLite database that is periodically backed up to the same channel.

## High-level layers

```
┌──────────────────────────────────────────────────────────────┐
│                       Kernel / FUSE                          │
└──────────────────────────────────────────────────────────────┘
                              ▲
                              │ syscalls (open, read, write…)
                              ▼
┌──────────────────────────────────────────────────────────────┐
│  internal/fs  — go-fuse Node/Inode handlers                  │
│     translates VFS ops → metadata + chunk ops                │
└──────────────────────────────────────────────────────────────┘
        │                          │                  │
        ▼                          ▼                  ▼
┌────────────────┐        ┌────────────────┐   ┌──────────────┐
│ internal/meta  │        │ internal/chunk │   │  internal/tg │
│  SQLite        │        │  LRU on disk   │   │  gotd/td     │
│  inode tree    │        │  download path │   │  channel ops │
│  chunk_map     │        └────────────────┘   └──────────────┘
│  journal       │                 │                  ▲
└────────────────┘                 └──────────────────┘
        │
        ▼
┌──────────────────┐
│ internal/snapshot│
│  serialize DB    │
│  post to channel │
└──────────────────┘
```

## Modules

| Module | Responsibility |
|---|---|
| `cmd/telfs` | CLI entrypoint (`login`, `channel set`, `mount`, …) and lifecycle. |
| `internal/fs` | `go-fuse` node implementations: `Root`, `Dir`, `File`, `Symlink`. Translates each VFS call into meta + chunk operations. |
| `internal/meta` | SQLite schema, migrations, queries. Owns the authoritative metadata. |
| `internal/chunk` | Fixed-size chunker (4 MiB), staging buffers, LRU disk cache. Hands chunks to `tg` for upload, fetches misses on read. |
| `internal/tg` | Thin wrapper over `gotd/td`. MTProto auth, channel resolution, message posting/fetching, FLOOD_WAIT handling. |
| `internal/snapshot` | Serializes the SQLite DB, gzips it, posts to the channel; replays meta-ops on cold mount. |
| `internal/crypto` | Stable `Cipher` interface. v1 is a no-op (identity). v2 plugs in AES-256-GCM. |
| `internal/config` | Config file (`.telfs/config.toml`) + CLI flag parsing. |

## Storage model (SQLite)

```sql
CREATE TABLE inodes (
  ino INTEGER PRIMARY KEY,
  kind TEXT NOT NULL,            -- 'file' | 'dir' | 'symlink'
  mode INTEGER NOT NULL,
  uid INTEGER, gid INTEGER,
  size INTEGER NOT NULL,
  nlink INTEGER NOT NULL DEFAULT 1,
  mtime INTEGER, ctime INTEGER,
  symlink_target TEXT
);

CREATE TABLE dirents (
  parent_ino INTEGER, name TEXT, child_ino INTEGER,
  PRIMARY KEY (parent_ino, name)
);

CREATE TABLE chunk_map (
  ino INTEGER, idx INTEGER,
  tg_message_id INTEGER,
  size INTEGER,
  PRIMARY KEY (ino, idx)
);

CREATE TABLE xattrs (
  ino INTEGER, name TEXT, value BLOB,
  PRIMARY KEY (ino, name)
);

CREATE TABLE journal (
  seq INTEGER PRIMARY KEY, op_json BLOB, posted_at INTEGER
);

CREATE TABLE meta_kv (key TEXT PRIMARY KEY, value BLOB);
```

The `journal` is a local write-ahead log of metadata mutations that have not
yet been posted to the channel as `meta-op` messages. On startup, any
unposted rows are replayed (idempotently) so we never lose mutations to a
crash between local commit and channel post.

## Critical paths

### Read (`read(fd, off, len)`)

1. Resolve `ino` from the FUSE file handle.
2. Compute the set of `(ino, idx)` chunks covering `[off, off+len)`.
3. For each chunk:
   - If present in `internal/chunk` LRU disk cache → use directly.
   - Else look up `tg_message_id` in `chunk_map`, download via `internal/tg`,
     write to cache.
4. Assemble the requested slice and return it.

### Write (`write(fd, off, buf)`)

1. Identify affected chunks (read-modify-write for the partial first/last).
2. Stage modified chunks in cache (dirty).
3. On `flush`/`fsync` or chunk boundary:
   - Upload each dirty chunk → record new `tg_message_id`.
   - If overwriting an existing chunk, delete the old channel message (best
     effort) to avoid leaks.
   - Append a `meta-op` to the local `journal` and post it to the channel.

### Cold mount

If `.telfs/db.sqlite` is missing or marked unclean:
1. Scan the channel for the latest `snapshot` message.
2. Download, gunzip, restore the DB.
3. Replay any `meta-op` messages with `seq > snapshot.seq`.

## Concurrency

- One `go-fuse` server goroutine per VFS call (the library handles this).
- `internal/tg` serializes MTProto calls behind a single connection; bulk
  uploads use parallel `messages.SendMedia` calls bounded by a semaphore.
- `internal/snapshot` runs in its own goroutine on a timer + mutation counter.

## What's deliberately *not* in v1

- Encryption (interface exists; impl is a no-op).
- Multi-writer / concurrent-mount support (single owner assumed).
- Tail-of-channel polling for external mutations.
- Compression of chunks (we let Telegram handle storage; chunks are dense).
