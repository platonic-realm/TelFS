# TelFS

A FUSE filesystem that uses a private Telegram channel as its storage backend.
Files are chunked and uploaded as channel messages; the filesystem hierarchy is
exposed to the user as a normal POSIX mount.

> Status: **early development**. See [`docs/architecture.md`](docs/architecture.md).

## How it works (one-paragraph version)

A local SQLite database is the authoritative metadata store (inodes, dir entries,
hardlink refcounts, xattrs, chunk → message-id mapping). File contents are split
into 4 MiB chunks and uploaded as messages to a private Telegram channel via the
MTProto user/client API (`gotd/td`). Reads pull chunks on demand and cache them
on disk (LRU). To survive loss of the local DB, TelFS periodically posts a
gzipped DB snapshot back to the channel and appends a write-ahead `meta-op`
message for each mutation, so a cold mount can rebuild state by scanning the
channel.

## Quick start (once implemented)

```sh
make build
./bin/telfs login                  # one-time MTProto auth
./bin/telfs channel set <chan-id>  # pick a private channel
./bin/telfs mount ./mnt            # foreground daemon

# in another shell
echo hi > mnt/hello.txt
cat mnt/hello.txt
fusermount -u mnt
```

## Design choices

| What | Choice |
|---|---|
| Language | Go (`hanwen/go-fuse`, `gotd/td`, `modernc.org/sqlite`) |
| Telegram API | MTProto user/client API (2 GB per file) |
| Metadata | Local SQLite + periodic channel snapshots |
| Chunk size | 4 MiB |
| POSIX | files, dirs, symlinks, hardlinks, xattrs (`user.*`) |
| Encryption | deferred to v2 (pipeline hook in place) |

See [`docs/architecture.md`](docs/architecture.md), [`docs/channel-format.md`](docs/channel-format.md),
and [`docs/recovery.md`](docs/recovery.md) for the gory details.

## License

MIT — see [`LICENSE`](LICENSE).
