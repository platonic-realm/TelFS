# TelFS

A FUSE filesystem that uses a private Telegram channel as its storage
backend. Files are split into 4 MiB chunks and uploaded as channel
messages; a local SQLite database holds the metadata. Periodic
gzipped DB snapshots are posted to the same channel so the filesystem
survives loss of the local DB.

## What works today

End-to-end verified on a real Telegram account + private channel:

| Capability | Status |
|---|---|
| Login (MTProto, phone + code + 2FA) | ✓ |
| Read/write mount (POSIX surface incl. hardlinks, symlinks, xattrs) | ✓ |
| Multi-chunk files up to ~2 GiB per file | ✓ |
| Cross-chunk-boundary reads/writes | ✓ verified by integration test |
| Cold-mount recovery from channel snapshot | ✓ |
| Read-only mode (`--readonly`) | ✓ |
| FLOOD_WAIT rate-limit handling | ✓ (gotd middleware) |
| Orphan chunk + stale snapshot GC | ✓ (`telfs gc`) |
| Encryption at rest | ✗ deferred to M7 |
| Multi-mounter coordination | ✗ assume one mount per channel |

## Quick start

You need Go 1.22+, FUSE (`fusermount` from your distro), and a
Telegram API ID + hash (free, get them at
<https://my.telegram.org/apps>).

```sh
# Build.
make build                                  # → bin/telfs

# Configure credentials (one-time). The .telfs/ directory is created
# in the current working directory and holds the session + local DB +
# config. Add api_id / api_hash to .telfs/config.toml, OR set them in
# the environment:
export TELFS_API_ID=12345678
export TELFS_API_HASH=...

# Log in (interactive — type the SMS / Telegram code that arrives, and
# your 2FA password if set).
./bin/telfs login

# Pick a channel to use as the backend. Create a *private* channel in
# Telegram first; then:
./bin/telfs channel list
./bin/telfs channel set <id>
./bin/telfs channel ping                    # smoke test: post + read back

# Mount.
mkdir mnt
./bin/telfs mount ./mnt                     # foreground; ^C to unmount
```

Then in another shell:

```sh
echo hi > mnt/hello.txt
cat mnt/hello.txt
mkdir mnt/notes
mv mnt/hello.txt mnt/notes/
ln mnt/notes/hello.txt mnt/notes/alias      # hardlink
ln -s notes/hello.txt mnt/link              # symlink
setfattr -n user.tag -v important mnt/notes/hello.txt
```

Press `^C` on the daemon to unmount. The unmount sequence:

1. Snapshot the meta DB → upload to channel as a new snapshot message.
2. `fusermount -u` the kernel mount.
3. Wait for in-flight FUSE requests to drain.
4. Tear down the MTProto session.

## Mount options

```
telfs mount [--readonly] [--allow-other] [--debug] <mountpoint>
```

- `--readonly` — all mutating ops fail with EROFS. Useful for verifying
  recovered state without risk.
- `--allow-other` — allow other users to access the mount. Needs
  `user_allow_other` in `/etc/fuse.conf`.
- `--debug` — log every FUSE op (very chatty).

## Recovery

If you lose `.telfs/db.sqlite`, the next `telfs mount` automatically
scans the channel for the most-recent snapshot and restores from it.
At worst you lose whatever was written between the last snapshot
cycle (cadence: every 5 minutes plus on clean unmount).

```sh
rm -rf .telfs/db.sqlite .telfs/cache
./bin/telfs mount ./mnt
# Local DB missing — scanning channel for snapshot…
# Found snapshot msg=29 (ts 2026-05-28T18:03:00Z, fs_uuid=…)
# Restored 1489 gzipped bytes → ./.telfs/db.sqlite
# Mounted at ./mnt. Press ^C to unmount.
```

If the channel has no snapshot (first-ever use), recovery is a no-op
and TelFS starts with an empty filesystem.

## Maintenance

```sh
./bin/telfs gc                              # dry-run report
./bin/telfs gc --yes                        # actually delete orphans
```

`telfs gc` walks the channel and identifies:

- **Orphan chunks** — document messages whose msg_id isn't in
  chunk_map (created by file overwrites/unlinks, which intentionally
  don't delete inline — see "Design choices" below).
- **Stale snapshots** — snapshot-caption messages other than the
  current one recorded in `meta_kv`.

The default is dry-run; pass `--yes` to delete.

## Configuration

`.telfs/config.toml` (gitignored — never commit it):

```toml
api_id = 12345678
api_hash = "..."
phone = "+15551234567"        # optional — prompted if missing
dc = 1                        # optional — starting datacenter; default 2

[channel]
id = 1234567890
access_hash = -9876543210123456789
title = "My TelFS"
```

Environment overrides:

| Var | Effect |
|---|---|
| `TELFS_DIR` | data directory (default `./.telfs`) |
| `TELFS_API_ID` | overrides config |
| `TELFS_API_HASH` | overrides config |
| `TELFS_PHONE` | skip the phone prompt at login |
| `TELFS_DC` | override starting datacenter |

## Design choices

| What | Choice | Why |
|---|---|---|
| Language | Go | `hanwen/go-fuse` + `gotd/td` + `modernc.org/sqlite` (pure-Go SQLite) |
| Telegram API | MTProto user API (gotd) | 2 GB per file; Bot API's 50 MB cap is too small for chunks |
| Metadata | local SQLite + periodic channel snapshots | Fast reads; recovery window = snapshot cadence |
| Chunk size | 4 MiB | Sequential read/write throughput vs `chunk_map` row count |
| POSIX surface | files, dirs, symlinks, hardlinks, xattrs (`user.*`) | Enough to host typical workloads |
| Encryption | deferred to M7 | Pipeline hook in place (`internal/crypto.Cipher` is a no-op identity) |
| Inline TG deletes | none for chunks; snapshots delete the prior one | M4 trade — never destroys user data inline; orphans cleaned by `telfs gc` |
| Snapshot cadence | every 5 min + on clean unmount | Bounded recovery window without burning network bandwidth |

## Known limits

- **Single mounter per channel.** Two concurrent mounts will race; no
  locking. The schema reserves `meta_kv[lock_ts]` for a future
  coordination mechanism.
- **Up-to-5-minute recovery window.** If the daemon crashes hard
  (kill -9, kernel panic, machine off), data written since the last
  cadence snapshot is lost. M5 deferred meta-op posting; revisit if
  this bites in real use.
- **No encryption yet.** Anyone with access to the channel (or
  Telegram itself) sees your chunk bytes. M7.
- **One channel = one TelFS.** `fs_uuid` is stored in `meta_kv` and
  baked into every snapshot caption; recovery filters by it, so
  reusing a channel for a fresh TelFS instance will leave old
  snapshots as garbage until `telfs gc` reaps them.
- **gotd default DC 2 may be firewalled.** If `telfs login` hangs at
  the handshake, try `dc = 1` in `config.toml` (or `TELFS_DC=1`).

## Architecture

See [`docs/architecture.md`](docs/architecture.md) for the module
layout, SQLite schema, channel message format, and recovery model.

## License

MIT — see [`LICENSE`](LICENSE).
