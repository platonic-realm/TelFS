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
| Encryption at rest (AES-256-GCM, Argon2id KDF) | ✓ — chunk bytes only; metadata still plaintext |
| Multi-mounter coordination | ✗ assume one mount per channel |

## Bot vs user auth

TelFS authenticates as either:

| Mode | Setup | Limits |
|---|---|---|
| **User** (default) | `telfs login` → phone + code + 2FA | Full dialog access, can scan channels by id, 2 GB per upload via MTProto |
| **Bot** | `telfs login --bot <token>` (token from @BotFather) | Same 2 GB per upload (MTProto — NOT the 50 MB HTTP Bot API), but bots can't enumerate dialogs so `channel set` needs both `--access-hash` and `<id>` |

Bot mode workflow:

```sh
# 1. Get the channel access_hash from a user-account TelFS, OR from your
#    own preferred tool. With a user profile already set up:
TELFS_PROFILE=user-acct ./bin/telfs channel info        # prints access_hash

# 2. Talk to @BotFather, create a bot, get a token.
# 3. In the Telegram app, add the bot to your private channel as ADMIN.

# 4. New profile + bot login + manual channel binding:
./bin/telfs profile create my-bot
TELFS_PROFILE=my-bot ./bin/telfs login --bot 123456:ABCDEF…
TELFS_PROFILE=my-bot ./bin/telfs channel set --access-hash <H> <channel-id>
TELFS_PROFILE=my-bot ./bin/telfs mount ~/External
```

The MTProto pipeline is identical between user and bot modes — same
chunker, same encryption, same snapshot cadence. The only behavioral
differences are: dialog enumeration is empty for bots, and the bot
must be a channel admin before posting.

## Quick start

You need Go 1.22+, FUSE (`fusermount` from your distro), and a
Telegram API ID + hash (free, get them at
<https://my.telegram.org/apps>).

```sh
# Build.
make build                                  # → bin/telfs

# (Optional) pick a non-default chunk size before first mount.
# Default is 4 MiB; valid range is 64 KiB..1.5 GiB, power of two.
# Once any chunk lands the choice is immutable.
./bin/telfs init --chunk-size $((16*1024*1024))   # e.g. 16 MiB

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

## Encryption

TelFS supports AES-256-GCM encryption for chunk contents. **Only chunk
bytes are encrypted; filenames, directory structure, sizes, and
timestamps are visible to anyone with channel access** because the
snapshot blob in the channel contains the SQLite metadata in the
clear. Plan accordingly — if metadata privacy matters, don't use
TelFS today.

Enable encryption on a **fresh** filesystem (it refuses if any chunk
already exists; the migration path is `cp -r` from an old TelFS to a
new encrypted one):

```sh
./bin/telfs encrypt init                    # interactive passphrase
# or for unattended setup:
TELFS_PASSPHRASE='your secret' ./bin/telfs encrypt init
./bin/telfs encrypt status
```

Once enabled, every mount requires the passphrase. Set
`TELFS_PASSPHRASE` to skip the prompt:

```sh
TELFS_PASSPHRASE='your secret' ./bin/telfs mount ./mnt
```

Wrong passphrase fails fast — a canary in `meta_kv` is decrypted
before any user data flows, so a typo surfaces immediately instead
of as cryptic EIOs.

### What encryption protects against

| Threat | Protected? |
|---|---|
| Telegram operator reading file contents | ✓ (chunks are ciphertext) |
| Telegram operator reading file *names* and tree structure | ✗ (snapshot blob is plaintext metadata) |
| Channel members reading file contents | ✓ |
| Someone with channel-WRITE access substituting an old ciphertext for the same `(ino, idx)` slot | ✗ (per-chunk replay; single-mounter assumption — you're the only legitimate poster) |
| Someone splicing ciphertexts between files | ✓ (AAD binds ciphertext to `(ino, idx)`) |
| Tampering with chunk bytes | ✓ (GCM tag fails) |
| Local-disk theft on a machine where you mount unattended via `TELFS_PASSPHRASE` env | partial (passphrase is in the shell env; key never persists to disk) |

### Crypto parameters

- Cipher: AES-256-GCM, 12-byte random nonce per chunk, 16-byte tag
- AAD: `(ino, idx)` packed big-endian (12 bytes)
- KDF: Argon2id (time=3, memory=64 MiB, threads=4)
- Salt: 16 random bytes per filesystem, stored in `meta_kv`
- Canary: encrypted `"telfs-canary-v1"` in `meta_kv`, verified at mount

The KDF parameters are stored in `meta_kv['crypto_argon_params']`,
so a future TelFS with stronger defaults can still mount older
filesystems.

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
| Chunk size | 4 MiB default, **per-FS configurable** | Sequential read/write throughput vs `chunk_map` row count. Set via `telfs init --chunk-size <N>` BEFORE first mount; immutable thereafter. Power of two, [64 KiB, 1.5 GiB]. |
| POSIX surface | files, dirs, symlinks, hardlinks, xattrs (`user.*`) | Enough to host typical workloads |
| Encryption | AES-256-GCM, Argon2id KDF, opt-in via `telfs encrypt init` | Chunk bytes only; metadata still plaintext in the channel |
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
- **Encryption protects chunk bytes only.** Filenames, sizes, and
  directory structure are visible to anyone with channel access
  (the snapshot blob carries the SQLite metadata in the clear). A
  future milestone could wrap the snapshot too.
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
