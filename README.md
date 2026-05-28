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
| Login — phone + code + 2FA (user account) | ✓ |
| Login — bot token via `auth.ImportBotAuthorization` | ✓ |
| Read/write mount (POSIX surface incl. hardlinks, symlinks, xattrs) | ✓ |
| Multi-chunk files up to ~2 GiB per file | ✓ |
| Cross-chunk-boundary reads/writes | ✓ verified by integration test |
| Cold-mount recovery from channel snapshot | ✓ |
| Read-only mode (`--readonly`) | ✓ |
| FLOOD_WAIT rate-limit handling | ✓ (gotd middleware) |
| Orphan chunk + stale snapshot GC | ✓ (`telfs gc`) |
| Channel-side integrity check | ✓ (`telfs fsck`) |
| One-screen profile / FS / channel status | ✓ (`telfs status`) |
| AES-256-GCM chunk encryption, Argon2id KDF | ✓ |
| AES-256-GCM snapshot envelope (metadata-at-rest) | ✓ |
| Persistent LRU chunk cache across daemon restarts | ✓ |
| Profiles + portable tar.gz export/import bundles | ✓ |
| Web management UI (dashboard, login, mount, browser) | ✓ (`telfs web`) |
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

# Pick a profile to live under. Profiles are isolated FS instances at
# ~/.config/telfs/profiles/<name>/ holding config + session + DB + cache.
./bin/telfs profile create main
./bin/telfs profile use main                # sticky default

# Configure credentials (one-time). Either edit the profile's config.toml
# directly OR set them in the environment:
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

If you lose the profile's `db.sqlite`, the next `telfs mount`
automatically scans the channel for the most-recent snapshot and
restores from it. At worst you lose whatever was written between the
last snapshot cycle (cadence: every 5 minutes plus on clean unmount).

```sh
PROFDIR=~/.config/telfs/profiles/main
rm -rf $PROFDIR/db.sqlite $PROFDIR/cache
./bin/telfs mount ./mnt
# Local DB missing — scanning channel for snapshot…
# Found snapshot msg=29 (ts 2026-05-28T18:03:00Z, fs_uuid=…)
# Restored 1489 gzipped bytes → db.sqlite (TFSE envelope decrypted)
# Mounted at ./mnt. Press ^C to unmount.
```

If the channel has no snapshot (first-ever use), recovery is a no-op
and TelFS starts with an empty filesystem.

## Encryption

TelFS supports AES-256-GCM encryption for both chunk contents AND the
metadata snapshot envelope. With encryption enabled:

- Every chunk's bytes are AES-GCM ciphertext, AAD-bound to `(ino, idx)`.
- Every snapshot posted to the channel is wrapped in a **TFSE envelope**
  — `["TFSE"][ver][hdr_len][hdr_json][ciphertext]` — so the SQLite
  metadata (filenames, directory tree, sizes, timestamps) lands as
  ciphertext too, not plaintext.

What an attacker with channel access still learns: the number of chunks,
their approximate sizes (4 MiB rounded), the cadence of mutations, and
the snapshot rhythm. Sizes can leak file lengths to within one chunk.

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
| Telegram operator reading file *names* and tree structure | ✓ (snapshot blob is TFSE-wrapped ciphertext) |
| Channel members reading file contents or metadata | ✓ |
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

The active profile's `config.toml` (created at `~/.config/telfs/profiles/<name>/config.toml`,
mode 0600 — never commit it):

```toml
api_id = 12345678
api_hash = "..."
auth_mode = "user"            # or "bot"
bot_token = ""                # set when auth_mode = "bot"
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
| `TELFS_PROFILE` | profile to load (beats `~/.config/telfs/active`) |
| `TELFS_API_ID` | overrides config |
| `TELFS_API_HASH` | overrides config |
| `TELFS_PHONE` | skip the phone prompt at login |
| `TELFS_DC` | override starting datacenter |
| `TELFS_PASSPHRASE` | FS encryption passphrase (skip prompt at mount) |

## Design choices

| What | Choice | Why |
|---|---|---|
| Language | Go | `hanwen/go-fuse` + `gotd/td` + `modernc.org/sqlite` (pure-Go SQLite) |
| Telegram API | MTProto user API (gotd) | 2 GB per file; Bot API's 50 MB cap is too small for chunks |
| Metadata | local SQLite + periodic channel snapshots | Fast reads; recovery window = snapshot cadence |
| Chunk size | 4 MiB default, **per-FS configurable** | Sequential read/write throughput vs `chunk_map` row count. Set via `telfs init --chunk-size <N>` BEFORE first mount; immutable thereafter. Power of two, [64 KiB, 1.5 GiB]. |
| POSIX surface | files, dirs, symlinks, hardlinks, xattrs (`user.*`) | Enough to host typical workloads |
| Encryption | AES-256-GCM, Argon2id KDF, opt-in via `telfs encrypt init` | Chunk bytes AND snapshot metadata (TFSE envelope); per-chunk AAD binds to `(ino, idx)` |
| Inline TG deletes | none for chunks; snapshots delete the prior one | M4 trade — never destroys user data inline; orphans cleaned by `telfs gc` |
| Snapshot cadence | every 5 min + on clean unmount | Bounded recovery window without burning network bandwidth |

## Profiles + portable bundles

A profile is a named directory under `~/.config/telfs/profiles/<name>/`
holding one filesystem's full local state — config, MTProto session,
SQLite metadata, and cache. Multiple profiles coexist independently
(each can bind to its own account and channel).

```sh
./bin/telfs profile create work
TELFS_PROFILE=work ./bin/telfs login            # interactive setup
TELFS_PROFILE=work ./bin/telfs channel set <id>
./bin/telfs profile use work                    # sticky default

./bin/telfs profile list                        # shows the active marker
./bin/telfs profile export bundle.tar.gz        # tar.gz of config+session+db
./bin/telfs profile import --profile work2 bundle.tar.gz   # restore elsewhere
```

A bundle contains your MTProto session credentials and (if encryption
is enabled) the salt + canary needed to derive the data key from
your passphrase. Treat it like an SSH private key.

- **Same-machine roundtrip is verified.** Exporting and re-importing
  into a sibling profile on the same host works end to end.
- **Cross-device use is the design intent, but is unverified.**
  Telegram may flag or invalidate a `session.json` reused from a
  different device/IP. If that happens you'll see auth errors at
  mount time on the destination; the workaround is to redo
  `telfs login` on that machine. `config.toml` and `db.sqlite`
  from the bundle still apply — just the session bits may not.

## Web management UI

`telfs web` boots a self-contained HTTP UI that covers the entire CLI
surface — dashboard, profile CRUD + tar.gz export/import, multi-step
phone login + bot-token login, channel binding, encryption
initialization, mount supervisor with HTMX-polled live log tail, and a
file browser that reads/writes through a chosen FUSE mountpoint.

```sh
./bin/telfs web                                  # 127.0.0.1:8080, no auth
./bin/telfs web --listen 0.0.0.0:8080 --token $(openssl rand -hex 32)
```

Defaults: loopback bind, no auth — the security perimeter is "can
reach loopback on this host." Non-loopback bind **requires** `--token`;
the server refuses to start otherwise. Token comparison uses
`crypto/subtle.ConstantTimeCompare`. Every POST form carries a
per-session CSRF token (random, in cookie + hidden field).

For HTTPS, terminate TLS in a reverse proxy in front of localhost.
The `--tls-cert` / `--tls-key` flags exist for in-process termination
but aren't the recommended path.

## Deployment

### Static binary (recommended for desktop / laptop)

```sh
make release                                     # → dist/telfs-vX.Y.Z-linux-amd64.tar.gz
```

The release target uses `CGO_ENABLED=0 -trimpath -ldflags '-s -w'` so
the resulting binary is fully static — copies straight to any Linux
host without runtime dependencies (except the kernel FUSE module and
`fusermount` in `$PATH` for unmount).

### Alpine container (recommended for headless / NAS / server)

```sh
docker build -t telfs:latest .
docker run --rm -it \
  --cap-add SYS_ADMIN --device /dev/fuse \
  -v ~/.config/telfs:/profiles \
  --mount type=bind,source=/srv/external,target=/mnt/telfs,bind-propagation=rshared \
  telfs:latest mount /mnt/telfs
```

The container ships only the static `telfs` binary plus `fuse3` and
`ca-certificates` (~18 MB compressed). FUSE inside a container needs
`--cap-add SYS_ADMIN --device /dev/fuse` and the host mountpoint must
be bind-mounted with `rshared` propagation so the FUSE mount is
visible to host processes.

Why both deployment paths? Static binary wins on a desktop where the
mount and the UI live alongside other native tools. Container wins on
headless servers / NAS appliances where the FUSE module is shared but
the userland is locked or distro-mismatched.

## Running unattended

The mount command is a foreground daemon. Backgrounding it with
`&` in an interactive shell is fine for testing, but if that shell
exits with `huponexit` on (or anything else terminates the daemon
abruptly), the kernel mount stays in `/proc/mounts` but `~/External`
silently reverts to writing through to the *local* filesystem
underneath. Files dropped in during that window land on disk
locally, not in the channel — and become invisible the moment
TelFS remounts.

For production use, run it under a supervisor that owns the
process lifecycle:

```ini
# ~/.config/systemd/user/telfs@.service
[Unit]
Description=TelFS mount (profile %i)
After=network-online.target

[Service]
Type=simple
Environment=TELFS_PROFILE=%i
EnvironmentFile=%h/.config/telfs/profiles/%i/passphrase.env
ExecStart=%h/Projects/TelFS/bin/telfs mount %h/External-%i
Restart=on-failure

[Install]
WantedBy=default.target
```

```sh
echo 'TELFS_PASSPHRASE=your-secret' > ~/.config/telfs/profiles/work/passphrase.env
chmod 600 ~/.config/telfs/profiles/work/passphrase.env
systemctl --user enable --now telfs@work.service
```

For interactive use, at minimum: `nohup ./bin/telfs mount ~/External &`
and verify the daemon survives a shell exit before trusting it
with data.

## Known limits

- **Single mounter per channel.** Two concurrent mounts will race; no
  locking. The schema reserves `meta_kv[lock_ts]` for a future
  coordination mechanism.
- **Up-to-5-minute recovery window.** If the daemon crashes hard
  (kill -9, kernel panic, machine off), data written since the last
  cadence snapshot is lost. M5 deferred meta-op posting; revisit if
  this bites in real use.
- **Encryption metadata leakage is bounded but not zero.** Snapshots
  are TFSE-wrapped ciphertext; the channel still exposes the count
  and size of chunk messages and the cadence of snapshots. File size
  to ~4 MiB resolution can be inferred from the chunk count.
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
