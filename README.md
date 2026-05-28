# TelFS

[![CI](https://github.com/platonic-realm/TelFS/actions/workflows/ci.yml/badge.svg)](https://github.com/platonic-realm/TelFS/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/platonic-realm/TelFS?display_name=tag)](https://github.com/platonic-realm/TelFS/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A POSIX filesystem that stores its data in a private Telegram channel.
Files are chunked, optionally encrypted with AES-256-GCM, and uploaded
as channel messages; a local SQLite database holds the metadata and is
periodically snapshotted back to the same channel so a fresh machine
with the right credentials and passphrase can mount and read every
byte. Pure-Go, single static binary, optional Alpine container.

```sh
telfs profile create main
telfs profile use main
telfs login                                  # phone + code + 2FA
telfs channel set <id>
telfs encrypt init                           # optional: AES-256-GCM
telfs trash enable --ttl 7d                  # optional: rm safety-net
telfs mount ~/External
```

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
| Trash safety-net — `rm` reroutes to `/.trash`, TTL GC | ✓ (`telfs trash`) |
| Multi-mounter coordination | ✗ assume one mount per channel |

## Bot vs user auth

TelFS supports both, and the data plane is identical: every chunk goes
over MTProto via gotd, capped at 2 GB per upload, regardless of which
identity you authenticate as. The HTTP Bot API is never used (so its
~50 MB per-file ceiling never applies). What changes between modes is
only the dialog/listing surface:

| Mode | Setup | Behavioral differences |
|---|---|---|
| **User** (default) | `telfs login` → phone + code + 2FA | Full dialog access — `channel list` enumerates your channels and `channel set <id>` auto-discovers the access_hash. |
| **Bot** | `telfs login --bot <token>` (token from @BotFather) | Bots cannot enumerate dialogs, so `channel set` requires `--access-hash` explicitly. The bot must be added to the target channel as an administrator before it can post. |

Bot-mode setup:

```sh
# 1. Get the channel access_hash from a user-account TelFS first
#    (or any tool you prefer):
TELFS_PROFILE=user-acct telfs channel info       # prints access_hash

# 2. Create a bot via @BotFather → get a token; add the bot to your
#    private channel as an ADMIN.

# 3. New profile + bot login + manual channel binding:
telfs profile create my-bot
TELFS_PROFILE=my-bot telfs login --bot 123456:ABCDEF…
TELFS_PROFILE=my-bot telfs channel set --access-hash <H> <channel-id>
TELFS_PROFILE=my-bot telfs mount ~/External
```

Everything past auth — chunker, encryption, snapshots, GC, trash — is
mode-agnostic.

## Install

Three options:

```sh
# 1. Pre-built static binary from a GitHub Release (no toolchain needed):
curl -L https://github.com/platonic-realm/TelFS/releases/latest/download/telfs-$(uname -s | tr '[:upper:]' '[:lower:]')-$(uname -m).tar.gz | tar xz
sudo install telfs-*/telfs /usr/local/bin/telfs

# 2. Build from source (requires Go 1.22+):
git clone https://github.com/platonic-realm/TelFS.git && cd TelFS
make build                                       # → bin/telfs

# 3. Alpine container (headless / NAS — see Deployment below):
docker pull ghcr.io/platonic-realm/telfs:latest
```

All three carry the same static `telfs` binary and need `fusermount`
(`fuse3`/`fuse2` packages) in `$PATH` at unmount time. Get a Telegram
API ID + hash from <https://my.telegram.org/apps>.

## Quick start

```sh
# (Optional) pick a non-default chunk size before first mount.
# Default is 4 MiB; valid range is 64 KiB..1.5 GiB, power of two.
# Once any chunk lands the choice is immutable.
telfs init --chunk-size $((16*1024*1024))        # e.g. 16 MiB

# Pick a profile to live under. Profiles are isolated FS instances at
# ~/.config/telfs/profiles/<name>/ holding config + session + DB + cache.
telfs profile create main
telfs profile use main                           # sticky default

# Configure credentials (one-time). Either edit the profile's config.toml
# directly OR set them in the environment:
export TELFS_API_ID=12345678
export TELFS_API_HASH=...

# Log in (interactive — type the SMS / Telegram code that arrives, and
# your 2FA password if set).
telfs login

# Pick a channel to use as the backend. Create a *private* channel in
# Telegram first; then:
telfs channel list
telfs channel set <id>
telfs channel ping                               # smoke test: post + read back

# Mount.
mkdir mnt
telfs mount ./mnt                                # foreground; ^C to unmount
```

Then in another shell:

```sh
echo hi > mnt/hello.txt
cat mnt/hello.txt
mkdir mnt/notes
mv mnt/hello.txt mnt/notes/
ln mnt/notes/hello.txt mnt/notes/alias           # hardlink
ln -s notes/hello.txt mnt/link                   # symlink
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
telfs mount ./mnt
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
telfs encrypt init                    # interactive passphrase
# or for unattended setup:
TELFS_PASSPHRASE='your secret' telfs encrypt init
telfs encrypt status
```

Once enabled, every mount requires the passphrase. Set
`TELFS_PASSPHRASE` to skip the prompt:

```sh
TELFS_PASSPHRASE='your secret' telfs mount ./mnt
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

### `telfs gc` — reclaim channel storage

```sh
telfs gc                                    # dry-run report
telfs gc --yes                              # actually delete orphans
```

Walks the channel and identifies:

- **Orphan chunks** — document messages whose msg_id isn't in
  `chunk_map` (created by file overwrites/unlinks, which intentionally
  don't delete inline — see "Design choices" below).
- **Stale snapshots** — snapshot-caption messages other than the
  current one recorded in `meta_kv`.

The default is dry-run; pass `--yes` to delete.

### `telfs fsck` — channel-side integrity check

```sh
telfs fsck                                  # check every chunk_map row
telfs fsck --fix                            # also drop unreadable rows
```

Walks `chunk_map` and confirms each referenced message still exists
on the channel and is reachable. Reports broken references; with
`--fix`, drops them from the local DB (the corresponding file becomes
short — see the report for which files).

### `telfs trash` — rm safety-net

```sh
telfs trash enable --ttl 7d                 # turn on, retain 7 days
telfs trash status                          # enabled / ttl / count
telfs trash list                            # oldest first
telfs trash empty                           # purge everything now
telfs trash disable                         # back to immediate-delete
```

When enabled, every kernel-issued `unlink`/`rmdir` from FUSE reroutes
into a top-level `/.trash/` directory instead of really deleting; a
background GC unlinks entries older than the TTL. Standard tools
restore via `mv /.trash/<unix-nano>-orig /restored`.

Semantics:

- The trashed dirent points at the same inode as the original — file
  contents and chunks are untouched, so a `mv` back out is a true
  restore.
- Unlinks *inside* `/.trash` actually delete, so the GC and
  `trash empty` work.
- `rm /.trash` itself returns `EPERM` — can't be removed via FUSE.
- `rm -rf foo/` flattens: the kernel issues per-file unlinks first,
  then `rmdir`. Each file lands in `/.trash` with a unique prefix;
  the empty `foo/` ends up there as a sibling. Restorable but not
  a preserved tree (v1 limitation).
- The TTL change takes effect on the next GC tick (~5 min) without
  a remount.

### `telfs status` — one-screen overview

```sh
telfs status
```

Active profile, channel binding, fs_uuid, chunk size, encryption
state, last snapshot, FUSE mount table, on-disk file sizes — all on
one screen. Useful first stop when something looks off.

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
| Language | Go | `hanwen/go-fuse` + `gotd/td` + `modernc.org/sqlite` (pure-Go SQLite). No cgo → fully-static release binary. |
| Telegram API | MTProto via `gotd/td` for both user *and* bot auth | One code path for the data plane; 2 GB per upload either way. The HTTP Bot API (with its ~50 MB per-file ceiling) is not used. |
| Metadata | local SQLite + periodic channel snapshots | Fast reads; recovery window = snapshot cadence |
| Chunk size | 4 MiB default, **per-FS configurable** | Sequential read/write throughput vs `chunk_map` row count. Set via `telfs init --chunk-size <N>` BEFORE first mount; immutable thereafter. Power of two, [64 KiB, 1.5 GiB]. |
| POSIX surface | files, dirs, symlinks, hardlinks, xattrs (`user.*`) | Enough to host typical workloads |
| Encryption | AES-256-GCM, Argon2id KDF, opt-in via `telfs encrypt init` | Chunk bytes AND snapshot metadata (TFSE envelope); per-chunk AAD binds to `(ino, idx)` |
| Inline TG deletes | none for chunks; snapshots delete the prior one | Never destroys user data inline; orphans cleaned by `telfs gc`. Trash safety-net layers on top for human-scale `rm` mistakes. |
| Snapshot cadence | every 5 min + on clean unmount | Bounded recovery window without burning network bandwidth |

## Profiles + portable bundles

A profile is a named directory under `~/.config/telfs/profiles/<name>/`
holding one filesystem's full local state — config, MTProto session,
SQLite metadata, and cache. Multiple profiles coexist independently
(each can bind to its own account and channel).

```sh
telfs profile create work
TELFS_PROFILE=work telfs login            # interactive setup
TELFS_PROFILE=work telfs channel set <id>
telfs profile use work                    # sticky default

telfs profile list                        # shows the active marker
telfs profile export bundle.tar.gz        # tar.gz of config+session+db
telfs profile import --profile work2 bundle.tar.gz   # restore elsewhere
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
telfs web                                  # 127.0.0.1:8080, no auth
telfs web --listen 0.0.0.0:8080 --token $(openssl rand -hex 32)
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

The container ships only the static `telfs` binary plus `fuse3`,
`ca-certificates`, and `tini` (~20 MB compressed). It runs every
TelFS subcommand — `mount`, `web`, `login`, `status`, `gc`, `fsck`,
`trash`, etc. — just like the static binary.

#### Pull or build

```sh
# Published image, multi-arch (amd64 + arm64):
docker pull ghcr.io/platonic-realm/telfs:latest
# Or pin a specific release:
docker pull ghcr.io/platonic-realm/telfs:v0.5

# Build locally:
make docker                                       # → telfs:<git-describe>
```

#### Required runtime flags (FUSE)

Three things are non-negotiable for FUSE inside a container:

| Flag | Why |
|---|---|
| `--cap-add SYS_ADMIN` | FUSE mounts need this capability. |
| `--device /dev/fuse` | The kernel character device must be passed through. |
| `--security-opt apparmor:unconfined` | On distros with strict AppArmor profiles (Ubuntu, Debian), the default profile blocks FUSE. Drop it for the TelFS container. |

#### Required bind mounts

| Container path | Host path | Purpose |
|---|---|---|
| `/root/.config/telfs` | `~/.config/telfs` | Profiles directory: session, db.sqlite, cache, config.toml. Read-write. |
| `/mnt/telfs` | wherever you want the FS to appear | Mountpoint. **Must use `bind-propagation=rshared`** so the FUSE mount inside the container is visible to host processes. |

#### First-time setup (inside the container)

```sh
# Spawn an interactive shell to walk through profile/login/channel.
# The same bind mounts as above — anything we write to /root/.config
# lands on the host's ~/.config/telfs.
docker run --rm -it \
  --cap-add SYS_ADMIN --device /dev/fuse \
  --security-opt apparmor:unconfined \
  -v $HOME/.config/telfs:/root/.config/telfs \
  --entrypoint sh \
  ghcr.io/platonic-realm/telfs:latest

# Inside the container:
telfs profile create main
telfs profile use main
telfs login                                       # interactive phone+code+2FA
telfs channel set <id>
telfs encrypt init                                # optional
telfs trash enable --ttl 7d                       # optional
exit
```

After that, the profile exists on the host; subsequent `mount` / `web`
runs reuse it.

#### Mount

```sh
mkdir -p /srv/external
docker run -d --name telfs-main \
  --restart unless-stopped \
  --cap-add SYS_ADMIN --device /dev/fuse \
  --security-opt apparmor:unconfined \
  -v $HOME/.config/telfs:/root/.config/telfs \
  --mount type=bind,source=/srv/external,target=/mnt/telfs,bind-propagation=rshared \
  -e TELFS_PROFILE=main \
  -e TELFS_PASSPHRASE=secret \
  ghcr.io/platonic-realm/telfs:latest mount /mnt/telfs
```

- `/srv/external` on the host is where files appear (your `ls /srv/external` from the host shows the channel contents).
- `-e TELFS_PASSPHRASE` is only needed for encrypted FSes. Omit otherwise.
- `tini` is the container PID 1; SIGTERM (`docker stop telfs-main`)
  triggers the in-mount watchdog and a clean final snapshot.

#### Web management UI

```sh
docker run -d --name telfs-web \
  -p 127.0.0.1:8080:8080 \
  -v $HOME/.config/telfs:/root/.config/telfs \
  ghcr.io/platonic-realm/telfs:latest \
  web --listen 0.0.0.0:8080
```

Or, with token auth for non-loopback exposure:

```sh
docker run -d --name telfs-web \
  -p 8080:8080 \
  -v $HOME/.config/telfs:/root/.config/telfs \
  ghcr.io/platonic-realm/telfs:latest \
  web --listen 0.0.0.0:8080 --token $(openssl rand -hex 32)
```

The web container does **not** need the FUSE capability/device — those
are only required if it itself runs a `mount` subcommand. To control
mounts from the web UI inside a container, give the same FUSE flags as
above; otherwise use the web container for setup/inspection and run
`mount` in a sibling container.

#### Docker Compose

```yaml
services:
  telfs-mount:
    image: ghcr.io/platonic-realm/telfs:latest
    command: ["mount", "/mnt/telfs"]
    restart: unless-stopped
    cap_add: [SYS_ADMIN]
    devices: ["/dev/fuse"]
    security_opt: ["apparmor:unconfined"]
    environment:
      TELFS_PROFILE: main
      TELFS_PASSPHRASE: secret
    volumes:
      - ${HOME}/.config/telfs:/root/.config/telfs
      - type: bind
        source: /srv/external
        target: /mnt/telfs
        bind:
          propagation: rshared

  telfs-web:
    image: ghcr.io/platonic-realm/telfs:latest
    command: ["web", "--listen", "0.0.0.0:8080"]
    restart: unless-stopped
    ports: ["127.0.0.1:8080:8080"]
    volumes:
      - ${HOME}/.config/telfs:/root/.config/telfs
```

#### Troubleshooting

- **`fuse: device not found, try 'modprobe fuse'`** — the kernel FUSE
  module isn't loaded on the host. `sudo modprobe fuse`. Persist via
  `/etc/modules-load.d/`.
- **`fusermount: mount failed: Operation not permitted`** — missing
  `--cap-add SYS_ADMIN` or AppArmor is blocking. Add `--security-opt
  apparmor:unconfined`.
- **The mount works inside the container but `ls /srv/external` on
  the host shows nothing** — your bind mount didn't use `rshared`.
  Re-run with the propagation flag.
- **`failed to drop capability cap_sys_admin`** — Docker daemon
  configured with `--no-new-privileges` globally. Lift it for this
  container.

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

For interactive use, at minimum: `nohup telfs mount ~/External &`
and verify the daemon survives a shell exit before trusting it
with data.

## Known limits

- **Single mounter per channel.** Two concurrent mounts will race; no
  locking. The schema reserves `meta_kv[lock_ts]` for a future
  coordination mechanism.
- **Up-to-5-minute recovery window.** If the daemon crashes hard
  (kill -9, kernel panic, machine off), data written since the last
  cadence snapshot is lost. A future channel-side journal would close
  this gap; see the roadmap.
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
- **Trash is flat, not a preserved tree.** `rm -rf foo/bar/` lands as
  N separate entries under `/.trash`, restorable individually but not
  as a tree. Acceptable for the safety-net use case (typo undos);
  for archival, snapshot the FS instead.

## Roadmap

- **Passphrase rotation** via KEK-wrapped data key so changing the
  passphrase doesn't require re-encrypting every chunk.
- **Channel-side journal between snapshots** to close the up-to-5-min
  crash window.
- **Read-ahead** for sequential FUSE reads — prefetch next-N chunks
  so `cat`/video playback doesn't round-trip per 4 MiB.
- **Content-addressed chunk dedup** — SHA-256 index on `chunk_map`;
  identical chunks share one channel message.
- **First-run wizard in the web UI** — guide a new user through
  profile/login/channel/encryption/mount in one form.

See the [issues](https://github.com/platonic-realm/TelFS/issues) tab
for the current state.

## Architecture

See [`docs/architecture.md`](docs/architecture.md) for the module
layout, SQLite schema, channel message format, and recovery model.

## License

MIT — see [`LICENSE`](LICENSE).
