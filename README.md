# TelFS

[![CI](https://github.com/platonic-realm/TelFS/actions/workflows/ci.yml/badge.svg)](https://github.com/platonic-realm/TelFS/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/platonic-realm/TelFS?display_name=tag)](https://github.com/platonic-realm/TelFS/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A POSIX filesystem that stores its data in a private Telegram channel.
Files are chunked, encrypted with AES-256-GCM, and uploaded as channel
messages; a local SQLite database holds the metadata and is
periodically snapshotted — encrypted — back to the same channel, so a
fresh machine with the account and the profile's secret can mount and
read every byte. On the channel nothing is readable without the secret
but message sizes and times. Pure-Go, single static binary.

```sh
telfs profile create main && telfs profile use main
telfs login                                  # sets the secret; phone + code + 2FA
telfs channel set <id>
telfs init                                   # new filesystem (always encrypted)
telfs key export                             # keep this with the secret
telfs trash enable --ttl 7d                  # optional: rm safety-net
telfs mount ~/External
```

## What works today

End-to-end verified on a real Telegram account + private channel:

| Capability | Status |
|---|---|
| Login — interactive phone + code + 2FA over MTProto (one identity per profile, logout supported) | ✓ |
| Read/write mount with full POSIX: files, dirs, hardlinks, symlinks, xattrs | ✓ |
| Multi-chunk files (no per-file ceiling), cross-chunk-boundary read/write | ✓ |
| Cold-mount recovery from channel snapshot, journal replay (≤5 s loss) | ✓ |
| `--readonly` mount, FLOOD_WAIT handling, persistent LRU chunk cache | ✓ |
| `telfs gc` — orphan chunks + stale snapshots | ✓ |
| `telfs fsck` — channel-side integrity check | ✓ |
| `telfs status` / `telfs doctor` — one-screen state / local invariant lint | ✓ |
| Format 2 (new filesystems): everything encrypted — chunks, captions, snapshots, journal; sizes Padmé-padded; Argon2id over 2 GiB | ✓ |
| One secret per profile from a prompt, a secret file, or KeePassXC; encrypted `session.enc`; non-dumpable processes | ✓ |
| Key header pinned in the channel + a text export for the break-glass kit; `telfs secret change` | ✓ |
| Self-describing chunks: rebuild the metadata from captions alone (`telfs recover --from-captions`) | ✓ |
| Profiles + portable tar.gz export/import bundles | ✓ |
| Web management UI: dashboard, login wizard, mount supervisor, file browser | ✓ (`telfs web`) |
| `telfs trash` — `rm` reroutes to `/.trash`, TTL GC | ✓ |
| Snapshot history + point-in-time restore (last 12 by default) | ✓ |
| Content-addressed chunk dedup, keyed hash (format 2; format 1: plaintext + convergent) | ✓ |
| Multi-mounter coordination | ✗ assume one mount per channel |

## Telegram API credentials (API ID + hash)

TelFS speaks the MTProto API directly — the same protocol the official
apps use (2 GB per upload; the HTTP Bot API and its 50 MB / 20 MB
caps are never used). MTProto requires every client application to
identify itself with an **API ID + API hash** pair. You create it once
and reuse it for every profile — it identifies the *application*, not
your account.

> Bot-token auth existed briefly and was removed: MTProto requires the
> api_id/api_hash pair even for bots (`auth.importBotAuthorization`
> takes them as parameters), so a bot identity saved nothing over a
> user login while adding channel-admin requirements and an
> identity-scoped access_hash trap. Auth is phone login, full stop.

### Creating the pair (one-time, ~2 minutes)

1. Open <https://my.telegram.org> and log in with your **phone
   number** — Telegram sends the confirmation code to your Telegram
   app (not SMS).
2. Click **API development tools**.
3. If you have no app yet, fill the short form: any **App title** and
   **Short name** (e.g. `telfs`), platform **Desktop** or **Other** —
   URL can stay empty. Submit.
4. The page now shows your **App api_id** (a number, e.g. `12345678`)
   and **App api_hash** (32 hex characters). That's the pair. It stays
   retrievable on that page forever — nothing to write down urgently.

Treat the hash like a password-ish secret: it can't read your account
by itself (a login still needs your phone + code), but don't publish
it.

### Entering the pair in TelFS

**Web (recommended):** run `telfs web`, open
<http://127.0.0.1:8080/setup> and follow the wizard — every step is an
inline form on that one page: profile → API ID + hash → phone login →
channel → optional encryption/compression → first mount. The
credentials form is also on each profile's page, and the login form
collects the pair inline if it's missing.

![TelFS setup wizard — the API credentials step](assets/setup-wizard.png)

**CLI:** either export them as environment variables before `telfs
login`:

```sh
export TELFS_API_ID=12345678 TELFS_API_HASH=0123456789abcdef0123456789abcdef
```

or put them in the active profile's config file
(`~/.config/telfs/profiles/<name>/config.toml`, mode 0600):

```toml
api_id = 12345678
api_hash = "0123456789abcdef0123456789abcdef"
```

The same pair works in every profile you'll ever create.

## Install

```sh
# Pre-built static binary (no toolchain):
curl -L https://github.com/platonic-realm/TelFS/releases/latest/download/telfs-$(uname -s | tr '[:upper:]' '[:lower:]')-$(uname -m).tar.gz | tar xz
sudo install telfs-*/telfs /usr/local/bin/telfs

# Build from source (Go 1.22+):
git clone https://github.com/platonic-realm/TelFS.git && cd TelFS && make build
```

Both paths produce the same static binary; `fusermount` (`fuse3` or
`fuse2`) must be in `$PATH` for unmount. Get a Telegram API ID + hash
at <https://my.telegram.org/apps>.

## Quick start (web — recommended)

One command; everything else happens in the browser:

```sh
telfs web
```

Open <http://127.0.0.1:8080> — you land on the `/setup` wizard, and
every step is an inline form on that one page:

1. **Create a profile** — it becomes active immediately.
2. **API ID + hash** — paste the pair from
   [my.telegram.org/apps](https://my.telegram.org/apps) (see
   [Telegram API credentials](#telegram-api-credentials-api-id--hash)).
3. **Secret** — where the profile's secret comes from: typed in,
   a secret file, or KeePassXC (see [Secrets](#secrets)).
4. **Phone login** — number → the code Telegram sends → 2FA if set.
   The first login sets the secret: the session is stored encrypted.
5. **Pick a channel** — your channels are listed; one click binds.
   (Create a private channel in the Telegram app first.)
6. **Create the filesystem** — chunk size and compression; always
   encrypted. Skip it if the channel already holds a TelFS
   filesystem: mounting recovers it.
7. **Mount** — type a mountpoint (existing empty dir) and hit Mount.
   You land on the mount's page with a live log.

That's it — the daemon runs detached (it survives closing `telfs
web`), your files are at the mountpoint, and the Files tab browses
them from the browser. Day-to-day driving — start/stop mounts, log
tails, Telegram traffic stats, rate limits — all lives in the same
UI. To make the console permanent, run `telfs web` as a user systemd
service (see [Running unattended](#running-unattended)).

## Quick start (CLI)

The same flow, scriptable:

```sh
# Profile = isolated FS instance at ~/.config/telfs/profiles/<name>/.
telfs profile create main && telfs profile use main

# Where the secret comes from (default: typed in). See "Secrets".
#   [secret] source = "keepassxc", entry = "Services/TelFS/main"   in config.toml

telfs login                                  # asks for the API pair if missing,
                                             # sets the secret, phone + code + 2FA
telfs channel list                           # find the channel id
telfs channel set <id>
telfs channel ping                           # smoke test

# A NEW filesystem (always encrypted). 4 MiB chunks by default; 64 KiB..1.5 GiB,
# power of two; chunk size and compression are fixed from here on.
telfs init [--chunk-size $((16*1024*1024))] [--compression zstd]
telfs key export > ~/telfs-main-key.txt      # → KeePassXC entry / break-glass kit

mkdir mnt && telfs mount ./mnt               # foreground; ^C to unmount
# background daemons: `telfs unmount <mountpoint|profile>` stops any
# telfs mount gracefully, however it was started
```

Then in another shell — write, hardlink, symlink, xattr all work:

```sh
echo hi > mnt/hello.txt && cat mnt/hello.txt
mkdir mnt/notes && mv mnt/hello.txt mnt/notes/
ln mnt/notes/hello.txt mnt/notes/alias       # hardlink
ln -s notes/hello.txt mnt/link               # symlink
setfattr -n user.tag -v important mnt/notes/hello.txt
```

On `^C` the daemon snapshots the meta DB to the channel, `fusermount
-u`s, drains in-flight requests, and tears down the MTProto session.

## Mount options

```
telfs mount [--readonly] [--allow-other] [--debug] <mountpoint>
```

`--allow-other` needs `user_allow_other` in `/etc/fuse.conf`.
`--debug` logs every FUSE op (very chatty).

**`--readonly` never writes to the channel** — no snapshots, no journal,
no uploads, no deletions, no pins: the Telegram session itself refuses
every channel write, and the kernel mount carries `ro`. That is the
way to run a recovery drill from another machine while the real mount
keeps running. (Local state — the restored DB, the cache — is still
written.)

**One writer per filesystem.** On one machine `telfs mount` refuses a
second mount of the same profile (two daemons would share its
database) and a read-write mount of a filesystem that is already
mounted under any profile. The mount source in `/proc/mounts` is
`telfs:<profile>:<fs_uuid>`, so every user and tool can see which
filesystem is mounted where. **Nothing yet stops a second machine**
from mounting the same channel read-write — two writers would silently
lose each other's changes. Mount read-write in one place only; drill
elsewhere with `--readonly`.

## Recovery

Lose the local DB? The next `telfs mount` scans the channel for the
most-recent snapshot and restores it. At worst you lose what was
written between the last snapshot/journal cycle (snapshot cadence: 5
min; journal poster: 5 s).

```sh
PROFDIR=~/.config/telfs/profiles/main
rm -rf $PROFDIR/db.sqlite $PROFDIR/cache
telfs mount ./mnt
# Local DB missing — scanning channel for snapshot…
# Found snapshot msg=29 (ts 2026-05-28T18:03:00Z)
# Restored 1489 gzipped bytes → db.sqlite (TFSE envelope decrypted)
# Mounted at ./mnt.
```

A channel with no TelFS filesystem is an error ("run `telfs init`") —
a filesystem is created only by `telfs init`, never implicitly. An
empty `db.sqlite` that neither `init` nor a recovery made (something
opened a missing database) is refused too, not mounted.

On a format-2 filesystem the mount first reads the **pinned key
message**, unlocks it with the profile's secret, then finds the newest
snapshot. If the key message is gone, recover with the export:

```sh
telfs mount --key-header ~/telfs-main-key.txt ./mnt   # re-posts + pins it
```

`--key-header` only applies when the local DB is missing; with a
`db.sqlite` present the mount refuses instead of ignoring it.

**Every snapshot gone?** `telfs recover --from-captions` rebuilds the
metadata from the chunk captions, the packs' encrypted indexes and any
journal batches still on the channel, and writes a per-file report of
what came back under which name. With the journal present the tree
comes back exactly; from captions alone, files come back under the name
they had when last uploaded (rsync's temporary names are mapped back),
while empty files, symlinks, extra hard-link names, deduplicated copies
and later renames are lost.

### Time travel: rolling back to a historical snapshot

Snapshots are retained on the channel (last 12 by default, ~1h at 5
min cadence). Older ones are auto-pruned.

```sh
telfs snapshot list                 # MSG-ID | AGE | TS | JOURNAL-SEQ
fusermount -u ~/External            # restore refuses while mounted
telfs snapshot restore 2890         # 30-min-ago snapshot
# Saves current DB as db.sqlite.pre-restore-<unix-ts>, then replaces.
telfs mount ~/External              # files now reflect that state
```

Restore is destructive to the current DB; the pre-restore copy beside
it lets you manually roll forward again.

## Encryption

A new filesystem (`telfs init`, **format 2**) is always encrypted, and
nothing TelFS posts is readable without the profile's secret except
the key header (salt, Argon2id parameters, the wrapped key), message
sizes and times:

| On the channel | Format 2 |
|---|---|
| Chunk and pack bodies | AES-256-GCM, random nonce, bound to the chunk's keyed content hash; the reader re-checks the hash |
| Snapshots, journal batches | AES-256-GCM under the metadata key — names, folders, sizes never in clear |
| Captions | every caption exactly 1024 characters: `tfs2` + base64 ciphertext (identity, size, mode, mtime, path hint) |
| Document names | every document is `blob` |
| Sizes | Padmé-padded (≤ 3.1 %; a full 4 MiB chunk pays nothing) |
| Key header | one pinned text message `tfs2k …` — useless without the secret |

Keys: the secret → Argon2id (1 pass, 2 GiB — RFC 9106's first choice)
→ key-encrypting key → wraps a random 32-byte data key. Chunk, hash,
metadata and caption keys derive from the data key by HKDF, one label
each. The session (`session.enc`) has its own salt and opens with the
secret alone.

AES-GCM with random nonces allows 2^32 encryptions per key; TelFS
counts them (`telfs status`, `fsck`), warns at 2^31 and turns the
filesystem read-only at 2^32 — about 3000 first full backups of a
large home folder away.

### Secrets

One secret per profile opens the session and the filesystem key. The
`[secret]` table of `config.toml` says where it comes from — never the
secret itself:

```toml
[secret]
source = "keepassxc"             # "prompt" (default) | "file" | "keepassxc"
entry  = "Services/TelFS/main"   # path inside KeePassXC's exposed Secret Service group
# file = "/srv/services/<group>/<service>/secrets/telfs"   # for source = "file"
```

- **prompt** — typed in; `TELFS_PASSPHRASE` skips the prompt. TelFS
  removes it from its own environment at start, so no process it
  starts inherits it.
- **file** — a regular file (no symlink), giving nothing to "others",
  at most 4 KiB; one trailing newline is ignored. What boot mounts use.
- **keepassxc** — `secret-tool lookup Path /<entry>` against the
  running KeePassXC (unlock or confirm in its window; 2-minute limit).

`--secret-from prompt|file|keepassxc` overrides the source for one
command. `telfs secret test` checks it (exit 0 ok, 2 source failed,
3 wrong secret). Every `telfs` process is non-dumpable: other
processes of yours cannot read its environment, memory maps or file
descriptors, and no core dump is written.

The key header is readable on the channel, so a short secret can be
guessed offline: use a KeePassXC-generated password of 32+ characters.

### Key header export

```sh
telfs key export [--out FILE]
```

prints the key header as a text block with a check line (catches a
mistyped copy). Keep it in the filesystem's KeePassXC entry and the
break-glass kit: without the secret it opens nothing; with it, it is
the whole key. `telfs status` and `telfs fsck` check that the channel's
pinned key message exists and matches; `telfs fsck --fix` repairs it.

### Changing the secret

```sh
telfs secret change --new-from-prompt | --new-file PATH | --new-entry ENTRY
```

re-wraps the data key and re-encrypts `session.enc` under the new
secret, replaces the key message and prints a new export. **No data is
re-encrypted** — the data key never changes for the life of a
filesystem. So an OLD secret together with any OLD copy of the key
header (an export, a snapshot from before the change) still opens the
filesystem: destroy old exports. The only remedy for a leaked data key
is a new filesystem and an rsync copy.

### Format 1 (filesystems from v0.22 and earlier)

Older filesystems keep working as before: optional AES-256-GCM chunks
(`aes-gcm-v1`/`v2`/`v3`) and encrypted snapshots — but the journal
reaches the channel in clear, chunk documents are named
`ino<N>-idx<M>`, and on an unencrypted one the profile's secret
protects only the Telegram session. They cannot be encrypted in place:
rsync their files into a new filesystem.

## Content-addressed chunk dedup

Format 2 hashes every chunk with a key derived from the filesystem key
(HMAC-SHA256, so equal content says nothing to anyone without it); on
format 1, plaintext and convergent-encrypted FSes use SHA-256. TelFS
consults a `chunk_blob`
forward index (`hash → tg_message_id, size`) before uploading. A hit
points the new `chunk_map` row at the existing message — no upload, no
extra channel storage. `cp file copy.bin`, intra-file duplication, and
re-uploaded archives all collapse to one channel message.

The index is a cache only; GC liveness derives from `chunk_map` alone:

- **Partial unlink is safe.** Removing one of N files referencing a
  shared chunk leaves the other N-1 rows; `gc` only deletes a message
  when no row references it.
- **Reuse is verified at runtime.** Before pointing a new row at an
  indexed message, the writer checks (in the same transaction) that
  some `chunk_map` row still references it. Stale entries (target
  reaped by an earlier `gc`) fall through to a fresh upload.

`telfs status` reports the distinct-blob count. `telfs gc` prunes
stale index entries after each channel cleanup.

**Format 2** binds each chunk to its keyed content hash, not to its
slot, so an encrypted chunk is shared between files without identical
ciphertext: dedup works on every new filesystem. Each dedup hit is also
journaled (encrypted) as a `ref`, so a rebuild without snapshots can
restore deduplicated files.

**Format 1 encryption interaction** (older filesystems). `aes-gcm-v1` /
`v2` use a random per-chunk nonce + `(ino, idx)` AAD, so identical
plaintext produces different ciphertext per slot — dedup can't apply.
Format-1 filesystems created with `--convergent` (`aes-gcm-v3`; no
longer offered for new filesystems) derive the chunk nonce as `HMAC-SHA256(DEK, label‖plaintext)[:12]`
and uses nil AAD, so identical plaintext under the same DEK produces
byte-identical ciphertext (which is what makes dedup work).

| Property | `aes-gcm-v2` (default) | `aes-gcm-v3` (`--convergent`) |
|---|---|---|
| Dedup on encrypted chunks | no | yes |
| Channel observer sees duplicate-chunk count | no | yes |
| Confirmation-of-file by observer without DEK | no | **no** (still needs DEK) |
| Tampering / replay detection (GCM tag) | yes | yes |
| Passphrase rotation | yes | yes |

Why convergent dedup is still safe: classic convergent encryption is
attackable because the key *is* `H(content)`. Here the nonce
derivation is keyed on the per-FS DEK; an observer without the DEK
cannot synthesize the ciphertext for a candidate plaintext, so
possession is not testable. The residual leak is strictly equality
detection. The standards-track equivalent is **AES-GCM-SIV** (RFC
8452); v3 is a hand-rolled thinner variant whose safety invariant is
that the HMAC input is the *full* chunk plaintext (a prefix/sample
would collapse distinct plaintexts onto the same nonce, catastrophic
for AES-GCM).

## Maintenance

```sh
telfs gc                         # dry-run: orphan chunks + stale snapshots
telfs gc --yes                   # actually delete
telfs fsck                       # check every chunk_map row against the channel
telfs fsck --fix                 # drop unreadable rows (file becomes short)
telfs status                     # profile / channel / FS / encryption / mount
telfs doctor                     # local invariant lint (see below)
telfs trash enable --ttl 7d      # rm safety-net (see below)
```

`gc` finds **orphan chunks** (msg_ids not in `chunk_map` — left behind
by overwrites/unlinks; see Design choices) and **stale snapshots**
(any snapshot caption other than the current `meta_kv` recorded one).
Default is dry-run.

### `telfs trash` — rm safety-net

```sh
telfs trash enable --ttl 7d      # turn on, retain 7 days
telfs trash status               # state, ttl, count
telfs trash list                 # oldest first
telfs trash empty                # purge now
telfs trash disable
```

Every kernel-issued `unlink`/`rmdir` reroutes into `/.trash/`; a
background GC actually deletes entries older than the TTL. Restore is
just `mv /.trash/<unix-nano>-orig /restored` — the trashed dirent
points at the same inode, so file content is untouched. Notes: unlinks
*inside* `/.trash` actually delete; `rm /.trash` itself returns EPERM;
`rm -rf foo/bar/` flattens — each file lands as a sibling in
`/.trash`, restorable individually but not as a preserved tree.

### `telfs doctor` — local invariant lint

```sh
telfs doctor                     # exit 0 on clean / warnings, 1 on errors
```

Read-only complement to `status` (state) and `fsck` (channel side).
Checks `config.toml` / `session.enc` permissions (warns above 0600; flags a plaintext v0.22 `session.json`),
`meta_kv` invariants (valid UUID, chunk_size power-of-two, crypto KV
consistent), `chunk_map` health (positive sizes/msg_ids, dedup ratio),
cache vs `chunk_map` (orphan cache files), snapshot + journal
high-water markers. Findings are graded `ok` / `info` / `warn` / `err`
and the exit code reflects any `err`, so it's usable as a periodic
health probe.

## Configuration

Active profile's `config.toml` at `~/.config/telfs/profiles/<name>/`,
mode 0600 (don't commit it):

```toml
api_id = 12345678
api_hash = "..."
phone = "+15551234567"        # optional — prompted if missing
dc = 1                        # starting datacenter; default 2

[identity]                    # written automatically on login
user_id = 123456789
username = "you"

[channel]
id = 1234567890
access_hash = -9876543210123456789
title = "My TelFS"
```

Environment overrides:

| Var | Effect |
|---|---|
| `TELFS_PROFILE` | active profile (beats `~/.config/telfs/active`) |
| `TELFS_API_ID` / `TELFS_API_HASH` | override config |
| `TELFS_PHONE` | skip phone prompt at login |
| `TELFS_DC` | override starting datacenter |
| `TELFS_PASSPHRASE` | the secret of a prompt-source profile (skips the prompt); removed from TelFS's environment at start, never inherited by a child |

## Design choices

| What | Choice | Why |
|---|---|---|
| Language | Go (`hanwen/go-fuse` + `gotd/td` + `modernc.org/sqlite`) | Pure-Go SQLite — no cgo, fully-static binary. |
| Telegram API | MTProto via `gotd/td`, user auth only | One data-plane path; 2 GB per upload. HTTP Bot API never used. |
| Metadata | Local SQLite + periodic channel snapshots | Fast reads; recovery window = snapshot cadence. |
| Chunk size | 4 MiB default, per-FS configurable via `telfs init --chunk-size`; immutable after first chunk; power of two ∈ [64 KiB, 1.5 GiB] | Throughput vs `chunk_map` row count. |
| POSIX surface | files, dirs, symlinks, hardlinks, xattrs (`user.*`) | Enough for typical workloads. |
| Encryption | Format 2: always on — AES-256-GCM, Argon2id 2 GiB, HKDF subkeys, Padmé padding, encrypted captions/journal | Nothing on the channel but the key header, sizes and times. Format 1 (old): opt-in, journal in clear. |
| Inline TG deletes | None for chunks; snapshot delete supersedes prior | Never destroys data inline; `gc` reclaims; trash layers on top. |
| Snapshot / journal cadence | Snapshot every 5 min + clean unmount; journal poster every 5 s | Bounded recovery window without burning bandwidth. |

## Profiles + portable bundles

A profile is a named dir under `~/.config/telfs/profiles/<name>/`
holding one FS's full local state (config + session + db + cache).
Multiple profiles coexist independently.

```sh
telfs profile create work
TELFS_PROFILE=work telfs login
TELFS_PROFILE=work telfs channel set <id>
telfs profile use work                      # sticky default
telfs profile list                          # shows active marker

telfs profile export bundle.tar.gz          # session.enc + config + db
telfs profile import --profile work2 bundle.tar.gz
```

A bundle holds the encrypted session (`session.enc`; a plaintext
v0.22 `session.json` is never exported), the config (only the
*location* of the secret) and the DB (with the wrapped key). Without
the secret it opens nothing — but keep it private all the same.

- **Same-machine roundtrip is verified.**
- **Cross-device use is the design intent but unverified.** Telegram
  may flag/invalidate a session reused from a different
  device/IP; the workaround on auth failure at the destination is to
  redo `telfs login` there. The bundle's `config.toml` and `db.sqlite`
  still apply.

## Web management UI

`telfs web` boots a self-contained HTTP UI covering the entire CLI:
dashboard, profile CRUD + export/import, multi-step phone login with
logout, one-click channel binding, encryption init, mount supervisor
with HTMX-polled log tail, lifetime Telegram stats (persisted in the
profile's SQLite, visible even while the daemon is down), and a file
browser through a chosen mountpoint.

First-time users land on a guided `/setup` wizard where every step is
an inline form on one page: create profile → API creds → phone login
(code + 2FA inline) → pick a channel from your dialog list → optional
encryption/compression → first mount. State is derived from the
filesystem on every page load — there's no separate wizard progress
file to drift.

```sh
telfs web                                       # 127.0.0.1:8080, no auth
telfs web --listen 0.0.0.0:8080 --token $(openssl rand -hex 32)
```

Loopback bind requires no auth (perimeter = "can reach loopback").
Non-loopback bind **requires** `--token`; the server refuses to start
otherwise. Token check via `crypto/subtle.ConstantTimeCompare`; every
POST form carries a per-session CSRF token. For HTTPS, terminate TLS
in a reverse proxy in front of localhost (`--tls-cert` / `--tls-key`
exist but aren't the recommended path).

## Deployment

**Static binary**: `make release` →
`dist/telfs-vX.Y.Z-linux-amd64.tar.gz`. Built with `CGO_ENABLED=0
-trimpath -ldflags '-s -w'`; copies to any Linux host (needs only
kernel FUSE + `fusermount`). Container images were dropped for now —
FUSE-in-Docker needs `SYS_ADMIN` + `/dev/fuse` + `rshared` bind
propagation, which is more ceremony than the static binary deserves.

## Running unattended

`telfs mount` is a foreground daemon. Backgrounding with `&` in a
shell is fine for testing — but if that shell exits abruptly, the
kernel mount may stay in `/proc/mounts` while writes silently fall
through to the *local* filesystem underneath. Files dropped during
that window land on disk locally, not in the channel.

For production, run under a supervisor:

```ini
# ~/.config/systemd/user/telfs@.service
[Unit]
Description=TelFS mount (profile %i)
After=network-online.target

[Service]
Type=simple
Environment=TELFS_PROFILE=%i
ExecStart=%h/.local/bin/telfs mount %h/External-%i
Restart=on-failure

[Install]
WantedBy=default.target
```

The profile reads its secret from a file — never from the unit's
environment:

```toml
# ~/.config/telfs/profiles/work/config.toml
[secret]
source = "file"
file   = "/home/you/.config/telfs/work.secret"   # chmod 0600 (or 0640 for a service group)
```

```sh
systemctl --user enable --now telfs@work.service
```

(A service mode with `Type=notify`, network-wait and several mounts at
once is the next phase.)

For interactive use, at minimum: `nohup telfs mount ~/External &` —
and verify the daemon survives a shell exit before trusting it with
data.

## Known limits

- **One read-write mounter per channel.** Enforced on one machine (see
  [Mount options](#mount-options)); across machines it is your
  discipline until a channel-side writer lease exists.
- **≤5 s recovery window.** A background journal poster (v0.13) drains
  the local `journal` table every 5 s and posts each pending op to the
  channel; recovery restores the latest snapshot then replays journal
  ops with seq > snapshot.seq. Currently journaled: `CreateChild`,
  `Link`, `Unlink`, `Rename`, `SetSize`. Setattr/xattr/chunk-map
  mutations aren't journaled yet (chunks themselves are independently
  recoverable).
- **Encryption metadata leakage is bounded, not zero.** Snapshots are
  TFSE ciphertext, but chunk count, chunk size to ~chunk-size
  resolution, and snapshot cadence remain visible.
- **One channel = one TelFS.** `fs_uuid` is stored in `meta_kv` and
  baked into every snapshot caption; recovery filters by it, so
  reusing a channel for a fresh instance leaves old snapshots as
  garbage until `telfs gc` reaps them.
- **gotd default DC 2 may be firewalled.** If `telfs login` hangs at
  handshake, try `dc = 1` in `config.toml` (or `TELFS_DC=1`).
- **Trash is flat.** `rm -rf foo/bar/` lands as N siblings under
  `/.trash`, restorable individually but not as a preserved tree —
  fine for typo-undo, use snapshots for archival.

## Roadmap

No named items outstanding. Open directions:

- Reproducible `telfs bench` for chunker/cache/upload regression
  detection.
- Better error UX in the web UI on failed login / network errors.
- Multi-mounter coordination (currently TelFS assumes one writer per
  channel).

See [issues](https://github.com/platonic-realm/TelFS/issues) for
current state.

## Architecture

See [`docs/architecture.md`](docs/architecture.md) for module layout,
SQLite schema, channel message format, and the recovery model.

## License

MIT — see [`LICENSE`](LICENSE).
