# 17 — Format 2 and the security base

Chapters 10 and 16 described TelFS as it stood in August 2026: chunks
optionally encrypted, snapshots wrapped in an encrypted envelope, the
Telegram session in a plain JSON file. This chapter covers the pass that
prepared TelFS to hold an off-site backup of a whole home folder — the
first phase (P1) of a plan whose later phases add scalable snapshots,
versions and a boot service. P1 is about one question: **what can anyone
who reads the channel, or copies the profile folder, learn or do?**

## What was still in the clear

An audit of every place that posts to the channel, on an *encrypted*
format-1 filesystem, found:

- **Journal batches** — every create, rename, unlink, link and size
  change, posted every ~5 s as plain JSON: names, parent inodes (so the
  whole folder tree), modes, owners, sizes, symlink targets. Snapshots
  were encrypted; the journal between them was not.
- **Document filenames** — `ino<N>-idx<M>` for chunks told an observer
  which messages form one file, and with the message sizes, each file's
  size.
- **Captions** — fs_uuid, sequence numbers, counts.
- **The session** — `session.json`, a full login to the Telegram
  account, readable by anything that can read the profile folder (and
  so by every backup of `/home`).

Format 2 is the answer to the first three; `session.enc` and
non-dumpable processes to the last.

## The key hierarchy

```
secret ──Argon2id(salt, 1 pass, 2 GiB)──► KEK ──wraps──► DEK (random, 32 B)
                                                          │
             HKDF-SHA256(DEK, label) ┌────────────────────┼─────────────────┐
             "telfs2 chunk"   chunk key   "telfs2 hash"  hash key   "telfs2 meta"  meta key   "telfs2 caption"  caption key

secret ──Argon2id(session salt)──► HKDF "telfs2 session" ──► session key   (NOT from the DEK)
```

`internal/crypto/v2.go` holds the primitives: `DeriveSubkey` (HKDF from
the standard library), `Box` (AES-256-GCM with a fresh random nonce per
seal and an optional `UseCounter`), `PadmeLen`, and `WrapDEKv2` with its
own AAD so a format-1 wrapped key can never be fed to format 2.

Two details carry weight:

- **The session key is not derived from the DEK.** The DEK lives in the
  database and in the channel; reading the channel needs the session.
  Deriving the session key from the DEK would make a cold recovery
  impossible. So `session.enc` carries its own salt and a mount runs
  Argon2id twice. (The first draft of the decision said otherwise; it
  was corrected before any code.)
- **Argon2id over 2 GiB.** The format-1 default was 3 passes over
  64 MiB: 33 ms on S9, cheap to brute-force once the key header is public.
  RFC 9106's first recommendation takes 0.30 s there. The parameters are
  stored per filesystem; `init --kdf-memory` lowers them for small
  machines.

### The nonce limit

Random 96-bit nonces are safe for 2^32 encryptions per key (NIST SP
800-38D). `UseCounter` counts each key's seals, `persistUses` lands the
counts in `meta_kv` (uncounted, so they don't keep the snapshot dirty),
the daemon warns at 2^31 and `Box.Seal` refuses at 2^32 — the filesystem
turns read-only. A first full backup of a 1.2-million-file home folder
uses about 1/3000 of the budget. A DB rebuilt by `recover --from-captions`
has no counts of its own; the rebuild seeds them from what it read (one
chunk-key use per chunk and pack extent, one meta-key use per pack index,
snapshot and journal body, one caption-key use per document). That is a
lower bound, since messages gc already deleted aren't seen, but not zero.

## Where the secret comes from

`internal/secret` reads the profile's secret from the source its
`config.toml` names (`[secret] source = "prompt" | "file" | "keepassxc"`):

- **file** — opened with `O_NOFOLLOW`, then checked on the *opened*
  descriptor (regular file, no "other" bits, ≤ 4 KiB): no gap between the
  check and the read. Owner and group are deliberately not checked: on
  S9 a boot service runs as `svc-<group>` and reads arash's `0640` file
  through the group.
- **keepassxc** — exactly what `svc-secret` does:
  `secret-tool lookup Path /<entry>`; the secret comes back through a
  pipe; after 2 s a "waiting for KeePassXC" line explains the pause.
- **prompt** — the terminal; `TELFS_PASSPHRASE` skips it.

`secret.CaptureEnv` runs in `main`'s `init`: it moves
`TELFS_PASSPHRASE` into memory and unsets it, so every process TelFS
starts afterwards — the mount daemon, go-fuse's `fusermount3 -u`,
mountctl's lazy unmount, `secret-tool` — inherits an environment
without it. One choke point instead of a dozen `cmd.Env` edits.

The web console never holds a key between requests ("derive, act,
drop"): each action that talks to Telegram reads the source (or a form
field for prompt profiles), opens `session.enc`, and lets the derived
key go with the request. A mount it starts gets a typed secret through
an inherited pipe (`--secret-fd 3`), never the environment.

## The session at rest

`internal/sessionstore` implements gotd's `session.Storage`:

```
"TFS2S" ‖ version ‖ uint16 header length ‖ header JSON {kdf,t,m,p,salt,n} ‖ nonce ‖ ciphertext ‖ tag
```

The header is the AAD; `n` counts writes. gotd's own `FileStorage`
rewrote `session.json` in place on every reconnect — a crash mid-write
left a torn file and a forced re-login — so `StoreSession` writes a
temp file, fsyncs, renames and fsyncs the directory. A v0.22 plaintext
`session.json` is refused (`telfs session encrypt` converts it).

### Non-dumpable

`internal/harden` calls `prctl(PR_SET_DUMPABLE, 0)` in every telfs
process. Measured on S9's kernel: another process of the same user is
then denied `/proc/<pid>/environ`, `exe`, `cwd`, `fd/*` and `maps` (the
files turn `root:root`), no core is dumped, and `cmdline`, `stat`,
`status` stay readable. Children are dumpable again after `exec` — hence
the environment stripping above.

> **Consequence nobody planned for:** `mountctl` found a daemon's profile
> by reading `TELFS_PROFILE` from its environ. On a non-dumpable daemon
> that read fails *silently* — and it guarded `profile delete`,
> `reset-auth` and the web logout. They would have proceeded with the
> profile mounted. The fix moves identity somewhere public on purpose:
> the FUSE mount's source is now `telfs:<profile>` in `/proc/mounts`,
> and the checks fail **closed** when a mount's profile can't be
> determined (`mountctl.ErrUnknownMount`). The daemon reports its own
> log path on `/status` — it may still read its own `/proc/self/fd/1`.

## Channel format 2 — `internal/wire`

Every message of a format-2 filesystem obeys the same rules:

- every document is named `blob`;
- every caption is **exactly 1024 characters**: `tfs2` + base64 of
  `nonce ‖ AES-GCM(caption key, JSON padded to 736 bytes) ‖ tag` —
  1024 is Telegram's caption limit without Premium, and a fixed length
  says nothing about the kind;
- every body is Padmé-padded *inside* the ciphertext, the real length
  with it.

A caption that doesn't start with `tfs2` or doesn't open under the
caption key is simply not ours (`ErrNotOurs`): foreign and format-1
messages are ignored, never deleted.

### Chunk bodies

```
plaintext body = 'c' ‖ v1 ‖ codec ‖ 0 ‖ uint32 plain len ‖ uint32 payload len ‖ payload ‖ zero padding
sealed under the chunk key, AAD = the keyed content hash
```

Padmé applies to the *payload*, not to header + payload: a full
uncompressed 4 MiB chunk is a power of two and pays nothing, so every
full chunk has the same wire size (an early sketch padded header +
payload and would have cost 3 % on every full chunk). `codec` records
what *this* chunk used — a chunk zstd doesn't shrink is stored raw.

The AAD is the hash, not `(ino, idx)`. That is what lets dedup share a
chunk between files without identical ciphertext (format 1's convergent
`v3` needed deterministic encryption for that). It also binds a message
to its content: a message swapped in from another chunk fails to
decrypt. After decrypting, the reader recomputes HMAC(plaintext) and
compares (`ErrHashMismatch`), so even a chunk *sealed* under a wrong
hash returns an error, never wrong data.

### Captions as a rebuild record

A chunk caption carries `{fs, ino, idx, h, len, codec, cs, fsz, mode,
mtime, nl, name}`. The last ones are for the day every snapshot is gone:
`name` is the inode's path at upload time (`meta.PathOfCached`, the
first dirent by `(parent, name)` so hard links resolve the same way
every time), truncated from the left to fit.

> **Bug caught by the test:** the first version took the file size from
> the database — which only learns a file's size when `close` commits it,
> *after* the uploads. Every caption said `fsz: 0`. The writer now passes
> its in-memory size; the rebuild also computes the size from the chunks.

### Packs

A pack is `extent₁ ‖ … ‖ extentₙ ‖ index`. Each extent is a chunk wire
exactly as above (so ranged reads still fetch one extent); the index is
an encrypted, zstd-compressed JSON list of the entries — the same facts
as a chunk caption — padded so the whole document is Padmé-sized. The
pack's caption says where the index sits (`IO`, `IL`), so a rebuild
fetches only that range.

### Snapshots and journal

`SealMeta("snap"|"journal", data)`: `uint64 length ‖ data ‖ padding`
under the meta key with a per-kind AAD. A new journal op, `ref`, records
each dedup hit (slot → existing message): a file whose content was
entirely deduplicated posts no message of its own, so without `ref` no
journal replay or rebuild could restore it.

### The key message

The one clear thing TelFS posts: a **text** message
`tfs2k <base64 JSON>` holding the KDF parameters, the salt, the wrapped
DEK and a `check` (the caption key's seal of the fs_uuid, which proves
the unwrap and names the filesystem). It is **pinned**, so recovery
finds it with one `channels.getFullChannel`; if someone unpins it, a
history scan for `tfs2k ` finds it. Why not keep the header inside each
snapshot as format 1 did? Because captions are now encrypted: recovery
could no longer tell which message *is* a snapshot without the key it
was looking for.

The header must never exist only as one Telegram message, so
`telfs key export` prints it as a text block with a check line (the
first 16 hex digits of its SHA-256 — a mistyped copy from paper fails
before use). `telfs status` and `fsck` compare the pinned message with
the local header byte for byte; the daemon checks at mount and every 6 h
and only warns; `fsck --fix` and `mount --key-header` repair.

`--key-header` applies only to a missing local DB; with a `db.sqlite`
present the mount refuses rather than ignoring the flag. If the export
is the very key message still on the channel (unpinned, say), recovery
goes through that message and re-pins it; the export is re-posted only
when the channel has no key message at all.

`secret change` re-wraps the DEK — it does not change it. So the order
of its steps matters and is crash-safe: post the new key message → write
`session.enc.new` → switch the local header → pin → rename the session
into place → delete the old key messages. A crash anywhere leaves a
state the next start finishes (the session opener adopts a leftover
`session.enc.new`; old key-message ids wait in `meta_kv`). And a mount
recovering from an *old* export never re-posts it over a newer key
message — that would bring the old secret back.

## One pipeline, two formats — `chunk.WireCodec`

The chunk pipeline (writer, packer, cache, reader, staging fallback,
blob prefetcher, fsck) used to take a `crypto.Cipher` and a
`compress.Codec` separately. It now takes one `WireCodec`:

```go
Format() int
Hash(plaintext) []byte                      // dedup identity (and AAD on format 2)
Encode(ino, idx, plaintext, hash) (wire, codec, error)
Decode(ino, idx, hash, wire) ([]byte, error)
ChunkDoc(ctx, info) (name, caption, error)  // direct uploads
PackDoc(ctx, items, extentsLen) (tail, name, caption, error)
```

`Format1Codec` is v0.22 byte for byte (the old constructors wrap it, so
40-odd tests didn't change); `Format2Codec` is the above. The cache's
API grew `GetChunk(ctx, key, meta.Chunk)` because format 2 needs the
row's hash to decrypt; staged wires keep it in their file name
(`<ino>-<idx>-<size>-<hash>.bin`) so a crash between staging and upload
doesn't strand an unreadable wire.

## Rebuilding without snapshots — `internal/rebuild`

`telfs recover --from-captions` walks the whole channel history,
decrypts every caption, reads each pack's index range, and (unless
`--journal ignore`) replays the journal batches it finds on the
*original* inode numbers. Inodes with content but no name afterwards
are placed at their newest name hint, with rsync's temporary names
mapped back — `.NAME.XXXXXX` (measured on S9's rsync 3.5.1) and
`.~tmp~/NAME` (`--delay-updates`); `--inplace` already writes the final
name. A deleted file whose messages haven't been collected would come
back from its captions, so the rebuild remembers every inode the
journal deleted and never resurrects it.

The unit test runs the agreed list — renames after upload, hard links,
duplicate content, rsync in all three modes, packed files — twice: with
the journal everything comes back exactly; from captions alone the
report shows which names survive and which can't (the deduplicated copy,
the empty file, the symlink, the second link name, the later rename).

## Smaller findings on the way

- **`fsck` flagged every packed file.** It downloaded a pack and tried
  to decrypt the whole blob as one chunk; on unencrypted filesystems it
  reported a size mismatch instead. It now downloads each message once
  and decodes each extent.
- **`status` could mint an empty database.** It opened `db.sqlite`
  without checking it existed; `meta.Open` creates one with a fresh
  fs_uuid, and the next mount then skips recovery. Same for the web's
  compression form. Both stat first now — the trap CLAUDE.md already
  recorded for the web. The break-glass drill then showed the other half:
  the P1 test tool made the same mistake, and `telfs mount` took the
  empty database for a filesystem and posted its empty snapshot. The
  mount now refuses a database that no `telfs init` and no recovery
  produced (format 1, never snapshotted, only the root directory, no
  chunks, no journal) — "never implicitly" holds for a stray file too.
- **`telfs unmount` and relative mountpoints.** mountctl finds a daemon by
  its command line (`telfs … mount … <mountpoint>`). Started as
  `telfs mount ./mnt`, the line said `./mnt` while `/proc/mounts` said
  `/home/…/mnt`: no match, so `unmount` "found no daemon" and detached the
  mount directly — no orderly shutdown, no final snapshot, an orphan daemon
  still writing. Old bug, but non-dumpable sealed the escape hatch (a
  daemon's `cwd` is unreadable now). `telfs mount` now re-execs itself with
  the absolute path, so the command line always names the mount exactly.
- **What the journal did not carry.** chmod/chown/utimes after creation
  and xattr changes were not journal ops. A snapshot covers them; a rebuild
  without snapshots could not (rsync creates files `0600` and sets the real
  mode and mtime last — the live test's rebuilt tree had the right names and
  content but nearly every mode and mtime wrong). Format 2 now journals
  `setattr`, `setxattr`, `rmxattr` and chunk-size promotions, compressed.
  Chasing the one mtime that still came back wrong uncovered three more
  bugs, each older than P1:
  1. `SetSize` wrote the size and its journal row in two transactions, so a
     `touch` landing between them was journaled in the wrong order;
  2. replaying a size change used the replay's clock as mtime;
  3. and the real culprit: closing a handle that wrote nothing still
     committed the size with mtime = now — `touch -d 2020-01-02 f` opens
     the file for writing, sets the date, closes, and the close erased the
     date. The kernel's 5-second attribute cache hid it: a `stat` right
     after looked fine.
  A fourth one surfaced in passing: replayed ops are not re-journaled, so
  after a recovery the local journal sequence stayed at the snapshot's —
  the next change reused sequence numbers already on the channel, and a
  later recovery would apply old ops twice. Recovery now advances it.
- **One writer, and drills that write nothing.** The mount source became
  `telfs:<profile>:<fs_uuid>`, so `telfs mount` can refuse a second daemon
  for a profile and a second writer for a filesystem on the same machine
  (an flock on `<profile>/daemon.lock` closes the start-up race). A second
  *machine* is not covered — nothing on Telegram is a compare-and-swap.
  `--readonly` is enforced in the session: every channel-writing call
  returns `ErrReadOnly`, so a recovery drill on another machine cannot post
  a snapshot that would compete with the real writer's.
- **`--no-recover` used to start an empty filesystem.** A filesystem is
  now created only by `telfs init`; a channel without one is an error.

## Invariants to keep

- Nothing posted for a format-2 filesystem is readable without the
  secret except the key message, sizes and times. `telfs debug observe
  --marker …` checks a live channel for exactly that.
- Format-2 chunk rows carry their hash; anything that moves or copies a
  chunk row (dedup, repair, rebuild, recovery) must keep it.
- Every caption is exactly `CaptionLen` characters.
- The session key never derives from the DEK.
- No telfs process puts a secret in an argv or an environment; the
  environment strip happens before anything else runs.
- Profile identity is read from `/proc/mounts` sources; checks that ask
  "is it mounted?" fail closed.
