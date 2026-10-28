# 18 — v1: TelFS as a backup service

Chapter 17 ended with a filesystem whose channel says nothing to an
observer and whose metadata can be rebuilt from captions alone. That was
the first phase of a longer plan. On 28 October 2026 the plan changed:
instead of five more phases, ship **v1** — the first version a set of
backup services on S9 can be built against (a boot mount, a daily backup
job, monitoring, notifications), and start a four-week trial on it.

A desktop mount and an off-site backup store want different things. The
desktop user writes a few files, looks at them, unmounts. A backup job
writes 400 GB in one night through a line that uploads at a fraction of
that, must learn *when* the channel holds the result, runs without a
human at the keyboard and without the secret, and will one day have to
bring back 300,000 small files in hours, not weeks. This chapter covers
the nine pieces v1 added for that, each in the order "what went wrong
without it, then how it works".

## The numbers that framed it

Everything below was decided against measurements from P1's scale test
(1,021,969 inodes, 26 October – 28 October 2026) and the upload
measurements of 28 October:

- a whole-DB snapshot of 1 M files: **107 MB** sealed — about 100 bytes
  per inode;
- the old cadence uploaded one at every mount, every 5 minutes, soon
  after every queue drain and at unmount — where it had **10 seconds**
  and failed every time at that size;
- recovery found the newest snapshot by **scanning history** (gave up
  after 20,000 messages), while a backup posts ~100 k chunk messages;
- the journal: 5–6 ops per file, ~117 bytes per file on the channel
  with chunk placements; a replay of 5.16 M ops took ~12 min;
- uploads: **66 KB/s–0.2 MB/s** on a line that carries 10–12.5 MB/s —
  a first 400 GB backup in about 70 days;
- cold reads through the mount: one download per file (~0.8 s each,
  ~120 files a minute) once the blob prefetcher stalled.

The v1 acceptance run (≈0.5 M files, ≥10 versions, a cold recovery
checked file by file, a timed restore, service boot and stop) has not
happened yet; the code below is unit-tested against fakes of the
channel.

## 1. The staging queue and backpressure

### What was there

Since the speed pass of August 2026 (chapter 06's "async close"),
`close()` returned at local speed: a file's dirty chunks were handed to
upload goroutines and the handle let go. Two things bounded that:
`DefaultStagedCapBytes` (256 MiB) of chunks in flight, and the wire
staged in `cache/pending/` for crash recovery (chapter 06, "Pending
wires on disk").

The 256 MiB looked like a disk limit and was a **RAM** limit. A chunk's
goroutine held its wire in memory until the upload landed, the packer
held every wire it was given, and `recoverPending` read every staged
wire into memory at mount. Over a slow line a 400 GB rsync would either
stall at 256 MiB (fine) or — had the cap been raised to use the disk —
eat the machine's memory.

### Two stages — `internal/chunk/writer_staged.go`

Queue mode is on whenever the pool has a staging directory and a
`DiskGate` (production always; tests without a `PendingDir` keep the
in-memory pipeline untouched). A chunk now goes through two stages:

```
stage 1 (dispatch goroutine, under the 256 MiB RAM allowance)
  hash → dedup lookup → encode → DiskGate.Acquire(wire len)
       → PersistPendingHashed → goroutine ends, memory released
stage 2 (reads the wire back from disk)
  direct chunk: a job on the pool's queue (workers = pool concurrency)
  small chunk:  Packer.SubmitStaged by reference (read at flush,
                at most one batch in memory)
```

The line between the stages is also a line in the failure semantics.
Before the wire is on disk, a failure (dedup lookup, encoding, the gate,
the write) puts the chunk back into the writer's dirty set — its data is
still in memory, exactly as before. After, the staged wire *is* the
chunk: a failed upload is retried from it with backoff (5 s doubling to
5 min) for as long as the daemon runs, so a network outage resumes by
itself; at shutdown the wire stays and the next mount's `recoverPending`
submits it — by reference, so recovering 50 GB of staged data never
loads it into memory.

### The gate — `internal/chunk/staging.go`

`DiskGate` counts staged bytes against `[staging] max_bytes` (default
50 GB) and keeps the staging filesystem above `[staging] min_free`
(default 5 GiB). Backpressure is a chain: above the cap,
`Acquire` blocks; the blocked stage-1 goroutine keeps its share of the
RAM allowance; once 256 MiB of those wait, the writer's dispatch blocks;
`write` and `close` slow down; rsync waits. A 400 GB rsync becomes an
rsync at upload speed instead of a full disk.

The floor has one case that waiting cannot fix: the filesystem is below
`min_free` and *nothing* TelFS staged is left to upload — no upload will
ever free the space. Then `Acquire` returns `ErrStagingFull`, the writer
keeps it as a sticky error, and `write`/`flush`/`fsync` fail with
`ENOSPC` instead of hanging forever.

> **The double release.** Staged bytes go back to the gate when the
> wire's file is removed. Two parties remove staged wires: the uploader
> after `PutChunk`, and an unlink or a newer generation of the same slot.
> If both called `Release(size)`, the gate's count would drift below
> reality and the cap would stop meaning anything. So bytes are released
> **only** by `DiskGate.Remove`: stat, remove, release *if this call
> removed the file*. Queue mode never calls `DeletePending` directly.

Two smaller decisions: the daemon's status reads the gate's counters and
an in-memory index of staged files instead of listing a directory that
may hold 100,000 wires on every two-second poll; and staged wires are
still not fsynced (a pre-existing choice — after a power loss the
mount's crash repair cuts files back to what reached the channel and
names them, chapter 17).

The web console's link lamp used to turn red at 1,000 staged wires — in
August that many meant the FK-violation cleanup of chapter 14 was
leaking. With the queue, 100,000 wires is what a first backup looks
like. The lamp now looks at bytes against the cap: yellow above 80 %,
red ("backlog") only while a writer actually waits.

## 2. The control socket

### Why the TCP port had to go

The daemon used to serve its JSON pages and Prometheus metrics on
`127.0.0.1:9090` by default. Three problems for a service:

- a second daemon (a second profile, or a version mount) could not bind
  the same port, so one of them was silently invisible;
- any local user can connect to a loopback TCP port, and the new
  commands — `sync`, `unmount`, `versions create|delete` — are not ones
  another user should be able to issue;
- the backup job, the monitoring and the notifications must not hold the
  secret; only the mount's own unit loads it. They need a channel to the
  daemon that needs no key at all.

### `internal/control`

Every mount serves `<profile>/daemon.sock`: an HTTP server on a Unix
socket, mode 0600 inside the 0700 profile directory. (`$XDG_RUNTIME_DIR`
was the obvious place, but a system service user has none.) The listener
also checks `SO_PEERCRED` on every connection and drops any whose uid is
not the daemon's — the file mode alone would let root-owned helpers or a
misconfigured directory through.

Three lifecycle rules:

- the socket is created **before** the FUSE mount — if it fails, no
  mount exists without a daemon that can be asked to stop — and served
  once everything else exists;
- a live socket is never taken over: `Listen` dials first and returns
  `ErrBusy` when someone answers; a stale file from a dead daemon is
  replaced;
- it is removed at shutdown.

Routes: `GET /status` (the schema-1 object below), `GET /sync`,
`POST /sync/begin`, `GET /sync/wait`, `POST /stop` (exactly SIGTERM),
`POST /fsck/quick`, the version routes, and the old `/telegram` and
`/cache` pages for the web console, which now reads the active
profile's socket. `--metrics-addr` still exists, opt-in, for Prometheus.

A version mount (section 6) gets its own socket,
`versions/v-<hash of the name>.sock` — a socket path is limited to 107
bytes and a version name may be 64 characters long.

### `status --json` and `unmount`

`telfs status --json` prints one object, **schema 1**: within it fields
are only ever added, so a monitoring script written today keeps working.
With a daemon it is the daemon's view (`statusV1` in
`cmd/telfs/daemonctl.go`); without one it is computed from local state
— the staging directory, the journal's unposted rows — with the
live-only fields null. It needs no secret.

`telfs unmount` asks the daemon over its socket (`POST /stop`) and
returns when **that process** has exited — not when the kernel mount is
gone, which happens long before the daemon finishes draining. No SIGKILL
(the old path escalated); `--timeout` bounds the wait. Daemons from
before v1 keep the signal path.

## 3. The sync barrier

### The question

"Is the backup I just wrote on the channel?" is not "is the upload queue
empty right now". A mount that keeps receiving writes may never be
empty, and writes made *after* the backup must not move the finish line.
What a backup job needs is a barrier: everything whose call had returned
before I asked.

Since P1's decision 9, format 2 journals every chunk placement in the
same transaction as its `chunk_map` row (chapter 17, "Where the chunks
went"). So no snapshot is needed: once the uploads have landed and the
journal up to their placements is posted, a cold recovery brings that
state back.

### Two phases — `daemonCtl.syncCheck`

```
POST /sync/begin   token.reg = pool.LastRegistered()      (uploads so far)
GET  /sync/wait    phase 1: pool.OpenUpTo(token.reg) == 0  (those landed)
                   then, once: token.journal = meta.LastJournalSeq()
                   phase 2: meta.UnpostedUpTo(token.journal) == 0, kick the poster
```

`UploadPool.Register` numbers every registration, so "the uploads before
the token" is a number, not a snapshot of a set. The journal target is
fixed only **after** phase 1: a chunk's placement is journaled when its
upload lands, so a target fixed at `begin` would miss exactly the ops
the barrier is for. Later writes register higher numbers and journal
higher seqs; neither moves the finish line.

`/sync/wait` is a long poll (at most 5 minutes per request, the CLI asks
every 30 s), so `telfs sync --wait --timeout 6h` holds no connection open
for six hours; tokens are dropped an hour after they were made.

Two consequences found while building it:

- **Data of a file still open counts once it is closed.** Dirty chunks
  of an open handle are not registered yet; `sync` cannot wait for
  writes that have not been handed to the pipeline.
- **Recovered wires register too.** `recoverPending` used to submit
  staged wires without registering them, so a sync right after a mount
  would have ignored them — and a `NewWriter` on the same inode did not
  wait for them, so a stale recovered wire could land *after* a newer
  write of the same chunk and overwrite it. Registering fixes both.

On format 1 the barrier is refused: its journal carries no chunk
placements, so "posted" would not mean "recoverable".

## 4. Service mode

`telfs mount --service` is what a systemd unit runs (`cmd/telfs/service.go`).

- **No prompts.** `noPrompt` is set; a prompt-source profile exits 2 at
  once instead of waiting forever for a terminal that a boot does not
  have.
- **`Type=notify` without cgo.** `internal/sdnotify` writes datagrams to
  `$NOTIFY_SOCKET`: `STATUS=` lines say what the service is doing or
  waiting for; `EXTEND_TIMEOUT_USEC` every 30 s until ready, so a boot
  without network waits instead of being killed after
  `TimeoutStartSec`; `READY=1` after "Mounted at" — the control socket
  is serving by then, so the backup unit ordered after it can talk to
  it; `STOPPING=1` at shutdown.
- **Waiting at boot.** `retryAtBoot` retries the cold recovery and the
  channel-access probe, 5 s doubling to 5 min — but only on failures
  worth waiting out: network errors, timeouts, `ErrNotConnected`,
  Telegram 5xx. A refusal (the channel is gone, the session revoked)
  ends the service with exit 6; a refused recovery with 7. (gotd's own
  first connect already waits forever.)

> **The retry that would have minted a filesystem.** A recovery that
> fails half-way leaves a `db.sqlite` behind. The next attempt would see
> a DB, skip recovery and mount whatever was in it — the trap chapter 17
> describes for `status`. Each retry therefore removes what a failed
> attempt left (DB, `-wal`, `-shm`) before it starts.

The exit codes are the interface the units are built on, the same for
every command:

| Code | Meaning |
|---|---|
| 0 | ok |
| 1 | usage or other error |
| 2 | secret source unavailable |
| 3 | wrong secret |
| 4 | timeout / not synced yet |
| 5 | not mounted (no daemon answers) |
| 6 | Telegram or the channel unreachable or refused |
| 7 | integrity problem (fsck findings, recovery refused, restore mismatch) |

2 used to be "usage error"; it moved to 1 so that 2 could mean "secret
source unavailable" — a unit must be able to tell "the credential is
missing" from "someone typed the command wrong". The example unit
(`contrib/systemd/telfs@.service`) sets `RestartPreventExitStatus=2 3 6 7`:
those need a human, and a restart loop would only fill the journal.

A stop drains the uploads under way (12 s), posts the journal (10 s)
and exits 0 inside a 30 s watchdog; what is still staged stays on disk
for the next start. A planned stop that must leave the channel complete
runs `telfs sync --wait` first.

## 5. The secret from systemd's encrypted credentials

P1 gave each profile a secret source: a prompt, a file, KeePassXC
(chapter 17). A boot service on S9 gets its secret through systemd:
`LoadCredentialEncrypted=telfs-secret:/etc/credstore.encrypted/telfs-%i`
decrypts it into the unit's own credentials directory. The profile says

```toml
[secret]
source = "file"
file   = "${CREDENTIALS_DIRECTORY}/telfs-secret"
```

`SecretConfig.ResolvedFile` expands **only** that variable — it is a
path, not a secret — and nothing else: unset (the command is not
running in that unit) is a source error, exit 2, with no fallback to any
other location; a `..` that would climb out of the directory is
rejected. The secret is read into memory and never copied to disk.
Measured on S9 (systemd 262): the credential is a regular 0400 file of
the service user in a 0500 directory on tmpfs, which passes P1's file
checks (regular, no symlink, no "other" bits, ≤ 4 KiB) unchanged.

Which commands need it is a design property, not an accident: the mount
and `restore` decrypt and need it; `status`, `sync`, `unmount`,
`versions` and `fsck --quick` go through the control socket and never
see it.

## 6. Snapshots that scale: journal first, and the anchor

### Rare snapshots

With every change in the journal, the newest snapshot plus the journal
after it *is* the filesystem; a snapshot only shortens a recovery's
replay. So the format-2 policy (`internal/snapshot/policy.go`) takes one
when the journal has grown by `[snapshot] every_ops` (500,000) since the
last, or `every` (24 h) has passed and the journal moved — and then at a
quiet moment (the upload queue drained), waiting at most an hour for
one. Nothing at mount when the seq is unchanged, nothing on drain,
nothing at unmount. A failed step backs off 1 → 30 min. At ~0.5 M files
that is a ~55 MB upload a few times a day at most instead of 107 MB
every five minutes.

`Policy.Decide(state, now)` is a pure function of a `PolicyState` read
from `meta_kv`, so the whole policy is one table test. What the shutdown
does instead of a final snapshot: the poster's own final drain runs when
the stop signal arrives, *before* the upload drain — so uploads that
land during the drain journal placements after it. The shutdown posts
those.

Replay cost bounds the threshold: P1 measured 5.16 M ops replayed in
~12 min, and a recovery of the 1 M-file snapshot plus a 597,012-op
journal in 1 min 48 s.

### The anchor

Rare snapshots make the scan problem worse — the newest snapshot may be
100,000 chunk messages back. The fix is a small **pinned** document,
the anchor (`internal/snapshot/anchor.go`, `internal/wire/anchor.go`):
caption kind `"a"`, body
`SealMeta("anchor", {v, fs, snap_msg, snap_seq, snap_ts, jnl_after, versions})`.
After each snapshot (or version change) a new anchor is posted and
pinned and the previous one deleted, which removes its pin.

Recovery:

```
pinned messages (messages.search, pinned filter)
  → the key message → unlock → the anchor
  → the snapshot it names
  → journal batches with id > jnl_after (getHistory, min_id walk)
  → refuse a hole in the seqs → replay → AdvanceJournalSeq
```

No scan, however many messages the channel holds. Filesystems from
before the anchor recover by the old scans and get an anchor at their
first mount.

> **jnl_after is not the snapshot's id.** The obvious boundary — "replay
> the journal batches posted after the snapshot message" — loses data.
> A 50 MB snapshot takes minutes to upload; the journal poster keeps
> posting meanwhile, and those batches carry ops the snapshot (taken
> when the upload *started*) does not contain, yet have *smaller*
> message ids than the snapshot. So `jnl_after` is the channel's newest
> message id read **before** the snapshot is taken. Walking from there
> may also meet batches whose ops the snapshot already holds; they are
> dropped by seq (`> snap_seq`), and the remaining seqs must be
> contiguous or the recovery refuses. Too early a boundary costs a few
> downloads; too late a one loses ops silently. The fake channel has a
> test for exactly this race.

Two pins broke an assumption: `channels.getFullChannel` names only the
*newest* pinned message, and the key message is older than the anchor.
`tg.Session.PinnedMessages` lists them all, and the key-header check
passes when any pinned key message matches.

Retention and `gc` never delete the snapshot the current anchor names;
`gc` leaves anchors (and any kind it does not recognise) alone.
`snapshot restore` marks a snapshot as owed (`snap_due`), so the next
mount takes one at once and the anchor follows the rolled-back state.

The format-2 code takes a `snapshot.Channel` interface, which
`*tg.Session` implements; policy, anchor and recovery are tested
against an in-memory channel.

What this is not: incremental. A snapshot is still the whole database
in one document, and a document holds at most 2 GiB — about 20 M files
at ~100 bytes each. True incremental snapshots are needed only far
beyond 10 M files, so they wait.

## 7. Versions

A backup store needs named points that nothing can change: "the backup
of 28 October". Three designs were on the table — A: a sealed whole-DB
snapshot under a name, read through a separate read-only mount; B: A
plus `/.versions/<name>/` inside the live mount; C: copy-on-write
versions inside the live DB. A won as the smallest correct one: a
version is **immutable by construction** (a posted message nothing
edits), and since v1 reclaims nothing, nothing a version needs can be
deleted — `telfs gc` simply refuses on a filesystem with versions.

`telfs versions create NAME` goes through the daemon, so it needs no
secret. It runs the sync barrier first — advanced by the client's long
polls, so nothing is posted when nobody waits any more — then posts
`snapshot.Take` as a `blob` with body `SealMeta("version", gz)` and a
caption of kind `"v"` (name, journal seq, created, DB size, DB SHA-256),
records it in the registry (`meta_kv versions`) and re-posts the anchor
with the version list. `--timeout` bounds the sync only (exit 4, nothing
posted). If the anchor post fails after the version is on the channel,
`anchor_due` is set and the policy loop re-posts it; create still exits
0, because the version exists.

`telfs mount --readonly --version NAME` downloads the version into
`<profile>/versions/NAME.sqlite` (reused while its size and SHA-256
match the registry), opens it immutable (`meta.OpenReadOnly`) and serves
it read-only with its own cache, lock and control socket. Three details
carry weight:

- **Its own cache.** The chunk cache is keyed by `(ino, idx)`; inode 42
  of a version is not inode 42 of today's tree.
- **The current key header.** A version's DB holds the key header of the
  day it was taken; after a `secret change` that header no longer opens
  with the secret. The version mount reads the header from the live DB.
- **The mount source ends in `:v=NAME`.** The single-writer guard
  ignores such mounts, so a version mount runs next to the live
  read-write mount and never blocks the boot mount. And
  `telfs unmount <mountpoint>` of a version mount uses the version's
  socket — the profile's socket would have stopped the live daemon.

After a recovery the registry comes from the anchor;
`recover --from-captions` finds versions by their captions. Each version
costs one snapshot on the channel, ~55 MB at 0.5 M files.

## 8. Restore and verification

### The stall

The 1 M test's md5 pass through the mount fell to one download per
file. The blob prefetcher (`internal/chunk/blobprefetch.go`, added in
the August speed pass) answers a cold read of a packed file with one
ranged download of the pack, forward from the miss, warming the
neighbours a walk is about to read. A span that found fewer than two
uncached extents kept its
*provisional* coverage `[miss, miss + 4 MiB)` — although it had examined
at most 2,048 extents, about half a megabyte of small files. Every later
miss inside the window counted as "covered" and was dropped. A
name-order walk produces exactly that pattern. The fix records, for a
span that warms nothing, only what it examined
(`TestBlobPrefetchStallAfterSkippedSpan`, written from the P2
reproduction, fails before it).

### Why a restore command anyway

Even fixed, a walk in name order (rsync, `cp -r`) reads in an order the
packs were not written in. The P1 verification tool did the opposite —
each message downloaded once, in message order — and checked 1,003,008
files (241 MB, 49 messages, 267 requests) in 3.5–4.5 minutes, where the
mount managed ~120 files a minute. `telfs restore` is that tool made
into a command (`internal/restore`):

- the plan comes from **one read transaction** (`meta.Store.Read`, a
  read-only view) of the live DB — beside a running mount — or of a
  version's DB;
- chunk rows are grouped by message; each message is downloaded
  **once**: a pack as one ranged read from its first to its last needed
  extent, a direct chunk whole; four workers, in message-id order,
  which is upload order;
- decoding is the filesystem's `WireCodec` — authenticated;
- it writes directories, regular files (empty and sparse too), symlinks,
  hard links, modes, mtimes, `user.*` xattrs; owners only as root.

### Verification

"Decrypted fine" proves the channel; it does not prove the copy on
disk. So every file written is **read back from DEST** and each chunk's
keyed hash compared with the DB row. The same check makes the restore
resumable: a file already right (size and hashes) is skipped, and in a
partly right file only the wrong chunks are downloaded again.

Problems are kinds, weighed for the exit code: **7** integrity (a chunk
missing from the DB, a message gone or damaged, a mismatch in DEST)
before **6** Telegram (the restore stops; a re-run resumes) before **4**
a live restore that met data the mount had not uploaded yet before
**1** DEST not writable. The full list goes to
`<profile>/restore-<time>.tsv` — never into DEST, so the restored tree
stays exactly the backup. The engine takes a `Downloader` and a `Codec`,
so a corrupted byte in a fake channel can be tested to exit 7 with the
file named.

## 9. `fsck --quick`

The full `fsck` downloads and decrypts every chunk — hours. Monitoring
needs something it can run daily, cheaply and without the secret: does
the channel still hold every message the filesystem references?

`internal/fsck.Quick` pages the referenced messages 100 at a time
(`meta.Store.MessageRefs`: one short `GROUP BY` over the message-id
index per page, so it never holds a read transaction open on a live DB)
and asks the channel about each page with **one** `channels.getMessages`
(`tg.Session.MessageDocs`: existence and document size, no content).
A message is *missing* when it is deleted, has no document, or the row's
id cannot be a message id; *too small* when its document is shorter than
its furthest packed extent (`pack_offset + pack_size`). Direct chunks
are checked for existence and a document only — their wire length is
not in the DB.
About 1,000 requests for 0.5 M files.

It runs inside the daemon (`POST /fsck/quick`, no secret) when one
answers, else with its own read-only session (secret). Network errors
are retried (6 attempts, 1 s doubling to 30 s), then it fails as exit 6;
findings are exit 7. It says nothing about content — a message that is
there but damaged is the full `fsck`'s, or `restore`'s, to find.

## 10. Parallel uploads

### Where the 66 KB/s went

The account's files live in data centre 5, ~170 ms away with ~1 %
packet loss. One TCP stream that far carries about MSS / RTT · 1.22 /
√loss — a fraction of a megabyte per second — however many upload parts
ride on it, and TelFS sent everything, one 512 KiB part at a time, over
gotd's one main connection. Measured with a stand-alone tool on 28
October (32 MiB each):

| Parts in flight × connections | MB/s |
|---|---|
| 1 × 1 (as TelFS did) | 0.214 |
| 4 × 1 | 0.233 |
| 2 × 2 | 0.327 |
| 2 × 4 | 1.128 |
| 2 × 8 | 4.154 |

More parts on one connection buy nothing; more connections do.

### `internal/tg`: a pool, a byte bucket, parts sent by TelFS

- **A pool of connections.** The session manager opens gotd's
  `Client.Pool(N)` (`[upload] connections`, default 8) for each
  connection cycle and closes it when the cycle ends; read-only sessions
  and one-shot CLI clients get none and upload as before. gotd's pool
  does **not** apply the client's middlewares, so its invoker is wrapped
  in the same chain by hand — RPC limiter, byte limiter, flood waiter,
  tracker — or flood waits, the adaptive limiter, the status page and
  the stall watchdog would all have been blind to uploads.
- **Parts sent by TelFS.** `uploadParts` keeps `parts_per_upload`
  (default 2) parts in flight. gotd's uploader could not: its streamed
  upload with more than one thread is a data race — the race detector
  found the reader goroutine setting the part total while senders read
  it — and its known-size path sends files up to 10 MiB one part at a
  time. The wire format stays the streamed one TelFS always used:
  `saveBigFilePart`, 512 KiB parts, `file_total_parts = -1` on every
  part but the last, which goes out after all others are saved.
- **A byte-rate cap.** `ByteLimiter` is a token bucket in bytes per
  second (`[upload] max_rate`, default 5 MB/s — about 40–50 % of the
  line) over `saveFilePart`/`saveBigFilePart`, burst one part, shared by
  every connection. The adaptive requests-per-minute limiter stays for
  flood waits.
- **FLOOD_PREMIUM_WAIT** — Telegram's upload limit for accounts without
  Premium — is its own type, waited out like `FLOOD_WAIT` and logged by
  name.

At 4 MB/s a 400 GB first backup takes about 28 hours instead of 70 days.
Two things are still open: the defaults are to be finalised with the
measurement run of 28 October, 21:47, and the adaptive limiter starts
every mount at 120 requests a minute (~1 MB/s of 512 KiB parts) and
needs ~13 minutes of flood-free uploading to reach 5 MB/s.

## What v1 leaves for later

- **Reclamation.** `gc` refuses while mounted and with versions; an
  always-mounted filesystem with versions only grows. A version-aware
  gc inside the daemon is the next big piece.
- **Dedup of packed files** and explicit reference counts.
- **`/.versions/` inside the live mount**, retention inside TelFS.
- **Incremental snapshots** (the 2 GiB ceiling).
- **A writer lease** on the channel: nothing stops a second machine.
- **The web console** for versions, restore and sync.

## Invariants to keep

- In queue mode, a chunk's failures before its wire is staged restore it
  to the dirty set; after, they are retried from the staged wire. Staged
  bytes are released only by `DiskGate.Remove`.
- Nothing reads a directory listing on a status poll; counters only.
- The control socket is 0600, checks the peer uid, is created before the
  FUSE mount, and is never taken over while live. Everything a job needs
  goes through it without the secret.
- A sync token is an upload registration number; its journal target is
  fixed only after those uploads landed. Anything that submits an upload
  — including `recoverPending` — registers it.
- `status --json` schema 1: fields are only ever added.
- Exit codes: 2 secret source, 3 wrong secret, 4 not synced / timeout,
  5 not mounted, 6 Telegram, 7 integrity — the same for every command.
- Format 2 takes snapshots by the policy only — never because of a
  mount, a drain or an unmount; the anchor's `jnl_after` is read
  *before* the snapshot is taken; the anchored snapshot is never
  deleted.
- A version is never edited; `gc` refuses while versions exist; a
  version mount writes nothing to the channel, uses its own cache, the
  live DB's key header and its own socket.
- `restore` never writes its report into DEST and verifies every file
  it wrote against the keyed hashes.
- Every upload RPC — on any connection — goes through the same
  middleware chain.
- New features refuse format 1 with a clear message; format 1 keeps
  working as in v0.22.
