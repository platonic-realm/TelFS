# 14 · A perf + correctness pass (the kernel-untar story)

This chapter walks through a single working session that hardened
several subsystems at once. It's a useful read for two reasons:

1. **It shows how to diagnose.** Every fix below started as a wrong
   guess that the daemon log eventually disproved. Reading the wrong
   guesses matters because that's the shape of real debugging.
2. **It anchors every code change** to a specific file:line, so you
   can correlate the chapter against `git log` for the session.

If you want the short version, skip to "Summary table" at the end.

## The trigger

A user ran `tar -xf linux-6.6.141.tar.xz` onto a mounted TelFS, then
`make tinyconfig`. The build failed:

```
HOSTLD  scripts/kconfig/conf
input in flex scanner failed
make[4]: *** [scripts/kconfig/Makefile:77: allnoconfig] Error 2
```

That error means flex's `fread()` returned 0 with `errno != EINTR`
— the kernel returned `-EIO` to userspace. So somewhere in
`Reader.ReadAt`, a chunk was failing to be served.

### Wrong guess #1 — the read-after-write barrier

The first suspicion was that `pending.Wait` was blocking too long
under the new 60-minute packer-upload timeout. The reasoning was
plausible: a Kconfig file was just untarred, the packer hadn't
flushed its batch yet, the R-A-W barrier sat there for minutes, and
somehow the kernel returned EIO.

This was almost certainly wrong as a primary cause, because FUSE's
kernel-side read path doesn't impose a timeout — a blocking ReadAt
would *hang*, not fail with EIO. We documented this guess and
proposed an "eager pending-dir fallback" to bypass the barrier when
staging had the bytes. That fix is still correct (see item 4 below),
but it was **not** the cause of the flex error.

### Looking at the actual log

```
$ grep -c "LIMIT_INVALID" /run/user/1000/telfs-web-main.log   # 11
$ grep -c "LOST chunk"   /run/user/1000/telfs-web-main.log    # 380
$ grep -c "FOREIGN KEY"  /run/user/1000/telfs-web-main.log    # 760
```

Two distinct failure modes had been quiet all along. The lesson:
**always grep the log before theorising**. The log told us the truth
in the first minute we asked it.

## Bug 1 — `LIMIT_INVALID` on `upload.getFile`

The daemon was issuing a ranged download like this:

```
download msg 36975 [15716556:15745125]: rpc error code 400: LIMIT_INVALID
```

That range is 32 KiB long, 4 KiB-aligned at both ends — the obvious
Telegram constraints were satisfied. But there's a less obvious one:
**`upload.getFile` rejects requests that cross a 1 MiB boundary**.

```
floor(offset / 1MiB) == floor((offset + limit − 1) / 1MiB)
```

For the failing request, `offset = 15716352 (aligned)`,
`offset + limit − 1 = 15749119`, and the boundary at
`15728640 = 15 × 1 MiB` falls inside that span. Telegram says no.

This bites *exactly* packed small-file extents. Small files live as
sub-ranges inside a 32 MiB packed blob at arbitrary offsets — most
will straddle a 1 MiB boundary somewhere. So every read of a small
Kconfig file from a packed blob had a high probability of failing.

### The fix (`internal/tg/session.go:455-467`)

```go
limit := int64(rangePartSize)
if cur+limit > alignedEnd {
    limit = alignedEnd - cur
}
mibBoundary := ((cur / (1 << 20)) + 1) * (1 << 20)
if cur+limit > mibBoundary {
    limit = mibBoundary - cur
}
```

When the loop is mid-MiB and would request bytes crossing the next
1 MiB line, we shorten this request to end *exactly* at that line
and let the loop make one extra round on the other side. Two
requests instead of one, but each one valid.

## Bug 2 — `FOREIGN KEY constraint failed` × 760

```
[writer] LOST chunk (ino=187219, writer detached, no retry):
  pack chunk 0: packer put_chunk: put chunk 187219/0:
  constraint failed: FOREIGN KEY constraint failed (787)
```

`chunk_map` has `FOREIGN KEY (ino) REFERENCES inodes(ino) ON DELETE
CASCADE`. The packer's `PutChunk` ran *after* the upload completed;
by then the inode had been unlinked. Tar re-extracting the kernel
tree overwrote the previously-extracted files, deleting their
inodes while their chunks were still queued in the packer.

The consecutive even-numbered inode IDs in the log (99198, 99200,
99202… and later 187104, 187106, 187108…) are the signature of
**two separate kernel extractions** with the second unlinking the
first one's tree.

### Local damage we'd been ignoring

Worse: the packer's `flush()` only deleted the staged wire on the
*success* branch. On FK-violation it left the wire in
`cache/pending/` forever. The /telegram queue panel showed 380
"Staged on disk" wires that never drained — once during runtime,
only the next daemon restart's `recoverPending` orphan-cleanup pass
would remove them.

### The fix — three sub-fixes

**11a. Cancel on unlink** (`internal/chunk/packer.go:DropInode`,
hooked via `internal/meta/store.go:SetInodeDeletedCallback`,
wired in `cmd/telfs/main.go` after Packer construction):

`Store.Unlink` and `Store.Rename` now notify a callback when an
inode row is actually deleted. The daemon hooks the packer:

```go
metaStore.SetInodeDeletedCallback(func(ino int64) {
    if n := packer.DropInode(ino); n > 0 {
        fmt.Fprintf(os.Stderr, "[packer] dropped %d pending wire(s) for unlinked ino=%d\n", n, ino)
    }
})
```

`DropInode` walks `p.pending`, pulls every wire with that `ino`,
deletes their staged files, and closes their result channels. The
upload bytes are never sent.

**11b. Silent drop on FK at `PutChunk`**
(`internal/chunk/packer.go:369-388`):

If the inode was already gone by the time we tried `PutChunk` (the
unlink happened *after* the upload started), we detect the FK via
`meta.IsFKViolation(err)`, delete the wire, log a single info line,
and close the result with no error. The orphan chunk on the channel
gets reaped by `telfs gc`.

**11c. Keep current behavior for non-FK errors.** Implicit: the
non-FK branch is unchanged. Wire stays on disk; next startup retries.

The combined effect is that the disk leak stops, log spam stops, and
upload bandwidth on doomed chunks stops.

## Adjacent fixes

The investigation surfaced several smaller issues that all landed in
the same pass.

### `internal/web/handlers.go` — `humanBytes` truncation

The old helper did integer right-shift: `n >> 30`. A 1.99 GiB value
rendered as "1 GiB", silently losing up to a unit of precision.
Rewritten to render with one decimal and strip trailing `.0`.

### `internal/tg/tracker.go` — tx/rx split

The /telegram tab's "RPCs sent · NN" line summed `encodedSize(input)`
across every RPC. For upload methods (`SaveBigFilePart` carries the
payload in the request) this matched expectations. For downloads
(`UploadGetFile` has a ~64 B request and the bytes come back in the
*response*), it was effectively zero. Download traffic was
invisible.

Now `End(...)` accepts an `rxBytes` parameter; the middleware
sniffs the output for `*tg.UploadFile` and feeds its `.Bytes` length
in. Per-row `Bytes` is whichever direction actually moved data; the
snapshot also exposes `TotalBytesTx` and `TotalBytesRx` separately.

### `internal/tg/session.go` — 128 KiB → 512 KiB upload parts

gotd's uploader defaults to 128 KiB per `upload.SaveBigFilePart`.
For a 32 MiB packed blob that's 256 RPCs. Adding `.WithPartSize(512
* 1024)` cuts that to 64 — directly freeing rate-limit budget for
the download lane.

### `internal/tg/ratelimit.go` — upload/download lane split

The single `rate.Limiter` was shared by every RPC, so a heavy packer
flush could starve reads. Now two limiters; the middleware routes by
input type (`UploadGetFileRequest` → download bucket, everything
else → upload bucket). Downloads default to unlimited.

Wired through to the web (a "Download cap" field on /telegram) and
the CLI (`telfs ratelimit set --upload N --download M`).

### `internal/chunk/reader.go` — eager pending-dir fallback

ReadAt now tries `meta.GetChunk` → on miss tries the staging dir →
only then waits on the R-A-W barrier. Freshly-written files are
served at filesystem-cache speed; the barrier still exists for the
brief packer-flush window where the wire was deleted before the
chunk_map row landed, but it's no longer on the hot path.

### `internal/config/config.go` + `--cache-cap` flag

The LRU disk cache cap was hard-coded at 1 GiB — fine for small
projects, far too small for a Linux kernel checkout or movie
streaming. Added a configurable knob with binary/decimal suffix
parsing (`8GiB`, `512MiB`, `2GB`). Order: `--cache-cap` flag wins,
then `config.toml`'s `cache_cap`, then `chunk.DefaultCacheCapBytes`.

### `Makefile` — static binary by default

`make build` now sets `CGO_ENABLED=0` so the local dev binary
matches what `make release` produces. `ldd bin/telfs` reports "not
a dynamic executable" — copyable to any Linux box.

### `cmd/telfs/main.go` — fusermount detection

`cmdMount` now `exec.LookPath`s `fusermount3` then `fusermount` and
prints a clear warning with install commands if neither is found.
Doesn't abort (root setups don't need it) but makes the missing
dependency visible.

### `internal/meta/store.go` — periodic WAL checkpoint

A 30 s background goroutine runs `PRAGMA wal_checkpoint(PASSIVE)`
to keep `db.sqlite-wal` from growing unbounded. We observed it at
116 MiB during the incident; PASSIVE never blocks readers or writers.

## Web UI changes

### Nav: 11 → 5 + Setup group

Top-level tabs collapsed to **Dashboard · Browse · Mounts · Telegram
· State**, with a "Setup ▾" dropdown holding the one-time
configuration pages (Profiles · Login · Channel · Encryption ·
Compression). The dropdown is a CSS-only `<details>` with a
delegated outside-click handler in `static/ui.js`.

### Global health pill

A live pill in the header polls `/telegram` every 5 s and shows:

| Pill | Trigger |
|---|---|
| 🟢 healthy | daemon online, last error > 5 min ago, < 100 staged wires |
| 🟡 busy | upload progress > 60 s stale, FLOOD_WAIT in last 5 min, or 100–999 staged wires |
| 🔴 issue | daemon offline, last error < 30 s, or ≥ 1000 staged wires (disk-leak signal) |

The 1000-wire threshold is specifically the "FK cleanup is lagging"
alarm; the same condition that caused the incident now lights up
red without anyone having to grep the log.

### Visual queue bars + tooltips

The upload-queue panel grew small `<div class="qbar">` strips next to
each row showing fill % against the relevant cap (pool slots,
packer target). Each row also has a `title=` tooltip explaining
what that queue actually is, in operator language.

### Cache panel on `/state`

The daemon now exposes `/cache` (size, cap, hit/miss counters) and
the /state page renders a panel with a fill bar, a hit-ratio
percentage, and a separate breakdown of the staging dir footprint
with a note that it isn't counted against the cap.

### Browse: combined toolbar

The three stacked Upload / New file / New folder cards collapsed
into one toolbar of `<details>` chips above the file list. A
sibling search box filters the current directory in pure CSS
(`oninput` + `data-row-name` attribute toggling).

### Inline confirm

Destructive table actions (`Delete`) replaced `onsubmit="return
confirm(...)"` with `<details class="row-confirm">` panels.
Matches the existing rename inline-form pattern.

### Dark-mode toggle

Header button cycles **system → light → dark → system**, persisted
to `localStorage`. CSS uses `[data-theme="dark"]` / `[data-theme="light"]`
selectors that mirror the existing `@media (prefers-color-scheme: dark)`
block.

## Summary table

| # | What | Where | Effect |
|---|---|---|---|
| 10 | LIMIT_INVALID fix | `internal/tg/session.go:455-467` | `make tinyconfig` works; movie seeks succeed. |
| 11 | FK violation handling | packer.go, store.go callbacks | Disk leak stops; log spam stops; bandwidth saved. |
| 1 | Tx/rx tracker split | `internal/tg/tracker.go` | Download traffic is visible. |
| 2 | humanBytes on Bytes column | `partials/telegram.html` | 131096 → "128 KiB". |
| 3 | 512 KiB part size | `internal/tg/session.go:308` | 4× fewer Save*FilePart RPCs per blob. |
| 4 | Eager pending fallback | `internal/chunk/reader.go` | Freshly-written reads at cache speed. |
| 5 | Upload/download lane split | `internal/tg/ratelimit.go` | Uploads can't starve reads. |
| 6 | Configurable cache cap | `internal/config/config.go` | `--cache-cap 8GiB` etc. |
| 7 | Cache panel | `/state` + `/cache` JSON | Live cache numbers in the UI. |
| 8 | `CGO_ENABLED=0` default | `Makefile` | `bin/telfs` is fully static. |
| 9 | fusermount detection | `cmd/telfs/main.go` | Clear warning instead of late failure. |
| 12 | Web UX overhaul | layout.html, style.css, ui.js, several partials | Nav collapsed, health pill, dark toggle, queue bars, inline confirm, browse toolbar. |
| 13 | WAL checkpointer | `internal/meta/store.go` + main.go | WAL stays bounded; next-mount opens are faster. |

## Lessons that generalise

1. **The log was right; we were wrong.** A few minutes of `grep`
   would have skipped two layers of theorising. Default to looking
   at the evidence first.
2. **One symptom can mask multiple bugs.** The flex error came from
   the LIMIT_INVALID path. The 380 stuck wires came from the FK
   path. Both were live at the same time on the same workload.
3. **A "blocking" bug and an "EIO" bug aren't the same thing.** When
   you have to explain the exact userspace symptom, that's a signal
   to slow down and check the actual returned error code.
4. **Defaults matter more than you think.** `make build` was
   producing dynamic binaries because Go's auto-cgo defaults to ON
   when a C compiler is present. Most users never noticed; some
   wondered why their dev binary didn't work on a server. One line
   in the Makefile fixed it.
5. **Capacity controls should be configurable from day one.** The
   1 GiB cache cap and single-lane rate limit were both fine for the
   developer who wrote them. They both became symptoms the day a
   real workload showed up.
