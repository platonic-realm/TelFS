# 11 — Operational features

The pieces that aren't about the data path. Most of them exist because
running a long-lived filesystem on top of a third-party channel turns
out to need more tools than just "mount" and "unmount."

## Trash safety-net

`internal/trash/`. Opt-in via `telfs trash enable [--ttl D]`.

The contract: when enabled, every `unlink` and `rmdir` from the FS
layer **moves** the entry into a top-level `/.trash` directory instead
of deleting. A separate GC goroutine inside the daemon reaps entries
older than the TTL.

### Why move-into-trash, not "soft delete"

The alternative would be a flag in `inodes` (e.g., `deleted_at`) and
returning ENOENT for marked rows. That has subtleties:

- `ls` of the directory needs to filter — every readdir now has
  conditional logic.
- Disk space attribution gets confused: a "deleted" file still has
  its chunks pinned by chunk_map.
- The `/.trash` UI is implicit; the user can't browse what's
  recoverable.

Move-into-trash is concrete: the entry literally exists at
`/.trash/<old-name>.<unique-suffix>`. `ls /.trash` shows you what's
recoverable. The TTL GC is exactly the same code path as a normal
`rm` from within `/.trash` (which deletes for real). Reusing the
unlink path keeps the surface small.

### The rename target name

When two files are unlinked with the same name, the second collides
with the first. We disambiguate by appending a counter or timestamp:

```go
func uniqueTrashName(ctx context.Context, m *meta.Store, trashIno int64, name string) string {
    // Try the bare name first.
    if _, err := m.Lookup(ctx, trashIno, name); errors.Is(err, meta.ErrNotFound) {
        return name
    }
    // Append timestamp-based suffix.
    for {
        candidate := fmt.Sprintf("%s.%d", name, time.Now().UnixNano())
        if _, err := m.Lookup(ctx, trashIno, candidate); errors.Is(err, meta.ErrNotFound) {
            return candidate
        }
    }
}
```

The collision-free guarantee is "until the universe runs out of
nanoseconds." Good enough for our purposes.

### Dir trash semantics

`rm -rf foo/` sends N file unlinks followed by an rmdir. Each unlink
becomes a rename into trash; the final rmdir's "foo" dir becomes
another rename. The result in trash is the dir plus a flat pile of its
former files, all at the top level of `/.trash`. The original tree
structure is not preserved.

This is a known limitation. Preserving the tree would require either
(a) moving the dir wholesale up front, before its contents are touched
(impossible — FUSE doesn't tell us about the rm intent), or (b)
reconstructing the tree at trash-time from the move history (complex,
error-prone). The flat layout is bounded scope for v1.

### TTL GC

`internal/trash/gc.go`. A goroutine runs every 5 minutes:

```go
for {
    select {
    case <-ctx.Done(): return
    case <-time.After(interval):
        ttl := loadTTL()  // re-read every tick
        EmptyOlderThan(ctx, m, trashIno, ttl)
    }
}
```

`loadTTL` re-reads `meta_kv.trash_ttl_secs` every tick — so `telfs
trash enable --ttl <D>` takes effect without remount.

`EmptyOlderThan` walks the trash dir, finds entries whose mtime is
older than TTL, and unlinks them. For files, that's a normal unlink
that drops chunk_map rows and lets `telfs gc` reap the messages.

## Rate limit

Chapter 04 covers the gotd middleware side. The user-facing surface:

- `meta_kv.tg_rate_limit_per_min` — 0 (or absent) means unlimited.
- `telfs ratelimit show|set N|clear` — CLI.
- `/telegram` tab on the web UI has a form.

The daemon polls the kv every 5 s and calls
`Client.RateLimiter().SetPerMinute(N)`. Changes take effect within a
few seconds of either path; no remount.

### Why per-minute, not per-second

Per-second was tried first. Two problems:

- The natural unit users think in ("60 requests per minute") is hard
  to translate by mental math from per-second ("1 req/s = 60/min").
- Burst handling is more natural at per-minute. We allow burst =
  cap/4 (clamped 1..32), so a cap of 120/min admits ~30 in a burst
  then trickles 2/s. With per-second, you'd need a separate burst
  parameter; per-minute folds it in.

### Why a sentinel `0` instead of a separate `enabled` boolean

Storing both an `enabled` bool and a `value` is two settings. Two
settings means two ways they can be inconsistent (enabled=true,
value=0). One setting where 0 means "off" is one source of truth.
The CLI's `clear` subcommand is just syntactic sugar for `set 0`.

## Orphan chunk + snapshot GC

`telfs gc`. The cmd subcommand reaches into the channel, finds
messages that are no longer referenced by anything, and deletes them.

The reachable set:

- Every `tg_message_id` in `chunk_map` is reachable.
- The current snapshot (`meta_kv.snap_msg_id`) is reachable.
- The current journal-op batches are reachable (the channel-side state
  the recovery code expects to find).

Anything else in the channel — leftover orphans from crashed uploads,
old snapshots beyond the retention window — is fair game.

The walk uses `messages.search` paged by message ID; `--pages N` caps
it at N pages. We default to walking the whole channel because the
operation is conservative (it only deletes things provably not
referenced), but the cap is there for users with very large channels
who want to bound the runtime.

The `--yes` flag is required to actually delete. Without it, gc prints
what it would delete and exits. This is the standard "no destructive
ops without explicit confirmation" pattern in the CLI.

## FSCK

`telfs fsck`. Walks every chunk_map row and verifies the referenced
channel message exists and has the expected size. Useful after a
suspected channel-side corruption (rare but documented in the wild
when Telegram has migration events).

`--fix` will, on mismatch, drop the bad chunk_map row and mark its
inode for repair (next mount-start, the repair pass shortens the file
to a consistent prefix). Without `--fix`, fsck just reports.

`--stop-after N` is a development tool — stop after seeing N
mismatches. Saves walk time when debugging a class of issues.

The walk uses `WalkChannelMessages`, the same paginator the GC uses.

## Doctor

`telfs doctor`. Local lints. Doesn't touch the channel. Checks:

- `config.toml` is mode 0600 (or 0400). World-readable creds get
  flagged.
- `session.json` is mode 0600. Same.
- `meta_kv.fs_uuid` is set. (A missing fs_uuid means recovery would
  refuse to apply — should never happen, but doctor catches it.)
- chunk_blob count vs chunk_map count ratio is plausible (high values
  suggest a stale chunk_blob index; low or zero on a plaintext FS
  suggests dedup never fired).
- Orphan cache files: cache entries on disk that don't appear in any
  chunk_map row. Recoverable space.

Each check has a one-line explanation. The output is intentionally
terse: doctor is meant to be the thing you run before filing a bug,
not a status dashboard.

## Profile management

`internal/config/`. Profiles are isolated `(config.toml + session.json
+ db.sqlite + cache/)` bundles under `~/.config/telfs/profiles/`. The
active profile is selected by:

1. `$TELFS_PROFILE` env var.
2. `~/.config/telfs/active` file (written by `telfs profile use <name>`).
3. Error (`config.ErrNoActiveProfile`).

There is **no** implicit fallback. That's deliberate: a user with two
profiles running a `telfs status` would get the wrong answer if there
were a default. Explicit selection avoids the class of "I wrote to the
wrong filesystem" mistakes.

Profiles are self-contained — `telfs profile export <name>` produces
a tar.gz of the whole directory, importable via
`telfs profile import < bundle.tar.gz` on another host. Useful for
"set up TelFS on my laptop, copy to my server."

### The legacy `./.telfs/` path

Before v0.3, TelFS stored everything in `./.telfs/` relative to the
current directory (with the env var `TELFS_DIR` as override). That
was a bad design — it meant the same shell working dir got different
filesystems depending on where you were. v0.4 removed it; the only
supported layout is the XDG-style `~/.config/telfs/profiles/<name>/`.

## `telfs status`

One-screen summary. Reads:

- The active profile name.
- The config and DB paths + their sizes/mtimes.
- The bound channel (id + title).
- Whether encryption is enabled, and which mode.
- The latest snapshot's message id.
- chunk_map count.
- The cache's on-disk size.
- Any active `/proc/mounts` entry for this profile.

Nothing destructive, nothing slow. Useful as "run this before doing
anything; if it looks wrong, stop."

## Why CLI before web

The CLI was built first; the web UI is a wrapper over it. The
discipline is: any feature added must work from the CLI first, then
get a web surface. We've never had to back off this — turns out
"works from a shell script" is a healthier default than "works in
the browser" for an operational tool.

The web's value is *exploration* (browse around, click "Apply") and
*remote operation over Tailscale* (the CLI has to be SSH'd to). The
CLI's value is *reproducibility* (script-friendly, idempotent,
diff-able output).

## Next chapter

[12 — The web UI](12-web-ui.md) — supervisor, mount adoption, partial
polls, CSRF.
