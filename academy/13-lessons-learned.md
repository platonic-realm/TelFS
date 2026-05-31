# 13 — Lessons learned

The previous chapters have called out individual war stories. This
chapter pulls the most generalizable lessons into one place. Each is
a pattern you can apply to a new subsystem.

## 1. Distinguish "transient" from "fatal" at the layer that knows

The pattern showed up four separate times:

- **Meta layer**: `SQLITE_BUSY` vs `SQLITE_BUSY_SNAPSHOT` look the same
  to the caller. The driver retries one and not the other.
- **TG layer**: `FLOOD_WAIT` is *the* normal response under load; the
  caller should not see it as an error.
- **Writer**: a chunk upload failure that restored the chunk to dirty
  is transient; one that happened after the writer is closed is fatal.
- **FUSE handlers**: `meta.ErrNotFound` is ENOENT; everything else is
  EIO. Mapping the latter to ENOENT silently breaks Go's
  `os.RemoveAll`.

The lesson: **the layer that has the information** about whether a
failure is recoverable is the one that should retry. Don't pass an
error up just because you got one — first decide whether it's
something the upper layer should ever have to know about.

Concretely, this looks like:

```go
func someOp(...) error {
    for attempt := 1; ; attempt++ {
        err := doWork(...)
        if err == nil { return nil }
        if !isTransient(err) || attempt >= maxAttempts {
            return err  // surface to caller
        }
        log.Printf("op: transient err on attempt %d/%d, retrying: %v", ...)
        // bounded backoff
    }
}
```

Every retry wrapper in TelFS — `meta.WithTx`, `meta.execWithRetry`,
`chunk.uploadWithRetry`, the web's recursive delete — has that
shape. Two parameters: max attempts, max backoff. Always log each
retry so cumulative load is visible.

## 2. Log every silent EIO

Returning `syscall.EIO` from a FUSE handler with no log line was the
single biggest debugging blocker we hit. Users saw EIO; the daemon
log was silent; we had no way to tell SQL busy from real corruption
from kernel context cancellation.

The fix was bulk: route every error path in `internal/fs` through a
`logMetaErr` helper or an explicit `log.Printf` before the EIO
return. The performance cost is negligible (a log line on the rare
error path); the diagnostic value is enormous.

The same applies to other "we returned an opaque error" surfaces:
the chunk writer's `recordTransient` logs to stderr before restoring;
the packer's flush logs on failure; etc. **Every error is allowed to
be invisible to the user, but no error is allowed to be invisible to
the daemon log.**

## 3. Decouple kernel ctx from internal ctx for write paths

FUSE hands handlers a kernel-side context with a deadline. If a meta
DB op queues under load (single-connection writer), the deadline can
fire *between* steps of a single FUSE handler. We'd see things like:

- CreateChild's tx already committed.
- GetInode immediately after sees `context.Canceled`.
- We return EIO to userspace.
- The inode is in the DB, but tar thinks it isn't.

`context.WithoutCancel` (Go 1.21+) is the right tool. Use it for any
*write-side* meta op where partial completion creates inconsistency.
Don't use it for genuinely cancellable work (Read with TG fetch
should remain cancellable so killing `cat` actually frees the
connection).

The rule we ended up with: **write-side meta ops use `metaCtx(ctx)`;
read-side ops keep `ctx`.** That single rule covered every case.

## 4. Persist before submitting; clean up after success

The pattern in the chunk pipeline:

```
write wire bytes to disk
        │
        ▼
submit to packer (or direct uploader)
        │
        ▼ (uploaded + chunk_map row written)
delete the on-disk wire
```

Three states:
1. Wire on disk, no chunk_map row. Recovery re-submits.
2. Wire on disk, chunk_map row exists (rare crash window). Recovery
   re-submits, creates a second message; old one becomes orphan.
3. No wire on disk, chunk_map row exists. Normal state.

State 2 wastes a TG message but doesn't lose data; `telfs gc`
reclaims the orphan. State 1 is the durability property we wanted.
The cost is a double write (disk + TG) per chunk — accepted because
losing user data after a crash is unacceptable.

The same pattern would apply to any "do something that might be
interrupted": stage the input durably, do the work, clean up the
input only on success.

## 5. Single-writer transactions need IMMEDIATE locks

SQLite under WAL with many concurrent writers + retry-on-busy will
work fine *until* you observe `SQLITE_BUSY_SNAPSHOT (517)`. At that
point you'll spend hours discovering that `busy_timeout` doesn't
apply to that error code.

The fix is `_txlock=immediate` in the DSN. Every BeginTx grabs the
write lock up front; concurrent writers wait at the lock (which
busy_timeout *does* handle); 517 can never fire.

The cost is reduced concurrency for read-only WithTx callers — but
almost nobody does that anyway in TelFS. The win is eliminating an
entire error class.

This generalizes: if your DB engine has multiple lock-acquisition
modes, the eager one is almost always safer for write-heavy
workloads. The deferred one is "smart" but only wins if your txs are
mostly reads with rare writes.

## 6. Async close is correct, sync close is durable

POSIX `close(2)` doesn't require durability. fsync does. We exploited
that:

- `Flush` (called at close) dispatches every dirty chunk and returns
  before the uploads land. Tar's close is fast.
- `Sync` (called at fsync) waits for all in-flight, verifies the
  dirty map is empty post-wait, surfaces failures.

Apps that need close-implies-durable call fsync. Apps that don't
care (most) benefit from the fast close.

The opposite design — synchronous close — is what most "naive" FUSE
fs do. Throughput drops dramatically on workloads like `tar`
extracting tens of thousands of files because each close blocks the
process while the upload completes.

The async-close pattern requires careful per-ino R-A-W barriers (the
`Pending` tracker), so readers don't see stale data. That's
significant complexity, but the throughput payoff is huge.

> See commit `adc68d0`.

## 7. Hot-reload via metadata polling, not signals

The rate limit, the trash TTL, and the compression codec are all
runtime-tunable. None requires a daemon restart.

The mechanism: write the value to a `meta_kv` key. The daemon has a
poller goroutine that re-reads every 5 s and applies the change. The
CLI write is one operation; the web write is another; both share the
same store.

Why polling, not signals or IPC?

- It works across processes (CLI and daemon don't share memory).
- It works across web restarts (no signal subscription to lose).
- It tolerates "the daemon was busy" — the next tick picks up the
  change.
- The polling cost is one cheap SQL read every 5 s. Negligible.

The implication: if you're writing a config setting that needs to
take effect at runtime, write it to `meta_kv` and add a polling line
in the daemon. Don't invent a side channel.

## 8. The web should be a wrapper over the CLI, not the reverse

Several features (rate limit, compression codec, encryption status,
mount adoption) started life as CLI-only and grew web UIs later.
That's the right direction. The reverse — building the web first,
then adding a CLI — would have produced an inconsistent CLI that
felt like an afterthought.

The discipline: every feature must work from the CLI (or via direct
meta_kv writes). The web reads/writes the same state. The web is for
discoverability and remote operation, not for things the CLI can't
do.

This kept the CLI healthy. It also made the web simpler — every web
handler is "render this CLI state" or "write this meta_kv key."

## 9. Don't trust the cookie's value as a security boundary, but accept it as a continuity signal

The CSRF cookie story:

- The cookie ↔ form equality is the security boundary (defeats
  cross-site forgery).
- The in-memory map of "valid tokens" was a janitorial mechanism, not
  a security one.
- Treating "token missing from map" as "token expired" silently
  invalidated every form after a web restart.

The fix: accept any well-formed cookie value as long as it matches
the form's value. The map ages tokens out for hygiene; it doesn't
contribute to safety.

Generalizable: think about *what* a cookie/token actually
authenticates. If it's "same browser as the one we minted the cookie
for," then the value is enough. If it's "session is valid right now,"
you need server-side state. CSRF is the former.

## 10. Test the failure paths, not just the happy paths

The `meta` and `chunk` packages have heavy test coverage of failure
modes:

- `fakeUploader.failFromIdx` — simulate upload failures starting at
  chunk N.
- `flakyUploader.failFirst` — fail the first N attempts.
- `TestRepairPartialFiles` — synthesize a gap in chunk_map and verify
  the repair pass cleans it up.
- `TestUploadWithRetryGivesUp` — verify the give-up path.

These tests caught real bugs. The naïve test of "writer.WriteAt
followed by Flush followed by md5 of the data" passes every time;
the bugs hide in "what if the network blips at attempt 3 of 5?"

Pattern: every retry wrapper has tests for (success on first try,
success after N retries, give up after max retries, ctx canceled
mid-backoff). Every persistence path has tests for (commit succeeds,
commit fails mid-flight, partial state recovered correctly).

## 11. Read your own log

Every diagnostic step today started with "what does the daemon log
say?" Half the time the answer was "nothing" (and the lesson was 2).
The other half, the log told us the exact line that fired.

The implication for code: when you write a hot-path goroutine, also
write a log line that lets you tell, in production, that it's running
correctly. The cost is small; the payoff at 3 AM with a user
reporting a bug is huge.

The implication for design: every CLI subcommand should print enough
context that "show me the output" is debuggable. We've trimmed
verbose output where it was noisy, but we've never removed something
that someone actually used to diagnose a problem.

## 12. Optimize for the dump

The data structures on the wire and in the DB are designed to be
inspectable.

- `chunk_map` is a flat table with obvious column names.
  `sqlite3 db.sqlite ".dump chunk_map"` gives you something a human
  can read.
- A journal-op message on the channel is a tagged record that
  `telfs debug dump-msg <msgid>` decodes.
- Snapshots are gzipped SQLite files — `gunzip` them and you have a
  DB you can browse.

The temptation to use a clever encoding (e.g., capnproto, custom
binary) was real. We rejected it for every layer except the
crypto envelope (where there's no choice). The wins from
"production debugging works" have repaid the cost a dozen times.

## 13. Refuse irreversible operations without confirmation

The CLI's destructive commands all take `--yes`:

- `telfs gc --yes` (delete orphan messages from the channel).
- `telfs profile delete <name> --yes`.
- `telfs snapshot restore --yes <id>`.

Without the flag, the command shows what it *would* do and exits.
With it, it does it.

The pattern is contagious — once you've internalized "irreversible
operations confirm explicitly," you start applying it everywhere.
The web's recursive delete asks "delete X and everything inside?"
The mount Stop asks "any process with open files there will see I/O
errors."

We never lost user data to a "wait you actually did it?" mistake.

## What patterns *aren't* here

A few popular patterns are deliberately not used:

- **Event bus.** Subsystems talk via direct interfaces. We considered
  a pub/sub channel for "snapshot landed" etc., and decided the
  indirection wasn't earning anything for our scale.
- **Dependency injection framework.** The daemon's `main` constructs
  every component by hand and wires them up. It's 200 lines and
  trivially debuggable. A DI framework would obscure the construction
  order, which matters for our startup invariants.
- **Async/await abstractions.** Go has goroutines + channels.
  Building "Promises" or "Futures" on top doesn't help.
- **A Result/Option-style error type.** Go errors are values; we
  use the conventions Go uses. No custom monadic types.

The default in TelFS is "use the standard library and basic Go
idioms; only reach for an abstraction when it's pulling its weight."
Several abstractions made it in (the cipher interface, the
Uploader/Fetcher interfaces, the middleware stack), each because it
let us test or extend a real component. Several never made it in,
and the code is easier to read for their absence.

## Where to go next

You've finished the academy. You should now be able to:

- Open any file in `internal/` and explain it.
- Predict what would break if you removed any single piece.
- Recognize the patterns we lean on and apply them.

The natural next step is to pick a small open issue and work it.
The repo's commit history is the runtime extension of this academy —
every fix has a commit message that explains the bug, the
diagnosis, and the rationale for the fix. Read along.
