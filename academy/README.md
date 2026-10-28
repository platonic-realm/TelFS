# The TelFS Academy

This directory replaces the old `docs/`. It's not a reference manual; it's
a sequence of lessons that walks you through every subsystem of TelFS and
explains **why** each piece looks the way it does. The goal is that someone
who finishes the academy can open any file in the repo and understand both
what it does and what alternatives were rejected to land on this shape.

## Who this is written for

You write Go. You've used databases, trees, caches, and concurrency
primitives. You don't need anyone to explain what a SQLite WAL is in
principle, but you might want to know *why TelFS picks IMMEDIATE
transactions over DEFERRED*. That's the level we aim at.

## How to read it

Read in order on a first pass. Each chapter assumes you've read the ones
before. The chapters are short enough to read in a sitting; the file
references inside them point at real code paths so you can keep
`internal/...` open in another window and trace along.

| Chapter | Topic | Why it matters |
|---|---|---|
| [01 — Introduction](01-introduction.md) | What TelFS is and isn't, the central design constraint | Frames every later decision |
| [02 — Architecture](02-architecture.md) | The layered diagram, write path, read path | Mental model you'll keep returning to |
| [03 — Tour of the repo](03-tour-of-the-repo.md) | Which package owns what, naming conventions | So you can find code |
| [04 — MTProto and gotd](04-mtproto-layer.md) | The Telegram side: client lifetime, middlewares, reconnect | The substrate everything else stands on |
| [05 — The metadata store](05-metadata-store.md) | SQLite schema, WAL, IMMEDIATE txs, retry-on-busy | The hardest correctness battlefield |
| [06 — The chunk pipeline](06-chunk-pipeline.md) | Writer, Reader, Pool, Cache, async close | Where data actually flows |
| [07 — Packer and dedup](07-packer-and-dedup.md) | Small-file aggregation, content addressing, multi-tier sizing | The throughput story |
| [08 — FUSE handlers](08-fuse-handlers.md) | Node + fileHandle, error mapping, ctx discipline | Where the POSIX surface lives |
| [09 — Snapshots and recovery](09-snapshots-and-recovery.md) | DB snapshot cadence, journal, cold-mount recovery | How a freshly-cloned host catches up |
| [10 — Crypto and compression](10-crypto-and-compression.md) | AES-GCM v1/v2/v3, KDF, canary, zstd, pipeline order | What we hand to the channel |
| [11 — Operational features](11-operational-features.md) | Trash, rate limit, GC, fsck, doctor | The CLI surface you actually live in |
| [12 — The web UI](12-web-ui.md) | Supervisor, mount adoption, partial polls, CSRF | Why the UI behaves the way it does |
| [13 — Lessons learned](13-lessons-learned.md) | Real bugs and what they taught us | Pattern library for future work |
| [14 — A perf + correctness pass](14-perf-correctness-pass.md) | Kernel-untar incident: LIMIT_INVALID, FK violations, rate-limit lane split, UX overhaul | Walks through one session that hardened several subsystems at once |
| [15 — The read-performance pass](15-read-perf-pass.md) | Singleflight, parallel ranged fetch, FileReference cache, RAM hot tier, seek detection, cancellable prefetch | What it takes to make reads bandwidth-bound instead of RTT-bound |
| [16 — The profile-identity pass](16-profile-identity-pass.md) | Identity-scoped access hashes, bot-auth removal, mount preflight, `telfs unmount`, the inline setup wizard, persistent stats | How "it didn't fail" turned out to be the worst bug report of all |
| [17 — Format 2 and the security base](17-format2-security-base.md) | Key hierarchy, secret sources, `session.enc`, non-dumpable processes, encrypted captions/journal, Padmé, the key message and its export, rebuilding from captions | What an observer of the channel — or a copy of the profile folder — can no longer learn |
| [18 — v1: TelFS as a backup service](18-v1-backup-service.md) | Disk-backed staging and backpressure, the control socket and the sync barrier, service mode, systemd credentials, journal-first snapshots and the anchor, versions, bulk restore with verification, `fsck --quick`, parallel uploads | What a backup job, a boot unit and a monitor need from a filesystem that a desktop user never asked for |

## Conventions

- **File references** look like `internal/chunk/writer.go:194` — `path:line` so editor tools can jump straight there.
- **Code excerpts** are usually trimmed to the relevant lines with `// ...`
  marking where I cut. The original always wins; if the excerpt and the
  source disagree, the source has been updated since this was written.
- **War stories** are called out in their own boxes because they're the
  most valuable teaching material — every one represents a real wrong turn
  and the correction.

If you find a chapter unclear, the right fix is to edit it. The academy is
the project's onboarding doc; treat it like a first-class artifact.
