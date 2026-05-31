# 03 — Tour of the repo

The top-level layout:

```
cmd/telfs/         — main entrypoint; one file per subcommand
internal/fs/       — go-fuse handlers; the POSIX surface
internal/meta/     — SQLite metadata store (modernc.org/sqlite)
internal/chunk/    — chunker, writer, reader, cache, pool, packer
internal/tg/       — gotd MTProto wrapper, session manager, middlewares
internal/snapshot/ — DB serialize/restore + cadence + journal poster
internal/crypto/   — AES-GCM v1/v2/v3, KDF, canary
internal/compress/ — zstd codec
internal/trash/    — rm safety-net (move to /.trash, GC by TTL)
internal/web/      — single-binary HTTP UI + supervisor
internal/config/   — config.toml + multi-profile resolution
internal/metrics/  — Prometheus-style /metrics handler
academy/           — this directory
```

`internal/...` is conventional Go module isolation — nothing outside this
repo imports those packages. The split into many small packages was a
deliberate choice early on: it keeps coupling visible (`go vet` will
complain about cyclic dependencies before you finish typing) and makes
each subsystem testable in isolation. The test files live next to the
code they test (`*_test.go` siblings), which is standard Go.

## `cmd/telfs/`

One subcommand per file. The structure:

| File | Subcommand | What it does |
|---|---|---|
| `main.go` | dispatcher | Top-level usage string + subcommand switch + the mount daemon (which is the most complex command, so it stays here) |
| `login.go` | `telfs login` | Phone+code+2FA auth flow |
| `channel.go` | `telfs channel` | `list`, `set`, `info`, `ping` |
| `init.go` | `telfs init` | Set `chunk_size`, `compression`, etc. before first mount |
| `encrypt.go` | `telfs encrypt` | `init`, `status`, `rotate` |
| `profile.go` | `telfs profile` | `list`, `show`, `create`, `delete`, `use`, `export`, `import` |
| `status.go` | `telfs status` | One-screen summary |
| `fsck.go` | `telfs fsck` | Channel-side integrity check |
| `gc.go` | `telfs gc` | Reclaim orphan chunks + old snapshots |
| `snapshot.go` | `telfs snapshot` | `list`, `restore` |
| `trash.go` | `telfs trash` | `enable`, `disable`, `status`, `list`, `empty` |
| `ratelimit.go` | `telfs ratelimit` | `show`, `set N`, `clear` |
| `doctor.go` | `telfs doctor` | Local invariant lint |
| `debug.go` | `telfs debug` | `seed-file`, `dump-msg` — surface tools used while debugging |
| `web.go` | `telfs web` | Start the web UI |

The `cmd/telfs/main.go` file is bigger than the others because the mount
daemon's startup is intricate: it has to handle cold recovery, set up
the metrics endpoint, configure the supervisor's lifecycle so the final
snapshot can post past SIGTERM, etc. Most of the rest of `cmd/telfs/`
is straightforward "parse flags, open meta store, do thing, close."

## `internal/fs/`

Three files:

- `fs.go` — `Backend` struct + `Node` type with all the go-fuse method
  implementations (Lookup, Readdir, Create, Mkdir, Unlink, Rmdir, …).
  This is the single largest piece of glue in the project.
- `handle.go` — `fileHandle` type with Read/Write/Flush/Fsync/Release.
  Per-open-fd state.
- `mount.go` — wraps go-fuse's `gofuse.Mount` with our options
  (`fsName`, `Debug`, `AllowOther`, `CAP_WRITEBACK_CACHE`).

There are no tests in `internal/fs/` because the package's surface is
entirely defined by go-fuse interfaces; meaningful testing requires a
real kernel FUSE mount, which is what `make integration` (and the bug
tickets we file when it fails) does instead.

## `internal/meta/`

| File | Owns |
|---|---|
| `store.go` | `Store` type, DSN, schema, `WithTx`, `execWithRetry`, busy detection |
| `inode.go` | inode CRUD; `GetInode`, `SetAttrs`, `SetSize` |
| `dirent.go` | `Lookup`, `Readdir`, `ReaddirInfo`, `CreateChild`, `Link`, `Unlink`, `Rename` |
| `chunk.go` | `chunk_map` operations; `PutChunk`, `GetChunk`, `ListChunks`, `ReuseChunkByHash`, content-addressed dedup |
| `journal.go` | journal table; `AppendJournal`, `PendingJournal`, `BatchMarkJournalPosted` |
| `kv.go` | `meta_kv` getter/setter |
| `xattr.go` | xattr storage (per-inode key→bytes) |
| `repair.go` | startup pass that truncates partially-uploaded files to their contiguous prefix |
| `stats.go` | aggregate counters used by `/state` |
| `replay.go` | replay a journal op into the live DB (cold recovery path) |

The package is deliberately wide and shallow. Every operation is one or
two SQL statements and a thin Go wrapper. There's almost no caching at
this layer — the cache lives in `internal/chunk`, where it can cache
*content*, not *metadata*. The metadata is small enough that SQLite's
own page cache plus `_pragma=journal_mode(WAL)` cover us.

## `internal/chunk/`

| File | Owns |
|---|---|
| `writer.go` | `Writer` struct, `WriteAt`, `Flush`, `Sync`, dispatch path, retry/restore |
| `reader.go` | `Reader.ReadAt`; coordinates with cache + pool |
| `cache.go` | LRU on-disk cache (`Cache`, `Key`, `Fetcher` interface, the `chunk.TGFetcher` concrete impl) |
| `pool.go` | `UploadPool` — global concurrency cap, per-ino "pending" R-A-W barrier, `Reservation` |
| `packer.go` | `Packer` — aggregates sub-256 KB chunks into shared blobs |
| `persist.go` | `cache/pending/` staging — wires on disk between dispatch and PutChunk |
| `upload_retry.go` | retry wrapper around `Uploader.UploadDocument` |
| `tier.go` | multi-tier chunk-size promotion logic |
| `tgfetcher.go` | concrete `Fetcher` that downloads from Telegram (ranged for packed chunks) |

The package interfaces it relies on are:

- `chunk.Uploader` — `UploadDocument(ctx, r io.Reader, filename, caption) (int, error)`.
  `*tg.Session` implements this.
- `chunk.Fetcher` (in `cache.go`) — pulls a chunk's bytes given a message ID.
- `meta.Store` — for `PutChunk`, `GetInode`, etc.

This decoupling lets us write extensive in-memory tests with a
`fakeUploader` (see `writer_test.go`) without touching real MTProto.

## `internal/tg/`

| File | Owns |
|---|---|
| `client.go` | `Client` — long-lived MTProto config; spawns `*telegram.Client` instances |
| `session.go` | `Session` — the user-facing wrapper that *components* hold |
| `manager.go` | `SessionManager` — keeps a stable `*Session` whose underlying gotd client can be swapped on reconnect |
| `auth.go` | `termAuth` (phone+code+2FA prompt for CLI) |
| `webauth.go` | adapter for the web's phone-login state machine |
| `channel.go` | channel binding helpers (normalize ID, resolve access_hash) |
| `floodwait.go` | `FloodWaiter` middleware — handles `FLOOD_WAIT_N` |
| `ratelimit.go` | `RateLimiter` middleware — operator-controlled requests/min cap |
| `tracking_middleware.go` | `TrackingMiddleware` — feeds the `Tracker` |
| `tracker.go` | `Tracker` — ring buffer of recent RPCs + aggregate counters; backs `/telegram` JSON |
| `snapshot.go` | `UploadSnapshot`, `FindLatestSnapshot`, `ListSnapshots`, etc. |
| `walk.go` | `WalkChannelMessages` — paginated walk for fsck/gc |
| `journal_batch.go` | `UploadJournalOpBatch` — pack many ops into one message |

The `Client` / `Session` / `SessionManager` split exists because gotd's
`*telegram.Client` is *single-shot* — once its `Run` returns, you can't
use it again. The manager's job is to spawn replacements when the
underlying connection wedges, so the rest of the daemon can hold a
single stable `*Session` reference for its entire lifetime.

## `internal/snapshot/`

| File | Owns |
|---|---|
| `snapshot.go` | `Take` (serialize the DB into a gzipped blob) + `Restore` (apply) |
| `manager.go` | `Manager.Run` — cadence loop; takes one immediately, then every 5 min |
| `journal_poster.go` | `Poster.Run` — drains journal table to channel periodically |
| `envelope.go` | snapshot encryption wrapper (TFSE — TelFS Snapshot Envelope) |
| `recovery.go` | cold-mount recovery: find latest snapshot, restore, replay journal |

## `internal/crypto/`

Three cipher modes have lived in the project; all are in `aes_gcm.go`:

| Mode constant | Wire format | What it does |
|---|---|---|
| `ModeAESGCMv1` | `aes-gcm-v1` | Passphrase → key directly via Argon2id; chunks encrypted with random nonce |
| `ModeAESGCMv2` | `aes-gcm-v2` | Passphrase wraps a random per-FS DEK; chunks encrypted with DEK |
| `ModeAESGCMv3` | `aes-gcm-v3` | Like v2, but with convergent (deterministic) nonces so encrypted chunks can dedup |

The mode is in `meta_kv:crypto_mode`. Once chosen at `telfs encrypt
init`, it never changes for the lifetime of the FS (changing modes
mid-flight would break decryption of existing chunks). The KDF
parameters are in `meta_kv:crypto_argon`; the canary value (used at
mount to verify the passphrase) is in `meta_kv:crypto_canary`. The
salt is in `meta_kv:crypto_salt`.

## `internal/compress/`

Two codecs: `none` and `zstd`. The choice is stored in
`meta_kv:compression_codec` and is also immutable after the first chunk
is written. The reasoning is the same as for the cipher mode: a reader
that fed a zstd frame to the no-op decompressor (or vice versa) would
silently return garbage.

## `internal/web/`

This package is bigger than most. The structure:

| File pattern | Owns |
|---|---|
| `server.go`, `router.go` | HTTP server lifecycle + route table |
| `handlers.go` | Dashboard + cross-cutting helpers (`basePage`, flash, helpers) |
| `state.go` | CSRF + flash cookie machinery |
| `*_handlers.go` | One file per top-level tab/section (channel, mount, encrypt, compress, browse, login, profile, state, telegram) |
| `supervisor.go` | `Supervisor` — spawns + tracks `telfs mount` children, adopts CLI-started mounts |
| `telegram.go` | `/telegram` data fetch + rate-limit form handlers |
| `repair_tracker.go` | live progress tracker for the repair button |
| `loginsessions.go` | the phone-login state machine (per-session `*tg.Client` + channels) |
| `templates/*` | go html/template templates, embedded via `embed.FS` |
| `static/*` | htmx.min.js + style.css, also embedded |

The deliberate decision to use Go templates + HTMX rather than a
JS-bundled SPA is the single biggest factor that keeps `internal/web`
maintainable. Chapter 12 walks the consequences.

## `internal/config/`

Profiles + the `config.toml` file format. The profile resolution order
is: `$TELFS_PROFILE` > `~/.config/telfs/active` (written by
`telfs profile use`) > error. There is no implicit fallback to a
default profile; that was an early choice motivated by "the user
should never accidentally write to the wrong filesystem."

## `internal/metrics/`

Simple Prometheus-style registry. The mount daemon optionally serves
`/metrics` on the address passed via `--metrics-addr`. The
`/telegram` JSON endpoint shares the same HTTP server. This package
exists mostly so the chunk pipeline can increment counters without
importing the web UI.

## Next chapter

[04 — MTProto and gotd](04-mtproto-layer.md) starts from the bottom of
the stack — the Telegram side of the wire.
