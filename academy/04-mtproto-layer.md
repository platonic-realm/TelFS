# 04 — MTProto and gotd

We start at the bottom — Telegram. Everything else in TelFS is a
consumer of the substrate this chapter describes.

## Why gotd, not the bot API

Telegram exposes two surfaces:

- **Bot API** (HTTPS, JSON, bot-only) — convenient, but capped at 50 MB
  per upload / 20 MB per `getFile`, no way to fetch a message by id,
  no channel-history access. A filesystem must randomly re-read old
  messages by stored id and rediscover state from history after losing
  its local DB — the Bot API can do neither. Not viable for us.
- **MTProto** (custom TCP protocol, binary, supports user accounts) —
  full power, raw bandwidth, 2 GiB per document, fetch-by-id, history.

TelFS uses MTProto — with user (phone) auth only. Bot-token auth over
MTProto existed briefly and was removed: MTProto requires the
api_id/api_hash application credentials even for bots
(`auth.importBotAuthorization` takes them as parameters), so a bot
identity saved no setup effort while adding channel-admin requirements
and an identity-scoped access_hash trap (chapter 16 tells that story). There are several Go implementations; we picked
[`gotd/td`](https://github.com/gotd/td) because it has the most
complete schema coverage, decent test discipline, and the cleanest
middleware story (which we lean on hard).

Important: gotd is a Telegram **client** library. Its job is to keep an
authenticated session, encrypt/decrypt MTProto frames, marshal RPC
calls, and surface a typed Go API for every Telegram method. It is
*not* a high-level "client for your app." We wrap it in
`internal/tg/Session` so the rest of the codebase doesn't have to know
which methods are called `messages.SendMedia` vs `upload.SaveBigFilePart`.

## The three nested types

This is the part that confuses new readers, so set aside a couple
minutes:

```
        Client                    (internal/tg/client.go)
          │  knows config, builds replacement clients
          ▼
        SessionManager            (internal/tg/manager.go)
          │  long-lived owner of a stable *Session
          ▼
        Session                   (internal/tg/session.go)
          │  what every component holds
          ▼
        *telegram.Client          (gotd/td)
          │  ephemeral; replaced on reconnect
          ▼
        MTProto wire
```

The shape is dictated by an unfortunate gotd property: a
`*telegram.Client` is **single-shot**. Once its `.Run(ctx, fn)` returns,
the client is dead. You can't re-call `.Run`. Reusing it returns
"client already closed."

For a one-off CLI command (`telfs channel list`) that's fine — make a
client, run, exit. For the mount daemon that runs for days and needs to
survive a wedged TCP connection, that's not workable.

So:

- **`Client`** (our type, `internal/tg/client.go`) holds the *config*
  for spawning gotd clients: API ID/hash, session file path, DC
  override, the shared `Tracker`, the shared `RateLimiter`. Its
  `newTG()` method constructs a fresh `*telegram.Client` each time.
- **`SessionManager`** (`internal/tg/manager.go`) runs a loop:
    1. Spawn a `*telegram.Client` via `Client.newTG()`.
    2. Inside its `Run` callback, atomically swap a fresh `api` pointer
       into our stable `*Session`.
    3. Block until the run exits.
    4. If it exited because of a stall (the watchdog fired) or a
       network error, sleep a beat and go to 1.
- **`Session`** (`internal/tg/session.go`) is what every consumer holds
  — chunk.Writer, chunk.Reader, snapshot.Manager. Its methods
  (`UploadDocument`, `PostMessage`, etc.) read `s.api` atomically and
  call the gotd method. If the manager swaps the api mid-call, the
  caller doesn't know.

```go
// session.go, the swap-protected accessor:
func (s *Session) currentAPI() (*tg.Client, error) {
    api := s.api.Load() // atomic.Pointer[tg.Client]
    if api == nil {
        return nil, errors.New("tg: session not connected")
    }
    return *api, nil
}
```

Why is this safe? Because each gotd call is independent. If the
underlying connection dies mid-RPC, gotd returns an error to *that
call*. The next call sees a fresh api pointer and tries again. The
manager's reconnect loop is the one keeping things alive.

> **War story.** Before the manager existed, every CLI subcommand
> spawned its own `*telegram.Client` and ran a one-shot callback. That
> works for `telfs channel list`. It does *not* work for `telfs channel
> ping`, which posts a test message and then reads it back — two
> separate gotd calls. The second one got "client already closed".
> The manager's whole reason for existing is to make "do many things
> over the same connection" a first-class operation. See commit `2acac79`.

## Middlewares: order matters a lot

gotd lets you wrap every RPC in middleware. We register three:

```go
// client.go
opts := telegram.Options{
    SessionStorage: storage,
    Middlewares: []telegram.Middleware{
        c.rateLimit,              // OUTERMOST: gate before anything else
        NewFloodWaiter(),         // FLOOD_WAIT handling
        &TrackingMiddleware{...}, // record raw attempts
    },
}
```

The semantics of order is: **first entry is outermost**, last is innermost.
So the call goes:

```
RateLimiter.Wait → FloodWaiter retry loop → TrackingMiddleware records → actual RPC
```

Picking the right order for each middleware took several wrong turns.

### `RateLimiter` (outermost)

`internal/tg/ratelimit.go`. Wraps the call in `r.limiter.Wait(ctx)`
where `r.limiter` is a `*rate.Limiter` from `golang.org/x/time/rate`.

We put it outermost because:

- Requests that get blocked here are *not yet* counted as in-flight by
  the tracker. So the `/telegram` panel's "in-flight: N" gauge means
  "N requests are riding an actual MTProto round trip," not "N are
  waiting in line."
- A FLOOD_WAIT retry inside FloodWaiter shouldn't burn through the
  rate-limit budget — but it doesn't, because we're outside the
  limiter at that point. The waiter sleeps until Telegram is ready;
  the limiter has already admitted the call.

The cap is in `meta_kv:tg_rate_limit_per_min` (0 = unlimited).
The mount daemon polls that key every 5 s and calls
`SetPerMinute(n)`, so CLI/web edits take effect without a remount.

### `FloodWaiter` (middle)

`internal/tg/floodwait.go`. Telegram returns `FLOOD_WAIT_N` as an
*error* to indicate "wait N seconds, then retry." This middleware
swallows the error, sleeps N seconds, and retries forever (bounded by
`MaxFloodWait = 60 min` per attempt — see the comment in the file for
why we picked 60 min over gotd-contrib's 2 min default).

Why not let `RateLimiter` prevent FLOOD_WAITs by being conservative?
Two reasons:

1. The right rate limit depends on your account's reputation, the
   channel's history, and what other clients are sharing the API
   token. We can't know it; the operator has to pick. Default is
   unlimited.
2. Even with a tight rate limit, Telegram occasionally surprises us
   with FLOOD_WAIT after a bulk operation (`telfs gc` deletes 1000
   messages → 5-minute cooldown). The waiter is the safety net.

### `TrackingMiddleware` (innermost)

`internal/tg/tracking_middleware.go`. Records every RPC into the
`Tracker`'s ring buffer, including raw FLOOD_WAIT errors before the
waiter retries them. That's intentional — the `/telegram` tab shows the
wire-level reality, not the user-facing outcome. If the FLOOD_WAIT
retry succeeds, you see two entries: one `err: FLOOD_WAIT_30`, one
`ok: ...`. That's how the operator can tell "we're hitting limits"
even though everything ultimately succeeded.

## The session storage on disk

gotd persists its session state — auth keys, server salt — in a file.
For us, that's `~/.config/telfs/profiles/<name>/session.json`. The
file is created by the login flow and read by every subsequent
connect. If you delete it, every subsequent `telfs ...` will return
`ErrNotAuthorized` until you `telfs login` again.

A small but important optimization: in `Client.Run`, we check whether
`session.json` exists **before** calling `tgc.Run`. If it doesn't, we
short-circuit and return `ErrNotAuthorized` without any network round
trip. Without that check, gotd performs an MTProto key exchange (3–6 s
on first connect) before our callback gets to ask "are we authorized?".
That made `telfs channel list` issued from a fresh profile feel broken;
the early-out makes it instant.

## The stall watchdog

The mount daemon's metadata stays consistent if Telegram is briefly
unavailable — uploads back up, the packer queues, the journal poster
waits. But if the underlying TCP connection wedges (kernel send buffer
full, no ACKs from Telegram, gotd doesn't expose an app-level
timeout), all that queuing fills disk + memory while the connection
silently rots.

The watchdog catches this. The daemon tracks "last successful upload"
in the tracker; a goroutine compares that to "now". If 90 s pass with
work pending but no progress, it calls `mgr.Reconnect()`, which kicks
the `SessionManager` loop out of its current `Run` (close the gotd
client) and spins up a fresh one. The old TCP socket is closed; a new
one opens; uploads resume.

Without the watchdog, the daemon would look healthy for the 15–30 min
it takes the kernel to time out the TCP connection on its own. The
watchdog brings recovery to seconds.

## Channel binding

The "which channel?" question is resolved at `telfs channel set`: the
account's dialog list is scanned and the ID resolves to an access hash
automatically (`--access-hash` exists as a manual override for
channels the scan can't see). Two properties worth internalizing:

- The access hash is **identity-scoped** — the same channel has a
  different hash for every account, and one account's hash is garbage
  under another. This is why `profile reset-auth` clears the channel
  binding along with the session.
- If the account isn't a member of the channel, no hash exists for it
  at all; membership is the actual prerequisite.

The resolved `(id, access_hash)` lives in `config.toml`. Every channel
RPC in the codebase calls `Session.channelPeer()` to translate
`config.Channel.{ID, AccessHash}` into the gotd-shaped
`*tg.InputPeerChannel`. There's only one path; if the config is wrong,
every channel op fails at the same place.

```go
// session.go
func (s *Session) channelPeer() (*tg.InputPeerChannel, *tg.InputChannel, error) {
    if s.cfg.Channel.ID == 0 {
        return nil, nil, errors.New("tg: no channel configured")
    }
    peer := &tg.InputPeerChannel{
        ChannelID:  s.cfg.Channel.ID,
        AccessHash: s.cfg.Channel.AccessHash,
    }
    ch := &tg.InputChannel{
        ChannelID:  s.cfg.Channel.ID,
        AccessHash: s.cfg.Channel.AccessHash,
    }
    return peer, ch, nil
}
```

## What channel messages look like

Telegram is a chat platform. The channel is just an append-only
collection of messages, each with a numeric `id`. We treat the message
ID as our **storage handle** for a chunk or a snapshot.

When we upload a chunk via `Session.UploadDocument`:

```go
// session.go (abridged)
func (s *Session) UploadDocument(ctx context.Context, r io.Reader,
                                 filename, caption string) (int, error) {
    api, err := s.currentAPI()
    if err != nil { return 0, err }

    peer, _, err := s.channelPeer()
    if err != nil { return 0, err }

    // gotd's uploader handles big-file chunking (128 KiB MTProto parts)
    // transparently and returns an InputFile.
    upl := uploader.NewUploader(api)
    f, err := upl.FromReader(ctx, filename, r)
    if err != nil { return 0, fmt.Errorf("upload: %w", err) }

    media := &tg.InputMediaUploadedDocument{
        File:     f,
        MimeType: "application/octet-stream",
        // …attributes…
    }
    sent, err := api.MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
        Peer:     peer,
        Media:    media,
        // … flags, random_id …
    })
    if err != nil { return 0, fmt.Errorf("send_media: %w", err) }

    return extractNewMessageID(sent)
}
```

The returned `int` is the channel message ID that ends up in
`chunk_map.tg_message_id`. That's the entire identity of an uploaded
chunk; everything else (size, hash, encryption keys) is in our local
metadata.

`extractNewMessageID` walks the `tg.UpdatesClass` response looking for
`UpdateNewChannelMessage`. The gotd reply shape varies depending on the
update path (rare but real), so we do an exhaustive walk rather than
assuming a specific position. The pattern lives in
`internal/tg/message.go`.

> **War story.** Early code returned the first numeric ID it found in
> the response. That happened to be the *previous* message ID for some
> account types. We were silently overwriting our own chunk_map rows
> for weeks before someone noticed reads returned garbage. The walk
> looks tedious, but it's defending against a real bug.

## Big-file part uploads

A 4 MiB chunk doesn't fit in one MTProto request. gotd's `uploader`
splits it into 128 KiB parts and sends them via
`UploadSaveBigFilePart`. You'll see those in the `/telegram` panel as
many fast small successes followed by one `MessagesSendMedia` to
finalize the document.

The implications:

- One "upload" can be ~30 MTProto RPCs.
- The rate limiter sees each part as a separate request.
- The packer's blob upload is N parts; if the rate limit is low, the
  parts trickle through and a single 32 MiB pack can take a minute.

This is why the rate-limit suggestions in the web UI are in the 60–120
range — not 60–120 *files*, but 60–120 *RPCs*. A 1-file upload of a
big file can be 30+ RPCs. Pick accordingly.

## Next chapter

[05 — The metadata store](05-metadata-store.md) — the hardest correctness
battlefield, by some margin.
