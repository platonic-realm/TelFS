# 12 — The web UI

> **Since v1 (October 2026)** the console reads the active profile's
> mount over its owner-only control socket (`<profile>/daemon.sock`)
> instead of the fixed TCP port it used to assume, and the link
> lamp classifies the upload backlog by the staging cap: see
> [chapter 18](18-v1-backup-service.md), sections 1 and 2.

`internal/web/`. Largest single package, but most of it is shape and
glue. The interesting decisions are about *what the web is* and *how
the web stays simple*.

## What the web is

Two jobs:

1. **A face on the CLI.** Anything you can do with `telfs ...` you
   should also be able to do in the browser. Forms drive subcommands,
   tables show what `telfs status` shows, etc.
2. **A supervisor for mount processes.** The web optionally spawns
   `telfs mount` children, captures their stdout/stderr to log files,
   tracks them across web restarts, and exposes Stop / Start / log
   tail.

The web is **not** a daemon. It doesn't hold the gotd session. It
doesn't open the meta DB for any sustained period; each handler
opens, queries, closes. That keeps the failure model simple — a
crashing web doesn't take the FS with it.

## What the web is not

- **Not a JS-bundled SPA.** No webpack, no React, no node. Templates
  + HTMX. The chapter justifies this further down.
- **Not a multi-user system.** Either trust everyone who can reach
  loopback (default), or set `--token <secret>` for bearer-token
  auth on a non-loopback bind. No per-user accounts.
- **Not a security boundary against a hostile local user.** Anyone
  who can connect to the listen port can do anything the CLI can.
  That's an explicit choice — the web is a UI, not a privilege
  fence.

## The templates + HTMX choice

The strongest piece of the web's design is its rendering stack. Go
`html/template` for everything, HTMX (~50 KB JS) for incremental
updates, no build step.

The temptation to use React (or Svelte, or Vue) was real. The
arguments against, in roughly the order they mattered:

- **Single binary.** TelFS is meant to be one executable. A JS bundle
  becomes a `dist/` directory that has to be embedded, served, and
  versioned. Templates embed via `embed.FS`; the JS asset is one
  static file.
- **No `npm`.** Adding a Node toolchain to the build is a tax we'd
  pay every CI run. We're already strict about Go's build hermeticity
  (no cgo). Adding Node would erode that.
- **Server-rendering means server-driven state.** With htmx, the
  server is the source of truth — every interaction round-trips. With
  a client-side framework, you have a parallel state model and
  serialization. We have *one* state to keep coherent (the meta DB);
  duplicating it in the browser would be effort with no benefit.
- **HTMX scales to our scope.** The interactivity we need is "auto-
  refresh this panel," "swap this fragment," "submit a form via
  POST." HTMX does all three with HTML attributes. We don't need
  state management, virtual DOM, etc.

The result: the entire UI is in ~30 Go template files + ~30 KB of
CSS. A new feature is `add a template, add a handler, route it`.
Onboarding cost for a new contributor is roughly "do you know Go and
HTML?"

## The Server type

`internal/web/server.go`:

```go
type Server struct {
    opts   Options
    state  *State            // CSRF + flash + login sessions
    logins *loginRegistry    // per-cookie phone-login state machines
    sup    *Supervisor       // mount process tracking
    repair *RepairTracker    // live repair progress
    mux    *http.ServeMux
    srv    *http.Server
}
```

Five components, all per-server-process. The HTTP server itself is
`net/http` with a `ServeMux`. Routes are registered in
`router.go`:

```go
s.mux.HandleFunc("GET /{$}", s.dashboardIndex)
s.mux.HandleFunc("GET /partials/status", s.statusPartial)
// ...
s.mux.HandleFunc("POST /telegram/ratelimit", s.telegramSetRateLimit)
```

Go 1.22's `mux.HandleFunc("METHOD /path", ...)` does method-aware
dispatch without a third-party router. We use it; routes are flat;
each handler does its thing.

## State, CSRF, flash

`state.go`. Two cookies:

- `telfs_csrf` — random 32-hex-char token. Every form embeds the
  same value; the POST handler checks that cookie ↔ form match.
- `telfs_flash` — one-shot status message, URL-encoded
  `<kind>|<msg>`. The next page render consumes it.

The CSRF design is "double-submit cookie." The cookie is HttpOnly +
SameSite=Strict + 24h MaxAge. The form value is whatever the cookie
is at render time. The check is `constant-time-compare(cookie, form)`.

### Why we don't require the token to be in our in-memory map

Originally `CheckCSRF` required the token to be present in
`s.state.csrfs` (an in-memory map keyed by token). The map was a
janitor list, age-out, etc.

That broke after every web restart: in-memory map empty, browser had
the cookie, the form had the matching value, but our check rejected
because the token wasn't "in the map." Every visible form silently
403'd after the web restarted.

The fix: trust the cookie ↔ form equality. The map is for janitorial
purposes only. As long as the cookie value looks like one of our
tokens (32 hex chars), accept it and re-register in the map.

The actual CSRF protection is the *same-origin guarantee* the cookie
provides — a malicious cross-site form can't read the cookie, so it
can't put the right value in its form. The map never contributed to
security; treating its absence as expiry was a bug.

> See commit `2a1351c`.

## Partial polls and the flash-eating bug

HTMX panels auto-refresh:

```html
<section id="tg-panel"
  hx-get="/partials/telegram"
  hx-trigger="every 2s"
  hx-swap="outerHTML">
  ...
</section>
```

Every 2 s, HTMX fetches `/partials/telegram` and replaces the section.
The fragment is rendered server-side via a partial template.

Initial implementation of these partial handlers reused the full-page
data path. They called `basePage(w, r)`, which:

- Calls `ConsumeFlash` (deletes the flash cookie).
- Calls `EnsureCSRF` (may rotate the cookie if not in map).

That meant: every 2 s, the flash was consumed before the user's
full-page render had a chance to show it. And after a web restart,
the partial poll rotated the CSRF cookie, breaking the still-rendered
form on the page.

The fix: partial handlers must **not** call `basePage`. They build
minimal data shapes with only the fields the partial template
actually references.

```go
func (s *Server) statusPartial(w http.ResponseWriter, r *http.Request) {
    d := struct {
        Mounts []string
    }{Mounts: activeMounts()}
    s.renderPartial(w, "partials/status.html", d)
}
```

If the partial doesn't reference flash or CSRF (and they don't), the
partial template doesn't need them in scope. No need to side-effect
the session.

## The supervisor

`internal/web/supervisor.go`. The shape:

```go
type MountProcess struct {
    Profile    string
    Mountpoint string
    LogPath    string
    Started    time.Time
    Adopted    bool
    Pid        int

    cmd *exec.Cmd
    done chan struct{}
    exitErr error
}

type Supervisor struct {
    mu       sync.Mutex
    procs    map[string]*MountProcess  // profile → process
    selfPath string
}
```

The supervisor knows about two kinds of mounts:

- **Supervised**: started by us via `Supervisor.Start`. We hold the
  `*exec.Cmd`, observed the spawn, opened the log file.
- **Adopted**: found in `/proc` after the fact. We have the PID and
  the mountpoint, but no `*exec.Cmd`. Probed for liveness via
  `kill(pid, 0)`.

The two cases share lifecycle methods: `Stop` (SIGTERM the process,
wait up to 25 s for orderly shutdown, then SIGKILL + lazy unmount).
The branching is contained in `signalProc` and `waitForExit`
helpers.

### Why adopt CLI-started mounts

Originally the supervisor knew only about its own children. A user
who started `telfs mount` from a shell got a "External" stub in the
UI with no Stop button, no log tail. That's a worse UX than the
CLI itself — the web should be a strict *superset* of the CLI.

`Reconcile()` runs on every `List` / `Get` call. It scans
`/proc/mounts` for `fuse.telfs` entries, walks `/proc/*/cmdline` to
find the matching pid, reads `TELFS_PROFILE` from
`/proc/<pid>/environ`, and resolves the log path from
`/proc/<pid>/fd/1` if it's a regular file. The discovered mounts get
inserted into `s.procs` with `Adopted: true`.

> See commit `a99bf08`.

### Why not a single long-running watcher

A goroutine that polls `/proc/mounts` once per second could keep the
map continuously fresh. We chose lazy reconciliation instead — scan
only when someone asks. Reasons:

- The data isn't useful unless someone is looking at it. The CLI tool
  shouldn't burn CPU watching state nobody reads.
- Lazy makes "stale data" impossible: the answer is always derived
  from `/proc` at the moment of the call. No race between the
  watcher's snapshot and the request.
- The scan cost is bounded: `/proc/mounts` is tiny; the per-pid
  cmdline walk is O(number of running mounts), not O(all pids on
  the host).

## The rate-limit form, end to end

This is a useful concrete walk because it touches everything.

1. **Render** (`telegramPage`):
   ```go
   data := tgPageData{
       pageBase:        s.basePage(w, r),       // ← sets CSRF cookie + consumes flash
       Source:          url,
       NowUnix:         time.Now().Unix(),
       RateLimitPerMin: readRateLimitFromMeta(r.Context()),  // ← fresh read from meta_kv
   }
   populateTGState(&data, url)  // fetches /telegram from daemon
   ```
   Template renders the form with `value="120"` if RateLimitPerMin is
   120, empty if 0.

2. **HTMX poll** (`telegramPartial`):
   ```go
   data := s.fetchTelegramStatePartial(r)  // ← no basePage call
   s.renderPartialBlock(w, "partials/telegram.html", "telegram_panel", data)
   ```
   Re-renders only the inner panel. Doesn't touch flash or CSRF.

3. **Submit** (`telegramSetRateLimit`):
   ```go
   if err := s.state.CheckCSRF(r); err != nil { ... }
   raw := strings.TrimSpace(r.FormValue("per_minute"))
   if raw == "" || raw == "clear" { raw = "0" }
   n, _ := strconv.Atoi(raw)
   // open meta, SetTGRateLimitPerMin, close
   SetFlash(w, "ok", fmt.Sprintf("rate limit set to %d ...", n))
   http.Redirect(w, r, "/telegram", http.StatusSeeOther)
   ```
   POST-redirect-GET. The flash cookie carries the success message.

4. **Daemon poller** picks it up within 5 s and calls
   `Client.RateLimiter().SetPerMinute(n)`. Future RPCs are gated.

5. **Next page render** sees the flash on the redirected GET (because
   the partial handler no longer consumes it). User sees "rate limit
   set to 120 — daemon picks it up within 5 s."

Five steps, all separate. Each is independently testable. None
depends on JavaScript state.

## Phone-login state machine

The trickiest piece of the web is the multi-step phone-login flow,
because gotd's `auth.NewFlow` is push-based — it calls our
`UserAuthenticator` methods asking for the phone, then the code, then
the password. Those calls happen *inside the gotd Run callback*,
asynchronously from the HTTP request that triggered them.

The shape (`internal/web/loginsessions.go`):

- A `loginSession` per browser cookie.
- Each owns a goroutine running gotd's `RunSession`.
- The `UserAuthenticator` adapter blocks on a channel waiting for the
  corresponding step's form submission.
- The HTTP handlers post values into the channels.

```
User loads /login/phone
       │
       ▼
POST /login/phone/start  → spawn loginSession, kick goroutine
       │                  goroutine: enter Run, hit Phone() callback
       │                  Phone() blocks on s.phoneCh
       ▼
                ←── form for code (rendered because phone was accepted by TG)
       │
POST /login/phone/code   → send code to s.codeCh
       │                  Code() unblocks, returns to gotd
       │                  goroutine: hit Password() (if 2FA on)
       │                  Password() blocks on s.pwCh
       ▼
                ←── form for 2FA password
       │
POST /login/phone/password → send to s.pwCh
       │                  goroutine: Run returns, session.json written
       ▼
                ←── redirect to /
```

The state machine has timeout (sessions expire after 10 min of
inactivity, the goroutine gets canceled). The trickiest invariant:
every send-into-channel must be matched by a receive, or the
goroutine deadlocks. The HTTP handlers carry this responsibility.

This is the most complex piece of the web, and since bot auth was
removed (chapter 16) it's the ONLY auth flow — which is why the setup
wizard embeds it inline: the `/setup` page reads the in-flight login
session's stage ("code" / "password") and renders the matching form,
so a reload lands back mid-flow instead of restarting. The step is
only considered DONE when the login *completed* (`[identity]`
recorded) — `session.json` alone is not proof, because gotd writes it
during the handshake, at the exact moment the code is sent.

## Why a separate process from the daemon

You could imagine the web embedded *inside* the daemon — same
process, same gotd session, no inter-process plumbing. We chose
separation:

- **Decoupled crashes.** A bug in the web shouldn't bring down the
  mount. A crashing daemon shouldn't blank the UI; the UI should
  show the crash.
- **Different lifetimes.** You might mount once and run the daemon
  for days; the web might come and go.
- **Different deployment shapes.** A user might want only the CLI
  with no web. Or only the web on a server that doesn't mount
  anything (browse + control mounts on other hosts — future work).

The cost: the web reaches the daemon's state via the daemon's debug
HTTP (`/telegram`) and via reading meta_kv from the same SQLite
file. That works because SQLite supports multi-process access in WAL
mode. We have to be careful not to write to a meta_kv key the daemon
also writes; the established convention is "web writes config-shaped
keys; daemon reads them via its poller."

## Next chapter

[13 — Lessons learned](13-lessons-learned.md) — the war stories that
shaped the codebase.
