# 16 — The profile-identity pass

This chapter walks through one live-testing session that reshaped how
TelFS thinks about *who* a profile is. It started with five innocent
bug reports and ended with bot auth deleted, a real setup wizard, a
`telfs unmount` command, and Telegram stats that survive restarts.
Like chapters 14 and 15, it's told in war-story order because the
*sequence* of discoveries is the lesson.

## The experiment that "didn't fail"

The trigger: a bot token was added to a profile that had previously
completed a phone login, deliberately WITHOUT adding the bot to the
channel — to see whether TelFS would notice. It didn't. The mount came
up, files listed fine, everything looked healthy.

Under the hood, everything was on fire. The daemon log showed a
`CHANNEL_INVALID` error every five seconds, around the clock:

```
[journal-poster] drain: post batch first_seq=12139 last_seq=12150:
    send media: rpcDoRequest: rpc error code 400: CHANNEL_INVALID
```

Three separate defects stacked to produce that silence:

1. **Channel access hashes are identity-scoped.** The `access_hash`
   stored in `config.toml` had been resolved by the *user* account.
   Under the bot's session it was garbage — every channel RPC failed.
   Nothing in the code knew hashes had an owner.
2. **Nothing verified the session's identity.** `auth_mode = "bot"`
   in config, a user session on disk — or vice versa — and every code
   path just proceeded as whatever the session happened to be. The CLI
   bot login was worse: it *skipped token validation entirely* if any
   session existed ("already authorized").
3. **Failures had nowhere to surface.** Metadata is local, so `ls`
   worked; reads came from cache; the errors lived only in a log
   nobody reads. The mount had no startup probe that would refuse to
   come up.

> **War story:** the "it didn't fail" report was *more* alarming than
> a crash would have been. A filesystem that accepts writes it can
> never durably store is worse than one that refuses to mount. Silent
> degradation is the failure mode to design against — loud and early
> beats quiet and eventual, every time.

## One identity per profile

The fix started as a hard lock: a profile's auth mode is fixed by its
first login; switching requires an explicit
`telfs profile reset-auth`, which wipes the session AND the channel
binding — because the access hash dies with the identity that
resolved it.

Then the question nobody had asked: *what is bot auth actually buying
us?* The apparent answer — "skip the my.telegram.org errand" — turned
out to be false. MTProto's `auth.importBotAuthorization` takes
`api_id` and `api_hash` as protocol parameters; the "token-only" bot
libraries either ride the HTTP Bot API (whose 50 MB / 20 MB caps,
missing fetch-by-id, and missing history access disqualify it for a
filesystem — recovery alone requires scanning channel history) or
quietly ship their author's api_id inside. Since the credential
errand exists either way, a bot identity added only costs:
channel-admin setup, a hash the user account can't share with it, and
a second auth path to maintain. So bot auth was deleted outright.
What remains:

- `Config.Identity` (`[identity]` in config.toml) records who the
  session is, written on every successful login.
- `Session.VerifyAccess` runs at mount preflight: one `users.getUsers`
  (is the session the kind of identity we support?) plus one
  `channels.getChannels` probe (can this identity reach the bound
  channel?). A broken profile now fails in seconds, naming the actor
  and the fix, instead of retrying forever.
- The journal poster treats `CHANNEL_INVALID` / `CHANNEL_PRIVATE` as
  non-retriable: one explanatory line, then backoff — an operator
  problem doesn't deserve a 5-second error loop.
- Leftover bot sessions from the removed feature are rejected at
  login and mount with a `reset-auth` hint; `Identity.IsBot` survives
  purely as that defense flag.

## The unmount that wedged

"Unmount doesn't work" reproduced beautifully: `fusermount3 -u`
returned EBUSY (a file manager held the mountpoint), `server.Wait()`
then blocked forever, and only the 30-second watchdog's lazy-unmount
`os.Exit` rescued the process — skipping the final snapshot on the
way out. The daemon's own escape hatch was punishing clean shutdowns.

The fix family:

- On unmount failure, check `/proc/mounts`: entry gone → someone else
  unmounted, fine; still there → lazy-detach (`fusermount -u -z`)
  inline so the drain and final snapshot still run.
- `internal/mountctl` centralizes the /proc scanning + signal + verify
  logic, and `telfs unmount <mountpoint|profile>` works on ANY daemon
  — CLI-started, web-supervised, or orphaned by a dead session —
  because it finds the process via /proc rather than trusting any
  supervisor's bookkeeping.
- The web's Stop button had a route bug on top: adopted daemons
  without a profile get synthetic `adopted:<mountpoint>` keys, whose
  slashes can't ride in a one-segment path wildcard. Stop now posts
  the profile as a form field.

## The web server that ignored you

"When I change the profile, nothing changes." Root cause: the web
process had `TELFS_PROFILE=test` inherited from its launching shell,
and the env var beats the `active` file in `config.Load()`. The Use
button dutifully wrote the file; every subsequent request ignored it.

`telfs web` now drops `TELFS_PROFILE` from its own environment at
startup (using it only to seed the active file when none exists). The
masthead shows the active profile on every page, because invisible
global state is how this class of bug stays hidden.

## The wizard that was just links

The original `/setup` wizard rendered a description of each step and a
link to some other page — a table of contents cosplaying as a wizard.
It became the real thing: every step is an inline form ON `/setup`
(create profile → API creds → phone login → channel pick → optional
encryption/compression → first mount), each posting to the step's
existing handler with `from=setup` so the handlers stay single-source
and redirect back to the wizard.

Two bugs found by actually walking it:

- **`session.json` is not "logged in."** gotd writes the session file
  during the MTProto handshake — i.e. the moment the login code is
  SENT. The wizard's auth step keyed on file existence, marked itself
  done, and skipped the code form while the user sat waiting to type
  the code. Completion now means the `[identity]` record exists,
  which is only written after a successful login.
- The in-flight login needed to survive a reload: the login session
  tracks its stage ("code" / "password") and the wizard renders the
  matching form. The login cookie's path had to widen from `/login`
  to `/` for `/setup` to even see it.

The same "one identity" rule grew its UX teeth here: the login page
refuses a second login while a session exists and offers **Log out**
instead (kept: the channel binding — same account resumes seamlessly;
a different account is caught by mount preflight and told to re-bind).
Logout is refused while the profile's daemon is mounted, because the
running session is in use.

## Stats that survive

The `/telegram` tab's counters (RPCs ok/err, wire bytes, per-method
totals) lived only in daemon memory: every restart reset them, and a
stopped daemon showed nothing. Now the tracker's lifetime totals
persist as one JSON row in the profile's `db.sqlite` (`meta_kv`, key
`tg_lifetime_stats`): seeded into the tracker at startup, written
back every minute and at shutdown — deliberately *before* the final
snapshot, so the stats ride the snapshot blob and survive even a
channel recovery. Because seeding ADDS the stored baseline into the
live counters, a later marshal always yields the full sum: plain
overwrites can never lose history. When the daemon is down, the web
falls back to the persisted row and labels it "lifetime totals,
daemon offline."

## The lessons, compressed

- **Ownership is part of a credential's type.** An access hash isn't
  "the channel's hash"; it's "*this account's* hash for the channel."
  Modeling that (reset-auth wipes both) removed a whole failure class.
- **Delete features that lost their reason.** Bot auth's premise
  ("skip the credential errand") was factually wrong at the protocol
  level. Once the premise died, keeping the feature was pure cost.
- **A file's existence is not a state machine.** `session.json`
  existing meant "handshake happened," not "user authenticated."
  Completion needs an explicit marker written at completion time.
- **Preflight beats retry loops.** Two cheap RPCs at mount time
  replaced an unbounded CHANNEL_INVALID storm with a one-line
  diagnosis.
- **Escape hatches must not punish the normal path.** The unmount
  watchdog "worked," but reaching it on every EBUSY meant clean
  shutdowns lost their final snapshot.
