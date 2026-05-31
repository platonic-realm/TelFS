# 01 — Introduction

## What TelFS is

TelFS is a Linux filesystem. You mount it like any other:

```sh
telfs mount /mnt/telfs
```

You then `cp`, `tar`, `vim`, `cat`, anything. The contents of the
mountpoint are stored as messages in a private Telegram channel that you
control. The local machine keeps a SQLite metadata DB so the directory
tree, permissions, and chunk index don't require talking to Telegram for
every `ls`.

That's it. There's nothing clever about the user-facing contract — POSIX
in, POSIX out. The interesting parts are all underneath.

## The central design constraint

> Telegram is the storage layer. We don't control it. We can't add an
> API. We can't pay for "premium IO." We get one transport — MTProto over
> long-lived TCP — and we get rate-limited like every other client.

Every architectural choice in TelFS is a consequence of that single
sentence. A few examples to set the tone:

- We chunk files because Telegram has a 2 GiB per-document ceiling and
  every send is a separate message.
- We *pack* small chunks into shared messages because each message costs
  one MTProto RPC and bursty workloads (e.g., extracting a Linux source
  tree, 90 k tiny files) would otherwise burn through the per-user
  request budget in seconds.
- We snapshot the local DB into the same channel because the channel is
  the only durable thing the operator owns. A laptop dying without a
  channel-side backup of the metadata would orphan every chunk.
- We retry transient FLOOD_WAITs internally because Telegram considers
  rate limiting a normal response, not an error — but tar considers any
  write returning EIO mid-extract a fatal error.

If a chapter ever feels arbitrary, asking "what would Telegram let us
do?" usually surfaces the why.

## What TelFS isn't

It helps to be explicit about what we *don't* try to do:

- **Not a high-throughput object store.** A LAN-attached NAS is several
  orders of magnitude faster. We trade speed for the operational
  property of being free, off-site, and zero-maintenance.
- **Not multi-writer.** A given filesystem has exactly one writer (the
  mounted daemon). Two machines mounting the same channel concurrently
  will corrupt each other's view. The CLI doesn't let you do it
  by mistake (the mount checks `meta_kv:fs_uuid` against the channel),
  but there's no clever distributed-consensus story.
- **Not a Telegram client.** We don't read user messages, expose the
  channel UI, or do anything but treat the channel as raw blob storage.
  The `gotd` library is doing the actual MTProto work; we're a focused
  consumer of its `tg.Client`.
- **Not a security boundary against Telegram.** Telegram's operators can
  see *that* you uploaded N bytes at time T. With encryption on
  (`telfs encrypt init`), they can't see the plaintext. They can still
  watch the metadata about your activity.

## What you'll learn

A reader who finishes this academy should be able to:

1. Open any file under `internal/` and explain it.
2. Predict what would break if you removed any single piece (e.g., "what
   if we deleted the packer?", "what if `_txlock=immediate` were
   reverted?").
3. Recognize the recurring patterns we lean on — write-ahead journal,
   per-inode read-after-write barrier, retry-on-busy, content-addressed
   dedup, hot-reload via meta_kv polling — and apply them to a new
   subsystem.
4. Understand the war stories well enough to avoid re-introducing fixed
   bugs. Each was expensive to find; the chapters call them out so the
   next reader doesn't repeat them.

## A note on style

The code itself is heavily commented. The comments in the code explain
*what* and *invariant-preserves-what*; this academy explains *why* the
code chose this shape over alternatives. Where the code's comments are
already at the right altitude, I quote them rather than rewrite. When a
file has a comment block that's particularly worth reading, the lesson
points at it explicitly.

The next chapter is the one diagram you'll come back to most often. Open
it before reading any subsystem-specific lesson.
