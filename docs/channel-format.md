# TelFS channel-message format

The Telegram channel is treated as an append-mostly log of three message kinds.
Each is identified by a small JSON header in the message text (for text-only
messages) or in the document caption (for messages with an attached file).

## 1. `chunk` — file-content slice

Posted as a document (binary upload). Caption JSON:

```json
{"k":"c","id":"<chunk-uuid>","sz":<bytes>}
```

Fields:
- `k`: `"c"` — kind discriminator.
- `id`: a v4 UUID minted client-side. Lets us cross-reference the chunk
  independent of `message_id` (useful during snapshot rebuild).
- `sz`: payload size in bytes.

The returned `message.id` is what TelFS stores in `chunk_map.tg_message_id`.

Chunks are immutable. Overwriting a file means uploading a new chunk and (best
effort) deleting the old message; if the delete fails, the chunk is orphaned
but harmless and will be cleaned up by a later GC pass.

## 2. `meta-op` — single filesystem mutation

Plain text message, JSON body:

```json
{"k":"m","seq":<N>,"op":"<op-name>","args":{...}}
```

Op names mirror VFS calls:

| op | args |
|---|---|
| `mkdir`    | `{"parent":<ino>,"name":"...","mode":<mode>,"ino":<new>}` |
| `create`   | `{"parent":<ino>,"name":"...","mode":<mode>,"ino":<new>}` |
| `unlink`   | `{"parent":<ino>,"name":"..."}` |
| `rmdir`    | `{"parent":<ino>,"name":"..."}` |
| `rename`   | `{"old_parent":<ino>,"old_name":"...","new_parent":<ino>,"new_name":"..."}` |
| `link`     | `{"parent":<ino>,"name":"...","target_ino":<ino>}` |
| `symlink`  | `{"parent":<ino>,"name":"...","target":"...","ino":<new>}` |
| `truncate` | `{"ino":<ino>,"size":<size>}` |
| `setattr`  | `{"ino":<ino>,"mode"?:..., "uid"?:..., "gid"?:..., "mtime"?:...}` |
| `chunk_set`| `{"ino":<ino>,"idx":<n>,"msg_id":<m>,"size":<s>}` |
| `chunk_del`| `{"ino":<ino>,"idx":<n>}` |
| `xattr_set`| `{"ino":<ino>,"name":"user.foo","value_b64":"..."}` |
| `xattr_del`| `{"ino":<ino>,"name":"user.foo"}` |

`seq` is a strictly-monotonic counter shared with the local journal so
recovery can `WHERE seq > snapshot.seq` and replay deterministically.

## 3. `snapshot` — gzipped DB dump

Posted as a document. Caption JSON:

```json
{"k":"snap","seq":<highest-seq-included>,"ts":<unix-seconds>,"fs_uuid":"..."}
```

The document is `gzip(sqlite3 .dump)` or, alternatively, the raw `.sqlite`
file gzipped. (Implementation chooses the cheaper one; the on-disk format is
recoverable either way.)

After a successful snapshot post, TelFS deletes:
- the previous `snapshot` message;
- all `meta-op` messages with `seq <= snapshot.seq`.

This keeps the channel from growing without bound while preserving full
recoverability from any single in-flight state.

## Filtering / discovery

To find the latest snapshot on cold mount, TelFS scans the channel newest-first
(`messages.getHistory`) and parses each message's text/caption as JSON. The
first message with `k:"snap"` is the latest snapshot. From there, all messages
with `k:"m"` and `seq > snapshot.seq` are replayed in order.

Non-JSON or unrecognized messages are ignored (the channel may contain manual
posts from the user — TelFS treats them as invisible).
