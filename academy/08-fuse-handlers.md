# 08 — FUSE handlers

The package: `internal/fs/`. Three files. Reads pretty quickly once
you know the patterns.

## What FUSE expects

FUSE is a kernel protocol. The kernel sends "ops" — one per syscall,
roughly — and the userspace daemon answers them. The
[hanwen/go-fuse](https://github.com/hanwen/go-fuse) library translates
those ops into Go interfaces; we implement the interfaces.

The interfaces we implement are listed at the top of `fs.go`:

```go
var (
    _ gofuse.NodeLookuper    = (*Node)(nil)
    _ gofuse.NodeReaddirer   = (*Node)(nil)
    _ gofuse.NodeCreater     = (*Node)(nil)
    _ gofuse.NodeMkdirer     = (*Node)(nil)
    _ gofuse.NodeUnlinker    = (*Node)(nil)
    _ gofuse.NodeRmdirer     = (*Node)(nil)
    _ gofuse.NodeOpener      = (*Node)(nil)
    _ gofuse.NodeRenamer     = (*Node)(nil)
    _ gofuse.NodeSymlinker   = (*Node)(nil)
    _ gofuse.NodeLinker      = (*Node)(nil)
    _ gofuse.NodeReadlinker  = (*Node)(nil)
    _ gofuse.NodeSetattrer   = (*Node)(nil)
    _ gofuse.NodeSetxattrer  = (*Node)(nil)
    _ gofuse.NodeGetxattrer  = (*Node)(nil)
    _ gofuse.NodeListxattrer = (*Node)(nil)
    _ gofuse.NodeRemovexattrer = (*Node)(nil)
)
```

A go-fuse "Node" represents a file or directory. We use **one Node
type for all kinds** (files, dirs, symlinks). The kind is stored in
the meta DB; the Node's `requireWrite()` etc. branch on it.

## One Node type, why

A more "OO" design would have separate `FileNode` / `DirNode` /
`SymlinkNode`. Reasons we don't:

- The kind is mutable in principle (rename can convert nothing into
  anything; but POSIX doesn't allow type changes, so this is moot).
- Go-fuse dispatches on the *Node*'s method set. Having dirNode
  implement Mkdir but not Read, etc., is what go-fuse expects. But we
  ended up needing nearly every method everywhere — because Setattr
  applies to files and dirs, Lookup applies to dirs, etc. The
  refactoring cost of splitting wasn't worth it.
- The branching is shallow. `if in.Kind != meta.KindDir { return
  syscall.ENOTDIR }` is a one-liner at the top of Mkdir and friends.

## The Backend bundle

`fs.go`:

```go
type Backend struct {
    Meta      *meta.Store
    Reader    *chunk.Reader
    Cache     *chunk.Cache
    Uploader  chunk.Uploader
    Pool      *chunk.UploadPool
    Packer    *chunk.Packer
    Cipher    crypto.Cipher
    Codec     compress.Codec
    ChunkSize int64
    ReadOnly  bool
    TrashIno  int64
}
```

Every Node holds `backend *Backend` (a pointer, shared by all nodes in
the tree). The mount daemon builds one Backend at startup; every Node
reaches into it via the same pointer. The Backend's fields are all
either thread-safe or immutable — no per-Node state. That's why the
tree can be many Nodes deep and we don't pay per-Node allocations.

## The handler patterns

The handlers fall into three categories: read-side, write-side,
helper. Let's look at one of each.

### Read-side: Lookup

```go
func (n *Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut)
    (*gofuse.Inode, syscall.Errno) {
    in, err := n.backend.Meta.Lookup(ctx, n.ino, name)
    if err != nil {
        return nil, logMetaErr("Lookup", n.ino, err)
    }
    childInode := n.newChildInode(ctx, in)
    fillAttr(&out.Attr, in)
    return childInode, 0
}
```

- One meta call.
- One inode wrap.
- Error path goes through `logMetaErr`, which maps `meta.ErrNotFound`
  to `ENOENT` and everything else to `EIO` *with a log line*.

The log-line discipline is critical. Earlier code did:

```go
// OLD
if err != nil {
    return nil, syscall.ENOENT  // every error → ENOENT
}
```

That's wrong: a transient SQL busy is not "this thing doesn't exist."
And it's catastrophic in the rm path: Go's `os.RemoveAll` interprets
ENOENT as "already deleted, success" and proceeds. A real meta error
made it think files were gone when they weren't; then the parent
rmdir succeeded for the same reason; net effect: the tree appeared
deleted but data was still on disk.

`logMetaErr` (defined further down in fs.go) does:

```go
func logMetaErr(op string, ino int64, err error) syscall.Errno {
    if err == nil { return 0 }
    if !errors.Is(err, meta.ErrNotFound) && !errors.Is(err, meta.ErrExists) &&
        !errors.Is(err, meta.ErrNotEmpty) && !errors.Is(err, meta.ErrIsDir) &&
        !errors.Is(err, meta.ErrNotDir) {
        log.Printf("fs.%s(ino=%d): %v", op, ino, err)
    }
    return mapMetaErr(err)
}
```

Sentinel errors map cleanly to their POSIX errnos. Anything else logs
to stderr and returns EIO. The log line is what saved us during the
"silent EIO in tar" debugging arc.

> See commits `2bc7473` (the original NOENT→EIO miscategorization fix)
> and `bf88e79` (added logging to every remaining silent EIO surface).

### Write-side: Create

```go
func (n *Node) Create(ctx context.Context, name string, _ uint32, mode uint32,
    out *fuse.EntryOut) (*gofuse.Inode, gofuse.FileHandle, uint32, syscall.Errno) {
    if !n.backend.canWrite() {
        return nil, nil, 0, syscall.EROFS
    }
    mctx := metaCtx(ctx)
    ino, err := n.backend.Meta.CreateChild(mctx, n.ino, name, meta.Inode{
        Kind: meta.KindFile,
        Mode: mode | uint32(syscall.S_IFREG),
        UID:  callerUID(ctx),
        GID:  callerGID(ctx),
    })
    if err != nil {
        return nil, nil, 0, logMetaErr("Create.CreateChild", n.ino, err)
    }
    child, err := n.backend.Meta.GetInode(mctx, ino)
    if err != nil {
        return nil, nil, 0, logMetaErr("Create.GetInode", ino, err)
    }
    childInode := n.newChildInode(ctx, child)
    w, werr := chunk.NewWriter(mctx, n.backend.Meta, ...)
    if werr != nil {
        log.Printf("fs.Create.NewWriter(ino=%d): %v", ino, werr)
        return nil, nil, 0, syscall.EIO
    }
    h := &fileHandle{node: childInode.Operations().(*Node), writer: w}
    fillAttr(&out.Attr, child)
    return childInode, h, fuse.FOPEN_KEEP_CACHE, 0
}
```

Two patterns to internalize from this handler:

#### `metaCtx(ctx)` for write-side meta calls

```go
func metaCtx(ctx context.Context) context.Context {
    return context.WithoutCancel(ctx)
}
```

FUSE hands us a context that has a kernel-side deadline. Under load,
meta DB ops queue (single-connection writer lock); if the deadline
fires *between* `CreateChild` (which already wrote the inode row) and
`GetInode` (which reads it), we return EIO to userspace — but the
inode is in the DB. Tar sees "Cannot open" on a file that's actually
there. Orphan inode, wasted disk.

`context.WithoutCancel` (Go 1.21+) returns a child context that
**doesn't** propagate cancellation but keeps values. Meta operations
that survive a kernel deadline see the work through; the FUSE op
returns success; tar's syscall finishes normally.

Reads (Lookup, Readdir, Read) keep the original `ctx` because cancel
is a legitimate signal there — if the user kills `cat`, we want the
TG fetch to cancel too.

> See commit `3980da6`.

#### File-handle return on Create

`Create` returns both an `Inode` and a `FileHandle`. POSIX says
`open(path, O_CREAT|...)` is atomic: create-and-open, no race. Returning
both honors that — the kernel gets a handle without a follow-up Open.

The handle wraps a fresh `chunk.Writer` for this fd. The Writer
doesn't share state with other fds on the same inode — that's
deliberate (consistent with FUSE semantics where each open is
independent).

### Helper: requireWrite

```go
func (n *Node) requireWrite() syscall.Errno {
    if !n.backend.canWrite() { return syscall.EROFS }
    return 0
}
```

Called at the top of every write-side handler. `canWrite()` is
`!ReadOnly && Uploader != nil && Cache != nil`. The `Uploader != nil`
check supports a "no telegram" mode useful in some tests.

The pattern of "small helpers shared across handlers" is what keeps
each handler readable. Setattr would be unreadable otherwise.

## The fileHandle

`handle.go`:

```go
type fileHandle struct {
    node *Node
    mu     sync.Mutex
    writer *chunk.Writer
}
```

- `node` lets the handle reach back to the FS tree (for Read, which
  uses `node.backend.Reader` to service reads outside the dirty map).
- `writer` is per-fd state.
- `mu` is held during Read/Write/Flush/Sync. go-fuse can dispatch
  ops on the same handle concurrently (different threads in the
  caller process); we serialize them in one place rather than
  pushing the question down to Writer.

The methods are nearly trivial:

```go
func (h *fileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
    h.mu.Lock()
    defer h.mu.Unlock()
    n, err := h.writer.WriteAt(ctx, data, off)
    if err != nil {
        log.Printf("fs.handle.Write(ino=%d, off=%d, len=%d): wrote %d, err=%v",
            h.node.ino, off, len(data), n, err)
        return uint32(n), syscall.EIO
    }
    return uint32(n), 0
}
```

That's the whole Write handler. The log line is the diagnostic
mechanism for the rare case where Writer's contract surfaces an error.

## CAP_WRITEBACK_CACHE

`mount.go`:

```go
fuseOpts.MountOptions.ExtraCapabilities = fuse.CAP_WRITEBACK_CACHE
```

This is one option, four lines of explanation in the source, and a
significant throughput win.

Without writeback cache, every `write(fd, "x", 1)` from userspace is a
FUSE WRITE op with 1 byte. A `dd if=/dev/zero bs=1 count=100000` is
100 k FUSE round trips.

With writeback cache, the kernel coalesces small writes in its page
cache and forwards us a single larger WRITE op when it forces back
(usually at MaxWrite, which is typically 128 KiB). Same `dd` becomes
maybe 800 FUSE ops, 125× fewer round trips.

The catch: writeback cache changes which side maintains mtime. With
WB, the kernel updates mtime on write, then sends Setattr to push it
to us. Our Setattr handles that mtime patch like any other. So the
feature just works for us; the catch was that we already accept mtime
updates in Setattr. Many FUSE filesystems don't (because they
synthesize mtime from somewhere else); for those, enabling
CAP_WRITEBACK_CACHE would lose mtime updates.

> See commit `696c20d` for the rationale.

## Setattr: the unusually wide handler

`Setattr` is the consolidated entry point for `chmod`, `chown`, `utime`,
and `truncate`. The kernel batches whatever attributes you tried to
change into a single op. Our handler:

1. Builds a `meta.SetAttrsPatch` from the present fields.
2. If any of mode/uid/gid/mtime is set, calls `SetAttrs`.
3. If size is set:
   - With an open handle, route through the handle's Writer.Truncate
     (so dirty bytes are accounted for).
   - Without one, mint a transient Writer just for the truncate.
4. Reads back the final inode for the attr reply.

The "transient Writer" pattern is fine because the writer doesn't take
expensive resources — no pool reservation, no goroutines spawned —
unless it dispatches uploads. A truncate-only invocation runs through
`meta.DeleteChunksAbove` + `meta.SetSize` and is done.

## Read: pull-through cache

`fileHandle.Read` doesn't go through the writer at all:

```go
func (h *fileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
    n, err := h.node.backend.Reader.ReadAt(ctx, h.node.ino, dest, off)
    if err != nil {
        log.Printf("fs.handle.Read(ino=%d, off=%d, len=%d): %v",
            h.node.ino, off, len(dest), err)
        return nil, syscall.EIO
    }
    return fuse.ReadResultData(dest[:n]), 0
}
```

The semantic implication: **reads from the same fd do NOT see writes
from that fd until Flush.** A process that does `write("abc")` then
`read()` from the same fd sees the pre-write bytes.

This is intentional and matches the FUSE contract for "the buffer
isn't durable until flush." Apps that need read-after-write within a
single fd should Sync between them. Most apps don't care (they either
write-only or read-only per fd).

## Trash interception

In Unlink and Rmdir:

```go
if n.backend.TrashIno != 0 && n.ino != n.backend.TrashIno {
    if err := trashRename(mctx, n.backend, n.ino, name); err != nil {
        return logMetaErr("Unlink.trash", n.ino, err)
    }
    return 0
}
```

When the trash safety-net is enabled (chapter 11), unlinks of files
NOT already in `/.trash` are turned into a rename into the trash dir.
The actual unlink (with chunk reclamation) happens later, by the
TTL-based GC.

The exception (`n.ino != n.backend.TrashIno`) is for the trash dir
itself — inside `/.trash`, unlinks proceed normally so the GC can
actually delete things.

## Why no Read tests in this package

`internal/fs` has zero `*_test.go` files. That's a deliberate choice:

- The interfaces are entirely defined by go-fuse. Testing them
  in-package would mostly verify "this method has this signature,"
  which the compiler already does.
- Real testing requires a kernel FUSE mount, which is a privileged
  operation. Our `make integration` target spins one up and runs
  through a battery of fs-level operations (cp, tar, md5sum, etc.).
- Per-handler unit tests would mostly mock `meta.Store` and check
  that the handler called the right meta method with the right args
  — verifying glue, not behavior.

The cost is that bugs in this layer are harder to catch in CI. The
payoff is that bugs that exist tend to be more subtle (we don't catch
"forgot to call canWrite()" with a unit test, but we do catch it the
first time someone tries to write to a readonly mount).

## Next chapter

[09 — Snapshots and recovery](09-snapshots-and-recovery.md) — how a
freshly cloned host catches up.
