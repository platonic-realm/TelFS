package fs

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"telfs/internal/chunk"
	"telfs/internal/compress"
	"telfs/internal/crypto"
	"telfs/internal/meta"
	"telfs/internal/trash"
)

// Backend bundles the dependencies every Node needs. It's shared by
// reference among all nodes in the mounted tree. The Cache + Uploader
// + Pool + ChunkSize fields are only needed for the write path; read-
// only mounts can leave them zero (or set ReadOnly = true to explicitly
// reject all mutations regardless).
type Backend struct {
	Meta     *meta.Store
	Reader   *chunk.Reader
	Cache    *chunk.Cache
	Uploader chunk.Uploader
	Pool     *chunk.UploadPool // backend-global upload concurrency + per-ino R-A-W barrier
	Packer   *chunk.Packer     // nil → no small-file packing
	Cipher   crypto.Cipher     // format 1: nil → NoopCipher (plaintext mode)
	Codec    compress.Codec    // format 1: nil → compress.None (no compression)
	// Wire, when set, is the chunk wire codec (format 2) and replaces
	// Cipher + Codec.
	Wire      chunk.WireCodec
	ChunkSize int64
	ReadOnly  bool

	// Trash safety-net. When TrashIno != 0, Unlink and Rmdir reroute
	// the dirent into /.trash/ instead of really deleting. A TTL GC
	// running outside the FS layer reaps aged entries. TrashIno == 0
	// (the default) means the FS layer behaves as if trash is off.
	TrashIno int64

	// usageCache memoizes meta.Usage for Statfs. File managers poll
	// statfs on every window repaint; without the cache each poll
	// would scan the inodes table.
	usageMu    sync.Mutex
	usageAt    time.Time
	usageBytes int64
	usageInos  int64
}

// usage returns (fileBytes, inodeCount) from the meta store, cached
// for a few seconds. Errors degrade to the last known values (zero on
// a cold cache) — statfs must not fail a repaint over a busy DB.
func (b *Backend) usage(ctx context.Context) (int64, int64) {
	const usageTTL = 5 * time.Second
	b.usageMu.Lock()
	defer b.usageMu.Unlock()
	if time.Since(b.usageAt) < usageTTL {
		return b.usageBytes, b.usageInos
	}
	if u, err := b.Meta.Usage(ctx); err == nil {
		b.usageBytes, b.usageInos = u.FileBytes, u.Inodes
		b.usageAt = time.Now()
	}
	return b.usageBytes, b.usageInos
}

// Shutdown drains any uploads that were detached when their handle's
// fd closed, up to ctx's deadline. After this returns the caller can
// safely tear down the upload pool's MTProto session. On timeout the
// pool ctx is canceled so still-running gotd round trips unwind.
//
// Order: drain the Packer FIRST so any pending small-file batches
// finish landing their chunk_map rows, then drain the Pool for any
// non-packed uploads. Reverse order would let the pool drain return
// while a packed-flush goroutine still held buffered wires.
func (b *Backend) Shutdown(ctx context.Context) {
	if b.Packer != nil {
		b.Packer.Close()
	}
	if b.Pool == nil {
		return
	}
	if !b.Pool.Drain(ctx) {
		// Watchdog: ctx timed out. Cancel the pool ctx so any goroutine
		// still waiting on a gotd round trip (or a sem slot) errors
		// out and unwinds; we don't wait for them.
		b.Pool.Shutdown()
		return
	}
	b.Pool.Shutdown()
}

// writeRoom is the space writes still have (see Statfs): 0 on a
// read-only mount; ok false without a staging gate.
func (b *Backend) writeRoom() (int64, bool) {
	if b.ReadOnly {
		return 0, true
	}
	if b.Pool == nil || b.Pool.Disk == nil {
		return 0, false
	}
	return b.Pool.Disk.Room()
}

// canWrite reports whether the backend supports mutating ops. False when
// either the write deps aren't wired or ReadOnly was explicitly set.
func (b *Backend) canWrite() bool {
	return !b.ReadOnly && b.Uploader != nil && b.Cache != nil
}

// requireWrite returns EROFS if the backend is read-only and 0 otherwise.
// Mutation entry points (Setattr, Mkdir, Create, Unlink, ...) call this
// first so a read-only mount surfaces consistent errors.
func (n *Node) requireWrite() syscall.Errno {
	if !n.backend.canWrite() {
		return syscall.EROFS
	}
	return 0
}

// Node is the single implementation that backs every kind of TelFS
// inode (file, directory, symlink). Each Node holds a meta inode id;
// per-call methods consult meta.Store to dispatch on the actual kind.
type Node struct {
	gofuse.Inode

	backend *Backend
	ino     int64
}

// Assert all the FUSE node interfaces we implement.
var (
	_ gofuse.NodeGetattrer      = (*Node)(nil)
	_ gofuse.NodeSetattrer      = (*Node)(nil)
	_ gofuse.NodeLookuper       = (*Node)(nil)
	_ gofuse.NodeReaddirer      = (*Node)(nil)
	_ gofuse.NodeOpendirHandler = (*Node)(nil)
	_ gofuse.NodeOpener         = (*Node)(nil)
	_ gofuse.NodeReader         = (*Node)(nil)
	_ gofuse.NodeReadlinker     = (*Node)(nil)
	_ gofuse.NodeCreater        = (*Node)(nil)
	_ gofuse.NodeMkdirer        = (*Node)(nil)
	_ gofuse.NodeSymlinker      = (*Node)(nil)
	_ gofuse.NodeLinker         = (*Node)(nil)
	_ gofuse.NodeUnlinker       = (*Node)(nil)
	_ gofuse.NodeRmdirer        = (*Node)(nil)
	_ gofuse.NodeRenamer        = (*Node)(nil)
	_ gofuse.NodeGetxattrer     = (*Node)(nil)
	_ gofuse.NodeSetxattrer     = (*Node)(nil)
	_ gofuse.NodeRemovexattrer  = (*Node)(nil)
	_ gofuse.NodeListxattrer    = (*Node)(nil)
	_ gofuse.NodeStatfser       = (*Node)(nil)
)

// NewRoot returns the root Node for a TelFS mount.
func NewRoot(b *Backend) *Node {
	return &Node{backend: b, ino: meta.RootIno}
}

// Statfs answers `statfs(2)` / `pathconf(_PC_NAME_MAX)` / df / etc.
//
// With a staging directory (every daemon mount, v1.1 S9) the numbers are
// real: FREE is what can still be written before writes fail — the
// staging filesystem's free space above its min_free floor (the staging
// cap only makes writes wait for uploads) — 0 on a read-only mount, and
// TOTAL is used + free. File managers showed the fictional 128 PB as is.
//
// Without one (the in-memory pipeline of tests) the TOTAL is fictional — a Telegram channel has no storage quota to
// report, but statfs must return something, so we claim 128 PiB with
// 4 KiB blocks. The value is deliberately large-but-bounded: an
// earlier 4 MiB × 4 ZiB choice overflowed some GUI file managers,
// which truncated to ~6 GB and displayed "drive almost full".
//
// USED is real: the summed logical size of every file (cached a few
// seconds — see Backend.usage), so `df -h` shows actual consumption
// against the fictional total instead of a permanently-empty disk.
// Inode counts likewise report the real live count against a huge
// fictional ceiling.
func (n *Node) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	const (
		bsize     uint32 = 4096    // 4 KiB — standard page size
		blockCnt  uint64 = 1 << 45 // 4 KiB * 2^45 = 128 PiB total
		fakeFiles uint64 = 1 << 32 // 4 G inodes — plenty
	)
	usedBytes, usedInos := n.backend.usage(ctx)
	usedBlocks := (uint64(usedBytes) + uint64(bsize) - 1) / uint64(bsize)
	if room, ok := n.backend.writeRoom(); ok {
		freeBlocks := uint64(room) / uint64(bsize)
		out.Blocks = usedBlocks + freeBlocks
		out.Bfree = freeBlocks
		out.Bavail = freeBlocks
		out.Files = fakeFiles
		out.Ffree = fakeFiles - min(uint64(usedInos), fakeFiles)
		out.Bsize = bsize
		out.Frsize = bsize
		out.NameLen = 255
		return 0
	}
	if usedBlocks > blockCnt {
		usedBlocks = blockCnt // fictional total exhausted; never underflow
	}
	usedFiles := uint64(usedInos)
	if usedFiles > fakeFiles {
		usedFiles = fakeFiles
	}
	out.Blocks = blockCnt
	out.Bfree = blockCnt - usedBlocks
	out.Bavail = blockCnt - usedBlocks
	out.Files = fakeFiles
	out.Ffree = fakeFiles - usedFiles
	out.Bsize = bsize
	out.Frsize = bsize
	out.NameLen = 255
	return 0
}

// Getattr fills *fuse.AttrOut from the meta store.
func (n *Node) Getattr(ctx context.Context, _ gofuse.FileHandle, out *fuse.AttrOut) syscall.Errno {
	in, err := n.backend.Meta.GetInode(ctx, n.ino)
	if err != nil {
		return logMetaErr("Getattr", n.ino, err)
	}
	fillAttr(&out.Attr, in)
	return 0
}

// Setattr handles chmod/chown/utime/truncate. FUSE batches all of these
// into one call; we apply them via meta.SetAttrs (+ Truncate for size).
func (n *Node) Setattr(ctx context.Context, fh gofuse.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if errno := n.requireWrite(); errno != 0 {
		return errno
	}
	patch := meta.SetAttrsPatch{}
	if mode, ok := in.GetMode(); ok {
		patch.Mode = &mode
	}
	if uid, ok := in.GetUID(); ok {
		patch.UID = &uid
	}
	if gid, ok := in.GetGID(); ok {
		patch.GID = &gid
	}
	if mtime, ok := in.GetMTime(); ok {
		t := mtime.Unix()
		patch.Mtime = &t
	}
	mctx := metaCtx(ctx)
	if patch.Mode != nil || patch.UID != nil || patch.GID != nil || patch.Mtime != nil {
		if err := n.backend.Meta.SetAttrs(mctx, n.ino, patch); err != nil {
			return logMetaErr("Setattr.SetAttrs", n.ino, err)
		}
	}
	if size, ok := in.GetSize(); ok {
		// If we have an open WRITE handle, use its Writer (it knows
		// about dirty chunks). Otherwise (no handle, or a read-only
		// handle with no writer) mint a transient Writer for the
		// truncate.
		if h, ok := fh.(*fileHandle); ok && h != nil && h.writer != nil {
			if err := h.writer.Truncate(mctx, int64(size)); err != nil {
				log.Printf("fs.Setattr.Truncate(ino=%d, size=%d): %v", n.ino, size, err)
				return syscall.EIO
			}
			// The handle's cached size/sparse hints are stale now —
			// force subsequent Reads through the GetInode slow path.
			h.markDirty()
		} else if errno := n.truncate(mctx, int64(size)); errno != 0 {
			return errno
		}
	}
	final, err := n.backend.Meta.GetInode(mctx, n.ino)
	if err != nil {
		return logMetaErr("Setattr.GetInode", n.ino, err)
	}
	fillAttr(&out.Attr, final)
	return 0
}

// truncate is the no-open-handle path for `truncate("path", size)`.
// Constructs a one-shot Writer just to run Truncate; no upload happens.
func (n *Node) truncate(ctx context.Context, size int64) syscall.Errno {
	if !n.backend.canWrite() {
		return syscall.EROFS
	}
	w, err := chunk.NewWriterWire(ctx, n.backend.Meta, n.backend.Cache, n.backend.Uploader, n.backend.wire(), n.backend.Pool, n.backend.Packer, n.ino, n.backend.chunkSize(), 0)
	if err != nil {
		log.Printf("fs.truncate.NewWriter(ino=%d): %v", n.ino, err)
		return syscall.EIO
	}
	defer w.Close()
	if err := w.Truncate(ctx, size); err != nil {
		log.Printf("fs.truncate.Truncate(ino=%d, size=%d): %v", n.ino, size, err)
		return syscall.EIO
	}
	// A shrink may have re-dirtied a trimmed boundary chunk; this
	// transient writer is Closed right after, so dispatch it now or
	// the trim never reaches the channel.
	if err := w.Flush(ctx); err != nil {
		log.Printf("fs.truncate.Flush(ino=%d): %v", n.ino, err)
		return syscall.EIO
	}
	return 0
}

// Lookup resolves (n, name) to a child Node, creating the go-fuse Inode
// child if it doesn't exist yet.
func (n *Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	in, err := n.backend.Meta.Lookup(ctx, n.ino, name)
	if err != nil {
		return nil, logMetaErr("Lookup", n.ino, err)
	}
	childInode := n.newChildInode(ctx, in)
	fillAttr(&out.Attr, in)
	return childInode, 0
}

// Readdir returns directory entries with kind+mode populated so the
// kernel doesn't need a follow-up Lookup per entry just for the type.
func (n *Node) Readdir(ctx context.Context) (gofuse.DirStream, syscall.Errno) {
	infos, err := n.backend.Meta.ReaddirInfo(ctx, n.ino)
	if err != nil {
		return nil, logMetaErr("Readdir", n.ino, err)
	}
	entries := make([]fuse.DirEntry, len(infos))
	for i, e := range infos {
		entries[i] = fuse.DirEntry{
			Name: e.Name,
			Ino:  uint64(e.Ino),
			Mode: kindToFuseMode(e.Kind),
		}
	}
	return gofuse.NewListDirStream(entries), 0
}

// OpendirHandle serves opendir(2). Returning FOPEN_CACHE_DIR (the
// bridge ORs in FOPEN_KEEP_CACHE) opts the listing into the kernel's
// readdir page cache: repeated getdents of an unchanged directory are
// answered without any FUSE upcall or SQL scan. The kernel invalidates
// its cached pages itself whenever a mutation (create/unlink/rename/
// rmdir) goes through this mount, so the cache is coherent under
// TelFS's single-writer design. The one out-of-band meta writer — the
// trash TTL GC — can leave a cached /.trash listing stale until the
// dir's attrs refresh; lookups of reaped names still miss correctly.
func (n *Node) OpendirHandle(_ context.Context, _ uint32) (gofuse.FileHandle, uint32, syscall.Errno) {
	return &dirHandle{node: n}, fuse.FOPEN_CACHE_DIR, 0
}

// dirHandle is the per-opendir state. It builds the entry list
// LAZILY, on the first Readdirent: with FOPEN_CACHE_DIR the kernel
// usually serves getdents from its page cache and never calls back at
// all, so an eager listing at opendir time would pay the SQL scan on
// exactly the calls the cache exists to eliminate.
type dirHandle struct {
	node *Node
	ds   gofuse.DirStream
}

// The bridge serializes calls on one file handle, so dirHandle needs
// no locking of its own.
var (
	_ gofuse.FileReaddirenter = (*dirHandle)(nil)
	_ gofuse.FileSeekdirer    = (*dirHandle)(nil)
	_ gofuse.FileReleasedirer = (*dirHandle)(nil)
)

// ensure materializes the underlying entry stream on first use.
func (d *dirHandle) ensure(ctx context.Context) syscall.Errno {
	if d.ds != nil {
		return 0
	}
	ds, errno := d.node.Readdir(ctx)
	if errno != 0 {
		return errno
	}
	d.ds = ds
	return 0
}

func (d *dirHandle) Readdirent(ctx context.Context) (*fuse.DirEntry, syscall.Errno) {
	if errno := d.ensure(ctx); errno != 0 {
		return nil, errno
	}
	if !d.ds.HasNext() {
		return nil, 0
	}
	e, errno := d.ds.Next()
	return &e, errno
}

func (d *dirHandle) Seekdir(ctx context.Context, off uint64) syscall.Errno {
	if errno := d.ensure(ctx); errno != 0 {
		return errno
	}
	if sd, ok := d.ds.(gofuse.FileSeekdirer); ok {
		return sd.Seekdir(ctx, off)
	}
	return syscall.ENOTSUP
}

func (d *dirHandle) Releasedir(context.Context, uint32) {
	if d.ds != nil {
		d.ds.Close()
	}
}

// Open allocates a fileHandle. Write-capable opens get a chunk.Writer
// for the handle's lifetime (O_TRUNC truncates to 0 first so writes
// start from a clean slate); read-only opens get a writer-less handle.
func (n *Node) Open(ctx context.Context, flags uint32) (gofuse.FileHandle, uint32, syscall.Errno) {
	// O_TRUNC counts as mutating even with O_RDONLY accmode (Linux
	// truncates on that combination).
	wantsWrite := flags&uint32(syscall.O_ACCMODE) != uint32(syscall.O_RDONLY) ||
		flags&uint32(syscall.O_TRUNC) != 0
	if !n.backend.canWrite() {
		// Read-only mount: reject any open with write flags.
		if wantsWrite {
			return nil, 0, syscall.EROFS
		}
		return nil, fuse.FOPEN_KEEP_CACHE, 0
	}
	if !wantsWrite {
		// Read-only open on a writable mount: NO chunk.Writer. The
		// writer exists for mutation, and constructing one also waits
		// on the pool's per-ino pending barrier — so opening a
		// freshly-written file for READ used to stall until its
		// uploads landed, even though the reader's staging fallback
		// serves the bytes immediately. Write-then-read pipelines
		// (compile → link, cp → md5sum) hit that constantly. Reads
		// need none of the writer's machinery; skipping it also drops
		// a per-open SQL round trip and the writer allocation.
		h := &fileHandle{node: n}
		h.lastReadEnd.Store(-1)
		if in, err := n.backend.Meta.GetInode(ctx, n.ino); err == nil {
			h.chunkSize = in.ChunkSize
			if h.chunkSize <= 0 {
				h.chunkSize = n.backend.chunkSize()
			}
			h.size = in.Size
			h.sparse = in.Sparse
		}
		return h, fuse.FOPEN_KEEP_CACHE, 0
	}
	w, err := chunk.NewWriterWire(ctx, n.backend.Meta, n.backend.Cache, n.backend.Uploader, n.backend.wire(), n.backend.Pool, n.backend.Packer, n.ino, n.backend.chunkSize(), 0)
	if err != nil {
		log.Printf("fs.Open.NewWriter(ino=%d): %v", n.ino, err)
		return nil, 0, syscall.EIO
	}
	if flags&uint32(syscall.O_TRUNC) != 0 {
		if err := w.Truncate(ctx, 0); err != nil {
			log.Printf("fs.Open.Truncate(ino=%d): %v", n.ino, err)
			w.Close()
			return nil, 0, syscall.EIO
		}
	}
	h := &fileHandle{node: n, writer: w}
	h.lastReadEnd.Store(-1) // sentinel: first Read counts as sequential
	// Populate the Read fast-path cache: one GetInode at Open time
	// to capture chunkSize + size. While the handle stays clean (no
	// writes), every Read skips its own GetInode SQL row and uses
	// these values. The first Write trips dirty=true on the handle,
	// which forces subsequent Reads back to ReadAt (which re-fetches
	// the inode) — covers the case where a write extended the file
	// past the cached size.
	if in, err := n.backend.Meta.GetInode(ctx, n.ino); err == nil {
		h.chunkSize = in.ChunkSize
		if h.chunkSize <= 0 {
			h.chunkSize = n.backend.chunkSize()
		}
		h.size = in.Size
		h.sparse = in.Sparse
	}
	// FOPEN_KEEP_CACHE keeps the kernel's page cache valid across opens,
	// which is correct because we invalidate the read cache on writes.
	return h, fuse.FOPEN_KEEP_CACHE, 0
}

// Read fulfills a read on a Node without an open handle (some kernels
// dispatch this for short paths). Delegates straight to chunk.Reader.
func (n *Node) Read(ctx context.Context, _ gofuse.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	nread, err := n.backend.Reader.ReadAt(ctx, n.ino, dest, off)
	if err != nil {
		if interrupted(ctx, err) {
			return nil, syscall.EINTR
		}
		log.Printf("fs.Read(ino=%d, off=%d, len=%d): %v", n.ino, off, len(dest), err)
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:nread]), 0
}

// Readlink returns the symlink target string.
func (n *Node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	in, err := n.backend.Meta.GetInode(ctx, n.ino)
	if err != nil {
		return nil, syscall.ENOENT
	}
	if in.Kind != meta.KindSymlink {
		return nil, syscall.EINVAL
	}
	return []byte(in.SymlinkTarget), 0
}

// Create handles `open(path, O_CREAT, mode)` — atomic create-and-open
// for a regular file.
func (n *Node) Create(ctx context.Context, name string, _ uint32, mode uint32, out *fuse.EntryOut) (*gofuse.Inode, gofuse.FileHandle, uint32, syscall.Errno) {
	if !n.backend.canWrite() {
		return nil, nil, 0, syscall.EROFS
	}
	mctx := metaCtx(ctx)
	ino, err := n.backend.Meta.CreateChild(mctx, n.ino, name, meta.Inode{
		Kind: meta.KindFile,
		Mode: mode | uint32(syscall.S_IFREG),
		UID:  callerUID(ctx),
		GID:  callerGID(ctx),
		// Seed the per-inode chunk size from the FS-configured value.
		// Without this the inodes.chunk_size column default (4 MiB)
		// silently overrode any custom `telfs init --chunk-size` for
		// every file created through FUSE.
		ChunkSize: n.backend.chunkSize(),
	})
	if err != nil {
		return nil, nil, 0, logMetaErr("Create.CreateChild", n.ino, err)
	}
	child, err := n.backend.Meta.GetInode(mctx, ino)
	if err != nil {
		return nil, nil, 0, logMetaErr("Create.GetInode", ino, err)
	}
	childInode := n.newChildInode(ctx, child)
	w, werr := chunk.NewWriterWire(mctx, n.backend.Meta, n.backend.Cache, n.backend.Uploader, n.backend.wire(), n.backend.Pool, n.backend.Packer, ino, n.backend.chunkSize(), 0)
	if werr != nil {
		log.Printf("fs.Create.NewWriter(ino=%d): %v", ino, werr)
		return nil, nil, 0, syscall.EIO
	}
	h := &fileHandle{node: childInode.Operations().(*Node), writer: w}
	h.lastReadEnd.Store(-1)
	fillAttr(&out.Attr, child)
	return childInode, h, fuse.FOPEN_KEEP_CACHE, 0
}

// Mkdir creates a directory child.
func (n *Node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	if errno := n.requireWrite(); errno != 0 {
		return nil, errno
	}
	mctx := metaCtx(ctx)
	ino, err := n.backend.Meta.CreateChild(mctx, n.ino, name, meta.Inode{
		Kind: meta.KindDir,
		Mode: mode | uint32(syscall.S_IFDIR),
		UID:  callerUID(ctx),
		GID:  callerGID(ctx),
	})
	if err != nil {
		return nil, logMetaErr("Mkdir.CreateChild", n.ino, err)
	}
	child, err := n.backend.Meta.GetInode(mctx, ino)
	if err != nil {
		return nil, logMetaErr("Mkdir.GetInode", ino, err)
	}
	childInode := n.newChildInode(ctx, child)
	fillAttr(&out.Attr, child)
	return childInode, 0
}

// Symlink creates a symbolic link child whose contents are `target`.
func (n *Node) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	if errno := n.requireWrite(); errno != 0 {
		return nil, errno
	}
	mctx := metaCtx(ctx)
	ino, err := n.backend.Meta.CreateChild(mctx, n.ino, name, meta.Inode{
		Kind:          meta.KindSymlink,
		Mode:          0o777 | uint32(syscall.S_IFLNK),
		SymlinkTarget: target,
		UID:           callerUID(ctx),
		GID:           callerGID(ctx),
	})
	if err != nil {
		return nil, logMetaErr("Symlink.CreateChild", n.ino, err)
	}
	child, err := n.backend.Meta.GetInode(mctx, ino)
	if err != nil {
		return nil, logMetaErr("Symlink.GetInode", ino, err)
	}
	childInode := n.newChildInode(ctx, child)
	fillAttr(&out.Attr, child)
	return childInode, 0
}

// Link creates a new dirent pointing at the target inode. POSIX
// forbids hardlinking directories.
func (n *Node) Link(ctx context.Context, target gofuse.InodeEmbedder, name string, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	if errno := n.requireWrite(); errno != 0 {
		return nil, errno
	}
	tn, ok := target.(*Node)
	if !ok {
		return nil, syscall.EXDEV
	}
	mctx := metaCtx(ctx)
	if err := n.backend.Meta.Link(mctx, n.ino, name, tn.ino); err != nil {
		if errors.Is(err, meta.ErrIsDir) {
			return nil, syscall.EPERM
		}
		return nil, logMetaErr("Link.Link", n.ino, err)
	}
	in, err := n.backend.Meta.GetInode(mctx, tn.ino)
	if err != nil {
		return nil, logMetaErr("Link.GetInode", tn.ino, err)
	}
	childInode := n.newChildInode(ctx, in)
	fillAttr(&out.Attr, in)
	return childInode, 0
}

// Unlink removes a non-directory child.
func (n *Node) Unlink(ctx context.Context, name string) syscall.Errno {
	if errno := n.requireWrite(); errno != 0 {
		return errno
	}
	in, err := n.backend.Meta.Lookup(ctx, n.ino, name)
	if err != nil {
		return logMetaErr("Unlink.lookup", n.ino, err)
	}
	if in.Kind == meta.KindDir {
		return syscall.EISDIR
	}
	// Refuse to remove the trash dir itself — `rm /.trash` would orphan
	// every parked entry. The user can `telfs trash empty` instead.
	if n.ino == meta.RootIno && in.Ino == n.backend.TrashIno {
		return syscall.EPERM
	}
	mctx := metaCtx(ctx)
	// Trash intercept: reroute the unlink to /.trash unless we're
	// already inside /.trash (where unlinks really delete, so the GC
	// and `telfs trash empty` can clean up).
	if n.backend.TrashIno != 0 && n.ino != n.backend.TrashIno {
		if err := trashRename(mctx, n.backend, n.ino, name); err != nil {
			return logMetaErr("Unlink.trash", n.ino, err)
		}
		return 0
	}
	if err := n.backend.Meta.Unlink(mctx, n.ino, name); err != nil {
		return logMetaErr("Unlink", n.ino, err)
	}
	return 0
}

// Rmdir removes an empty directory child.
func (n *Node) Rmdir(ctx context.Context, name string) syscall.Errno {
	if errno := n.requireWrite(); errno != 0 {
		return errno
	}
	mctx := metaCtx(ctx)
	in, err := n.backend.Meta.Lookup(mctx, n.ino, name)
	if err != nil {
		return logMetaErr("Rmdir.lookup", n.ino, err)
	}
	if in.Kind != meta.KindDir {
		return syscall.ENOTDIR
	}
	// Never let anyone rmdir the trash root via FUSE.
	if n.ino == meta.RootIno && in.Ino == n.backend.TrashIno {
		return syscall.EPERM
	}
	// Trash intercept for empty dirs. The kernel issues per-file
	// unlinks before rmdir, so a `rm -rf foo/` reaches us as N file
	// unlinks (each moved into trash) followed by rmdir of the now-
	// empty foo. The dir itself ends up in trash as a sibling of those
	// files — a flat layout that's slightly less ergonomic than a
	// preserved tree but bounded scope for v1.
	if n.backend.TrashIno != 0 && n.ino != n.backend.TrashIno {
		if err := trashRename(mctx, n.backend, n.ino, name); err != nil {
			return logMetaErr("Rmdir.trash", n.ino, err)
		}
		return 0
	}
	if err := n.backend.Meta.Unlink(mctx, n.ino, name); err != nil {
		return logMetaErr("Rmdir", n.ino, err)
	}
	return 0
}

// trashRename is the shared move-to-trash helper used by Unlink and
// Rmdir.
func trashRename(ctx context.Context, b *Backend, parentIno int64, name string) error {
	return trash.MoveToTrash(ctx, b.Meta, b.TrashIno, parentIno, name)
}

// Rename relocates oldName under n to newName under newParent. The
// directory-overwrite-empty-dir POSIX rule is enforced by meta.Rename.
func (n *Node) Rename(ctx context.Context, oldName string, newParent gofuse.InodeEmbedder, newName string, _ uint32) syscall.Errno {
	if errno := n.requireWrite(); errno != 0 {
		return errno
	}
	np, ok := newParent.(*Node)
	if !ok {
		return syscall.EXDEV
	}
	if err := n.backend.Meta.Rename(metaCtx(ctx), n.ino, oldName, np.ino, newName); err != nil {
		return logMetaErr("Rename", n.ino, err)
	}
	return 0
}

// Setxattr stores an extended attribute. We only accept the user.*
// namespace; other namespaces (trusted.*, system.*, security.*) need
// special privileges TelFS doesn't try to emulate.
func (n *Node) Setxattr(ctx context.Context, attr string, data []byte, _ uint32) syscall.Errno {
	if errno := n.requireWrite(); errno != 0 {
		return errno
	}
	if !isUserXattr(attr) {
		return syscall.EOPNOTSUPP
	}
	if err := n.backend.Meta.SetXattr(metaCtx(ctx), n.ino, attr, data); err != nil {
		return logMetaErr("Setxattr", n.ino, err)
	}
	return 0
}

// Getxattr reads an extended attribute value into dest.
func (n *Node) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	if !isUserXattr(attr) {
		return 0, syscall.EOPNOTSUPP
	}
	v, err := n.backend.Meta.GetXattr(ctx, n.ino, attr)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			return 0, syscall.ENODATA
		}
		return 0, logMetaErr("Getxattr", n.ino, err)
	}
	if len(dest) < len(v) {
		return uint32(len(v)), syscall.ERANGE
	}
	return uint32(copy(dest, v)), 0
}

// Listxattr returns the names of all xattrs on this inode, encoded as
// NUL-terminated byte sequences.
func (n *Node) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	names, err := n.backend.Meta.ListXattrs(ctx, n.ino)
	if err != nil {
		return 0, logMetaErr("Listxattr", n.ino, err)
	}
	var total int
	for _, name := range names {
		total += len(name) + 1
	}
	if len(dest) < total {
		return uint32(total), syscall.ERANGE
	}
	out := dest[:0]
	for _, name := range names {
		out = append(out, name...)
		out = append(out, 0)
	}
	return uint32(total), 0
}

// Removexattr deletes an xattr.
func (n *Node) Removexattr(ctx context.Context, attr string) syscall.Errno {
	if errno := n.requireWrite(); errno != 0 {
		return errno
	}
	if !isUserXattr(attr) {
		return syscall.EOPNOTSUPP
	}
	if err := n.backend.Meta.RemoveXattr(metaCtx(ctx), n.ino, attr); err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			return syscall.ENODATA
		}
		log.Printf("fs.Removexattr(ino=%d, attr=%q): %v", n.ino, attr, err)
		return syscall.EIO
	}
	return 0
}

// newChildInode wraps a meta.Inode in a go-fuse child node with the
// correct StableAttr (so the kernel knows the file type up front).
func (n *Node) newChildInode(ctx context.Context, in meta.Inode) *gofuse.Inode {
	child := &Node{backend: n.backend, ino: in.Ino}
	return n.NewInode(ctx, child, gofuse.StableAttr{
		Ino:  uint64(in.Ino),
		Mode: kindToFuseMode(in.Kind),
	})
}

// fillAttr copies a meta.Inode into a fuse.Attr.
func fillAttr(a *fuse.Attr, in meta.Inode) {
	a.Ino = uint64(in.Ino)
	a.Size = uint64(in.Size)
	// st_blocks is in 512-byte units regardless of Blksize. Without
	// it, `du` reported ~0 for every file. Sparse files honestly
	// report their logical size here too — we don't track per-chunk
	// allocation, and overstating beats understating for a tool whose
	// job is "how much am I storing".
	a.Blocks = (uint64(in.Size) + 511) / 512
	a.Blksize = 4096
	a.Mode = in.Mode
	a.Uid = in.UID
	a.Gid = in.GID
	a.Nlink = uint32(in.Nlink)
	a.Mtime = uint64(in.Mtime)
	a.Atime = uint64(in.Mtime) // we don't track atime
	a.Ctime = uint64(in.Ctime)
}

// kindToFuseMode returns just the file-type bits S_IFREG/S_IFDIR/S_IFLNK.
// go-fuse's StableAttr.Mode is a mask, not the full mode.
func kindToFuseMode(k meta.Kind) uint32 {
	switch k {
	case meta.KindDir:
		return fuse.S_IFDIR
	case meta.KindSymlink:
		return fuse.S_IFLNK
	default:
		return fuse.S_IFREG
	}
}

// callerUID / callerGID extract the calling user's UID/GID from the
// FUSE caller context. Fall back to 0 if unavailable.
func callerUID(ctx context.Context) uint32 {
	if c, ok := fuse.FromContext(ctx); ok {
		return c.Uid
	}
	return 0
}
func callerGID(ctx context.Context) uint32 {
	if c, ok := fuse.FromContext(ctx); ok {
		return c.Gid
	}
	return 0
}

// chunkSize returns the configured chunk size, falling back to the
// package default.
// wire returns the chunk wire codec: Wire on format 2, else format 1
// over Cipher + Codec.
func (b *Backend) wire() chunk.WireCodec {
	if b.Wire != nil {
		return b.Wire
	}
	return chunk.NewFormat1Codec(b.Cipher, b.Codec)
}

func (b *Backend) chunkSize() int64 {
	if b.ChunkSize > 0 {
		return b.ChunkSize
	}
	return chunk.ChunkSize
}

// metaCtx returns a context that inherits values from ctx but never
// gets canceled. Use for meta-DB operations inside FUSE write
// handlers (Create, Mkdir, Setattr, ...) where the kernel-issued
// FUSE ctx has a short deadline and canceling a half-committed tx
// would leave inconsistent state on disk (or surface EIO to
// userspace right after CreateChild already wrote the row to
// SQLite). The meta operations are local, fast (milliseconds), and
// idempotent — letting them complete past a kernel timeout is
// strictly safer than aborting them.
//
// Don't use for genuinely-cancellable ops like Read (TG fetch) where
// cancellation saves real work.
func metaCtx(ctx context.Context) context.Context {
	return context.WithoutCancel(ctx)
}

// mapMetaErr translates meta.* sentinels into POSIX-shaped errnos.
func mapMetaErr(err error) syscall.Errno {
	switch {
	case errors.Is(err, meta.ErrNotFound):
		return syscall.ENOENT
	case errors.Is(err, meta.ErrExists):
		return syscall.EEXIST
	case errors.Is(err, meta.ErrNotEmpty):
		return syscall.ENOTEMPTY
	case errors.Is(err, meta.ErrIsDir):
		return syscall.EISDIR
	case errors.Is(err, meta.ErrNotDir):
		return syscall.ENOTDIR
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// FUSE interrupt: the kernel cancels an op's ctx when the
		// calling process takes ANY signal — including the Go
		// runtime's own SIGURG preemption, so every Go program on
		// the mount trips this constantly. EINTR tells the kernel
		// to restart the syscall transparently; the old EIO surfaced
		// as a spurious "input/output error" in userspace.
		return syscall.EINTR
	}
	return syscall.EIO
}

// logMetaErr maps a meta error to its POSIX errno AND logs any
// non-sentinel cause (SQL errors, ctx cancellation, ...). The
// callers used to do `if err != nil { return syscall.ENOENT }`
// blindly — which the kernel reported as "this thing doesn't exist"
// and Go's os.RemoveAll then treated as "already deleted, success".
// So a transient SQLite busy timeout deep in a tree-wipe used to
// silently produce a "deleted everything" result while leaving
// files in place. Routing through this helper instead means the
// kernel sees EIO on real errors (RemoveAll surfaces them), and
// the daemon log records what actually went wrong.
func logMetaErr(op string, ino int64, err error) syscall.Errno {
	if err == nil {
		return 0
	}
	// Cancellations are FUSE interrupts (see mapMetaErr) — routine,
	// not worth a log line each.
	if !errors.Is(err, meta.ErrNotFound) && !errors.Is(err, meta.ErrExists) &&
		!errors.Is(err, meta.ErrNotEmpty) && !errors.Is(err, meta.ErrIsDir) &&
		!errors.Is(err, meta.ErrNotDir) &&
		!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		log.Printf("fs.%s(ino=%d): %v", op, ino, err)
	}
	return mapMetaErr(err)
}

// isUserXattr reports whether `name` is in Linux's "user." xattr
// namespace, the only one TelFS supports in v1.
func isUserXattr(name string) bool { return strings.HasPrefix(name, "user.") }

// interrupted reports whether a FUSE op failed because its ctx was
// canceled (kernel interrupt or unmount teardown) rather than a real
// fault. Checks the ctx itself too: not every layer wraps the ctx
// sentinel into its returned error.
func interrupted(ctx context.Context, err error) bool {
	return ctx.Err() != nil ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
