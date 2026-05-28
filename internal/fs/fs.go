package fs

import (
	"context"
	"errors"
	"strings"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"telfs/internal/chunk"
	"telfs/internal/crypto"
	"telfs/internal/meta"
)

// Backend bundles the dependencies every Node needs. It's shared by
// reference among all nodes in the mounted tree. The Cache + Uploader
// + ChunkSize fields are only needed for the write path; read-only
// mounts can leave them zero (or set ReadOnly = true to explicitly
// reject all mutations regardless).
type Backend struct {
	Meta      *meta.Store
	Reader    *chunk.Reader
	Cache     *chunk.Cache
	Uploader  chunk.Uploader
	Cipher    crypto.Cipher // nil → NoopCipher (plaintext mode)
	ChunkSize int64
	ReadOnly  bool
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
	_ gofuse.NodeGetattrer     = (*Node)(nil)
	_ gofuse.NodeSetattrer     = (*Node)(nil)
	_ gofuse.NodeLookuper      = (*Node)(nil)
	_ gofuse.NodeReaddirer     = (*Node)(nil)
	_ gofuse.NodeOpener        = (*Node)(nil)
	_ gofuse.NodeReader        = (*Node)(nil)
	_ gofuse.NodeReadlinker    = (*Node)(nil)
	_ gofuse.NodeCreater       = (*Node)(nil)
	_ gofuse.NodeMkdirer       = (*Node)(nil)
	_ gofuse.NodeSymlinker     = (*Node)(nil)
	_ gofuse.NodeLinker        = (*Node)(nil)
	_ gofuse.NodeUnlinker      = (*Node)(nil)
	_ gofuse.NodeRmdirer       = (*Node)(nil)
	_ gofuse.NodeRenamer       = (*Node)(nil)
	_ gofuse.NodeGetxattrer    = (*Node)(nil)
	_ gofuse.NodeSetxattrer    = (*Node)(nil)
	_ gofuse.NodeRemovexattrer = (*Node)(nil)
	_ gofuse.NodeListxattrer   = (*Node)(nil)
	_ gofuse.NodeStatfser      = (*Node)(nil)
)

// NewRoot returns the root Node for a TelFS mount.
func NewRoot(b *Backend) *Node {
	return &Node{backend: b, ino: meta.RootIno}
}

// Statfs answers `statfs(2)` / `pathconf(_PC_NAME_MAX)` / df / etc.
// Without this, go-fuse returns a zeroed StatfsOut and tools that
// consult NameLen think every filename is too long ("name too long"
// errors from mkdir/touch in some file managers and shells).
//
// We don't know the channel's remaining capacity, so block counts
// are large-but-fictional: report ~1 PiB free out of 1 PiB total.
// Inode counts mirror those — Telegram doesn't impose a per-channel
// hard cap that maps cleanly to statfs.
func (n *Node) Statfs(_ context.Context, out *fuse.StatfsOut) syscall.Errno {
	const (
		bsize     uint32 = 4 << 20 // 4 MiB — matches our chunk size
		oneEB     uint64 = 1 << 50 // 1 PiB worth of blocks at 4 MiB each = a lot
		fakeFiles uint64 = 1 << 32 // 4 G inodes — plenty
	)
	out.Blocks = oneEB
	out.Bfree = oneEB
	out.Bavail = oneEB
	out.Files = fakeFiles
	out.Ffree = fakeFiles
	out.Bsize = bsize
	out.Frsize = bsize
	out.NameLen = 255
	return 0
}

// Getattr fills *fuse.AttrOut from the meta store.
func (n *Node) Getattr(ctx context.Context, _ gofuse.FileHandle, out *fuse.AttrOut) syscall.Errno {
	in, err := n.backend.Meta.GetInode(ctx, n.ino)
	if err != nil {
		return syscall.ENOENT
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
	if patch.Mode != nil || patch.UID != nil || patch.GID != nil || patch.Mtime != nil {
		if err := n.backend.Meta.SetAttrs(ctx, n.ino, patch); err != nil {
			if errors.Is(err, meta.ErrNotFound) {
				return syscall.ENOENT
			}
			return syscall.EIO
		}
	}
	if size, ok := in.GetSize(); ok {
		// If we have an open handle, use its Writer (it knows about
		// dirty chunks). Otherwise mint a transient Writer for the
		// truncate.
		if h, ok := fh.(*fileHandle); ok && h != nil {
			if err := h.writer.Truncate(ctx, int64(size)); err != nil {
				return syscall.EIO
			}
		} else if errno := n.truncate(ctx, int64(size)); errno != 0 {
			return errno
		}
	}
	final, err := n.backend.Meta.GetInode(ctx, n.ino)
	if err != nil {
		return syscall.EIO
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
	w, err := chunk.NewWriter(ctx, n.backend.Meta, n.backend.Cache, n.backend.Uploader, n.backend.Cipher, n.ino, n.backend.chunkSize(), 0)
	if err != nil {
		return syscall.EIO
	}
	defer w.Close()
	if err := w.Truncate(ctx, size); err != nil {
		return syscall.EIO
	}
	return 0
}

// Lookup resolves (n, name) to a child Node, creating the go-fuse Inode
// child if it doesn't exist yet.
func (n *Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	in, err := n.backend.Meta.Lookup(ctx, n.ino, name)
	if err != nil {
		return nil, syscall.ENOENT
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
		return nil, syscall.EIO
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

// Open allocates a fileHandle backed by a chunk.Writer for the file's
// lifetime. Handles O_TRUNC by truncating to 0 before returning the
// handle so subsequent writes start from a clean slate.
func (n *Node) Open(ctx context.Context, flags uint32) (gofuse.FileHandle, uint32, syscall.Errno) {
	if !n.backend.canWrite() {
		// Read-only mount: reject any open with write flags.
		if flags&uint32(syscall.O_WRONLY) != 0 || flags&uint32(syscall.O_RDWR) != 0 {
			return nil, 0, syscall.EROFS
		}
		return nil, fuse.FOPEN_KEEP_CACHE, 0
	}
	w, err := chunk.NewWriter(ctx, n.backend.Meta, n.backend.Cache, n.backend.Uploader, n.backend.Cipher, n.ino, n.backend.chunkSize(), 0)
	if err != nil {
		return nil, 0, syscall.EIO
	}
	if flags&uint32(syscall.O_TRUNC) != 0 {
		if err := w.Truncate(ctx, 0); err != nil {
			w.Close()
			return nil, 0, syscall.EIO
		}
	}
	h := &fileHandle{node: n, writer: w}
	// FOPEN_KEEP_CACHE keeps the kernel's page cache valid across opens,
	// which is correct because we invalidate the read cache on writes.
	return h, fuse.FOPEN_KEEP_CACHE, 0
}

// Read fulfills a read on a Node without an open handle (some kernels
// dispatch this for short paths). Delegates straight to chunk.Reader.
func (n *Node) Read(ctx context.Context, _ gofuse.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	nread, err := n.backend.Reader.ReadAt(ctx, n.ino, dest, off)
	if err != nil {
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
	ino, err := n.backend.Meta.CreateChild(ctx, n.ino, name, meta.Inode{
		Kind: meta.KindFile,
		Mode: mode | uint32(syscall.S_IFREG),
		UID:  callerUID(ctx),
		GID:  callerGID(ctx),
	})
	if err != nil {
		return nil, nil, 0, mapMetaErr(err)
	}
	child, err := n.backend.Meta.GetInode(ctx, ino)
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}
	childInode := n.newChildInode(ctx, child)
	w, werr := chunk.NewWriter(ctx, n.backend.Meta, n.backend.Cache, n.backend.Uploader, n.backend.Cipher, ino, n.backend.chunkSize(), 0)
	if werr != nil {
		return nil, nil, 0, syscall.EIO
	}
	h := &fileHandle{node: childInode.Operations().(*Node), writer: w}
	fillAttr(&out.Attr, child)
	return childInode, h, fuse.FOPEN_KEEP_CACHE, 0
}

// Mkdir creates a directory child.
func (n *Node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	if errno := n.requireWrite(); errno != 0 {
		return nil, errno
	}
	ino, err := n.backend.Meta.CreateChild(ctx, n.ino, name, meta.Inode{
		Kind: meta.KindDir,
		Mode: mode | uint32(syscall.S_IFDIR),
		UID:  callerUID(ctx),
		GID:  callerGID(ctx),
	})
	if err != nil {
		return nil, mapMetaErr(err)
	}
	child, err := n.backend.Meta.GetInode(ctx, ino)
	if err != nil {
		return nil, syscall.EIO
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
	ino, err := n.backend.Meta.CreateChild(ctx, n.ino, name, meta.Inode{
		Kind:          meta.KindSymlink,
		Mode:          0o777 | uint32(syscall.S_IFLNK),
		SymlinkTarget: target,
		UID:           callerUID(ctx),
		GID:           callerGID(ctx),
	})
	if err != nil {
		return nil, mapMetaErr(err)
	}
	child, err := n.backend.Meta.GetInode(ctx, ino)
	if err != nil {
		return nil, syscall.EIO
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
	if err := n.backend.Meta.Link(ctx, n.ino, name, tn.ino); err != nil {
		if errors.Is(err, meta.ErrIsDir) {
			return nil, syscall.EPERM
		}
		return nil, mapMetaErr(err)
	}
	in, err := n.backend.Meta.GetInode(ctx, tn.ino)
	if err != nil {
		return nil, syscall.EIO
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
		return syscall.ENOENT
	}
	if in.Kind == meta.KindDir {
		return syscall.EISDIR
	}
	if err := n.backend.Meta.Unlink(ctx, n.ino, name); err != nil {
		return mapMetaErr(err)
	}
	return 0
}

// Rmdir removes an empty directory child.
func (n *Node) Rmdir(ctx context.Context, name string) syscall.Errno {
	if errno := n.requireWrite(); errno != 0 {
		return errno
	}
	in, err := n.backend.Meta.Lookup(ctx, n.ino, name)
	if err != nil {
		return syscall.ENOENT
	}
	if in.Kind != meta.KindDir {
		return syscall.ENOTDIR
	}
	if err := n.backend.Meta.Unlink(ctx, n.ino, name); err != nil {
		return mapMetaErr(err)
	}
	return 0
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
	if err := n.backend.Meta.Rename(ctx, n.ino, oldName, np.ino, newName); err != nil {
		return mapMetaErr(err)
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
	if err := n.backend.Meta.SetXattr(ctx, n.ino, attr, data); err != nil {
		return mapMetaErr(err)
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
		return 0, syscall.EIO
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
		return 0, syscall.EIO
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
	if err := n.backend.Meta.RemoveXattr(ctx, n.ino, attr); err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			return syscall.ENODATA
		}
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
func (b *Backend) chunkSize() int64 {
	if b.ChunkSize > 0 {
		return b.ChunkSize
	}
	return chunk.ChunkSize
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
	}
	return syscall.EIO
}

// isUserXattr reports whether `name` is in Linux's "user." xattr
// namespace, the only one TelFS supports in v1.
func isUserXattr(name string) bool { return strings.HasPrefix(name, "user.") }
