package fs

import (
	"context"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"telfs/internal/chunk"
	"telfs/internal/meta"
)

// Backend bundles the dependencies every Node needs. It's shared by
// reference among all nodes in the mounted tree.
type Backend struct {
	Meta   *meta.Store
	Reader *chunk.Reader
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
	_ gofuse.NodeGetattrer  = (*Node)(nil)
	_ gofuse.NodeLookuper   = (*Node)(nil)
	_ gofuse.NodeReaddirer  = (*Node)(nil)
	_ gofuse.NodeOpener     = (*Node)(nil)
	_ gofuse.NodeReader     = (*Node)(nil)
	_ gofuse.NodeReadlinker = (*Node)(nil)
)

// NewRoot returns the root Node for a TelFS mount.
func NewRoot(b *Backend) *Node {
	return &Node{backend: b, ino: meta.RootIno}
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

// Lookup resolves (n, name) to a child Node, creating the go-fuse Inode
// child if it doesn't exist yet.
func (n *Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	in, err := n.backend.Meta.Lookup(ctx, n.ino, name)
	if err != nil {
		return nil, syscall.ENOENT
	}
	child := &Node{backend: n.backend, ino: in.Ino}
	stable := gofuse.StableAttr{
		Ino:  uint64(in.Ino),
		Mode: kindToFuseMode(in.Kind),
	}
	childInode := n.NewInode(ctx, child, stable)
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

// Open accepts read-only opens; any write flag fails with EROFS until M4.
func (n *Node) Open(_ context.Context, flags uint32) (gofuse.FileHandle, uint32, syscall.Errno) {
	if flags&uint32(syscall.O_WRONLY) != 0 || flags&uint32(syscall.O_RDWR) != 0 {
		return nil, 0, syscall.EROFS
	}
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

// Read fulfills a read by delegating to chunk.Reader, which pulls the
// covering chunk(s) through the LRU cache (and downloads on miss).
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
