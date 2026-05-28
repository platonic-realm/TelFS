package meta

// Kind discriminates the three inode kinds TelFS supports in v1.
type Kind string

const (
	KindFile    Kind = "file"
	KindDir     Kind = "dir"
	KindSymlink Kind = "symlink"
)

// Inode is the in-memory representation of a row in the `inodes` table.
//
// All times are unix seconds. Nlink is the POSIX hardlink refcount for
// regular files. For directories TelFS does NOT maintain the POSIX
// `2 + nsubdirs` convention — directory nlink is unused. The plan defers
// any tooling that depends on it.
type Inode struct {
	Ino           int64
	Kind          Kind
	Mode          uint32
	UID, GID      uint32
	Size          int64
	Nlink         uint32
	Mtime, Ctime  int64
	SymlinkTarget string // empty unless Kind == KindSymlink
}

// Dirent is a row in the `dirents` table — a (parent, name) → child link.
// A single inode may be referenced by multiple dirents (hardlinks).
type Dirent struct {
	ParentIno int64
	Name      string
	ChildIno  int64
}

// Chunk is a row in the `chunk_map` table — a file-chunk slot pointing at
// a Telegram message that holds the chunk payload.
//
// TODO(M5): if/when we let xattrs exceed Telegram's text-message size
// limit, large xattrs may be promoted to documents and tracked here too
// (or in a parallel xattr_blobs table). For now xattr values stay inline
// in the `xattrs` table.
type Chunk struct {
	Ino         int64
	Idx         int32
	TGMessageID int64
	Size        int32
}
