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
	// ChunkSize is the per-inode chunk size, in bytes. Set at
	// inode creation to the FS default; the writer may promote it
	// to a higher tier (16/32/64 MiB) at first dispatch when it
	// sees that the in-flight write is large enough to warrant
	// fewer, bigger chunks. Always non-zero.
	ChunkSize int64
	// Sparse marks a file that legitimately contains holes — chunk
	// slots inside [0, size) with no chunk_map row. Set by the writer
	// when a write skips past unwritten space and by truncate-grow.
	// The reader returns zeros for missing chunks of a sparse file;
	// for non-sparse files a missing chunk keeps meaning "data lost"
	// (EIO), preserving corruption detection. Repair likewise leaves
	// sparse files' gaps alone.
	Sparse bool
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
	// PackOffset and PackSize are non-zero when this chunk is an
	// EXTENT into a shared "packed blob" message — multiple small
	// files share one TG message to amortize the per-message round-
	// trip cost. For an unpacked chunk (the common case for files
	// >= chunk.PackThreshold), both are zero and the reader treats
	// the entire TG message as this chunk's wire bytes.
	//
	// Stored as SQLite NULL when zero (`pack_offset IS NULL` in
	// chunk_map); existing pre-packer rows decode as zero/zero
	// without migration, so older filesystems keep working.
	PackOffset int64
	PackSize   int32
	// Hash is the chunk's keyed content hash on format-2 filesystems
	// (HMAC-SHA256 under the FS hash key): the AAD its wire is sealed
	// under, rechecked after decryption, and its dedup identity. Nil
	// on format 1 (column NULL).
	Hash []byte
}

// IsPacked reports whether this chunk is an extent into a shared
// packed blob (vs. owning its TG message exclusively).
func (c Chunk) IsPacked() bool { return c.PackSize > 0 }
