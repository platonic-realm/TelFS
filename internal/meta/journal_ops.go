package meta

// JournalOp is the on-the-wire JSON shape of a metadata mutation
// recorded in the local `journal` table and posted to the channel as
// a journal-op message. Recovery deserializes these and replays them
// against a freshly-restored meta store to close the up-to-5-min
// crash window between snapshots.
//
// One JournalOp covers one Store mutation. Op identifies which kind
// (so the replay dispatcher knows how to apply it); the remaining
// fields are a superset that each op kind populates as needed. We
// keep them all in a flat struct rather than a discriminated union
// to make the JSON simple to hand-inspect and to make adding new op
// kinds backward-compatible (older clients ignore fields they don't
// recognize).
//
// Currently journaled ops:
//   - create: CreateChild — creates an inode + dirent in one tx
//   - link:   Link        — adds a hardlink dirent, bumps nlink
//   - unlink: Unlink      — removes a dirent (and the inode if
//                            nlink hits 0; chunks/xattrs cascade)
//   - rename: Rename      — moves a dirent
//   - setsize: SetSize    — explicit truncate (size change)
//
// Not journaled (yet): xattr ops, generic SetAttrs, chunk_map updates.
// Chunks are independently recoverable from the channel itself; xattrs
// and posix attrs degrade gracefully when missing on replay.
type JournalOp struct {
	Op string `json:"op"`

	// Common dirent fields
	Parent    int64  `json:"parent,omitempty"`
	Name      string `json:"name,omitempty"`
	NewParent int64  `json:"new_parent,omitempty"`
	NewName   string `json:"new_name,omitempty"`

	// Inode fields
	Ino           int64  `json:"ino,omitempty"`
	Target        int64  `json:"target,omitempty"`
	Kind          Kind   `json:"kind,omitempty"`
	Mode          uint32 `json:"mode,omitempty"`
	UID           uint32 `json:"uid,omitempty"`
	GID           uint32 `json:"gid,omitempty"`
	Size          int64  `json:"size,omitempty"`
	Mtime         int64  `json:"mtime,omitempty"`
	SymlinkTarget string `json:"symlink_target,omitempty"`
}

// Journal op type tags. Kept short to minimize per-op payload size on
// the channel.
const (
	OpCreateChild = "create"
	OpLink        = "link"
	OpUnlink      = "unlink"
	OpRename      = "rename"
	OpSetSize     = "setsize"
)
