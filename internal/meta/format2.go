package meta

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Format-2 meta_kv keys (spec P1 §5). The values are opaque bytes to
// this package; internal/wire and cmd/telfs give them meaning.
const (
	// KVFormat is "2" on a format-2 filesystem; missing means format 1.
	KVFormat = "format"
	// KVKeyHeader is the exact key header JSON (the key message's and
	// the export's content); status/fsck compare the pinned message
	// against it byte for byte.
	KVKeyHeader = "key_header"
	// KVKeyMsgID is the channel message id of the current key message.
	KVKeyMsgID = "key_msg_id"
	// KVKeyMsgOld lists (comma-separated) old key message ids still to
	// delete after an interrupted `secret change`.
	KVKeyMsgOld = "key_msg_old"
	// KVKeyCheck is the daemon's last pinned-header check (JSON).
	KVKeyCheck = "key_check"
	// KVUses holds the per-key encryption counts (JSON).
	KVUses = "crypto2_uses"
	// KVCreated / KVSecretSet are RFC 3339 times for the export.
	KVCreated   = "fs_created"
	KVSecretSet = "secret_set"
)

// Format returns 2 for a format-2 filesystem, else 1.
func (s *Store) Format(ctx context.Context) (int, error) {
	v, err := s.GetKV(ctx, KVFormat)
	if errors.Is(err, ErrNotFound) {
		return 1, nil
	}
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(v)))
	if err != nil {
		return 0, fmt.Errorf("meta: bad %s value %q", KVFormat, v)
	}
	return n, nil
}

// SetJournalRefs turns on `ref` journal ops for dedup hits (format 2).
func (s *Store) SetJournalRefs(on bool) { s.journalRefs.Store(on) }

// PathOf returns one path of ino from the root (the name hint of chunk
// captions, P1 §4.3): the first dirent by (parent, name) at each level,
// so hard links resolve deterministically. ("", false) when the inode
// has no name (unlinked while open) or the walk hits a loop.
func (s *Store) PathOf(ctx context.Context, ino int64) (string, bool) {
	return s.PathOfCached(ctx, ino, nil)
}

// PathOfCached is PathOf with a memo of directory paths (dirs may be
// nil): a pack of thousands of files from a few folders resolves each
// folder once.
func (s *Store) PathOfCached(ctx context.Context, ino int64, dirs map[int64]string) (string, bool) {
	if ino == RootIno {
		return "/", true
	}
	type step struct {
		parent int64
		name   string
	}
	var steps []step
	cur := ino
	base := ""
	for depth := 0; ; depth++ {
		if depth >= 4096 {
			return "", false
		}
		var parent int64
		var name string
		err := s.db.QueryRowContext(ctx,
			`SELECT parent_ino, name FROM dirents WHERE child_ino = ? ORDER BY parent_ino, name LIMIT 1`,
			cur).Scan(&parent, &name)
		if err != nil {
			return "", false
		}
		steps = append(steps, step{parent, name})
		if parent == RootIno {
			base = ""
			break
		}
		if p, ok := dirs[parent]; ok {
			base = p
			break
		}
		cur = parent
	}
	// steps[len-1] hangs off base; build outward, memoizing folders.
	path := base
	for i := len(steps) - 1; i >= 0; i-- {
		path = path + "/" + steps[i].name
		if i > 0 && dirs != nil {
			// steps[i-1]'s parent is the folder whose path we just built.
			dirs[steps[i-1].parent] = path
		}
	}
	return path, true
}
