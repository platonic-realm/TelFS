package meta

import (
	"context"
	"encoding/json"
	"fmt"
)

// ReplayOp applies a single JournalOp to the store. Used by
// channel-side recovery to bring a restored snapshot forward through
// post-snapshot mutations.
//
// Each op is applied as a best-effort idempotent operation:
//   - create: rejects ErrExists (the snapshot already had it)
//   - link: rejects ErrExists
//   - unlink: rejects ErrNotFound (already gone)
//   - rename: tolerates either ErrNotFound (source already moved) OR
//     a successful re-rename to the same target — idempotent
//   - setsize: always applied if the inode exists
//
// On any non-tolerable error, returns it so the caller can log + stop
// the replay.
func (s *Store) ReplayOp(ctx context.Context, op JournalOp) error {
	switch op.Op {
	case OpCreateChild:
		child := Inode{
			Kind:          op.Kind,
			Mode:          op.Mode,
			UID:           op.UID,
			GID:           op.GID,
			Nlink:         1,
			Mtime:         op.Mtime,
			Ctime:         op.Mtime,
			SymlinkTarget: op.SymlinkTarget,
		}
		_, err := s.CreateChild(ctx, op.Parent, op.Name, child)
		if err == ErrExists {
			return nil // already created — idempotent
		}
		return err
	case OpLink:
		err := s.Link(ctx, op.Parent, op.Name, op.Target)
		if err == ErrExists {
			return nil
		}
		return err
	case OpUnlink:
		err := s.Unlink(ctx, op.Parent, op.Name)
		if err == ErrNotFound {
			return nil // already gone — idempotent
		}
		return err
	case OpRename:
		err := s.Rename(ctx, op.Parent, op.Name, op.NewParent, op.NewName)
		if err == ErrNotFound {
			// Source may already be moved; double-check the destination
			// exists already.
			if _, derr := s.Lookup(ctx, op.NewParent, op.NewName); derr == nil {
				return nil
			}
		}
		return err
	case OpSetSize:
		return s.SetSize(ctx, op.Ino, op.Size)
	default:
		return fmt.Errorf("unknown journal op kind %q", op.Op)
	}
}

// DecodeOp parses a journal op JSON payload into a JournalOp.
func DecodeOp(payload []byte) (JournalOp, error) {
	var op JournalOp
	if err := json.Unmarshal(payload, &op); err != nil {
		return JournalOp{}, fmt.Errorf("decode journal op: %w", err)
	}
	return op, nil
}
