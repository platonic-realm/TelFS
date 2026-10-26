package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// ReplayOp applies a single JournalOp to the store. Used by
// channel-side recovery to bring a restored snapshot forward through
// post-snapshot mutations.
//
// Two properties are load-bearing:
//
//   - Creates are applied at the JOURNALED ino (createChildAt), never
//     auto-assigned. Auto-assignment made determinism depend on every
//     single create replaying successfully — one skipped create
//     shifted every later ino and silently attached sizes/hardlinks
//     to the wrong files.
//
//   - Journaling is suppressed for the whole call (s.replaying): the
//     ops being applied already live on the channel; re-journaling
//     would re-post them all as fresh journal messages.
//
// Each op is applied as a best-effort idempotent operation:
//   - create: tolerates ErrExists (the snapshot already had it)
//   - setsparse: tolerates ErrNotFound (inode unlinked later in the
//     journal — the create that would recreate it was also unlinked)
//   - link: tolerates ErrExists
//   - unlink: tolerates ErrNotFound (already gone)
//   - rename: tolerates ErrNotFound when the destination already
//     exists (re-rename)
//   - setsize: tolerates ErrNotFound (inode unlinked later)
//
// On any non-tolerable error, returns it so the caller can stop the
// replay (continuing past a failed op would corrupt later state).
func (s *Store) ReplayOp(ctx context.Context, op JournalOp) error {
	s.replaying.Store(true)
	defer s.replaying.Store(false)
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
			ChunkSize:     op.ChunkSize, // 0 (legacy op) → default applied downstream
			Sparse:        op.Sparse,
		}
		var err error
		if op.Ino > 0 {
			err = s.createChildAt(ctx, op.Parent, op.Name, child, op.Ino)
		} else {
			// Legacy journal message without an ino — best effort.
			_, err = s.CreateChild(ctx, op.Parent, op.Name, child)
		}
		if errors.Is(err, ErrExists) {
			return nil // already created — idempotent
		}
		return err
	case OpSetSparse:
		err := s.SetInodeSparse(ctx, op.Ino)
		if errors.Is(err, ErrNotFound) {
			return nil // inode unlinked later in the journal
		}
		return err
	case OpLink:
		err := s.Link(ctx, op.Parent, op.Name, op.Target)
		if errors.Is(err, ErrExists) {
			return nil
		}
		return err
	case OpUnlink:
		err := s.Unlink(ctx, op.Parent, op.Name)
		if errors.Is(err, ErrNotFound) {
			return nil // already gone — idempotent
		}
		return err
	case OpRename:
		err := s.Rename(ctx, op.Parent, op.Name, op.NewParent, op.NewName)
		if errors.Is(err, ErrNotFound) {
			// Source may already be moved; double-check the destination
			// exists already.
			if _, derr := s.Lookup(ctx, op.NewParent, op.NewName); derr == nil {
				return nil
			}
		}
		return err
	case OpSetSize:
		err := s.SetSize(ctx, op.Ino, op.Size)
		if errors.Is(err, ErrNotFound) {
			return nil // inode unlinked later in the journal
		}
		return err
	case OpRef:
		err := s.PutChunk(ctx, Chunk{Ino: op.Ino, Idx: op.Idx, TGMessageID: op.MsgID,
			Size: op.ChunkLen, PackOffset: op.PackOffset, PackSize: op.PackSize, Hash: op.Hash})
		if IsFKViolation(err) {
			return nil // inode unlinked later in the journal
		}
		return err
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
