package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// CreateInode inserts a new inode row and returns the assigned ino. If
// Mtime/Ctime are zero they default to time.Now().Unix(); if Nlink is zero
// it defaults to 1.
func (s *Store) CreateInode(ctx context.Context, in Inode) (int64, error) {
	return createInode(ctx, s.db, in)
}

// createInodeTx is the *sql.Tx variant used by compound operations.
func createInodeTx(ctx context.Context, tx *sql.Tx, in Inode) (int64, error) {
	return createInode(ctx, tx, in)
}

func createInode(ctx context.Context, e execQuerier, in Inode) (int64, error) {
	now := time.Now().Unix()
	if in.Mtime == 0 {
		in.Mtime = now
	}
	if in.Ctime == 0 {
		in.Ctime = now
	}
	if in.Nlink == 0 {
		in.Nlink = 1
	}
	var st any
	if in.SymlinkTarget != "" {
		st = in.SymlinkTarget
	}
	// If caller didn't specify chunk_size, leave the column default
	// to take effect (4 MiB).
	var chunkSize any
	if in.ChunkSize > 0 {
		chunkSize = in.ChunkSize
	} else {
		chunkSize = nil
	}
	res, err := e.ExecContext(ctx,
		`INSERT INTO inodes(kind, mode, uid, gid, size, nlink, mtime, ctime, symlink_target, chunk_size, sparse)
		 VALUES (?,?,?,?,?,?,?,?,?,COALESCE(?,4194304),?)`,
		string(in.Kind), in.Mode, in.UID, in.GID, in.Size, in.Nlink, in.Mtime, in.Ctime, st, chunkSize, boolToInt(in.Sparse))
	if err != nil {
		return 0, fmt.Errorf("insert inode: %w", err)
	}
	return res.LastInsertId()
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// GetInode returns the inode row for ino, or ErrNotFound.
func (s *Store) GetInode(ctx context.Context, ino int64) (Inode, error) {
	return getInode(ctx, s.db, ino)
}

func getInode(ctx context.Context, e execQuerier, ino int64) (Inode, error) {
	var in Inode
	var kind string
	var target sql.NullString
	var sparse int
	err := e.QueryRowContext(ctx,
		`SELECT ino, kind, mode, uid, gid, size, nlink, mtime, ctime, symlink_target, chunk_size, sparse
		   FROM inodes WHERE ino = ?`, ino).
		Scan(&in.Ino, &kind, &in.Mode, &in.UID, &in.GID, &in.Size, &in.Nlink, &in.Mtime, &in.Ctime, &target, &in.ChunkSize, &sparse)
	if errors.Is(err, sql.ErrNoRows) {
		return Inode{}, ErrNotFound
	}
	if err != nil {
		return Inode{}, fmt.Errorf("get inode %d: %w", ino, err)
	}
	in.Kind = Kind(kind)
	in.SymlinkTarget = target.String
	in.Sparse = sparse != 0
	return in, nil
}

// SetInodeSparse marks ino as containing holes. One-way: nothing ever
// clears the flag (a file that once had holes keeps zero-fill read
// semantics for its missing chunks; that's always safe, it just
// forgoes data-lost detection for that file). Journaled so recovery
// preserves hole semantics for files that went sparse after the last
// snapshot.
func (s *Store) SetInodeSparse(ctx context.Context, ino int64) error {
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE inodes SET sparse = 1 WHERE ino = ?`, ino)
		if err != nil {
			return fmt.Errorf("set sparse %d: %w", ino, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrNotFound
		}
		_, err = s.appendJournalTx(ctx, tx, JournalOp{Op: OpSetSparse, Ino: ino, Sparse: true})
		return err
	})
}

// SetInodeChunkSize updates the per-inode chunk size. Called by the
// writer on first dispatch when it picks a tier larger than the
// default. Refuses if the inode already has chunks (changing would
// invalidate the (ino, idx) → offset arithmetic for existing rows).
func (s *Store) SetInodeChunkSize(ctx context.Context, ino int64, size int64) error {
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		var n int
		if err := tx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM chunk_map WHERE ino = ?`, ino).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return fmt.Errorf("set chunk_size: inode %d already has %d chunk(s)", ino, n)
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE inodes SET chunk_size = ? WHERE ino = ?`, size, ino); err != nil {
			return err
		}
		// Format 2: journal the promotion — a replay that kept the
		// create op's chunk size would lay the file's chunks out wrong.
		if s.journalV2.Load() {
			_, err := s.appendJournalTx(ctx, tx, JournalOp{Op: OpSetChunkSize, Ino: ino, ChunkSize: size})
			return err
		}
		return nil
	})
}

// SetAttrs updates the mutable attributes of an inode. Pass nil for fields
// that should be left untouched. Returns ErrNotFound if the inode is gone.
type SetAttrsPatch struct {
	Mode  *uint32
	UID   *uint32
	GID   *uint32
	Size  *int64
	Mtime *int64
}

func (s *Store) SetAttrs(ctx context.Context, ino int64, p SetAttrsPatch) error {
	return s.setAttrs(ctx, ino, p, true)
}

// setAttrs updates an inode's attributes; journal=true also records a
// `setattr` op on format 2 (SetSize journals its own op instead).
func (s *Store) setAttrs(ctx context.Context, ino int64, p SetAttrsPatch, journal bool) error {
	if journal && s.journalV2.Load() && !s.replaying.Load() {
		return s.WithTx(ctx, func(tx *sql.Tx) error {
			cur, err := getInode(ctx, tx, ino)
			if err != nil {
				return err
			}
			p = dropUnchanged(cur, p)
			if err := setAttrsTx(ctx, tx, ino, p); err != nil {
				return err
			}
			op := JournalOp{Op: OpSetAttr, Ino: ino}
			if p.Mode != nil {
				op.Mode, op.Set = *p.Mode, append(op.Set, "mode")
			}
			if p.UID != nil {
				op.UID, op.Set = *p.UID, append(op.Set, "uid")
			}
			if p.GID != nil {
				op.GID, op.Set = *p.GID, append(op.Set, "gid")
			}
			if p.Mtime != nil {
				op.Mtime, op.Set = *p.Mtime, append(op.Set, "mtime")
			}
			if p.Size != nil {
				op.Size, op.Set = *p.Size, append(op.Set, "size")
			}
			if len(op.Set) == 0 {
				return nil // nothing changed: nothing to replay
			}
			_, err = s.appendJournalTx(ctx, tx, op)
			return err
		})
	}
	return s.setAttrsExec(ctx, ino, p)
}

// dropUnchanged removes the fields of p that already hold their value
// (the mode compared on its permission bits: go-fuse passes only those,
// while create stores the file type too). rsync's fchmod of a temp file
// to the mode it was created with and the kernel's repeats of the
// current mtime made 3 of rsync's 8 journal ops per file no-ops (P1
// scale pilot). The UPDATE still bumps ctime, which is not journaled,
// so the live row and a replayed one stay identical.
func dropUnchanged(cur Inode, p SetAttrsPatch) SetAttrsPatch {
	if p.Mode != nil && *p.Mode&0o7777 == cur.Mode&0o7777 {
		p.Mode = nil
	}
	if p.UID != nil && *p.UID == cur.UID {
		p.UID = nil
	}
	if p.GID != nil && *p.GID == cur.GID {
		p.GID = nil
	}
	if p.Mtime != nil && *p.Mtime == cur.Mtime {
		p.Mtime = nil
	}
	if p.Size != nil && *p.Size == cur.Size {
		p.Size = nil
	}
	return p
}

// setAttrsTx is the UPDATE inside a transaction.
func setAttrsTx(ctx context.Context, tx *sql.Tx, ino int64, p SetAttrsPatch) error {
	cols, args := attrCols(p)
	args = append(args, ino)
	res, err := tx.ExecContext(ctx, `UPDATE inodes SET `+joinComma(cols)+` WHERE ino = ?`, args...)
	if err != nil {
		return fmt.Errorf("update inode %d: %w", ino, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

func attrCols(p SetAttrsPatch) ([]string, []any) {
	cols := []string{"ctime = ?"}
	args := []any{time.Now().Unix()}
	if p.Mode != nil {
		cols = append(cols, "mode = ?")
		args = append(args, *p.Mode)
	}
	if p.UID != nil {
		cols = append(cols, "uid = ?")
		args = append(args, *p.UID)
	}
	if p.GID != nil {
		cols = append(cols, "gid = ?")
		args = append(args, *p.GID)
	}
	if p.Size != nil {
		cols = append(cols, "size = ?")
		args = append(args, *p.Size)
	}
	if p.Mtime != nil {
		cols = append(cols, "mtime = ?")
		args = append(args, *p.Mtime)
	}
	return cols, args
}

func (s *Store) setAttrsExec(ctx context.Context, ino int64, p SetAttrsPatch) error {
	now := time.Now().Unix()
	// Build SET clause dynamically. ctime always bumps on any attr change.
	cols := []string{"ctime = ?"}
	args := []any{now}
	if p.Mode != nil {
		cols = append(cols, "mode = ?")
		args = append(args, *p.Mode)
	}
	if p.UID != nil {
		cols = append(cols, "uid = ?")
		args = append(args, *p.UID)
	}
	if p.GID != nil {
		cols = append(cols, "gid = ?")
		args = append(args, *p.GID)
	}
	if p.Size != nil {
		cols = append(cols, "size = ?")
		args = append(args, *p.Size)
	}
	if p.Mtime != nil {
		cols = append(cols, "mtime = ?")
		args = append(args, *p.Mtime)
	}
	args = append(args, ino)
	res, err := s.execWithRetry(ctx,
		`UPDATE inodes SET `+joinComma(cols)+` WHERE ino = ?`, args...)
	if err != nil {
		return fmt.Errorf("update inode %d: %w", ino, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// SetSize is a convenience for the common "after truncate or full write"
// case where size + mtime advance together. Also journals the size
// change for channel-side recovery (the file length is otherwise
// inferable only from chunk_map, and chunks aren't journaled — the
// channel messages themselves are the source of truth for chunks).
//
// The UPDATE and its journal row commit in ONE transaction, so the
// journal's order is the order the changes were applied. (They used to
// be two transactions: a `touch` landing in between was journaled
// BEFORE the size change it followed, and a replay then overwrote the
// touch's mtime — found in the P1 live test.)
func (s *Store) SetSize(ctx context.Context, ino int64, size int64) error {
	return s.setSizeAt(ctx, ino, size, time.Now().Unix())
}

// setSizeAt is SetSize with an explicit mtime (replay passes the
// recorded one).
func (s *Store) setSizeAt(ctx context.Context, ino int64, size, mtime int64) error {
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		if err := setAttrsTx(ctx, tx, ino, SetAttrsPatch{Size: &size, Mtime: &mtime}); err != nil {
			return err
		}
		_, err := s.appendJournalTx(ctx, tx, JournalOp{
			Op: OpSetSize, Ino: ino, Size: size, Mtime: mtime,
		})
		return err
	})
}

func joinComma(cols []string) string {
	out := ""
	for i, c := range cols {
		if i > 0 {
			out += ", "
		}
		out += c
	}
	return out
}

// execQuerier is the intersection of *sql.DB and *sql.Tx — lets internal
// helpers operate on either.
type execQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}
