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
	res, err := e.ExecContext(ctx,
		`INSERT INTO inodes(kind, mode, uid, gid, size, nlink, mtime, ctime, symlink_target)
		 VALUES (?,?,?,?,?,?,?,?,?)`,
		string(in.Kind), in.Mode, in.UID, in.GID, in.Size, in.Nlink, in.Mtime, in.Ctime, st)
	if err != nil {
		return 0, fmt.Errorf("insert inode: %w", err)
	}
	return res.LastInsertId()
}

// GetInode returns the inode row for ino, or ErrNotFound.
func (s *Store) GetInode(ctx context.Context, ino int64) (Inode, error) {
	return getInode(ctx, s.db, ino)
}

func getInode(ctx context.Context, e execQuerier, ino int64) (Inode, error) {
	var in Inode
	var kind string
	var target sql.NullString
	err := e.QueryRowContext(ctx,
		`SELECT ino, kind, mode, uid, gid, size, nlink, mtime, ctime, symlink_target
		   FROM inodes WHERE ino = ?`, ino).
		Scan(&in.Ino, &kind, &in.Mode, &in.UID, &in.GID, &in.Size, &in.Nlink, &in.Mtime, &in.Ctime, &target)
	if errors.Is(err, sql.ErrNoRows) {
		return Inode{}, ErrNotFound
	}
	if err != nil {
		return Inode{}, fmt.Errorf("get inode %d: %w", ino, err)
	}
	in.Kind = Kind(kind)
	in.SymlinkTarget = target.String
	return in, nil
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
	res, err := s.db.ExecContext(ctx,
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
// case where size + mtime advance together.
func (s *Store) SetSize(ctx context.Context, ino int64, size int64) error {
	now := time.Now().Unix()
	return s.SetAttrs(ctx, ino, SetAttrsPatch{Size: &size, Mtime: &now})
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
