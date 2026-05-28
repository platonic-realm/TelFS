package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ErrExists is returned when a dirent already exists at the target name.
var ErrExists = errors.New("meta: name already exists")

// ErrNotEmpty is returned by Unlink/Rename when attempting to remove or
// overwrite a non-empty directory.
var ErrNotEmpty = errors.New("meta: directory not empty")

// ErrIsDir / ErrNotDir signal kind mismatches on rename overwrites. The
// FUSE layer maps these to EISDIR / ENOTDIR.
var (
	ErrIsDir  = errors.New("meta: target is a directory")
	ErrNotDir = errors.New("meta: target is not a directory")
)

// Lookup resolves (parent, name) to the child inode. Returns ErrNotFound
// if no such dirent exists.
func (s *Store) Lookup(ctx context.Context, parent int64, name string) (Inode, error) {
	return lookup(ctx, s.db, parent, name)
}

func lookup(ctx context.Context, e execQuerier, parent int64, name string) (Inode, error) {
	// Single JOIN: dirent + inode in one round trip. This is FUSE's hot
	// path (one Lookup per path component on every syscall).
	var in Inode
	var kind string
	var target sql.NullString
	err := e.QueryRowContext(ctx,
		`SELECT i.ino, i.kind, i.mode, i.uid, i.gid, i.size, i.nlink, i.mtime, i.ctime, i.symlink_target
		   FROM dirents d JOIN inodes i ON i.ino = d.child_ino
		  WHERE d.parent_ino = ? AND d.name = ?`,
		parent, name,
	).Scan(&in.Ino, &kind, &in.Mode, &in.UID, &in.GID, &in.Size, &in.Nlink, &in.Mtime, &in.Ctime, &target)
	if errors.Is(err, sql.ErrNoRows) {
		return Inode{}, ErrNotFound
	}
	if err != nil {
		return Inode{}, fmt.Errorf("lookup %d/%q: %w", parent, name, err)
	}
	in.Kind = Kind(kind)
	in.SymlinkTarget = target.String
	return in, nil
}

// Readdir returns the entries of a directory in arbitrary order. The caller
// is responsible for any sorting/pagination required by the FUSE layer.
func (s *Store) Readdir(ctx context.Context, parent int64) ([]Dirent, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT parent_ino, name, child_ino FROM dirents WHERE parent_ino = ?`, parent)
	if err != nil {
		return nil, fmt.Errorf("readdir %d: %w", parent, err)
	}
	defer rows.Close()
	var out []Dirent
	for rows.Next() {
		var d Dirent
		if err := rows.Scan(&d.ParentIno, &d.Name, &d.ChildIno); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// DirentInfo carries everything FUSE needs to fill out a fuse.DirEntry
// (plus a getattr cache hint) in one JOIN, avoiding a per-child round
// trip during `ls -l`.
type DirentInfo struct {
	Name string
	Ino  int64
	Kind Kind
	Mode uint32
	Size int64
}

// ReaddirInfo returns a directory's entries joined with their inode
// attributes in a single query.
func (s *Store) ReaddirInfo(ctx context.Context, parent int64) ([]DirentInfo, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT d.name, i.ino, i.kind, i.mode, i.size
		   FROM dirents d JOIN inodes i ON i.ino = d.child_ino
		  WHERE d.parent_ino = ?
		  ORDER BY d.name`,
		parent,
	)
	if err != nil {
		return nil, fmt.Errorf("readdirinfo %d: %w", parent, err)
	}
	defer rows.Close()
	var out []DirentInfo
	for rows.Next() {
		var e DirentInfo
		var kind string
		if err := rows.Scan(&e.Name, &e.Ino, &kind, &e.Mode, &e.Size); err != nil {
			return nil, err
		}
		e.Kind = Kind(kind)
		out = append(out, e)
	}
	return out, rows.Err()
}

// CreateChild atomically creates a new inode under (parent, name). Fails
// with ErrExists if the name is already taken; ErrNotFound if parent is
// missing. The new inode starts at nlink=1.
func (s *Store) CreateChild(ctx context.Context, parent int64, name string, child Inode) (int64, error) {
	var newIno int64
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		// Verify parent exists and is a directory.
		pin, err := getInode(ctx, tx, parent)
		if err != nil {
			return err
		}
		if pin.Kind != KindDir {
			return ErrNotDir
		}
		// Check name isn't taken.
		if _, err := lookup(ctx, tx, parent, name); err == nil {
			return ErrExists
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}
		ino, err := createInodeTx(ctx, tx, child)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO dirents(parent_ino, name, child_ino) VALUES (?,?,?)`,
			parent, name, ino); err != nil {
			return fmt.Errorf("insert dirent: %w", err)
		}
		newIno = ino
		return nil
	})
	return newIno, err
}

// Link adds a new dirent (parent, name) → target and increments target's
// nlink. Returns ErrExists if name is taken, ErrNotFound if target or
// parent is missing. Hardlinking directories is forbidden (ErrIsDir on
// target).
func (s *Store) Link(ctx context.Context, parent int64, name string, target int64) error {
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		pin, err := getInode(ctx, tx, parent)
		if err != nil {
			return err
		}
		if pin.Kind != KindDir {
			return ErrNotDir
		}
		tin, err := getInode(ctx, tx, target)
		if err != nil {
			return err
		}
		if tin.Kind == KindDir {
			return ErrIsDir
		}
		if _, err := lookup(ctx, tx, parent, name); err == nil {
			return ErrExists
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO dirents(parent_ino, name, child_ino) VALUES (?,?,?)`,
			parent, name, target); err != nil {
			return fmt.Errorf("insert dirent: %w", err)
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE inodes SET nlink = nlink + 1 WHERE ino = ?`, target); err != nil {
			return fmt.Errorf("bump nlink: %w", err)
		}
		return nil
	})
}

// Unlink removes the dirent at (parent, name), decrements the child
// inode's nlink, and if nlink hits zero deletes the inode (cascading
// through chunk_map / xattrs / any remaining dirents).
//
// For directories, ErrNotEmpty is returned if the directory has children.
// Symlinks behave like regular files.
func (s *Store) Unlink(ctx context.Context, parent int64, name string) error {
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		_, _, err := unlinkInTx(ctx, tx, parent, name)
		return err
	})
}

// unlinkInTx is the reusable transactional core. Returns the child inode
// that was unlinked and whether it was fully deleted (nlink hit 0).
func unlinkInTx(ctx context.Context, tx *sql.Tx, parent int64, name string) (childIno int64, deleted bool, err error) {
	child, err := lookup(ctx, tx, parent, name)
	if err != nil {
		return 0, false, err
	}
	if child.Kind == KindDir {
		// Reject if any dirent has this dir as a parent.
		var n int
		if err := tx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM dirents WHERE parent_ino = ?`, child.Ino).Scan(&n); err != nil {
			return 0, false, fmt.Errorf("count dir children: %w", err)
		}
		if n > 0 {
			return 0, false, ErrNotEmpty
		}
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM dirents WHERE parent_ino = ? AND name = ?`, parent, name); err != nil {
		return 0, false, fmt.Errorf("delete dirent: %w", err)
	}
	// Directories always have nlink=1 in our model (we don't track POSIX
	// dir nlink). Removing the single dirent always drops them to 0.
	res, err := tx.ExecContext(ctx,
		`UPDATE inodes SET nlink = nlink - 1 WHERE ino = ?`, child.Ino)
	if err != nil {
		return 0, false, fmt.Errorf("dec nlink: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return 0, false, ErrNotFound
	}
	var nlink uint32
	if err := tx.QueryRowContext(ctx,
		`SELECT nlink FROM inodes WHERE ino = ?`, child.Ino).Scan(&nlink); err != nil {
		return 0, false, fmt.Errorf("read nlink: %w", err)
	}
	if nlink == 0 {
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM inodes WHERE ino = ?`, child.Ino); err != nil {
			return 0, false, fmt.Errorf("delete inode: %w", err)
		}
		return child.Ino, true, nil
	}
	return child.Ino, false, nil
}

// Rename moves (oldParent, oldName) to (newParent, newName), overwriting
// any existing dirent at the target per POSIX semantics:
//
//   - target is a regular file or symlink: replaced (its inode unlinked).
//   - target is an empty directory and source is also a dir: replaced.
//   - target is a non-empty directory: ErrNotEmpty.
//   - kind mismatch (file→dir or dir→file): ErrIsDir / ErrNotDir.
//   - source and target dirent are the same: no-op.
//
// Cross-directory moves are supported; moving a directory into one of its
// own descendants is rejected with a cycle error.
func (s *Store) Rename(ctx context.Context, oldParent int64, oldName string, newParent int64, newName string) error {
	if oldParent == newParent && oldName == newName {
		return nil
	}
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		src, err := lookup(ctx, tx, oldParent, oldName)
		if err != nil {
			return err
		}
		// Verify newParent exists and is a directory.
		np, err := getInode(ctx, tx, newParent)
		if err != nil {
			return err
		}
		if np.Kind != KindDir {
			return ErrNotDir
		}
		// Forbid moving a directory into its own descendant.
		if src.Kind == KindDir {
			if cyc, err := isAncestor(ctx, tx, src.Ino, newParent); err != nil {
				return err
			} else if cyc {
				return fmt.Errorf("meta: rename would create a cycle")
			}
		}
		// Handle existing target.
		if dst, err := lookup(ctx, tx, newParent, newName); err == nil {
			if dst.Ino == src.Ino {
				// Per POSIX rename(2): if oldpath and newpath refer to the
				// same file (existing hardlinks of the same inode), do
				// nothing and return success.
				return nil
			}
			if dst.Kind == KindDir {
				if src.Kind != KindDir {
					return ErrIsDir
				}
				// Both dirs: target must be empty.
				var n int
				if err := tx.QueryRowContext(ctx,
					`SELECT COUNT(*) FROM dirents WHERE parent_ino = ?`, dst.Ino).Scan(&n); err != nil {
					return err
				}
				if n > 0 {
					return ErrNotEmpty
				}
			} else if src.Kind == KindDir {
				return ErrNotDir
			}
			if _, _, err := unlinkInTx(ctx, tx, newParent, newName); err != nil {
				return err
			}
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}
		// Move the dirent.
		if _, err := tx.ExecContext(ctx,
			`UPDATE dirents SET parent_ino = ?, name = ? WHERE parent_ino = ? AND name = ?`,
			newParent, newName, oldParent, oldName); err != nil {
			return fmt.Errorf("move dirent: %w", err)
		}
		return nil
	})
}

// isAncestor reports whether `maybeAncestor` is an ancestor of `start`
// (i.e., reachable by walking parent_ino links upward).
func isAncestor(ctx context.Context, tx *sql.Tx, maybeAncestor, start int64) (bool, error) {
	cur := start
	for cur != 0 && cur != RootIno {
		if cur == maybeAncestor {
			return true, nil
		}
		var parent int64
		err := tx.QueryRowContext(ctx,
			`SELECT parent_ino FROM dirents WHERE child_ino = ? LIMIT 1`, cur).Scan(&parent)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("walk ancestors: %w", err)
		}
		cur = parent
	}
	return cur == maybeAncestor, nil
}
