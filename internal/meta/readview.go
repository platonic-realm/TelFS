package meta

import (
	"context"
	"database/sql"
	"fmt"
)

// ReadView is a consistent read of the metadata DB: every query runs in
// ONE read transaction, so the view is the DB as of its first query —
// writes a running daemon commits afterwards are invisible (SQLite's WAL
// snapshot isolation). `telfs restore` plans from it while the live
// mount keeps working.
type ReadView struct{ tx *sql.Tx }

// Child is a directory entry with its inode.
type Child struct {
	Name string
	Inode
}

// Xattr is one extended attribute.
type Xattr struct {
	Name  string
	Value []byte
}

// Read runs fn on a consistent read view and ends the transaction when
// fn returns. The transaction is read-only (a plain BEGIN, also on a
// store opened read-write with _txlock=immediate: it takes no write
// lock). Keep fn short on a live DB: an open read transaction holds back
// WAL checkpoints.
func (s *Store) Read(ctx context.Context, fn func(*ReadView) error) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("meta: begin read: %w", err)
	}
	defer tx.Rollback()
	return fn(&ReadView{tx: tx})
}

// Lookup resolves (parent, name) as Store.Lookup does.
func (v *ReadView) Lookup(ctx context.Context, parent int64, name string) (Inode, error) {
	return lookup(ctx, v.tx, parent, name)
}

// GetInode returns the inode row for ino, or ErrNotFound.
func (v *ReadView) GetInode(ctx context.Context, ino int64) (Inode, error) {
	return getInode(ctx, v.tx, ino)
}

// Children returns a directory's entries with their inodes, by name
// (byte order).
func (v *ReadView) Children(ctx context.Context, parent int64) ([]Child, error) {
	rows, err := v.tx.QueryContext(ctx,
		`SELECT d.name, i.ino, i.kind, i.mode, i.uid, i.gid, i.size, i.nlink, i.mtime, i.ctime, i.symlink_target, i.chunk_size, i.sparse
		   FROM dirents d JOIN inodes i ON i.ino = d.child_ino
		  WHERE d.parent_ino = ?
		  ORDER BY d.name`, parent)
	if err != nil {
		return nil, fmt.Errorf("children of %d: %w", parent, err)
	}
	defer rows.Close()
	var out []Child
	for rows.Next() {
		var c Child
		var kind string
		var target sql.NullString
		var sparse int
		if err := rows.Scan(&c.Name, &c.Ino, &kind, &c.Mode, &c.UID, &c.GID, &c.Size, &c.Nlink, &c.Mtime, &c.Ctime,
			&target, &c.ChunkSize, &sparse); err != nil {
			return nil, fmt.Errorf("children of %d: %w", parent, err)
		}
		c.Kind = Kind(kind)
		c.SymlinkTarget = target.String
		c.Sparse = sparse != 0
		out = append(out, c)
	}
	return out, rows.Err()
}

// Chunks returns every chunk_map row of ino in idx order.
func (v *ReadView) Chunks(ctx context.Context, ino int64) ([]Chunk, error) {
	rows, err := v.tx.QueryContext(ctx, `SELECT `+chunkCols+` FROM chunk_map WHERE ino = ? ORDER BY idx`, ino)
	if err != nil {
		return nil, fmt.Errorf("chunks of %d: %w", ino, err)
	}
	return scanChunks(rows)
}

// AllXattrs returns every extended attribute, by inode and name. One
// query: xattrs are few next to inodes.
func (v *ReadView) AllXattrs(ctx context.Context) (map[int64][]Xattr, error) {
	rows, err := v.tx.QueryContext(ctx, `SELECT ino, name, value FROM xattrs ORDER BY ino, name`)
	if err != nil {
		return nil, fmt.Errorf("xattrs: %w", err)
	}
	defer rows.Close()
	out := map[int64][]Xattr{}
	for rows.Next() {
		var ino int64
		var x Xattr
		if err := rows.Scan(&ino, &x.Name, &x.Value); err != nil {
			return nil, fmt.Errorf("xattrs: %w", err)
		}
		out[ino] = append(out[ino], x)
	}
	return out, rows.Err()
}
