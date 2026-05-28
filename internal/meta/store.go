package meta

import (
	"context"
	cryptorand "crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"
)

// RootIno is the inode number of the filesystem root. It's hard-coded so
// FUSE can return a stable value for stat("/") without consulting the DB.
const RootIno int64 = 1

// ErrNotFound is returned by Get-style methods when no row matches.
var ErrNotFound = errors.New("meta: not found")

// Store owns the local SQLite metadata database. It is safe for use by
// multiple goroutines; SQLite handles serialization internally.
type Store struct {
	db *sql.DB
}

// Open opens (or creates) the metadata DB at path. Parent directories must
// already exist. The schema is created on first use; subsequent opens are
// idempotent.
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("meta: mkdir parent: %w", err)
	}
	// Pragma options are passed via the connection URL.
	// _pragma=foreign_keys(1) is required for ON DELETE CASCADE to fire.
	dsn := path + "?_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("meta: open %s: %w", path, err)
	}
	// TODO(M3): re-evaluate this cap. WAL allows many concurrent readers
	// alongside one writer, so capping conns to 1 serializes the FUSE
	// daemon's parallel reads needlessly. For M2's test workload it's
	// invisible; for the read-mount in M3 it's a contention point.
	db.SetMaxOpenConns(1)

	s := &Store{db: db}
	if err := s.initSchema(context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf("meta: init schema: %w", err)
	}
	return s, nil
}

// Close releases the database handle. Safe to call multiple times.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// DB exposes the underlying *sql.DB for advanced callers (e.g. snapshot
// dump). Most code should use the typed methods on Store and Tx.
func (s *Store) DB() *sql.DB { return s.db }

// schemaSQL is the v1 schema. All tables are created idempotently; this is
// the single migration TelFS ships in M2. Foreign keys cascade from
// inodes(ino) so a single DELETE on inodes cleans up chunks/xattrs/dirents.
const schemaSQL = `
CREATE TABLE IF NOT EXISTS inodes (
  ino            INTEGER PRIMARY KEY AUTOINCREMENT,
  kind           TEXT    NOT NULL,
  mode           INTEGER NOT NULL,
  uid            INTEGER NOT NULL DEFAULT 0,
  gid            INTEGER NOT NULL DEFAULT 0,
  size           INTEGER NOT NULL DEFAULT 0,
  nlink          INTEGER NOT NULL DEFAULT 1,
  mtime          INTEGER NOT NULL DEFAULT 0,
  ctime          INTEGER NOT NULL DEFAULT 0,
  symlink_target TEXT
);

CREATE TABLE IF NOT EXISTS dirents (
  parent_ino INTEGER NOT NULL,
  name       TEXT    NOT NULL,
  child_ino  INTEGER NOT NULL,
  PRIMARY KEY (parent_ino, name),
  FOREIGN KEY (parent_ino) REFERENCES inodes(ino) ON DELETE CASCADE,
  FOREIGN KEY (child_ino)  REFERENCES inodes(ino) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_dirents_child ON dirents(child_ino);

CREATE TABLE IF NOT EXISTS chunk_map (
  ino           INTEGER NOT NULL,
  idx           INTEGER NOT NULL,
  tg_message_id INTEGER NOT NULL,
  size          INTEGER NOT NULL,
  PRIMARY KEY (ino, idx),
  FOREIGN KEY (ino) REFERENCES inodes(ino) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS xattrs (
  ino   INTEGER NOT NULL,
  name  TEXT    NOT NULL,
  value BLOB    NOT NULL,
  PRIMARY KEY (ino, name),
  FOREIGN KEY (ino) REFERENCES inodes(ino) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS journal (
  seq       INTEGER PRIMARY KEY AUTOINCREMENT,
  op_json   BLOB    NOT NULL,
  posted_at INTEGER
);

CREATE TABLE IF NOT EXISTS meta_kv (
  key   TEXT PRIMARY KEY,
  value BLOB NOT NULL
);
`

// Key in meta_kv that holds this filesystem's UUID. Set once on first
// Open and never changes. Used by M5 snapshot/recovery to make sure a
// channel containing messages from a prior TelFS instance can't poison
// the recovery of a new one.
const KVFSUUID = "fs_uuid"

// initSchema creates tables and seeds the root inode if it doesn't exist.
func (s *Store) initSchema(ctx context.Context) error {
	for _, stmt := range splitStmts(schemaSQL) {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec %q: %w", firstLine(stmt), err)
		}
	}
	// Seed root inode if missing. Using explicit ino=1 ensures auto-increment
	// will hand out 2,3,... thereafter. Owner is the mounting user; a 0:0
	// root + mode 0755 would deny writes to anyone but root, which is the
	// wrong default for a personal FS. The user can chown later if needed.
	const rootMode = 0o40755 // dir | rwxr-xr-x
	if _, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO inodes(ino, kind, mode, uid, gid, nlink, mtime, ctime)
		   VALUES (?, 'dir', ?, ?, ?, 1, strftime('%s','now'), strftime('%s','now'))`,
		RootIno, rootMode, uint32(os.Getuid()), uint32(os.Getgid()),
	); err != nil {
		return err
	}
	// Bootstrap fs_uuid on first ever open. INSERT OR IGNORE keeps it
	// idempotent.
	if _, err := s.GetKV(ctx, KVFSUUID); err != nil {
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		uuid, err := randomUUID()
		if err != nil {
			return fmt.Errorf("generate fs_uuid: %w", err)
		}
		if err := s.PutKV(ctx, KVFSUUID, []byte(uuid)); err != nil {
			return fmt.Errorf("seed fs_uuid: %w", err)
		}
	}
	return nil
}

// FSUUID returns this filesystem's UUID, set when the DB was first
// created. Stable across reopens.
func (s *Store) FSUUID(ctx context.Context) (string, error) {
	v, err := s.GetKV(ctx, KVFSUUID)
	if err != nil {
		return "", err
	}
	return string(v), nil
}

// randomUUID returns a RFC-4122 v4 hex UUID.
func randomUUID() (string, error) {
	var b [16]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		return "", err
	}
	// version 4, variant 10
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

// splitStmts splits a multi-statement SQL string on semicolons followed by
// newline. modernc.org/sqlite doesn't support multi-statement Exec out of
// the box.
func splitStmts(sql string) []string {
	parts := strings.Split(sql, ";")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return strings.TrimSpace(s[:i])
	}
	return strings.TrimSpace(s)
}

// WithTx runs fn in a single transaction. fn must use the *sql.Tx for all
// DB access. On error, the transaction is rolled back; on success, it's
// committed.
func (s *Store) WithTx(ctx context.Context, fn func(*sql.Tx) error) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
				err = fmt.Errorf("%w (rollback: %v)", err, rbErr)
			}
			return
		}
		err = tx.Commit()
	}()
	return fn(tx)
}
