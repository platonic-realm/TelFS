package meta

import (
	"context"
	cryptorand "crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

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
	// busy_timeout is generous (60s) so a queued reader/writer waits
	// out long checkpoints or batch writes instead of returning
	// SQLITE_BUSY to the caller — which under load surfaces as EIO
	// to userspace (rm/tar). Plain "wait it out" is fine; SQLite is
	// honest about busy and doesn't deadlock.
	dsn := path + "?_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(60000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("meta: open %s: %w", path, err)
	}
	// MaxOpenConns(1) used to be set here as a conservative default,
	// but it bottlenecks the FUSE workload badly: every Lookup,
	// Readdir, Getattr queues behind any in-flight write transaction,
	// and a heavy write (e.g., a journal-poster batch UPDATE) blocks
	// all reads for hundreds of ms — long enough for kernel-side
	// FUSE op contexts to time out and surface as EIO in userspace.
	// WAL allows many concurrent readers alongside one writer, so
	// raising the cap lets reads parallelize freely. Writes still
	// serialize at the SQLite level; busy_timeout absorbs that.
	db.SetMaxOpenConns(8)

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
  symlink_target TEXT,
  -- chunk_size: per-inode chunk size in bytes, set at create from
  -- FS default (meta_kv:chunk_size) and possibly promoted by the
  -- writer at first dispatch when the workload warrants bigger
  -- chunks (multi-tier). Always non-zero for files.
  chunk_size     INTEGER NOT NULL DEFAULT 4194304
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
  -- pack_offset, pack_size: see docs/architecture and meta.Chunk.
  pack_offset   INTEGER,
  pack_size     INTEGER,
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

CREATE TABLE IF NOT EXISTS chunk_blob (
  hash          BLOB    PRIMARY KEY,
  tg_message_id INTEGER NOT NULL,
  size          INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_chunk_blob_msgid ON chunk_blob(tg_message_id);
CREATE INDEX IF NOT EXISTS idx_chunk_map_msgid  ON chunk_map(tg_message_id);
`

// Key in meta_kv that holds this filesystem's UUID. Set once on first
// Open and never changes. Used by M5 snapshot/recovery to make sure a
// channel containing messages from a prior TelFS instance can't poison
// the recovery of a new one.
const KVFSUUID = "fs_uuid"

// KVChunkSize records the chunk size (in bytes, decimal string) this
// filesystem was committed to. Set once on first Open if missing and
// IMMUTABLE thereafter once any chunk exists — every chunk_map row's
// (ino, idx) tuple is computed from offset/chunk_size, so changing the
// size would silently un-map every existing file.
const KVChunkSize = "chunk_size"

// DefaultChunkSize is the fallback when a fresh filesystem doesn't
// specify a chunk size — 4 MiB, the same value used since M0.
const DefaultChunkSize int64 = 4 << 20

// MinChunkSize and MaxChunkSize bound what `telfs init` will accept.
// Below 64 KiB per-message overhead dominates and chunk_map row count
// balloons; above ~1.5 GiB and you risk Telegram's 2 GB per-document
// hard cap. Within these bounds, only powers of two are allowed —
// makes the (off / size) and (off % size) arithmetic exact and the
// schema-validation logic trivial.
const (
	MinChunkSize int64 = 64 << 10   // 64 KiB
	MaxChunkSize int64 = 1536 << 20 // 1.5 GiB
)

// KVCompressionCodec records the per-FS chunk-payload compression
// codec ("none" or "zstd"). IMMUTABLE once chunks exist — a reader
// that switches codecs mid-FS would feed a zstd frame to a no-op
// decompressor (or vice versa) and shred every chunk. `telfs init`
// is the only setter; the writer refuses to change it when
// chunk_map is non-empty.
const KVCompressionCodec = "compression_codec"

// Trash safety-net KV keys. When KVTrashEnabled is set to "1", every
// kernel-issued unlink/rmdir from FUSE is rerouted to a top-level
// `.trash/` directory; a TTL GC unlinks them for real after
// KVTrashTTLSecs seconds.
const (
	KVTrashEnabled = "trash_enabled"
	KVTrashTTLSecs = "trash_ttl_secs"
)

// DefaultTrashTTLSecs is the fallback TTL when trash is enabled
// without an explicit duration — one week.
const DefaultTrashTTLSecs int64 = 7 * 24 * 60 * 60

// initSchema creates tables and seeds the root inode if it doesn't exist.
func (s *Store) initSchema(ctx context.Context) error {
	for _, stmt := range splitStmts(schemaSQL) {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec %q: %w", firstLine(stmt), err)
		}
	}
	// Idempotent migration: add pack_offset and pack_size to
	// chunk_map on filesystems that predate the small-file packer.
	// SQLite has no `ADD COLUMN IF NOT EXISTS`, so we eat the
	// "duplicate column name" error from a re-run on a fresh DB
	// where the CREATE TABLE above already includes them. Any
	// other error surfaces normally.
	for _, alter := range []string{
		`ALTER TABLE chunk_map ADD COLUMN pack_offset INTEGER`,
		`ALTER TABLE chunk_map ADD COLUMN pack_size INTEGER`,
		`ALTER TABLE inodes ADD COLUMN chunk_size INTEGER NOT NULL DEFAULT 4194304`,
	} {
		if _, err := s.db.ExecContext(ctx, alter); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("migrate schema: %w", err)
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
	// Bootstrap chunk_size if missing. Existing filesystems (created
	// before this kv was introduced) get the default, which is also
	// what they were already using — so a no-op semantically.
	if _, err := s.GetKV(ctx, KVChunkSize); err != nil {
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		if err := s.PutKV(ctx, KVChunkSize,
			[]byte(strconv.FormatInt(DefaultChunkSize, 10)),
		); err != nil {
			return fmt.Errorf("seed chunk_size: %w", err)
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

// ChunkSize returns the chunk size this filesystem was committed to,
// or DefaultChunkSize if the kv is missing (filesystems initialized
// before this column was introduced).
func (s *Store) ChunkSize(ctx context.Context) (int64, error) {
	v, err := s.GetKV(ctx, KVChunkSize)
	if errors.Is(err, ErrNotFound) {
		return DefaultChunkSize, nil
	}
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(string(v), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("chunk_size kv malformed (%q): %w", string(v), err)
	}
	return n, nil
}

// SetChunkSize writes the chunk size kv. Caller is responsible for
// refusing the change when chunks already exist — this method just
// stores the value.
func (s *Store) SetChunkSize(ctx context.Context, n int64) error {
	if err := ValidateChunkSize(n); err != nil {
		return err
	}
	return s.PutKV(ctx, KVChunkSize, []byte(strconv.FormatInt(n, 10)))
}

// CompressionCodec returns the per-FS chunk-payload compression
// codec name ("none" or "zstd"). Returns "none" when the kv is
// missing — that's how legacy filesystems (pre-compression support)
// report through the same API.
func (s *Store) CompressionCodec(ctx context.Context) (string, error) {
	v, err := s.GetKV(ctx, KVCompressionCodec)
	if errors.Is(err, ErrNotFound) {
		return "none", nil
	}
	if err != nil {
		return "", err
	}
	return string(v), nil
}

// SetCompressionCodec writes the codec kv. Caller is responsible for
// refusing the change when chunks already exist — switching codecs
// mid-FS would shred every chunk written under the old one.
func (s *Store) SetCompressionCodec(ctx context.Context, name string) error {
	if name != "none" && name != "zstd" {
		return fmt.Errorf("meta: unknown compression codec %q (want none or zstd)", name)
	}
	return s.PutKV(ctx, KVCompressionCodec, []byte(name))
}

// TrashEnabled reports whether the trash safety-net is active for this
// FS. Default: false.
func (s *Store) TrashEnabled(ctx context.Context) (bool, error) {
	v, err := s.GetKV(ctx, KVTrashEnabled)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return string(v) == "1", nil
}

// SetTrashEnabled toggles the trash safety-net. The caller is
// responsible for any one-time bootstrap (e.g., creating /.trash).
func (s *Store) SetTrashEnabled(ctx context.Context, on bool) error {
	if on {
		return s.PutKV(ctx, KVTrashEnabled, []byte("1"))
	}
	return s.DeleteKV(ctx, KVTrashEnabled)
}

// TrashTTL returns the configured retention as a duration, or the
// default (7 days) if the kv is missing.
func (s *Store) TrashTTL(ctx context.Context) (time.Duration, error) {
	v, err := s.GetKV(ctx, KVTrashTTLSecs)
	if errors.Is(err, ErrNotFound) {
		return time.Duration(DefaultTrashTTLSecs) * time.Second, nil
	}
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(string(v), 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("trash_ttl_secs kv malformed (%q)", string(v))
	}
	return time.Duration(n) * time.Second, nil
}

// SetTrashTTL writes the retention. A zero duration is treated as the
// default. Negative durations are rejected.
func (s *Store) SetTrashTTL(ctx context.Context, d time.Duration) error {
	if d < 0 {
		return fmt.Errorf("trash ttl must be non-negative")
	}
	secs := int64(d / time.Second)
	if secs == 0 {
		secs = DefaultTrashTTLSecs
	}
	return s.PutKV(ctx, KVTrashTTLSecs, []byte(strconv.FormatInt(secs, 10)))
}

// ValidateChunkSize enforces the [MinChunkSize, MaxChunkSize]
// power-of-two range. Returns nil if n is acceptable.
func ValidateChunkSize(n int64) error {
	if n < MinChunkSize || n > MaxChunkSize {
		return fmt.Errorf("chunk_size %d out of range [%d, %d]", n, MinChunkSize, MaxChunkSize)
	}
	if n&(n-1) != 0 {
		return fmt.Errorf("chunk_size %d must be a power of two", n)
	}
	return nil
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
//
// Retries on SQLITE_BUSY (or its synonyms) up to txMaxAttempts times
// with exponential backoff. SQLite returns BUSY when a write tx can't
// acquire the database write lock within busy_timeout — this is the
// scenario where MaxOpenConns > 1 lets multiple goroutines race for
// the SQLite-level write lock and one loses. fn MUST be idempotent
// across retries; that's the contract for every existing caller (each
// is a self-contained set of inserts/updates wrapped in BEGIN, and a
// rolled-back tx leaves no observable state behind).
func (s *Store) WithTx(ctx context.Context, fn func(*sql.Tx) error) (err error) {
	const txMaxAttempts = 5
	backoff := 10 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err = s.withTxOnce(ctx, fn)
		if err == nil {
			return nil
		}
		if !isBusyErr(err) || attempt >= txMaxAttempts {
			return err
		}
		log.Printf("meta.WithTx: SQLITE_BUSY on attempt %d/%d, retrying after %s: %v",
			attempt, txMaxAttempts, backoff, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > 500*time.Millisecond {
			backoff = 500 * time.Millisecond
		}
	}
}

// withTxOnce is the single-attempt transaction body. Split out from
// WithTx so the retry loop in WithTx stays readable.
func (s *Store) withTxOnce(ctx context.Context, fn func(*sql.Tx) error) (err error) {
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

// isBusyErr reports whether err is (or wraps) one of modernc.org/sqlite's
// busy-condition errors. We string-match because the modernc driver doesn't
// expose its constants in a stable API surface; the substrings are stable
// across driver versions ("SQLITE_BUSY", "database is locked").
func isBusyErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "SQLITE_BUSY") ||
		strings.Contains(msg, "database is locked")
}
