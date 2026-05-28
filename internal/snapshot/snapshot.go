package snapshot

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"telfs/internal/meta"
)

// Snapshot is a consistent point-in-time view of a TelFS metadata DB,
// produced via SQLite's VACUUM INTO (which builds a defragmented,
// fully-flushed copy regardless of in-flight writes on the source).
//
// Bytes are gzipped — the uncompressed SQLite file is ~1.5–3× larger
// for typical metadata DBs and gzip shrinks it back to ~30–40% of that.
type Snapshot struct {
	// Bytes is the gzipped SQLite file contents.
	Bytes []byte
	// FSUUID identifies the TelFS instance this snapshot belongs to.
	// Recovery rejects snapshots whose UUID doesn't match the configured
	// channel (defensive: a re-used channel could contain old TelFS
	// snapshots from a deleted DB).
	FSUUID string
	// JournalSeq is the highest journal seq included in this snapshot;
	// recovery replays meta-ops with seq > JournalSeq. M5 doesn't post
	// meta-ops yet, so this is informational (set to LastJournalSeq at
	// snapshot time).
	JournalSeq int64
}

// Take produces a Snapshot of the DB rooted at the provided meta.Store.
// The Store remains open and usable during and after the snapshot — the
// VACUUM INTO is a separate read-consistent copy.
//
// VACUUM INTO holds a write lock on the source for the duration of the
// copy. For metadata-sized DBs (typically < 100 MiB) this is well under
// a second; concurrent writers block briefly.
func Take(ctx context.Context, m *meta.Store) (*Snapshot, error) {
	uuid, err := m.FSUUID(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot: fetch fs_uuid: %w", err)
	}
	seq, err := m.LastJournalSeq(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot: last journal seq: %w", err)
	}

	tmpDir, err := os.MkdirTemp("", "telfs-snap-*")
	if err != nil {
		return nil, fmt.Errorf("snapshot: mkdir temp: %w", err)
	}
	defer os.RemoveAll(tmpDir)
	tmpPath := filepath.Join(tmpDir, "snap.sqlite")

	// VACUUM INTO produces a single-file, consistent copy. SQLite
	// rejects 'main' as a target name, so we just pass the absolute path
	// as a string literal in the SQL.
	if _, err := m.DB().ExecContext(ctx, fmt.Sprintf("VACUUM INTO %s", sqlString(tmpPath))); err != nil {
		return nil, fmt.Errorf("snapshot: VACUUM INTO: %w", err)
	}

	src, err := os.Open(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("snapshot: open copy: %w", err)
	}
	defer src.Close()

	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := io.Copy(gw, src); err != nil {
		return nil, fmt.Errorf("snapshot: gzip: %w", err)
	}
	if err := gw.Close(); err != nil {
		return nil, fmt.Errorf("snapshot: gzip close: %w", err)
	}

	return &Snapshot{
		Bytes:      buf.Bytes(),
		FSUUID:     uuid,
		JournalSeq: seq,
	}, nil
}

// Restore writes the gzipped snapshot bytes to dbPath, overwriting any
// existing file. The caller is responsible for ensuring nothing else
// has the destination DB open at the time of restore.
func Restore(ctx context.Context, gzipped []byte, dbPath string) error {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		return fmt.Errorf("restore: mkdir parent: %w", err)
	}
	gr, err := gzip.NewReader(bytes.NewReader(gzipped))
	if err != nil {
		return fmt.Errorf("restore: gunzip: %w", err)
	}
	defer gr.Close()
	tmpPath := dbPath + ".restore.tmp"
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("restore: open tmp: %w", err)
	}
	if _, err := io.Copy(f, gr); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("restore: write tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("restore: close tmp: %w", err)
	}
	// Best-effort sanity check: the restored file should open as a
	// sqlite DB. We don't want to clobber dbPath with garbage.
	if err := verifySQLiteFile(ctx, tmpPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("restore: %w", err)
	}
	return os.Rename(tmpPath, dbPath)
}

// verifySQLiteFile opens the file as a SQLite DB and runs a trivial
// query. Catches corrupt downloads / truncated streams before we
// overwrite the live DB.
func verifySQLiteFile(ctx context.Context, path string) error {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)")
	if err != nil {
		return fmt.Errorf("verify open: %w", err)
	}
	defer db.Close()
	row := db.QueryRowContext(ctx, "SELECT count(*) FROM inodes")
	var n int
	if err := row.Scan(&n); err != nil {
		return fmt.Errorf("verify scan: %w", err)
	}
	return nil
}

// sqlString returns s formatted as a single-quoted SQL string literal,
// doubling any internal quotes. Used to embed a file path in a VACUUM
// INTO statement without going through ?-style binding (SQLite doesn't
// allow VACUUM INTO ?, only literals).
func sqlString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
