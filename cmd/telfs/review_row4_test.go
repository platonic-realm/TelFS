package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"telfs/internal/config"
	"telfs/internal/meta"
	"telfs/internal/snapshot"
)

// `snapshot restore` on format 2 (review fix): the rolled-back DB keeps
// today's key header, registry and anchor id, owes a snapshot, and its
// journal continues one seq PAST the abandoned state's newest — all in
// one transaction.
func TestApplyRollback2(t *testing.T) {
	ctx := context.Background()
	m, err := meta.Open(filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	m.PutKV(ctx, meta.KVFormat, []byte("2"))
	m.SetJournalV2(true)
	m.CreateChild(ctx, meta.RootIno, "old", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	r := rollback2{header: []byte(`{"h":1}`), anchor: []byte("4321"), versions: []byte(`[{"name":"v","msg":9}]`), seq: 150}
	if err := applyRollback2(ctx, m, r, 77); err != nil {
		t.Fatal(err)
	}
	if seq, _ := m.LastJournalSeq(ctx); seq != 151 {
		t.Fatalf("journal seq %d, want 151 (one past the abandoned 150)", seq)
	}
	if why := snapshot.Owed(ctx, m); !strings.Contains(why, "restored from snapshot 77") {
		t.Fatalf("owed %q", why)
	}
	kv, _ := m.GetKVs(ctx, meta.KVKeyHeader, snapshot.KVAnchorMsgID, snapshot.KVVersions, snapshot.KVSnapMsgIDs)
	if string(kv[meta.KVKeyHeader]) != `{"h":1}` || string(kv[snapshot.KVAnchorMsgID]) != "4321" ||
		string(kv[snapshot.KVVersions]) != string(r.versions) || string(kv[snapshot.KVSnapMsgIDs]) != "77" {
		t.Fatalf("kept state %q", kv)
	}
	// The next op skips seq 151: a recovery through the old anchor sees
	// 101..150 then 152 — a hole, and refuses.
	m.CreateChild(ctx, meta.RootIno, "new", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	if seq, _ := m.LastJournalSeq(ctx); seq != 152 {
		t.Fatalf("next op got seq %d, want 152", seq)
	}
	// No registry today: the rolled-back DB has none either.
	m.PutKV(ctx, snapshot.KVVersions, []byte(`[{"name":"old"}]`))
	if err := applyRollback2(ctx, m, rollback2{header: []byte("{}"), seq: 1}, 78); err != nil {
		t.Fatal(err)
	}
	if _, err := m.GetKV(ctx, snapshot.KVVersions); err == nil {
		t.Fatal("a registry the current DB does not have came back")
	}
}

// A failed rollback record puts the previous DB back.
func TestUndoRestore(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "db.sqlite")
	backup := db + ".pre-restore-1"
	os.WriteFile(db, []byte("restored"), 0o600)
	os.WriteFile(db+"-wal", []byte("w"), 0o600)
	os.WriteFile(backup, []byte("previous"), 0o600)
	os.WriteFile(backup+"-shm", []byte("s"), 0o600)
	if err := undoRestore(db, backup); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(db); string(b) != "previous" {
		t.Fatalf("db holds %q", b)
	}
	if _, err := os.Stat(db + "-wal"); !os.IsNotExist(err) {
		t.Fatal("the restored DB's WAL must go")
	}
	if b, _ := os.ReadFile(db + "-shm"); string(b) != "s" {
		t.Fatal("the backup's sidecar must come back")
	}
}

// gc keeps the journal the anchored snapshot needs even when the scan
// did not reach that snapshot.
func TestJournalPruneBound(t *testing.T) {
	for _, tc := range []struct {
		min       int64
		anchor    int
		seen      bool
		anchorSeq int64
		want      int64
		name      string
	}{
		{100, 0, false, -1, 100, "no anchor"},
		{100, 5, true, 40, 100, "anchored snapshot seen (its seq is in min already)"},
		{100, 5, false, 40, 40, "anchored snapshot beyond the scan"},
		{100, 5, false, -1, -1, "beyond the scan, seq unknown: prune nothing"},
		{-1, 5, false, 40, -1, "no retained snapshot seen: prune nothing"},
	} {
		if got := journalPruneBound(tc.min, tc.anchor, tc.seen, tc.anchorSeq); got != tc.want {
			t.Errorf("%s: %d, want %d", tc.name, got, tc.want)
		}
	}
}

// While a snapshot is owed (format 2), nothing reports synced: the
// barrier, the daemon's status and the offline status.
func TestSnapshotOwedIsNotSynced(t *testing.T) {
	ctx := context.Background()
	d, m, pool := newSyncDaemon(t)
	m.PutKV(ctx, meta.KVFormat, []byte("2"))
	tok := &syncTok{reg: pool.LastRegistered(), created: time.Now()}
	if rep := d.syncCheck(ctx, tok); !rep.Synced {
		t.Fatalf("nothing owed: %+v", rep)
	}
	if err := markSettingChanged(ctx, m, "trash settings changed"); err != nil {
		t.Fatal(err)
	}
	rep := d.syncCheck(ctx, tok)
	if rep.Synced || rep.Phase != "snapshot" || !strings.Contains(rep.SnapshotDue, "trash settings changed") {
		t.Fatalf("owed: %+v", rep)
	}
	m.DeleteKV(ctx, snapshot.KVSnapDue) // the policy loop's snapshot landed
	if rep := d.syncCheck(ctx, tok); !rep.Synced {
		t.Fatalf("after the snapshot: %+v", rep)
	}

	dir := filepath.Join(t.TempDir(), "p")
	cfg := &config.Config{DataDir: dir}
	os.MkdirAll(dir, 0o700)
	om, err := meta.Open(cfg.DBPath())
	if err != nil {
		t.Fatal(err)
	}
	om.PutKV(ctx, meta.KVFormat, []byte("2"))
	snapshot.MarkDue(ctx, om, "restored from snapshot 5")
	om.Close()
	st, err := offlineStatus(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if st.Sync.Synced || !strings.Contains(st.Sync.SnapshotDue, "restored from snapshot 5") {
		t.Fatalf("offline: %+v", st.Sync)
	}
}
