package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"telfs/internal/chunk"
	"telfs/internal/config"
	"telfs/internal/control"
	"telfs/internal/meta"
	"telfs/internal/snapshot"
)

func newSyncDaemon(t *testing.T) (*daemonCtl, *meta.Store, *chunk.UploadPool) {
	t.Helper()
	dir := t.TempDir()
	m, err := meta.Open(filepath.Join(dir, "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	m.SetJournalV2(true)
	pool := chunk.NewUploadPool(1)
	t.Cleanup(pool.Shutdown)
	cfg := &config.Config{DataDir: dir}
	return &daemonCtl{cfg: cfg, format: 2, meta: m, pool: pool, poster: &snapshot.Poster{Meta: m}}, m, pool
}

func postAll(t *testing.T, m *meta.Store) {
	t.Helper()
	pend, _ := m.PendingJournal(context.Background())
	var seqs []int64
	for _, e := range pend {
		seqs = append(seqs, e.Seq)
	}
	if err := m.BatchMarkJournalPosted(context.Background(), seqs, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
}

// `telfs sync` (spec v1 §3): it waits for the uploads registered before it
// was asked, then for the journal up to the seq reached once those landed
// (their chunk placements included); later work does not move the target.
func TestSyncBarrier(t *testing.T) {
	ctx := context.Background()
	d, m, pool := newSyncDaemon(t)
	ino, _ := m.CreateChild(ctx, meta.RootIno, "a", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	before := pool.Register(ino)
	tok := &syncTok{reg: pool.LastRegistered(), created: time.Now()}
	after := pool.Register(ino) // dispatched after the sync was asked for
	defer after.Release()
	if rep := d.syncCheck(ctx, tok); rep.Synced || rep.Phase != "uploads" || rep.PendingUploads != 1 {
		t.Fatalf("with an earlier upload open: %+v", rep)
	}
	// The earlier upload lands; its chunk placement is journaled.
	m.PutChunk(ctx, meta.Chunk{Ino: ino, Idx: 0, TGMessageID: 5, Size: 10, Hash: make([]byte, 32)})
	before.Release()
	rep := d.syncCheck(ctx, tok)
	if rep.Synced || rep.Phase != "journal" || rep.JournalUnposted == 0 {
		t.Fatalf("journal not yet posted: %+v", rep)
	}
	postAll(t, m)
	// New work after the target was fixed does not hold the sync back.
	m.CreateChild(ctx, meta.RootIno, "b", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	if rep := d.syncCheck(ctx, tok); !rep.Synced {
		t.Fatalf("want synced, got %+v", rep)
	}
}

// The control routes work over the real socket: begin + wait.
func TestSyncOverControlSocket(t *testing.T) {
	d, m, _ := newSyncDaemon(t)
	path := control.Path(d.cfg.DataDir)
	ln, err := control.Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go control.Serve(ctx, ln, path, d.handler())
	c := control.Client(path, 5*time.Second)
	m.CreateChild(context.Background(), meta.RootIno, "x", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	resp, err := c.Post(control.BaseURL+"/sync/begin", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	var begin syncReply
	json.NewDecoder(resp.Body).Decode(&begin)
	resp.Body.Close()
	wait := func() syncReply {
		resp, err := c.Get(fmt.Sprintf("%s/sync/wait?token=%d&max=300ms", control.BaseURL, begin.Token))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var rep syncReply
		json.NewDecoder(resp.Body).Decode(&rep)
		return rep
	}
	if rep := wait(); rep.Synced || rep.Phase != "journal" {
		t.Fatalf("unposted journal: %+v", rep)
	}
	postAll(t, m)
	if rep := wait(); !rep.Synced {
		t.Fatalf("want synced: %+v", rep)
	}
	resp, err = c.Get(fmt.Sprintf("%s/sync/wait?token=%d&max=1s", control.BaseURL, begin.Token))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("a finished token answered %d, want 404", resp.StatusCode)
	}
}

// `telfs status --json` without a daemon: local state only, no secret.
func TestOfflineStatus(t *testing.T) {
	ctx := context.Background()
	// A profile name no real mount carries: status finds a profile's mount
	// by name in /proc/mounts, machine-wide (S9 runs a live "backup").
	dir := filepath.Join(t.TempDir(), "telfs-offline-status-test")
	if err := os.MkdirAll(filepath.Join(dir, "cache", "pending"), 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{DataDir: dir}
	m, err := meta.Open(cfg.DBPath())
	if err != nil {
		t.Fatal(err)
	}
	m.PutKV(ctx, meta.KVFormat, []byte("2"))
	m.SetJournalV2(true)
	m.CreateChild(ctx, meta.RootIno, "f", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	m.Close()
	os.WriteFile(filepath.Join(cfg.StagingDir(), "3-0-10.bin"), make([]byte, 10), 0o600)
	st, err := offlineStatus(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if st.Schema != 1 || st.Profile != "telfs-offline-status-test" || st.Format != 2 || st.Mounted || st.Link != nil || st.PID != nil {
		t.Fatalf("status %+v", st)
	}
	if st.Sync.Synced || st.Sync.PendingUploads != 1 || st.Sync.PendingBytes != 10 || st.Sync.JournalUnposted == 0 {
		t.Fatalf("sync %+v", st.Sync)
	}
	if st.Staging.Dir != cfg.StagingDir() || st.Staging.CapBytes != chunk.DefaultStagingMaxBytes {
		t.Fatalf("staging %+v", st.Staging)
	}
	b, _ := json.Marshal(st)
	var back map[string]any
	json.Unmarshal(b, &back)
	for _, k := range []string{"schema", "telfs_version", "fs_uuid", "mounted", "read_only", "link", "sync", "snapshot", "versions", "upload", "staging", "key_header", "crypto_uses", "last_error"} {
		if _, ok := back[k]; !ok {
			t.Errorf("field %q missing from the JSON", k)
		}
	}
}

func TestExitCodesV1(t *testing.T) {
	for err, want := range map[error]int{
		fmt.Errorf("x: %w", errNotSynced):  4,
		fmt.Errorf("x: %w", errNotMounted): 5,
		fmt.Errorf("x: %w", errTelegram):   6,
		fmt.Errorf("x: %w", errIntegrity):  7,
	} {
		if got := exitCode(err); got != want {
			t.Errorf("%v: exit %d, want %d", err, got, want)
		}
	}
}

// status --json's upload block (spec v1 §3): the configured [upload] pool
// size and byte-rate cap, and the measured rate.
func TestUploadStatus(t *testing.T) {
	cfg := &config.Config{}
	if got := uploadStatus(cfg, 1234); *got != (statusUpload{Connections: 8, RateCapBytesPerS: 5_000_000, RateBytesPerS: 1234}) {
		t.Fatalf("defaults: %+v", *got)
	}
	conns := 0
	cfg.Upload = config.UploadConfig{Connections: &conns, MaxRate: "0"}
	if got := uploadStatus(cfg, 0); *got != (statusUpload{}) {
		t.Fatalf("no pool, no cap: %+v", *got)
	}
	b, _ := json.Marshal(uploadStatus(&config.Config{}, 7))
	if string(b) != `{"connections":8,"rate_cap_bytes_per_s":5000000,"rate_bytes_per_s":7}` {
		t.Fatalf("JSON %s", b)
	}
}

func TestDescribeUpload(t *testing.T) {
	for u, want := range map[config.UploadSettings]string{
		{Connections: 8, PartsPerUpload: 2, MaxRate: 5_000_000}:  "8 connections, 2 parts in flight per upload, capped at 5 MB/s",
		{Connections: 16, PartsPerUpload: 1, MaxRate: 1_250_000}: "16 connections, 1 part in flight per upload, capped at 1.25 MB/s",
		{Connections: 0, PartsPerUpload: 2, MaxRate: 0}:          "the main connection only, no rate cap",
	} {
		if got := describeUpload(u); got != want {
			t.Errorf("%+v: %q, want %q", u, got, want)
		}
	}
}
