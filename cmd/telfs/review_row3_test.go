package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"telfs/internal/config"
	"telfs/internal/meta"
	"telfs/internal/snapshot"
	"telfs/internal/wire"
)

// A version whose take keeps finding a file without all its chunks is
// taken again after a new sync, and after the last take fails with exit
// 4 naming the file — nothing posted. Once the file is whole it works.
func TestVersionCreateRetakesThenExits4(t *testing.T) {
	ctx := context.Background()
	_, m, _, ch, keys := versionDaemon(t)
	ino, _ := m.CreateChild(ctx, meta.RootIno, "half.bin", meta.Inode{Kind: meta.KindFile, Mode: 0o100644, ChunkSize: 1 << 20})
	m.SetSize(ctx, ino, 2<<20)
	m.PutChunk(ctx, meta.Chunk{Ino: ino, Idx: 0, TGMessageID: 7, Size: 1 << 20, Hash: make([]byte, 32)})
	postAll(t, m)
	err := dispatch(ctx, []string{"telfs", "versions", "create", "daily", "--timeout", "30s"})
	if exitCode(err) != 4 || !strings.Contains(err.Error(), "/half.bin") || !strings.Contains(err.Error(), "5 takes") {
		t.Fatalf("incomplete file: %v (exit %d), want exit 4 naming /half.bin", err, exitCode(err))
	}
	if got := ch.kinds(keys, wire.KindVersion); len(got) != 0 {
		t.Fatalf("posted %v", got)
	}
	m.PutChunk(ctx, meta.Chunk{Ino: ino, Idx: 1, TGMessageID: 8, Size: 1 << 20, Hash: make([]byte, 32)})
	postAll(t, m)
	if err := dispatch(ctx, []string{"telfs", "versions", "create", "daily", "--timeout", "30s"}); err != nil {
		t.Fatalf("whole again: %v", err)
	}
}

// A job lives while a client waits, not by the clock: an abandoned one
// (its client went away) does not block its name; one a client still
// waits for is never purged, however old.
func TestVersionJobLifetime(t *testing.T) {
	d, _, pool, _, _ := versionDaemon(t)
	now := time.Now()
	old := now.Add(-3 * time.Hour)
	for _, tc := range []struct {
		j    versionJob
		want bool
		name string
	}{
		{versionJob{state: verSyncing, lastSeen: now}, false, "polled just now"},
		{versionJob{state: verSyncing, lastSeen: old, waiters: 1}, false, "a client waits, created long ago"},
		{versionJob{state: verSyncing, lastSeen: now, gone: true}, true, "its client went away"},
		{versionJob{state: verSyncing, lastSeen: now.Add(-versionIdle - time.Second)}, true, "nobody came back"},
		{versionJob{state: verPosting, lastSeen: old, gone: true}, false, "being posted"},
		{versionJob{state: verDone, lastSeen: old}, true, "result never collected"},
	} {
		if got := tc.j.abandoned(now); got != tc.want {
			t.Errorf("%s: abandoned %v, want %v", tc.name, got, tc.want)
		}
	}

	// A create whose client was killed mid-sync: the retry of the same
	// name is not answered "being created".
	open := pool.Register(1)
	defer open.Release()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- dispatch(ctx, []string{"telfs", "versions", "create", "daily"}) }()
	waitFor(t, func() bool { d.verMu.Lock(); defer d.verMu.Unlock(); return len(d.verJobs) == 1 })
	time.Sleep(300 * time.Millisecond)
	cancel() // SIGTERM / ^C: the CLI gives the job up
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled create: %v", err)
	}
	waitFor(t, func() bool { d.verMu.Lock(); defer d.verMu.Unlock(); return len(d.verJobs) == 0 })

	// A client that vanished without canceling (kill -9): its job is
	// abandoned once the wait it was in ends, and the retry replaces it.
	d.verMu.Lock()
	d.verNext++
	d.verJobs[d.verNext] = &versionJob{name: "weekly", state: verSyncing, tok: &syncTok{}, done: make(chan struct{}),
		lastSeen: time.Now(), gone: true}
	d.verMu.Unlock()
	open.Release()
	if err := dispatch(context.Background(), []string{"telfs", "versions", "create", "weekly", "--timeout", "10s"}); err != nil {
		t.Fatalf("retry after a vanished client: %v", err)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not reached")
}

// A version cache filled for another version of the same name (deleted,
// made again) is emptied before use; the same version keeps its cache.
func TestPrepareVersionCache(t *testing.T) {
	cfg := &config.Config{DataDir: t.TempDir()}
	v1 := wire.AnchorVersion{Name: "daily", Msg: 10, SHA256: "aa", Size: 4096}
	dir, err := prepareVersionCache(cfg, v1)
	if err != nil {
		t.Fatal(err)
	}
	cached := filepath.Join(dir, "5-0.bin")
	os.WriteFile(cached, []byte("v1 plaintext"), 0o600)
	if _, err := prepareVersionCache(cfg, v1); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(cached); err != nil {
		t.Fatal("the same version must keep its cache")
	}
	v2 := wire.AnchorVersion{Name: "daily", Msg: 20, SHA256: "bb", Size: 4096}
	if _, err := prepareVersionCache(cfg, v2); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(cached); !os.IsNotExist(err) {
		t.Fatal("another version's cached plaintext must go")
	}
	// A cache from before the identity mark (no mark) is not trusted.
	os.Remove(filepath.Join(dir, versionCacheID))
	os.WriteFile(cached, []byte("?"), 0o600)
	if _, err := prepareVersionCache(cfg, v2); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(cached); !os.IsNotExist(err) {
		t.Fatal("an unmarked cache must be emptied")
	}
}

// A damaged version exits 7 (not restarted in a loop by the unit), a
// channel failure 6.
func TestVersionDownloadErr(t *testing.T) {
	for err, want := range map[error]int{
		fmt.Errorf("x: %w", snapshot.ErrVersionIntegrity):                       7,
		fmt.Errorf("%w: download: %w", snapshot.ErrChannel, errors.New("gone")): 6,
		fmt.Errorf("dial: %w", syscall.ECONNREFUSED):                            6,
		errors.New("disk full"):                                                 1,
	} {
		if got := exitCode(versionDownloadErr(err)); got != want {
			t.Errorf("%v: exit %d, want %d", err, got, want)
		}
	}
}
