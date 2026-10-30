package chunk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"

	"telfs/internal/meta"
)

// Queue mode (spec v1, row 9): staged-but-not-uploaded bytes are capped on
// disk, memory is released once a wire is staged, failures are retried from
// the staged copy.

// stageUploader is a thread-safe test uploader: optionally gated, failing
// its first `fails` calls.
type stageUploader struct {
	gate    chan struct{} // nil = open
	fails   atomic.Int32
	calls   atomic.Int32
	nextID  atomic.Int64
	mu      sync.Mutex
	uploads map[int][]byte
}

func newStageUploader() *stageUploader { return &stageUploader{uploads: map[int][]byte{}} }

func (u *stageUploader) UploadDocument(ctx context.Context, r io.Reader, _, _ string) (int, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	u.calls.Add(1)
	if u.gate != nil {
		select {
		case <-u.gate:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	if u.fails.Add(-1) >= 0 {
		return 0, errors.New("simulated outage")
	}
	id := int(u.nextID.Add(1))
	u.mu.Lock()
	u.uploads[id] = data
	u.mu.Unlock()
	return id, nil
}

func newQueuePool(t *testing.T, conc int, diskCap int64) (*UploadPool, string) {
	t.Helper()
	dir := t.TempDir()
	p := NewUploadPool(conc)
	p.PendingDir = dir
	p.PendingIndex = NewPendingIndex()
	p.Disk = NewDiskGate(dir, diskCap, 0)
	p.retryBase = 10 * time.Millisecond
	t.Cleanup(p.Shutdown)
	return p, dir
}

func newQueueFile(t *testing.T, m *meta.Store, name string, chunkSize int64) int64 {
	t.Helper()
	ino, err := m.CreateChild(context.Background(), meta.RootIno, name, meta.Inode{Kind: meta.KindFile, Mode: 0o100644, ChunkSize: chunkSize})
	if err != nil {
		t.Fatal(err)
	}
	return ino
}

func dirBytes(t *testing.T, dir string) (n int64, files int) {
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		if fi, err := e.Info(); err == nil && !e.IsDir() {
			n += fi.Size()
			files++
		}
	}
	return n, files
}

func countRows(t *testing.T, m *meta.Store, ino int64) int {
	t.Helper()
	rows, err := m.ListChunks(context.Background(), ino)
	if err != nil {
		t.Fatal(err)
	}
	return len(rows)
}

// Writes block once the disk cap AND the in-memory hand-off are full, the
// staging directory never holds more than the cap (+ nothing), and
// everything lands once uploads resume.
func TestQueueModeCapsStagedBytesAndBlocksWriters(t *testing.T) {
	m := newTestMeta(t)
	ctx := context.Background()
	pool, dir := newQueuePool(t, 1, 192) // 3 wires of 64 B
	pool.staged = semaphore.NewWeighted(128)
	pool.stagedCap = 128 // the in-memory hand-off holds 2 more
	up := newStageUploader()
	up.gate = make(chan struct{})
	ino := newQueueFile(t, m, "f", 64)
	w, err := NewWriter(ctx, m, newTestCache(t, nil), up, nil, nil, pool, nil, ino, 64, 0)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("0123456789abcdef"), 40) // 640 B = 10 chunks
	if _, err := w.WriteAt(ctx, payload, 0); err != nil {
		t.Fatal(err)
	}
	var maxStaged atomic.Int64
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if n, _ := dirBytes(t, dir); n > maxStaged.Load() {
				maxStaged.Store(n)
			}
			time.Sleep(time.Millisecond)
		}
	}()
	done := make(chan error, 1)
	go func() { done <- w.Flush(ctx) }()
	time.Sleep(300 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("Flush returned (%v) while staging and the hand-off were full", err)
	default:
	}
	if _, _, blocked := pool.Disk.Stats(); !blocked {
		t.Fatal("gate does not report a blocked writer")
	}
	close(up.gate)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Flush still blocked after uploads resumed")
	}
	dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if !pool.Drain(dctx) {
		t.Fatal("pool did not drain")
	}
	close(stop)
	if got := maxStaged.Load(); got > 192 {
		t.Fatalf("staging directory held %d B, cap 192", got)
	}
	if n := countRows(t, m, ino); n != 10 {
		t.Fatalf("chunk rows %d, want 10", n)
	}
	if n, files := dirBytes(t, dir); n != 0 || files != 0 {
		t.Fatalf("staging directory not empty: %d B in %d files", n, files)
	}
	if used, _, _ := pool.Disk.Stats(); used != 0 {
		t.Fatalf("gate still counts %d B", used)
	}
}

// A staged upload that keeps failing is retried from its staged copy —
// never put back into memory — until it lands; fsync waits for it.
func TestQueueModeRetriesFromStagedCopy(t *testing.T) {
	old := uploadRetryFirst
	uploadRetryFirst = time.Millisecond
	defer func() { uploadRetryFirst = old }()
	m := newTestMeta(t)
	ctx := context.Background()
	pool, dir := newQueuePool(t, 2, 1<<20)
	up := newStageUploader()
	up.fails.Store(7) // more than one uploadWithRetry round (5 attempts)
	ino := newQueueFile(t, m, "f", 64)
	w, err := NewWriter(ctx, m, newTestCache(t, nil), up, nil, nil, pool, nil, ino, 64, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteAt(ctx, bytes.Repeat([]byte("x"), 100), 0); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- w.Sync(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Sync did not return")
	}
	if n := countRows(t, m, ino); n != 2 {
		t.Fatalf("chunk rows %d, want 2", n)
	}
	if len(w.dirty) != 0 {
		t.Fatalf("%d chunk(s) were put back into memory", len(w.dirty))
	}
	if c := up.calls.Load(); c < 8 {
		t.Fatalf("uploader called %d times, want ≥ 8 (retries from the staged copy)", c)
	}
	if n, _ := dirBytes(t, dir); n != 0 {
		t.Fatalf("staging directory holds %d B", n)
	}
}

// Packed chunks are queued by reference and read at flush; a failed flush
// puts them back and the packer retries; the gate ends at zero.
func TestQueueModePackerRetriesStagedBatch(t *testing.T) {
	old := uploadRetryFirst
	uploadRetryFirst = time.Millisecond
	defer func() { uploadRetryFirst = old }()
	m := newTestMeta(t)
	ctx := context.Background()
	pool, dir := newQueuePool(t, 2, 1<<20)
	up := newStageUploader()
	up.fails.Store(6) // one whole flush fails (5 attempts), the next succeeds
	packer := NewPacker(m, up, 0, 1<<20, 20*time.Millisecond)
	packer.SetPendingIndex(pool.PendingIndex)
	packer.SetDiskGate(pool.Disk)
	packer.retryBase = 10 * time.Millisecond
	t.Cleanup(packer.Close)
	ino := newQueueFile(t, m, "f", 64)
	w, err := NewWriter(ctx, m, newTestCache(t, nil), up, nil, nil, pool, packer, ino, 64, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteAt(ctx, bytes.Repeat([]byte("p"), 320), 0); err != nil { // 5 chunks
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- w.Sync(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Sync did not return")
	}
	rows, _ := m.ListChunks(ctx, ino)
	if len(rows) != 5 {
		t.Fatalf("chunk rows %d, want 5", len(rows))
	}
	for _, r := range rows {
		if r.PackSize == 0 {
			t.Fatalf("row %+v is not packed", r)
		}
	}
	if n, _ := dirBytes(t, dir); n != 0 {
		t.Fatalf("staging directory holds %d B", n)
	}
	if used, _, _ := pool.Disk.Stats(); used != 0 {
		t.Fatalf("gate still counts %d B", used)
	}
}

// Recovery hands staged wires to the packer by reference: nothing is read
// into memory until the flush, and the row lands from the file alone.
func TestQueueModeRecoveredWireByReference(t *testing.T) {
	m := newTestMeta(t)
	pool, dir := newQueuePool(t, 1, 1<<20)
	up := newStageUploader()
	packer := NewPacker(m, up, 0, 1<<20, 10*time.Millisecond)
	packer.SetPendingIndex(pool.PendingIndex)
	packer.SetDiskGate(pool.Disk)
	t.Cleanup(packer.Close)
	ino := newQueueFile(t, m, "f", 64)
	wire := []byte("recovered wire bytes")
	path, err := PersistPendingHashed(dir, ino, 0, int32(len(wire)), nil, wire)
	if err != nil {
		t.Fatal(err)
	}
	pool.Disk.Add(int64(len(wire)))
	got := make(chan packResult, 1)
	packer.SubmitStaged(ino, 0, int32(len(wire)), int64(len(wire)), path, nil, "", 0, func(r packResult) { got <- r })
	select {
	case r := <-got:
		if r.err != nil {
			t.Fatal(r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("staged wire never flushed")
	}
	if n := countRows(t, m, ino); n != 1 {
		t.Fatalf("chunk rows %d, want 1", n)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("staged wire still on disk: %v", err)
	}
	if used, _, _ := pool.Disk.Stats(); used != 0 {
		t.Fatalf("gate still counts %d B", used)
	}
	up.mu.Lock()
	defer up.mu.Unlock()
	if len(up.uploads) != 1 || !bytes.Contains(up.uploads[1], wire) {
		t.Fatalf("uploaded %d blob(s); want one holding the staged wire", len(up.uploads))
	}
}

// Below the free-space floor with nothing staged, waiting could never end:
// the write fails (ENOSPC at the file layer) instead of hanging or filling
// the disk.
func TestQueueModeStagingFloorFailsWrites(t *testing.T) {
	m := newTestMeta(t)
	ctx := context.Background()
	pool, dir := newQueuePool(t, 1, 0)
	pool.Disk = NewDiskGate(dir, 0, 1<<30)
	pool.Disk.statfs = func(string) (int64, error) { return 1 << 20, nil } // 1 MiB free, floor 1 GiB
	ino := newQueueFile(t, m, "f", 64)
	w, err := NewWriter(ctx, m, newTestCache(t, nil), newStageUploader(), nil, nil, pool, nil, ino, 64, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteAt(ctx, []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	_ = w.Flush(ctx) // dispatch is asynchronous; stage 1 fails behind it
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err := w.WriteAt(ctx, []byte("more"), 4)
		if errors.Is(err, ErrStagingFull) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("WriteAt err %v, want ErrStagingFull", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := w.Flush(ctx); !errors.Is(err, ErrStagingFull) {
		t.Fatalf("Flush err %v, want ErrStagingFull", err)
	}
	if n, _ := dirBytes(t, dir); n != 0 {
		t.Fatalf("something was staged (%d B) below the floor", n)
	}
}

// Remove releases a staged file's bytes once, however many deleters race.
func TestDiskGateRemoveReleasesOnce(t *testing.T) {
	dir := t.TempDir()
	g := NewDiskGate(dir, 0, 0)
	a, b := filepath.Join(dir, "a"), filepath.Join(dir, "b")
	os.WriteFile(a, make([]byte, 10), 0o600)
	os.WriteFile(b, make([]byte, 20), 0o600)
	g.Add(30)
	g.Remove(a)
	g.Remove(a)
	if used, _, _ := g.Stats(); used != 20 {
		t.Fatalf("used %d, want 20", used)
	}
}

// A file deleted while its full-size chunks wait in the disk-backed queue:
// the waiting chunks are dropped, never uploaded (S9, 2026-10-30: every one
// was uploaded and then "dropped orphan chunk … kept for gc"). Only the
// upload already in flight lands, and is dropped as an orphan.
func TestQueueModeDeletedFileIsNotUploaded(t *testing.T) {
	m := newTestMeta(t)
	ctx := context.Background()
	pool, dir := newQueuePool(t, 1, 1<<20)
	m.SetInodeDeletedCallback(func(ino int64) { DropStagedInode(nil, pool.PendingIndex, pool.Disk, ino) })
	up := newStageUploader()
	up.gate = make(chan struct{})
	ino := newQueueFile(t, m, "f", 64)
	w, err := NewWriter(ctx, m, newTestCache(t, nil), up, nil, nil, pool, nil, ino, 64, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteAt(ctx, bytes.Repeat([]byte("x"), 64*4), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor := func(what string, ok func() bool) {
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); !ok(); time.Sleep(5 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatal("timed out waiting for " + what)
			}
		}
	}
	// One upload in flight (held by the gate), three staged behind it.
	waitFor("4 staged wires and 1 upload in flight", func() bool {
		_, files := dirBytes(t, dir)
		return files == 4 && up.calls.Load() == 1
	})
	if err := m.Unlink(ctx, meta.RootIno, "f"); err != nil {
		t.Fatal(err)
	}
	// The callback runs asynchronously; the in-flight wire is removed too
	// (its upload already holds the bytes).
	waitFor("the deleted file's staged wires to go", func() bool { _, files := dirBytes(t, dir); return files == 0 })
	close(up.gate)
	waitFor("the queue to drain", func() bool { open, _ := pool.OpenStats(); return open == 0 })
	if c := up.calls.Load(); c != 1 {
		t.Fatalf("uploader called %d times, want 1 (only the upload already in flight)", c)
	}
	if used, _, _ := pool.Disk.Stats(); used != 0 {
		t.Fatalf("staging gate still counts %d B", used)
	}
	if n := countRows(t, m, ino); n != 0 {
		t.Fatalf("%d chunk rows for a deleted file", n)
	}
	w.Close()
}
