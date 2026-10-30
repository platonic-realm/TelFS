package chunk

import (
	"context"
	"errors"
	"os"
	"sync"
	"syscall"
	"time"
)

// ErrStagingFull: the staging directory's filesystem is below its free-space
// floor and nothing TelFS staged could free it by uploading — waiting would
// never end, so the write fails (the file layer maps it to ENOSPC).
var ErrStagingFull = errors.New("staging area full: the staging filesystem is below its free-space floor")

// DefaultStagingMaxBytes and DefaultStagingMinFree are the v1 defaults
// (spec v1, row 9): up to 50 GB of written-but-not-yet-uploaded data on
// disk, and never fill the staging filesystem below 5 GiB free.
const (
	DefaultStagingMaxBytes int64 = 50_000_000_000
	DefaultStagingMinFree  int64 = 5 << 30
)

// DiskGate bounds the bytes staged in the pending directory — written
// through the mount, not yet uploaded (spec v1, row 9). A wire reserves its
// size before it is written to disk and gives it back when its file is
// removed after the upload landed (Remove). Above the cap, Acquire blocks
// until uploads catch up; the writer's dispatch path then blocks too once
// the in-memory hand-off (DefaultStagedCapBytes) is full, so a 400 GB
// rsync slows to upload speed instead of filling the disk.
type DiskGate struct {
	dir   string
	cap   int64 // 0 = no cap
	floor int64 // keep at least this much free on dir's filesystem (0 = off)

	mu      sync.Mutex
	used    int64
	waiters int
	changed chan struct{}

	// statfs is replaceable in tests.
	statfs func(dir string) (free int64, err error)
}

// NewDiskGate makes a gate for the staging directory dir.
func NewDiskGate(dir string, capBytes, minFree int64) *DiskGate {
	return &DiskGate{dir: dir, cap: capBytes, floor: minFree, changed: make(chan struct{}), statfs: freeBytes}
}

func freeBytes(dir string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return int64(st.Bavail) * int64(st.Bsize), nil
}

// Acquire reserves n bytes for a wire about to be staged. It blocks while
// the staged bytes would exceed the cap (a single wire larger than the cap
// passes when nothing else is staged) or while the staging filesystem would
// drop below its floor; it fails with ErrStagingFull when the floor is the
// problem and nothing is staged (no upload could free space).
func (g *DiskGate) Acquire(ctx context.Context, n int64) error {
	for {
		g.mu.Lock()
		capOK := g.cap <= 0 || g.used == 0 || g.used+n <= g.cap
		floorOK := true
		if capOK && g.floor > 0 {
			if free, err := g.statfs(g.dir); err == nil && free-n < g.floor {
				floorOK = false
			}
		}
		if capOK && floorOK {
			g.used += n
			g.mu.Unlock()
			return nil
		}
		if !floorOK && g.used == 0 {
			g.mu.Unlock()
			return ErrStagingFull
		}
		g.waiters++
		ch := g.changed
		g.mu.Unlock()
		// Woken by a Release; the timer re-checks the floor (space can
		// be freed by others too).
		t := time.NewTimer(time.Second)
		select {
		case <-ch:
		case <-t.C:
		case <-ctx.Done():
		}
		t.Stop()
		g.mu.Lock()
		g.waiters--
		g.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

// Add counts wires that are already staged (found at mount by recovery).
// It never blocks: they are on disk whether the cap likes it or not.
func (g *DiskGate) Add(n int64) {
	g.mu.Lock()
	g.used += n
	g.mu.Unlock()
}

// Release gives back n bytes and wakes waiters.
func (g *DiskGate) Release(n int64) {
	if n <= 0 {
		return
	}
	g.mu.Lock()
	g.used -= n
	if g.used < 0 {
		g.used = 0
	}
	close(g.changed)
	g.changed = make(chan struct{})
	g.mu.Unlock()
}

// Remove deletes a staged wire and releases its bytes — only when THIS call
// removed the file, so two deleters (the uploader and an unlink, say) can
// never release the same bytes twice. A nil gate just deletes.
func (g *DiskGate) Remove(path string) bool {
	if path == "" {
		return false
	}
	var size int64
	if fi, err := os.Stat(path); err == nil {
		size = fi.Size()
	}
	if err := os.Remove(path); err != nil {
		return false // already gone (or not removable): nothing to release
	}
	if g != nil {
		g.Release(size)
	}
	return true
}

// Stats reports the staged bytes, the cap and whether a writer is waiting.
func (g *DiskGate) Stats() (used, capBytes int64, blocked bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.used, g.cap, g.waiters > 0
}

// Dir is the staging directory.
func (g *DiskGate) Dir() string { return g.dir }

// Room is what can still be written before writes fail: the staging
// filesystem's free space above the min_free floor (the staging cap only
// makes writes wait for uploads, it never fails them). ok is false when
// the free space cannot be read. statfs(2) of the mount reports it as the
// free space (v1.1 S9).
func (g *DiskGate) Room() (room int64, ok bool) {
	if g == nil {
		return 0, false
	}
	free, err := g.statfs(g.dir)
	if err != nil {
		return 0, false
	}
	if room = free - g.floor; room < 0 {
		room = 0
	}
	return room, true
}
