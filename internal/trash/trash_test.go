package trash

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"telfs/internal/meta"
)

// newTestStore opens a fresh meta.Store in a tempdir.
func newTestStore(t *testing.T) *meta.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "telfs.sqlite")
	s, err := meta.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// addFile creates a regular file child of parent with the given size
// and mtime. Returns the new inode number.
func addFile(t *testing.T, s *meta.Store, parent int64, name string, size int64, mtime int64) int64 {
	t.Helper()
	in := meta.Inode{
		Kind:  meta.KindFile,
		Mode:  0o100644,
		Nlink: 1,
		Size:  size,
		Mtime: mtime,
		Ctime: mtime,
	}
	ino, err := s.CreateChild(context.Background(), parent, name, in)
	if err != nil {
		t.Fatalf("CreateChild(%q): %v", name, err)
	}
	return ino
}

func TestEnsureRootDirCreates(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	ino, err := EnsureRootDir(ctx, s)
	if err != nil {
		t.Fatalf("EnsureRootDir: %v", err)
	}
	if ino == 0 {
		t.Fatal("ino should be non-zero")
	}
	// Looking it up returns the same ino.
	in, err := s.Lookup(ctx, meta.RootIno, DirName)
	if err != nil {
		t.Fatal(err)
	}
	if in.Ino != ino {
		t.Errorf("lookup returned %d, want %d", in.Ino, ino)
	}
	if in.Kind != meta.KindDir {
		t.Errorf("kind: got %s, want dir", in.Kind)
	}
}

func TestEnsureRootDirIdempotent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	a, _ := EnsureRootDir(ctx, s)
	b, _ := EnsureRootDir(ctx, s)
	if a != b {
		t.Errorf("EnsureRootDir is not idempotent: %d vs %d", a, b)
	}
}

func TestMoveToTrashRenames(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	trashIno, _ := EnsureRootDir(ctx, s)
	fileIno := addFile(t, s, meta.RootIno, "victim.txt", 100, time.Now().Unix())

	if err := MoveToTrash(ctx, s, trashIno, meta.RootIno, "victim.txt"); err != nil {
		t.Fatalf("MoveToTrash: %v", err)
	}
	// victim.txt is gone from root.
	if _, err := s.Lookup(ctx, meta.RootIno, "victim.txt"); err == nil {
		t.Errorf("victim.txt still resolves under root after move-to-trash")
	}
	// One entry now lives under /.trash with a timestamped prefix and
	// the same inode (so its data is preserved).
	ents, err := s.Readdir(ctx, trashIno)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 1 {
		t.Fatalf("expected 1 entry in /.trash, got %d", len(ents))
	}
	if ents[0].ChildIno != fileIno {
		t.Errorf("inode changed by trash move: got %d, want %d", ents[0].ChildIno, fileIno)
	}
	// Name preserves the original suffix.
	if !endsWith(ents[0].Name, "-victim.txt") {
		t.Errorf("trashed name should end with -victim.txt: %q", ents[0].Name)
	}
}

func TestGCOnceDropsExpired(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	trashIno, _ := EnsureRootDir(ctx, s)

	now := time.Now()
	// Two entries directly placed under /.trash: one ancient, one fresh.
	addFile(t, s, trashIno, "old.txt", 10, now.Add(-30*24*time.Hour).Unix())
	addFile(t, s, trashIno, "new.txt", 10, now.Unix())

	removed, err := GCOnce(ctx, s, trashIno, 7*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Errorf("GCOnce removed %d, want 1", removed)
	}
	ents, _ := s.Readdir(ctx, trashIno)
	if len(ents) != 1 || ents[0].Name != "new.txt" {
		t.Errorf("after GC: expected just new.txt, got %+v", ents)
	}
}

func TestGCOnceKeepsAllWhenTTLLong(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	trashIno, _ := EnsureRootDir(ctx, s)

	addFile(t, s, trashIno, "a.txt", 1, time.Now().Unix())
	addFile(t, s, trashIno, "b.txt", 1, time.Now().Unix())

	removed, err := GCOnce(ctx, s, trashIno, 365*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Errorf("expected 0 removed under long ttl, got %d", removed)
	}
}

// TestUniqueNameTwoCallsSameSecond covers the collision case at the
// nanosecond level — calls within the same second should still
// produce distinct names because we use UnixNano.
func TestUniqueNameTwoCalls(t *testing.T) {
	t1 := time.Date(2026, 5, 29, 0, 0, 0, 1, time.UTC)
	t2 := time.Date(2026, 5, 29, 0, 0, 0, 2, time.UTC)
	a := uniqueName(t1, "file.txt")
	b := uniqueName(t2, "file.txt")
	if a == b {
		t.Errorf("uniqueName collided: %q == %q", a, b)
	}
}

func endsWith(s, suffix string) bool {
	if len(s) < len(suffix) {
		return false
	}
	return s[len(s)-len(suffix):] == suffix
}
