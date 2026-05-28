package meta

import (
	"context"
	"path/filepath"
	"testing"
)

// newTestStore opens a fresh Store backed by a temp-dir DB file. The
// store is closed automatically via t.Cleanup.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "telfs.sqlite")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	return context.Background()
}

func TestOpenCreatesRoot(t *testing.T) {
	s := newTestStore(t)
	root, err := s.GetInode(ctxT(t), RootIno)
	if err != nil {
		t.Fatalf("GetInode root: %v", err)
	}
	if root.Ino != RootIno {
		t.Fatalf("root ino = %d, want %d", root.Ino, RootIno)
	}
	if root.Kind != KindDir {
		t.Fatalf("root kind = %q, want dir", root.Kind)
	}
	if root.Mode&0o777 != 0o755 {
		t.Fatalf("root mode = %o, want 0755 perm bits", root.Mode)
	}
}

func TestOpenIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "telfs.sqlite")
	s1, err := Open(path)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	// Create a file so we can check it survives reopen.
	ino, err := s1.CreateChild(ctxT(t), RootIno, "stable.txt", Inode{Kind: KindFile, Mode: 0o100644})
	if err != nil {
		t.Fatalf("CreateChild: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer s2.Close()
	got, err := s2.GetInode(ctxT(t), ino)
	if err != nil {
		t.Fatalf("GetInode after reopen: %v", err)
	}
	if got.Kind != KindFile {
		t.Fatalf("kind after reopen = %q, want file", got.Kind)
	}
}
