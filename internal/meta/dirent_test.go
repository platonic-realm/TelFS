package meta

import (
	"errors"
	"sort"
	"testing"
)

// mkdir creates a child directory under parent and returns its ino.
func mkdir(t *testing.T, s *Store, parent int64, name string) int64 {
	t.Helper()
	ino, err := s.CreateChild(ctxT(t), parent, name, Inode{Kind: KindDir, Mode: 0o40755})
	if err != nil {
		t.Fatalf("mkdir %d/%s: %v", parent, name, err)
	}
	return ino
}

// mkfile creates a child regular file under parent and returns its ino.
func mkfile(t *testing.T, s *Store, parent int64, name string) int64 {
	t.Helper()
	ino, err := s.CreateChild(ctxT(t), parent, name, Inode{Kind: KindFile, Mode: 0o100644})
	if err != nil {
		t.Fatalf("create %d/%s: %v", parent, name, err)
	}
	return ino
}

func TestLookupNotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Lookup(ctxT(t), RootIno, "ghost")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestCreateChildRejectsDuplicateName(t *testing.T) {
	s := newTestStore(t)
	mkfile(t, s, RootIno, "a.txt")
	_, err := s.CreateChild(ctxT(t), RootIno, "a.txt", Inode{Kind: KindFile, Mode: 0o100644})
	if !errors.Is(err, ErrExists) {
		t.Fatalf("err = %v, want ErrExists", err)
	}
}

func TestCreateChildRejectsNonDirParent(t *testing.T) {
	s := newTestStore(t)
	fileIno := mkfile(t, s, RootIno, "a.txt")
	_, err := s.CreateChild(ctxT(t), fileIno, "x", Inode{Kind: KindFile, Mode: 0o100644})
	if !errors.Is(err, ErrNotDir) {
		t.Fatalf("err = %v, want ErrNotDir", err)
	}
}

func TestReaddirInfoReturnsKindAndMode(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	_, _ = s.CreateChild(ctx, RootIno, "f", Inode{Kind: KindFile, Mode: 0o100644, Size: 17})
	_, _ = s.CreateChild(ctx, RootIno, "d", Inode{Kind: KindDir, Mode: 0o40755})
	_, _ = s.CreateChild(ctx, RootIno, "l", Inode{Kind: KindSymlink, Mode: 0o120777, SymlinkTarget: "f"})
	infos, err := s.ReaddirInfo(ctx, RootIno)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 3 {
		t.Fatalf("got %d entries, want 3", len(infos))
	}
	// Returned in name order.
	if infos[0].Name != "d" || infos[1].Name != "f" || infos[2].Name != "l" {
		t.Fatalf("order = %s/%s/%s, want d/f/l", infos[0].Name, infos[1].Name, infos[2].Name)
	}
	if infos[0].Kind != KindDir || infos[1].Kind != KindFile || infos[2].Kind != KindSymlink {
		t.Fatalf("kinds wrong: %+v", infos)
	}
	if infos[1].Size != 17 {
		t.Fatalf("file size = %d, want 17", infos[1].Size)
	}
}

func TestReaddirReturnsChildren(t *testing.T) {
	s := newTestStore(t)
	mkfile(t, s, RootIno, "a")
	mkfile(t, s, RootIno, "b")
	mkdir(t, s, RootIno, "c")
	entries, err := s.Readdir(ctxT(t), RootIno)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	sort.Strings(names)
	want := []string{"a", "b", "c"}
	if len(names) != 3 || names[0] != want[0] || names[1] != want[1] || names[2] != want[2] {
		t.Fatalf("names = %v, want %v", names, want)
	}
}

func TestLinkBumpsNlink(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	ino := mkfile(t, s, RootIno, "orig")
	if err := s.Link(ctx, RootIno, "hard", ino); err != nil {
		t.Fatalf("Link: %v", err)
	}
	got, _ := s.GetInode(ctx, ino)
	if got.Nlink != 2 {
		t.Fatalf("nlink = %d, want 2", got.Nlink)
	}
	// Both names should resolve to the same ino.
	a, _ := s.Lookup(ctx, RootIno, "orig")
	b, _ := s.Lookup(ctx, RootIno, "hard")
	if a.Ino != b.Ino {
		t.Fatalf("orig.Ino=%d, hard.Ino=%d", a.Ino, b.Ino)
	}
}

func TestLinkOnDirRejected(t *testing.T) {
	s := newTestStore(t)
	d := mkdir(t, s, RootIno, "d")
	err := s.Link(ctxT(t), RootIno, "alias", d)
	if !errors.Is(err, ErrIsDir) {
		t.Fatalf("err = %v, want ErrIsDir", err)
	}
}

func TestUnlinkFileDeletesInodeWhenNlinkZero(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	ino := mkfile(t, s, RootIno, "f")
	if err := s.Unlink(ctx, RootIno, "f"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetInode(ctx, ino); !errors.Is(err, ErrNotFound) {
		t.Fatalf("inode still present: %v", err)
	}
}

func TestUnlinkHardlinkPreservesInode(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	ino := mkfile(t, s, RootIno, "orig")
	if err := s.Link(ctx, RootIno, "alias", ino); err != nil {
		t.Fatal(err)
	}
	if err := s.Unlink(ctx, RootIno, "orig"); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetInode(ctx, ino)
	if err != nil {
		t.Fatalf("inode disappeared though alias still exists: %v", err)
	}
	if got.Nlink != 1 {
		t.Fatalf("nlink = %d, want 1", got.Nlink)
	}
	via, err := s.Lookup(ctx, RootIno, "alias")
	if err != nil || via.Ino != ino {
		t.Fatalf("alias broken: ino=%d err=%v", via.Ino, err)
	}
}

func TestUnlinkNonEmptyDirRejected(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	d := mkdir(t, s, RootIno, "d")
	mkfile(t, s, d, "inner")
	err := s.Unlink(ctx, RootIno, "d")
	if !errors.Is(err, ErrNotEmpty) {
		t.Fatalf("err = %v, want ErrNotEmpty", err)
	}
}

// TestRenameOverwriteFile is the discriminator test the advisor flagged:
// rename of file A onto an existing file B must replace B atomically.
// B's inode (and its chunks/xattrs) must be gone afterwards; A's name must
// no longer resolve.
func TestRenameOverwriteFile(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	srcIno := mkfile(t, s, RootIno, "src")
	dstIno := mkfile(t, s, RootIno, "dst")
	// Give dst some chunks and an xattr so we can verify cascade cleanup.
	if err := s.PutChunk(ctx, Chunk{Ino: dstIno, Idx: 0, TGMessageID: 111, Size: 4096}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetXattr(ctx, dstIno, "user.tag", []byte("doomed")); err != nil {
		t.Fatal(err)
	}

	if err := s.Rename(ctx, RootIno, "src", RootIno, "dst"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	// src name gone.
	if _, err := s.Lookup(ctx, RootIno, "src"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("src still resolves: %v", err)
	}
	// dst name now points at src's inode.
	got, err := s.Lookup(ctx, RootIno, "dst")
	if err != nil {
		t.Fatalf("dst lookup: %v", err)
	}
	if got.Ino != srcIno {
		t.Fatalf("dst.Ino = %d, want srcIno=%d", got.Ino, srcIno)
	}
	// Old dst inode gone.
	if _, err := s.GetInode(ctx, dstIno); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old dst inode still present: %v", err)
	}
	// Its chunks and xattrs cascaded away.
	chunks, _ := s.ListChunks(ctx, dstIno)
	if len(chunks) != 0 {
		t.Fatalf("orphan chunks left: %+v", chunks)
	}
	if _, err := s.GetXattr(ctx, dstIno, "user.tag"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("orphan xattr left: %v", err)
	}
}

func TestRenameFileOntoDirRejected(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	mkfile(t, s, RootIno, "f")
	mkdir(t, s, RootIno, "d")
	err := s.Rename(ctx, RootIno, "f", RootIno, "d")
	if !errors.Is(err, ErrIsDir) {
		t.Fatalf("err = %v, want ErrIsDir", err)
	}
}

func TestRenameDirOntoFileRejected(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	mkdir(t, s, RootIno, "d")
	mkfile(t, s, RootIno, "f")
	err := s.Rename(ctx, RootIno, "d", RootIno, "f")
	if !errors.Is(err, ErrNotDir) {
		t.Fatalf("err = %v, want ErrNotDir", err)
	}
}

func TestRenameDirOntoNonEmptyDirRejected(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	mkdir(t, s, RootIno, "src")
	dst := mkdir(t, s, RootIno, "dst")
	mkfile(t, s, dst, "inside")
	err := s.Rename(ctx, RootIno, "src", RootIno, "dst")
	if !errors.Is(err, ErrNotEmpty) {
		t.Fatalf("err = %v, want ErrNotEmpty", err)
	}
}

func TestRenameDirOntoEmptyDirReplaces(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	srcDir := mkdir(t, s, RootIno, "src")
	mkfile(t, s, srcDir, "payload")
	dst := mkdir(t, s, RootIno, "dst") // empty
	if err := s.Rename(ctx, RootIno, "src", RootIno, "dst"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	// dst should now be the moved srcDir inode.
	got, _ := s.Lookup(ctx, RootIno, "dst")
	if got.Ino != srcDir {
		t.Fatalf("dst.Ino = %d, want %d", got.Ino, srcDir)
	}
	// old empty dst inode is gone.
	if _, err := s.GetInode(ctx, dst); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old dst inode still present: %v", err)
	}
}

func TestRenameAncestorCycleRejected(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	d1 := mkdir(t, s, RootIno, "d1")
	mkdir(t, s, d1, "d2")
	// Try to move d1 into d1/d2 — would create a cycle.
	err := s.Rename(ctx, RootIno, "d1", d1, "d2-newhome")
	if err == nil {
		t.Fatalf("expected cycle error")
	}
}

// Per POSIX rename(2), renaming one hardlink onto another hardlink of the
// same inode is a no-op: both names continue to exist and nlink is
// unchanged. This matches Linux's behavior — `mv a b` when both already
// point to the same file does not remove `a`.
func TestRenameSameInodeIsNoOp(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	ino := mkfile(t, s, RootIno, "a")
	if err := s.Link(ctx, RootIno, "b", ino); err != nil {
		t.Fatal(err)
	}
	if err := s.Rename(ctx, RootIno, "a", RootIno, "b"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	// Both names should still resolve, nlink stays at 2.
	if _, err := s.Lookup(ctx, RootIno, "a"); err != nil {
		t.Fatalf("a stopped resolving: %v", err)
	}
	got, _ := s.Lookup(ctx, RootIno, "b")
	if got.Ino != ino {
		t.Fatalf("b broken")
	}
	if got.Nlink != 2 {
		t.Fatalf("nlink = %d, want 2 (no-op)", got.Nlink)
	}
}

func TestRenameNoOp(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	ino := mkfile(t, s, RootIno, "x")
	if err := s.Rename(ctx, RootIno, "x", RootIno, "x"); err != nil {
		t.Fatalf("Rename no-op: %v", err)
	}
	got, _ := s.Lookup(ctx, RootIno, "x")
	if got.Ino != ino {
		t.Fatalf("rename no-op changed ino")
	}
}
