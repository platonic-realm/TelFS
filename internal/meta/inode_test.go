package meta

import (
	"errors"
	"testing"
	"time"
)

func TestCreateInodeAssignsIDAndDefaults(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	before := time.Now().Unix()
	ino, err := s.CreateInode(ctx, Inode{Kind: KindFile, Mode: 0o100644})
	if err != nil {
		t.Fatalf("CreateInode: %v", err)
	}
	if ino <= RootIno {
		t.Fatalf("ino = %d, want > %d", ino, RootIno)
	}
	got, err := s.GetInode(ctx, ino)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if got.Nlink != 1 {
		t.Fatalf("default nlink = %d, want 1", got.Nlink)
	}
	if got.Mtime < before || got.Ctime < before {
		t.Fatalf("times not auto-stamped: mtime=%d ctime=%d before=%d", got.Mtime, got.Ctime, before)
	}
}

func TestGetInodeNotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.GetInode(ctxT(t), 99999)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestSetAttrsAdvancesCtime(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	ino, err := s.CreateInode(ctx, Inode{Kind: KindFile, Mode: 0o100644, Ctime: 1000})
	if err != nil {
		t.Fatal(err)
	}
	mode := uint32(0o100600)
	if err := s.SetAttrs(ctx, ino, SetAttrsPatch{Mode: &mode}); err != nil {
		t.Fatalf("SetAttrs: %v", err)
	}
	got, err := s.GetInode(ctx, ino)
	if err != nil {
		t.Fatal(err)
	}
	if got.Mode != mode {
		t.Fatalf("mode = %o, want %o", got.Mode, mode)
	}
	if got.Ctime <= 1000 {
		t.Fatalf("ctime not advanced: %d", got.Ctime)
	}
}

func TestSymlinkRoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	ino, err := s.CreateChild(ctx, RootIno, "link", Inode{
		Kind:          KindSymlink,
		Mode:          0o120777,
		SymlinkTarget: "/etc/hosts",
	})
	if err != nil {
		t.Fatalf("CreateChild symlink: %v", err)
	}
	got, err := s.GetInode(ctx, ino)
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != KindSymlink {
		t.Fatalf("kind = %q, want symlink", got.Kind)
	}
	if got.SymlinkTarget != "/etc/hosts" {
		t.Fatalf("target = %q, want /etc/hosts", got.SymlinkTarget)
	}
	// Symlinks resolve via dirent like any other inode.
	via, err := s.Lookup(ctx, RootIno, "link")
	if err != nil {
		t.Fatal(err)
	}
	if via.SymlinkTarget != "/etc/hosts" {
		t.Fatalf("Lookup -> target = %q", via.SymlinkTarget)
	}
}

func TestSetSizeUpdatesSizeAndMtime(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	ino, err := s.CreateInode(ctx, Inode{Kind: KindFile, Mode: 0o100644, Mtime: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetSize(ctx, ino, 4096); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetInode(ctx, ino)
	if err != nil {
		t.Fatal(err)
	}
	if got.Size != 4096 {
		t.Fatalf("size = %d, want 4096", got.Size)
	}
	if got.Mtime <= 1000 {
		t.Fatalf("mtime not advanced: %d", got.Mtime)
	}
}
