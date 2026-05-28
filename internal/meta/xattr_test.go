package meta

import (
	"bytes"
	"errors"
	"testing"
)

func TestXattrRoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	ino := mkfile(t, s, RootIno, "f")
	if err := s.SetXattr(ctx, ino, "user.tag", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetXattr(ctx, ino, "user.tag")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("got %q, want %q", got, "hello")
	}
	// Overwrite.
	_ = s.SetXattr(ctx, ino, "user.tag", []byte("world"))
	got, _ = s.GetXattr(ctx, ino, "user.tag")
	if !bytes.Equal(got, []byte("world")) {
		t.Fatalf("after overwrite: %q", got)
	}
	// List + remove.
	_ = s.SetXattr(ctx, ino, "user.other", []byte("x"))
	names, _ := s.ListXattrs(ctx, ino)
	if len(names) != 2 || names[0] != "user.other" || names[1] != "user.tag" {
		t.Fatalf("ListXattrs = %v", names)
	}
	if err := s.RemoveXattr(ctx, ino, "user.tag"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetXattr(ctx, ino, "user.tag"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("xattr survived remove: %v", err)
	}
}

func TestXattrCascadeOnInodeDelete(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	ino := mkfile(t, s, RootIno, "f")
	_ = s.SetXattr(ctx, ino, "user.tag", []byte("v"))
	if err := s.Unlink(ctx, RootIno, "f"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetXattr(ctx, ino, "user.tag"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("xattr survived inode delete: %v", err)
	}
}
