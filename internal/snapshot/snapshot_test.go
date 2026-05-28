package snapshot

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"telfs/internal/meta"
)

func TestRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.sqlite")

	// Build a small DB.
	src, err := meta.Open(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	ino, err := src.CreateChild(ctx, meta.RootIno, "alpha.txt", meta.Inode{
		Kind: meta.KindFile, Mode: 0o100644, Size: 42,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := src.SetXattr(ctx, ino, "user.tag", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	uuid, _ := src.FSUUID(ctx)

	// Snapshot.
	snap, err := Take(ctx, src)
	if err != nil {
		t.Fatalf("Take: %v", err)
	}
	if snap.FSUUID != uuid {
		t.Fatalf("snap.FSUUID = %q, want %q", snap.FSUUID, uuid)
	}
	if len(snap.Bytes) == 0 {
		t.Fatal("snap.Bytes empty")
	}
	src.Close()

	// Restore to a different path.
	dstPath := filepath.Join(dir, "restored.sqlite")
	if err := Restore(ctx, snap.Bytes, dstPath); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Verify the restored DB has the expected contents.
	restored, err := meta.Open(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()

	got, err := restored.Lookup(ctx, meta.RootIno, "alpha.txt")
	if err != nil {
		t.Fatalf("Lookup after restore: %v", err)
	}
	if got.Size != 42 {
		t.Fatalf("restored size = %d, want 42", got.Size)
	}
	v, err := restored.GetXattr(ctx, got.Ino, "user.tag")
	if err != nil {
		t.Fatalf("xattr after restore: %v", err)
	}
	if string(v) != "hello" {
		t.Fatalf("restored xattr = %q", v)
	}
	// FSUUID preserved.
	restoredUUID, _ := restored.FSUUID(ctx)
	if restoredUUID != uuid {
		t.Fatalf("restored UUID = %q, want %q", restoredUUID, uuid)
	}
}

func TestRestoreRejectsCorruptBytes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dstPath := filepath.Join(dir, "out.sqlite")
	err := Restore(ctx, []byte("not gzipped at all"), dstPath)
	if err == nil {
		t.Fatalf("Restore should have failed on garbage input")
	}
	if _, err := os.Stat(dstPath); !os.IsNotExist(err) {
		t.Fatalf("Restore created %s despite failure (err=%v)", dstPath, err)
	}
}
