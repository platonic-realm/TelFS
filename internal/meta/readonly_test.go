package meta

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// OpenReadOnly never creates a DB, refuses every write, reads beside a
// live writer, and (immutable) leaves no sidecar files.
func TestOpenReadOnly(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.sqlite")
	if _, err := OpenReadOnly(missing, false); err == nil {
		t.Fatal("a missing DB must be an error")
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("OpenReadOnly created the DB")
	}
	path := filepath.Join(dir, "a b.sqlite") // a space: the path goes through a file: URI
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	w.PutKV(ctx, "k", []byte("v1"))
	r, err := OpenReadOnly(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	w.PutKV(ctx, "k", []byte("v2")) // the writer keeps writing
	if v, err := r.GetKV(ctx, "k"); err != nil || string(v) != "v2" {
		t.Fatalf("read beside a writer: %q %v", v, err)
	}
	if err := r.PutKV(ctx, "x", []byte("y")); err == nil {
		t.Fatal("a read-only store accepted a write")
	}

	copyPath := filepath.Join(t.TempDir(), "copy.sqlite")
	if _, err := w.DB().ExecContext(ctx, "VACUUM INTO '"+copyPath+"'"); err != nil {
		t.Fatal(err)
	}
	im, err := OpenReadOnly(copyPath, true)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := im.GetKV(ctx, "k"); err != nil || string(v) != "v2" {
		t.Fatalf("immutable read: %q %v", v, err)
	}
	im.Close()
	ents, _ := os.ReadDir(filepath.Dir(copyPath))
	if len(ents) != 1 {
		t.Fatalf("immutable open left files behind: %v", ents)
	}
	junk := filepath.Join(dir, "junk")
	os.WriteFile(junk, []byte("not a database"), 0o600)
	if _, err := OpenReadOnly(junk, true); err == nil {
		t.Fatal("a non-DB file must be refused")
	}
}
