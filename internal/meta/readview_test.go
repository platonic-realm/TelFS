package meta

import (
	"context"
	"path/filepath"
	"testing"
)

// A ReadView is one snapshot: writes committed by the live writer after
// its first query stay invisible to it, on a read-only store beside the
// writer and on the writer's own store alike.
func TestReadViewIsConsistent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db.sqlite")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	a, err := w.CreateChild(ctx, RootIno, "a", Inode{Kind: KindFile, Mode: 0o100644, Size: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.PutChunk(ctx, Chunk{Ino: a, Idx: 0, TGMessageID: 7, Size: 3, Hash: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	if err := w.SetXattr(ctx, a, "user.x", []byte("v")); err != nil {
		t.Fatal(err)
	}
	ro, err := OpenReadOnly(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	for name, s := range map[string]*Store{"read-only": ro, "writer": w} {
		err := s.Read(ctx, func(v *ReadView) error {
			kids, err := v.Children(ctx, RootIno)
			if err != nil {
				return err
			}
			// The writer goes on: a new file, a new chunk row.
			b, err := w.CreateChild(ctx, RootIno, "b-"+name, Inode{Kind: KindFile, Mode: 0o100644})
			if err != nil {
				return err
			}
			if err := w.PutChunk(ctx, Chunk{Ino: a, Idx: 1, TGMessageID: 8, Size: 1}); err != nil {
				return err
			}
			kids2, err := v.Children(ctx, RootIno)
			if err != nil {
				return err
			}
			if len(kids2) != len(kids) {
				t.Errorf("%s: the view saw a later create (%d → %d entries)", name, len(kids), len(kids2))
			}
			if _, err := v.GetInode(ctx, b); err != ErrNotFound {
				t.Errorf("%s: the view saw inode %d created after it began: %v", name, b, err)
			}
			cs, err := v.Chunks(ctx, a)
			if err != nil {
				return err
			}
			if len(cs) != 1 {
				t.Errorf("%s: chunks %v, want only the first", name, cs)
			}
			in, err := v.Lookup(ctx, RootIno, "a")
			if err != nil || in.Ino != a || in.Size != 3 {
				t.Errorf("%s: lookup a = %+v, %v", name, in, err)
			}
			xs, err := v.AllXattrs(ctx)
			if err != nil {
				return err
			}
			if len(xs[a]) != 1 || xs[a][0].Name != "user.x" || string(xs[a][0].Value) != "v" {
				t.Errorf("%s: xattrs %v", name, xs)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		// Clean up the second chunk so the next store starts alike.
		if err := w.DeleteChunk(ctx, a, 1); err != nil {
			t.Fatal(err)
		}
	}
}
