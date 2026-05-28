package meta

import (
	"bytes"
	"errors"
	"testing"
)

func TestKVUpsertAndDelete(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	if err := s.PutKV(ctx, "fs_uuid", []byte("abc")); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetKV(ctx, "fs_uuid")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("abc")) {
		t.Fatalf("got %q", got)
	}
	// Overwrite.
	_ = s.PutKV(ctx, "fs_uuid", []byte("def"))
	got, _ = s.GetKV(ctx, "fs_uuid")
	if !bytes.Equal(got, []byte("def")) {
		t.Fatalf("after overwrite: %q", got)
	}
	// Delete.
	if err := s.DeleteKV(ctx, "fs_uuid"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetKV(ctx, "fs_uuid"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("kv survived delete: %v", err)
	}
}
