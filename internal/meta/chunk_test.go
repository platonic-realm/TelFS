package meta

import (
	"errors"
	"testing"
)

func TestChunksRoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	ino := mkfile(t, s, RootIno, "f")
	for i, mid := range []int64{10, 20, 30} {
		if err := s.PutChunk(ctx, Chunk{Ino: ino, Idx: int32(i), TGMessageID: mid, Size: 4096}); err != nil {
			t.Fatalf("PutChunk %d: %v", i, err)
		}
	}
	chunks, err := s.ListChunks(ctx, ino)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 {
		t.Fatalf("got %d chunks, want 3", len(chunks))
	}
	for i, c := range chunks {
		if c.Idx != int32(i) {
			t.Fatalf("chunks not ordered: %+v", chunks)
		}
	}
}

func TestPutChunkOverwrites(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	ino := mkfile(t, s, RootIno, "f")
	_ = s.PutChunk(ctx, Chunk{Ino: ino, Idx: 0, TGMessageID: 100, Size: 4096})
	_ = s.PutChunk(ctx, Chunk{Ino: ino, Idx: 0, TGMessageID: 200, Size: 8192})
	got, _ := s.GetChunk(ctx, ino, 0)
	if got.TGMessageID != 200 || got.Size != 8192 {
		t.Fatalf("after overwrite: %+v", got)
	}
}

func TestDeleteChunksAbove(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	ino := mkfile(t, s, RootIno, "f")
	for i := int32(0); i < 5; i++ {
		_ = s.PutChunk(ctx, Chunk{Ino: ino, Idx: i, TGMessageID: int64(i + 1), Size: 4096})
	}
	n, err := s.DeleteChunksAbove(ctx, ino, 2)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("deleted %d, want 3", n)
	}
	chunks, _ := s.ListChunks(ctx, ino)
	if len(chunks) != 2 {
		t.Fatalf("after delete: %d chunks", len(chunks))
	}
}

func TestChunkCascadeOnInodeDelete(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	ino := mkfile(t, s, RootIno, "f")
	_ = s.PutChunk(ctx, Chunk{Ino: ino, Idx: 0, TGMessageID: 99, Size: 4096})
	if err := s.Unlink(ctx, RootIno, "f"); err != nil {
		t.Fatal(err)
	}
	// Inode gone -> chunks should be gone too via ON DELETE CASCADE.
	if _, err := s.GetChunk(ctx, ino, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("chunk survived inode delete: %v", err)
	}
}
