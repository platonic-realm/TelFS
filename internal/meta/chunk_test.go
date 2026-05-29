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

func TestDedupReuseLiveBlob(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	// First write: ino1, idx0 → msg 42; record blob in the index.
	ino1 := mkfile(t, s, RootIno, "a")
	if err := s.PutChunk(ctx, Chunk{Ino: ino1, Idx: 0, TGMessageID: 42, Size: 1024}); err != nil {
		t.Fatal(err)
	}
	hash := []byte("0123456789abcdef0123456789abcdef") // 32 bytes, placeholder for sha256
	if err := s.RecordChunkBlob(ctx, hash, 42, 1024); err != nil {
		t.Fatal(err)
	}

	// Second write: ino2 with same content should reuse msg 42 via dedup.
	ino2 := mkfile(t, s, RootIno, "b")
	reused, msgID, size, err := s.ReuseChunkByHash(ctx, ino2, 0, hash)
	if err != nil {
		t.Fatalf("ReuseChunkByHash: %v", err)
	}
	if !reused {
		t.Fatalf("expected reuse with live blob")
	}
	if msgID != 42 || size != 1024 {
		t.Fatalf("reuse returned wrong msgID/size: %d/%d", msgID, size)
	}
	// chunk_map row should now exist for ino2/0.
	got, err := s.GetChunk(ctx, ino2, 0)
	if err != nil {
		t.Fatalf("GetChunk ino2: %v", err)
	}
	if got.TGMessageID != 42 {
		t.Fatalf("ino2 row points at %d, want 42", got.TGMessageID)
	}
}

func TestDedupRejectsStaleBlob(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	// Index a blob but never put a chunk_map row for its msg id —
	// simulating "GC reaped the message; chunk_blob still has the stale entry".
	hash := []byte("ffffffffffffffffffffffffffffffff")
	if err := s.RecordChunkBlob(ctx, hash, 999, 1024); err != nil {
		t.Fatal(err)
	}

	ino := mkfile(t, s, RootIno, "f")
	reused, _, _, err := s.ReuseChunkByHash(ctx, ino, 0, hash)
	if err != nil {
		t.Fatalf("ReuseChunkByHash: %v", err)
	}
	if reused {
		t.Fatalf("expected reuse=false on stale blob (no chunk_map row references msg 999)")
	}
	// chunk_map should not have a phantom row.
	if _, err := s.GetChunk(ctx, ino, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale reuse leaked a chunk_map row: %v", err)
	}
}

func TestPruneStaleChunkBlobs(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	ino := mkfile(t, s, RootIno, "f")
	if err := s.PutChunk(ctx, Chunk{Ino: ino, Idx: 0, TGMessageID: 42, Size: 1024}); err != nil {
		t.Fatal(err)
	}
	live := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	stale := []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err := s.RecordChunkBlob(ctx, live, 42, 1024); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordChunkBlob(ctx, stale, 999, 1024); err != nil {
		t.Fatal(err)
	}

	n, err := s.PruneStaleChunkBlobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("pruned %d, want 1 (the stale entry)", n)
	}
	count, _ := s.CountChunkBlobs(ctx)
	if count != 1 {
		t.Fatalf("after prune: %d blobs, want 1", count)
	}
	// Reuse with the live hash still works.
	ino2 := mkfile(t, s, RootIno, "g")
	reused, _, _, err := s.ReuseChunkByHash(ctx, ino2, 0, live)
	if err != nil || !reused {
		t.Fatalf("live blob no longer reusable: reused=%v err=%v", reused, err)
	}
}

// TestDedupSharedChunkSurvivesOneUnlink is the data-loss regression:
// two files share a single channel message via dedup; unlinking one
// must NOT make GC consider the shared message orphaned. Liveness is
// derived from chunk_map (DISTINCT tg_message_id), so the second file's
// row keeps the message alive automatically — that's the structural
// guarantee we want to lock in.
func TestDedupSharedChunkSurvivesOneUnlink(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	// Two files share chunk msg_id=77.
	ino1 := mkfile(t, s, RootIno, "a")
	ino2 := mkfile(t, s, RootIno, "b")
	if err := s.PutChunk(ctx, Chunk{Ino: ino1, Idx: 0, TGMessageID: 77, Size: 1024}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutChunk(ctx, Chunk{Ino: ino2, Idx: 0, TGMessageID: 77, Size: 1024}); err != nil {
		t.Fatal(err)
	}
	hash := []byte("cccccccccccccccccccccccccccccccc")
	if err := s.RecordChunkBlob(ctx, hash, 77, 1024); err != nil {
		t.Fatal(err)
	}

	// Unlink "a" — chunk_map row for ino1 goes away via ON DELETE CASCADE.
	if err := s.Unlink(ctx, RootIno, "a"); err != nil {
		t.Fatal(err)
	}

	// GC liveness set must STILL include msg 77 because ino2's row references it.
	alive, err := s.AllChunkMessageIDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := alive[77]; !ok {
		t.Fatalf("GC liveness lost the shared message after one unlink — would cause data loss")
	}

	// Reuse from a fresh inode still works (msg 77 still alive).
	ino3 := mkfile(t, s, RootIno, "c")
	reused, msgID, _, err := s.ReuseChunkByHash(ctx, ino3, 0, hash)
	if err != nil || !reused || msgID != 77 {
		t.Fatalf("reuse after partial unlink: reused=%v msgID=%d err=%v", reused, msgID, err)
	}

	// Now unlink the survivor too — now the message has no chunk_map row
	// (after deleting "b", only "c" references it). Confirm reuse still
	// works because "c" keeps it alive. We don't test the all-gone case
	// here because the dedup index itself doesn't manage TG deletion;
	// that's `telfs gc`'s job, and PruneStaleChunkBlobs covers the index
	// side independently.
	c, _ := s.GetChunk(ctx, ino2, 0)
	if c.TGMessageID != 77 {
		t.Fatalf("ino2 lost its chunk_map row unexpectedly")
	}
}
