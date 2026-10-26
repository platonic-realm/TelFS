package chunk

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"telfs/internal/compress"
	"telfs/internal/meta"
	"telfs/internal/wire"
)

// recUploader records every document with its name and caption, and
// serves them back (whole or ranged) like the channel would.
type recUploader struct {
	mu   sync.Mutex
	docs map[int]recDoc
	next atomic.Int64
}

type recDoc struct {
	name, caption string
	data          []byte
}

func newRecUploader() *recUploader { return &recUploader{docs: map[int]recDoc{}} }

func (u *recUploader) UploadDocument(_ context.Context, r io.Reader, name, caption string) (int, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	id := int(u.next.Add(1))
	u.mu.Lock()
	u.docs[id] = recDoc{name: name, caption: caption, data: b}
	u.mu.Unlock()
	return id, nil
}

func (u *recUploader) Fetch(_ context.Context, _ Key, msgID int64) ([]byte, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	d, ok := u.docs[int(msgID)]
	if !ok {
		return nil, errors.New("no such message")
	}
	return append([]byte(nil), d.data...), nil
}

func (u *recUploader) FetchRange(ctx context.Context, k Key, msgID int64, off, n int64) ([]byte, error) {
	b, err := u.Fetch(ctx, k, msgID)
	if err != nil {
		return nil, err
	}
	if off+n > int64(len(b)) {
		return nil, errors.New("range past end")
	}
	return b[off : off+n], nil
}

func (u *recUploader) all() map[int]recDoc {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make(map[int]recDoc, len(u.docs))
	for k, v := range u.docs {
		out[k] = v
	}
	return out
}

type f2env struct {
	m      *meta.Store
	keys   *wire.Keys
	wc     *Format2Codec
	up     *recUploader
	cache  *Cache
	pool   *UploadPool
	packer *Packer
}

func newF2Env(t *testing.T) *f2env {
	t.Helper()
	m := newTestMeta(t)
	dek := make([]byte, 32)
	rand.Read(dek)
	keys, err := wire.DeriveKeys(dek, "test-fs-uuid", &wire.Counters{})
	if err != nil {
		t.Fatal(err)
	}
	z, _ := compress.NewZstd()
	wc, err := NewFormat2Codec(keys, z, m)
	if err != nil {
		t.Fatal(err)
	}
	up := newRecUploader()
	cache, err := NewCacheWire(t.TempDir(), 64<<20, up, wc)
	if err != nil {
		t.Fatal(err)
	}
	packer := NewPacker(m, up, 1024, 0, 0)
	packer.SetWire(wc)
	t.Cleanup(packer.Close)
	m.SetJournalRefs(true)
	return &f2env{m: m, keys: keys, wc: wc, up: up, cache: cache, pool: NewUploadPool(4), packer: packer}
}

func (e *f2env) write(t *testing.T, parent int64, name string, data []byte) int64 {
	t.Helper()
	ctx := context.Background()
	ino, err := e.m.CreateChild(ctx, parent, name, meta.Inode{Kind: meta.KindFile, Mode: 0o100644, ChunkSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWriterWire(ctx, e.m, e.cache, e.up, e.wc, e.pool, e.packer, ino, 4096, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteAt(ctx, data, 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	w.Close()
	return ino
}

func TestFormat2Pipeline(t *testing.T) {
	ctx := context.Background()
	e := newF2Env(t)
	dir, err := e.m.CreateChild(ctx, meta.RootIno, "docs", meta.Inode{Kind: meta.KindDir, Mode: 0o40755})
	if err != nil {
		t.Fatal(err)
	}
	marker := "PLANTED-MARKER-7f3a"
	big := bytes.Repeat([]byte(marker+" compressible text "), 700) // 26,600 bytes → 7 direct chunks
	rnd := make([]byte, 9000)
	rand.Read(rnd)
	small := []byte(marker + " a small file that goes into a pack")

	bigIno := e.write(t, dir, "big-"+marker+".txt", big)
	rndIno := e.write(t, dir, "random.bin", rnd)
	smallIno := e.write(t, dir, "small-"+marker+".txt", small)
	dupIno := e.write(t, dir, "copy-of-big.txt", big) // dedup: no new direct uploads

	// 1. What the channel sees: only `blob` documents, fixed-length
	//    captions, no marker anywhere.
	docs := e.up.all()
	var chunkCaps, packCaps int
	packed := map[int64]wire.PackEntry{}
	for id, d := range docs {
		if d.name != wire.DocName {
			t.Fatalf("doc %d named %q", id, d.name)
		}
		if len(d.caption) != wire.CaptionLen {
			t.Fatalf("doc %d caption length %d", id, len(d.caption))
		}
		for _, hay := range [][]byte{d.data, []byte(d.caption)} {
			if bytes.Contains(hay, []byte(marker)) {
				t.Fatalf("doc %d leaks the marker", id)
			}
		}
		c, err := e.keys.OpenCaption(d.caption)
		if err != nil {
			t.Fatalf("doc %d caption: %v", id, err)
		}
		switch c.K {
		case wire.KindChunk:
			chunkCaps++
			if !strings.HasPrefix(c.Name, "/docs/") || len(c.H) != wire.HashLen || c.CS != 4096 {
				t.Fatalf("chunk caption: %+v", c)
			}
		case wire.KindPack:
			packCaps++
			entries, err := e.keys.OpenPackIndex(d.data[c.IO : c.IO+c.IL])
			if err != nil || len(entries) != c.N {
				t.Fatalf("pack index: %d entries, caption says %d: %v", len(entries), c.N, err)
			}
			for _, en := range entries {
				packed[en.Ino] = en
			}
			if c.IO+c.IL != int64(len(d.data)) {
				t.Fatal("index must end the pack")
			}
		}
	}
	if en, ok := packed[smallIno]; !ok || en.Name != "/docs/small-"+marker+".txt" || en.FSz != int64(len(small)) {
		t.Fatalf("small file's pack entry: %+v", packed[smallIno])
	}
	if en, ok := packed[rndIno]; !ok || en.Idx != 2 || en.FSz != int64(len(rnd)) {
		t.Fatalf("random.bin tail's pack entry must carry the file size: %+v", en)
	}
	// big: all chunks direct (its tail is ≥ the 1024-byte pack threshold);
	// random: 2 full chunks direct, its 808-byte tail packed; small → packed;
	// the copy of big uploads nothing (dedup).
	nBig := (len(big) + 4095) / 4096
	if len(big)%4096 < 1024 {
		t.Fatal("test setup: big's tail must stay direct")
	}
	if chunkCaps != nBig+2 || packCaps < 1 {
		t.Fatalf("captions: %d chunk, %d pack", chunkCaps, packCaps)
	}

	// 2. chunk_map rows carry hashes; the duplicate shares big's messages.
	bigRows, _ := e.m.ListChunks(ctx, bigIno)
	dupRows, _ := e.m.ListChunks(ctx, dupIno)
	if len(bigRows) != len(dupRows) || len(bigRows) != nBig {
		t.Fatalf("rows: big %d dup %d", len(bigRows), len(dupRows))
	}
	for i := range bigRows {
		if len(bigRows[i].Hash) != wire.HashLen || bigRows[i].TGMessageID != dupRows[i].TGMessageID {
			t.Fatalf("row %d: %+v vs %+v", i, bigRows[i], dupRows[i])
		}
	}
	// …and every dedup hit was journaled as a `ref` op (N2).
	pend, _ := e.m.PendingJournal(ctx)
	refs := 0
	for _, j := range pend {
		var op meta.JournalOp
		json.Unmarshal(j.OpJSON, &op)
		if op.Op == meta.OpRef && op.Ino == dupIno && len(op.Hash) == wire.HashLen && op.MsgID > 0 {
			refs++
		}
	}
	if refs != nBig {
		t.Fatalf("ref ops for the duplicate: %d, want %d", refs, nBig)
	}

	// 3. Everything reads back through a COLD cache (download + decrypt
	//    + hash recheck).
	cold, _ := NewCacheWire(t.TempDir(), 64<<20, e.up, e.wc)
	r := NewReader(e.m, cold, nil, 4096)
	for ino, want := range map[int64][]byte{bigIno: big, rndIno: rnd, smallIno: small, dupIno: big} {
		got := make([]byte, len(want)+10)
		n, err := r.ReadAt(ctx, ino, got, 0)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("read ino %d: %v", ino, err)
		}
		if !bytes.Equal(got[:n], want) {
			t.Fatalf("ino %d: content differs (%d bytes read)", ino, n)
		}
	}

	// 4. Swapping one chunk's message for another's must fail to read
	//    (AAD = hash), not return the other content.
	r0, r1 := bigRows[0], bigRows[1]
	r0.TGMessageID = r1.TGMessageID
	if err := e.m.PutChunk(ctx, r0); err != nil {
		t.Fatal(err)
	}
	cold2, _ := NewCacheWire(t.TempDir(), 64<<20, e.up, e.wc)
	if _, err := cold2.GetChunk(ctx, Key{Ino: bigIno, Idx: 0}, r0); err == nil {
		t.Fatal("a swapped message must not decrypt under the slot's hash")
	}
}

func TestFormat2PendingNamesKeepHash(t *testing.T) {
	dir := t.TempDir()
	h := bytes.Repeat([]byte{0xab}, 32)
	p, err := PersistPendingHashed(dir, 12, 3, 999, h, []byte("wire"))
	if err != nil {
		t.Fatal(err)
	}
	ents, _ := ScanPending(dir)
	if len(ents) != 1 || ents[0].Path != p || !bytes.Equal(ents[0].Hash, h) || ents[0].PlaintextSize != 999 {
		t.Fatalf("scan: %+v", ents)
	}
	// Format-1 names still parse, without a hash.
	PersistPending(dir, 1, 2, 3, []byte("w"))
	ents, _ = ScanPending(dir)
	if len(ents) != 2 {
		t.Fatalf("scan both: %+v", ents)
	}
}
