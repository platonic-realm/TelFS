package snapshot

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"telfs/internal/meta"
	"telfs/internal/wire"
)

// fakeGate is an upload pool's registration counter.
type fakeGate struct {
	reg  atomic.Uint64
	open atomic.Int64
	// onLast, when set, runs on every LastRegistered call (n = call count).
	onLast func(n int)
	calls  int
}

func (g *fakeGate) LastRegistered() uint64 {
	g.calls++
	if g.onLast != nil {
		g.onLast(g.calls)
	}
	return g.reg.Load()
}

func (g *fakeGate) OpenUpTo(uint64) int { return int(g.open.Load()) }

// A version is taken only while nothing is in flight: an upload open, or
// a registration made while the copy was taken (a file's new size may be
// in the copy without its chunk), posts nothing.
func TestVersionNotTakenWhileUploadsInFlight(t *testing.T) {
	f, _ := snapshotted(t)
	n := len(f.ch.idsDesc(0))
	g := &fakeGate{}
	g.open.Store(1)
	_, _, err := CreateVersion(f.ctx, f.ch, f.m, f.keys, "daily", g, nil)
	var inc *IncompleteError
	if !errors.As(err, &inc) || inc.Open != 1 || !errors.Is(err, ErrVersionIncomplete) {
		t.Fatalf("upload open: %v", err)
	}
	g.open.Store(0)
	// A registration while the copy is taken (the second look differs).
	g.calls = 0
	g.onLast = func(call int) {
		if call == 2 {
			g.reg.Add(1)
		}
	}
	if _, _, err := CreateVersion(f.ctx, f.ch, f.m, f.keys, "daily", g, nil); !errors.As(err, &inc) {
		t.Fatalf("registration during the take: %v", err)
	}
	if got := len(f.ch.idsDesc(0)); got != n {
		t.Fatalf("%d message(s) posted by incomplete takes", got-n)
	}
	if vs, _ := LoadVersions(f.ctx, f.m); len(vs) != 0 {
		t.Fatalf("registry %+v", vs)
	}
	g.onLast = nil
	if _, _, err := CreateVersion(f.ctx, f.ch, f.m, f.keys, "daily", g, nil); err != nil {
		t.Fatalf("quiet: %v", err)
	}
}

// A file with its size but without all its chunks (not sparse) makes the
// take incomplete, naming the file; sparse holes and stray rows past the
// end do not.
func TestVersionNotTakenWithIncompleteFiles(t *testing.T) {
	f, _ := snapshotted(t)
	ino, err := f.m.CreateChild(f.ctx, meta.RootIno, "half.bin", meta.Inode{Kind: meta.KindFile, Mode: 0o100644, ChunkSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.m.SetSize(f.ctx, ino, 3<<20); err != nil {
		t.Fatal(err)
	}
	f.m.PutChunk(f.ctx, meta.Chunk{Ino: ino, Idx: 0, TGMessageID: 5, Size: 1 << 20, Hash: make([]byte, 32)})
	sp, _ := f.m.CreateChild(f.ctx, meta.RootIno, "holes.img", meta.Inode{Kind: meta.KindFile, Mode: 0o100644, ChunkSize: 1 << 20})
	f.m.SetSize(f.ctx, sp, 8<<20)
	f.m.SetInodeSparse(f.ctx, sp)
	_, _, err = CreateVersion(f.ctx, f.ch, f.m, f.keys, "daily", nil, nil)
	var inc *IncompleteError
	if !errors.As(err, &inc) || inc.Count != 1 || len(inc.Files) != 1 || inc.Files[0] != "/half.bin" {
		t.Fatalf("incomplete file: %v (%+v)", err, inc)
	}
	if !strings.Contains(err.Error(), "/half.bin") {
		t.Fatalf("the error must name the file: %v", err)
	}
	if got := f.ch.kinds(f.keys, wire.KindVersion); len(got) != 0 {
		t.Fatalf("posted %v", got)
	}
	for idx := int32(1); idx < 3; idx++ {
		f.m.PutChunk(f.ctx, meta.Chunk{Ino: ino, Idx: idx, TGMessageID: 5 + int64(idx), Size: 1 << 20, Hash: make([]byte, 32)})
	}
	if _, _, err := CreateVersion(f.ctx, f.ch, f.m, f.keys, "daily", nil, nil); err != nil {
		t.Fatalf("complete: %v", err)
	}
}

// The registry change and the anchor_due mark land together, whatever
// the request's context does after the version is uploaded; a later
// successful anchor clears the mark.
func TestVersionRecordedAfterCancel(t *testing.T) {
	f, mgr := snapshotted(t)
	ctx, cancel := context.WithCancel(f.ctx)
	f.ch.onUpload = func(string) { cancel() } // stopped while the version uploads
	f.ch.failPin = true
	v, warn, err := CreateVersion(ctx, f.ch, f.m, f.keys, "weekly", nil, nil)
	if err != nil || warn == "" {
		t.Fatalf("create: %v %q", err, warn)
	}
	if vs, _ := LoadVersions(f.ctx, f.m); len(vs) != 1 || vs[0] != v {
		t.Fatalf("the uploaded version must be recorded: %+v", vs)
	}
	if st, _ := LoadPolicyState(f.ctx, f.m); !st.AnchorDue {
		t.Fatal("anchor_due must be set with the registry")
	}
	f.ch.failPin = false
	if _, err := PostAnchor(f.ctx, f.ch, f.m, f.keys, nil); err != nil {
		t.Fatal(err)
	}
	if st, _ := LoadPolicyState(f.ctx, f.m); st.AnchorDue || mgr == nil {
		t.Fatal("a posted anchor listing the registry clears the mark")
	}
}

// An anchor posted while the registry changes does not clear the mark
// of that change.
func TestPostAnchorKeepsANewerMark(t *testing.T) {
	f, _ := snapshotted(t)
	f.ch.onUpload = func(string) {
		if err := storeVersionsDue(f.ctx, f.m, []wire.AnchorVersion{{Name: "late", Msg: 3}}); err != nil {
			t.Error(err)
		}
	}
	if _, err := PostAnchor(f.ctx, f.ch, f.m, f.keys, nil); err != nil {
		t.Fatal(err)
	}
	if st, _ := LoadPolicyState(f.ctx, f.m); !st.AnchorDue {
		t.Fatal("the anchor posted does not list \"late\": its mark must stay")
	}
	if a := f.newestAnchor(t); len(a.Versions) != 0 {
		t.Fatalf("anchor %+v", a.Versions)
	}
}

// A downloaded version that does not decrypt or is not the registry's DB
// is an integrity problem (exit 7 in the CLI), not a channel failure.
func TestDownloadVersionIntegrity(t *testing.T) {
	f, _ := snapshotted(t)
	v, _, err := CreateVersion(f.ctx, f.ch, f.m, f.keys, "daily", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "daily.sqlite")
	bad := v
	bad.SHA256 = strings.Repeat("0", 64)
	if err := DownloadVersion(f.ctx, f.ch, f.keys, bad, path); !errors.Is(err, ErrVersionIntegrity) {
		t.Fatalf("SHA-256 mismatch: %v", err)
	}
	bad = v
	bad.Size++
	if err := DownloadVersion(f.ctx, f.ch, f.keys, bad, path); !errors.Is(err, ErrVersionIntegrity) {
		t.Fatalf("size mismatch: %v", err)
	}
	m, _ := f.ch.get(v.Msg)
	m.body = []byte("damaged")
	if err := DownloadVersion(f.ctx, f.ch, f.keys, v, path); !errors.Is(err, ErrVersionIntegrity) {
		t.Fatalf("does not decrypt: %v", err)
	}
	f.ch.DeleteChannelMessages(f.ctx, v.Msg)
	if err := DownloadVersion(f.ctx, f.ch, f.keys, v, path); errors.Is(err, ErrVersionIntegrity) || !errors.Is(err, ErrChannel) {
		t.Fatalf("message gone: %v", err)
	}
}
