package rebuild

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"telfs/internal/chunk"
	"telfs/internal/compress"
	"telfs/internal/meta"
	"telfs/internal/tg"
	"telfs/internal/wire"
)

// fakeChannel is the channel: documents with captions, posted in order.
type fakeChannel struct {
	mu   sync.Mutex
	msgs []fakeMsg
}

type fakeMsg struct {
	caption string
	data    []byte
}

func (c *fakeChannel) UploadDocument(_ context.Context, r io.Reader, _, caption string) (int, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = append(c.msgs, fakeMsg{caption: caption, data: b})
	return len(c.msgs), nil // ids 1, 2, 3, … in posting order
}

func (c *fakeChannel) get(id int) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if id < 1 || id > len(c.msgs) || c.msgs[id-1].data == nil {
		return nil, errors.New("no such message")
	}
	return c.msgs[id-1].data, nil
}

func (c *fakeChannel) Fetch(_ context.Context, _ chunk.Key, id int64) ([]byte, error) {
	b, err := c.get(int(id))
	return append([]byte(nil), b...), err
}

func (c *fakeChannel) FetchRange(ctx context.Context, k chunk.Key, id int64, off, n int64) ([]byte, error) {
	b, err := c.get(int(id))
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), b[off:off+n]...), nil
}

// Source for the rebuild.
func (c *fakeChannel) Walk(_ context.Context, fn func(Message) error) error {
	c.mu.Lock()
	msgs := append([]fakeMsg(nil), c.msgs...)
	c.mu.Unlock()
	for i := len(msgs) - 1; i >= 0; i-- { // newest first, like the real walk
		if msgs[i].data == nil {
			continue // deleted
		}
		if err := fn(Message{ID: i + 1, HasDoc: true, Caption: msgs[i].caption}); err != nil {
			return err
		}
	}
	return nil
}

func (c *fakeChannel) ReadRange(_ context.Context, id int, off, n int64) ([]byte, error) {
	b, err := c.get(id)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), b[off:off+n]...), nil
}

func (c *fakeChannel) ReadAll(_ context.Context, id int) ([]byte, error) {
	b, err := c.get(id)
	return append([]byte(nil), b...), err
}

// deleteJournal drops every journal batch (the captions-only case).
func (c *fakeChannel) deleteKind(keys *wire.Keys, kind string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for i := range c.msgs {
		if cp, err := keys.OpenCaption(c.msgs[i].caption); err == nil && cp.K == kind {
			c.msgs[i].data = nil
			n++
		}
	}
	return n
}

// fs is a format-2 filesystem writing into the fake channel.
type fs struct {
	t      *testing.T
	m      *meta.Store
	keys   *wire.Keys
	wc     *chunk.Format2Codec
	ch     *fakeChannel
	cache  *chunk.Cache
	pool   *chunk.UploadPool
	packer *chunk.Packer
}

const cs = 4096

func newFS(t *testing.T) *fs {
	t.Helper()
	m, err := meta.Open(filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	uuid, _ := m.FSUUID(context.Background())
	dek := make([]byte, 32)
	rand.Read(dek)
	keys, _ := wire.DeriveKeys(dek, uuid, nil)
	z, _ := compress.NewZstd()
	wc, _ := chunk.NewFormat2Codec(keys, z, m)
	ch := &fakeChannel{}
	cache, _ := chunk.NewCacheWire(t.TempDir(), 64<<20, ch, wc)
	packer := chunk.NewPacker(m, ch, 1024, 0, 0)
	packer.SetWire(wc)
	t.Cleanup(packer.Close)
	m.SetJournalRefs(true)
	return &fs{t: t, m: m, keys: keys, wc: wc, ch: ch, cache: cache, pool: chunk.NewUploadPool(4), packer: packer}
}

func (f *fs) mkdir(parent int64, name string) int64 {
	ino, err := f.m.CreateChild(context.Background(), parent, name, meta.Inode{Kind: meta.KindDir, Mode: 0o40755})
	if err != nil {
		f.t.Fatal(err)
	}
	return ino
}

func (f *fs) write(parent int64, name string, data []byte) int64 {
	ctx := context.Background()
	ino, err := f.m.CreateChild(ctx, parent, name, meta.Inode{Kind: meta.KindFile, Mode: 0o100644, ChunkSize: cs})
	if err != nil {
		f.t.Fatal(err)
	}
	w, err := chunk.NewWriterWire(ctx, f.m, f.cache, f.ch, f.wc, f.pool, f.packer, ino, cs, 0)
	if err != nil {
		f.t.Fatal(err)
	}
	if len(data) > 0 {
		if _, err := w.WriteAt(ctx, data, 0); err != nil {
			f.t.Fatal(err)
		}
	}
	if err := w.Sync(ctx); err != nil {
		f.t.Fatal(err)
	}
	w.Close()
	return ino
}

// postJournal posts the pending journal rows as one format-2 batch,
// exactly as snapshot.UploadJournal2 does.
func (f *fs) postJournal() {
	ctx := context.Background()
	pend, err := f.m.PendingJournal(ctx)
	if err != nil || len(pend) == 0 {
		return
	}
	var ops []tg.JournalBatchEntry
	var seqs []int64
	for _, e := range pend {
		ops = append(ops, tg.JournalBatchEntry{Seq: e.Seq, Op: e.OpJSON})
		seqs = append(seqs, e.Seq)
	}
	js, _ := json.Marshal(struct {
		Ops []tg.JournalBatchEntry `json:"ops"`
	}{ops})
	body, _ := f.keys.SealMeta("journal", js)
	capt, _ := f.keys.SealCaption(wire.Caption{K: wire.KindJournal, First: ops[0].Seq, Last: ops[len(ops)-1].Seq, N: len(ops)})
	f.ch.UploadDocument(ctx, bytes.NewReader(body), wire.DocName, capt)
	f.m.BatchMarkJournalPosted(ctx, seqs, 1)
}

func rnd(n int) []byte { b := make([]byte, n); rand.Read(b); return b }

// readTree reads every regular file of a store back through the channel.
func readTree(t *testing.T, m *meta.Store, wc chunk.WireCodec, ch *fakeChannel) map[string][]byte {
	t.Helper()
	ctx := context.Background()
	cache, _ := chunk.NewCacheWire(t.TempDir(), 64<<20, ch, wc)
	r := chunk.NewReader(m, cache, nil, cs)
	out := map[string][]byte{}
	var walk func(dir int64, prefix string)
	walk = func(dir int64, prefix string) {
		ents, _ := m.Readdir(ctx, dir)
		for _, e := range ents {
			in, _ := m.GetInode(ctx, e.ChildIno)
			p := prefix + "/" + e.Name
			switch in.Kind {
			case meta.KindDir:
				walk(e.ChildIno, p)
			case meta.KindSymlink:
				out[p] = []byte("symlink→" + in.SymlinkTarget)
			default:
				buf := make([]byte, in.Size)
				n, err := r.ReadAt(ctx, e.ChildIno, buf, 0)
				if err != nil && !errors.Is(err, io.EOF) {
					out[p] = []byte(fmt.Sprintf("READ ERROR: %v", err))
					continue
				}
				out[p] = buf[:n]
			}
		}
	}
	walk(meta.RootIno, "")
	return out
}

// TestRebuildArashCases is Arash's list (2026-10-25): renames after
// upload, hard links, duplicate content, rsync temporary names (default
// and --inplace, plus --delay-updates), packed files; every snapshot
// deleted; rebuild with and without the journal; report what came back
// under which name.
func TestRebuildArashCases(t *testing.T) {
	ctx := context.Background()
	f := newFS(t)
	docs := f.mkdir(meta.RootIno, "docs")
	big := rnd(3*cs + 2000) // 4 direct chunks
	small := []byte("a small file that lands in a pack")
	f.write(docs, "a.txt", big)                    // direct chunks
	f.write(docs, "small.txt", small)              // packed
	f.write(docs, ".report.pdf.Ab12Cd", rnd(5000)) // rsync default: temp name at upload…
	reportData, _ := f.m.Lookup(ctx, docs, ".report.pdf.Ab12Cd")
	f.m.Rename(ctx, docs, ".report.pdf.Ab12Cd", docs, "report.pdf") // …then renamed
	inplace := rnd(6000)
	f.write(docs, "inplace.txt", inplace) // rsync --inplace: final name at upload
	f.m.Link(ctx, docs, "a-link.txt", mustLookup(t, f.m, docs, "a.txt").Ino)
	f.write(docs, "copy.txt", big) // duplicate content: dedup, no upload
	f.write(docs, "old-name.txt", rnd(7000))
	f.m.Rename(ctx, docs, "old-name.txt", docs, "new-name.txt") // rename after upload
	f.write(docs, "deleted.txt", rnd(5000))
	f.m.Unlink(ctx, docs, "deleted.txt") // messages stay until gc
	f.write(docs, "empty.txt", nil)
	f.m.CreateChild(ctx, docs, "link", meta.Inode{Kind: meta.KindSymlink, Mode: 0o120777, SymlinkTarget: "a.txt"})
	tmp := f.mkdir(docs, ".~tmp~")
	f.write(tmp, "delayed.txt", rnd(4500)) // rsync --delay-updates
	f.m.Rename(ctx, tmp, "delayed.txt", docs, "delayed.txt")
	f.m.Unlink(ctx, docs, ".~tmp~")
	f.postJournal()
	_ = reportData

	want := readTree(t, f.m, f.wc, f.ch)
	if string(want["/docs/a.txt"]) != string(big) || string(want["/docs/copy.txt"]) != string(big) {
		t.Fatal("test setup: original tree reads wrong")
	}

	run := func(useJournal bool) (map[string][]byte, *Result) {
		m2, err := meta.Open(filepath.Join(t.TempDir(), "rebuilt.sqlite"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { m2.Close() })
		res, err := Run(ctx, f.ch, f.keys, m2, Options{UseJournal: useJournal})
		if err != nil {
			t.Fatal(err)
		}
		wc2, _ := chunk.NewFormat2Codec(f.keys, nil, m2)
		return readTree(t, m2, wc2, f.ch), res
	}

	// (a) Every snapshot deleted (this fake channel never had one), the
	//     journal kept: the tree comes back exactly.
	got, res := run(true)
	report(t, "with journal", res)
	for p, w := range want {
		if !bytes.Equal(got[p], w) {
			t.Errorf("with journal: %s differs or is missing (got %d bytes, want %d)", p, len(got[p]), len(w))
		}
	}
	for p := range got {
		if _, ok := want[p]; !ok && !strings.HasPrefix(p, "/lost+found") {
			t.Errorf("with journal: unexpected %s", p)
		}
	}

	// (b) Journal batches deleted too: captions only.
	if n := f.ch.deleteKind(f.keys, wire.KindJournal); n == 0 {
		t.Fatal("no journal batch to delete")
	}
	got, res = run(false)
	report(t, "captions only", res)
	expect := map[string]string{
		"/docs/a.txt":        "a.txt (its upload-time name; the link name is lost)",
		"/docs/small.txt":    "packed file",
		"/docs/report.pdf":   "rsync default: mapped back from .report.pdf.Ab12Cd",
		"/docs/inplace.txt":  "rsync --inplace",
		"/docs/old-name.txt": "renamed after upload: comes back under the OLD name",
		"/docs/delayed.txt":  "rsync --delay-updates: mapped back from .~tmp~/",
		"/docs/deleted.txt":  "deleted after upload: comes back (no journal to say so)",
	}
	for p, why := range expect {
		if _, ok := got[p]; !ok {
			t.Errorf("captions only: %s missing (%s)", p, why)
		}
	}
	if !bytes.Equal(got["/docs/a.txt"], big) || !bytes.Equal(got["/docs/inplace.txt"], inplace) || !bytes.Equal(got["/docs/small.txt"], small) {
		t.Error("captions only: content differs")
	}
	for _, lost := range []string{"/docs/copy.txt", "/docs/empty.txt", "/docs/link", "/docs/a-link.txt", "/docs/new-name.txt"} {
		if _, ok := got[lost]; ok {
			t.Errorf("captions only: %s came back — the captions cannot know it", lost)
		}
	}
}

func mustLookup(t *testing.T, m *meta.Store, parent int64, name string) meta.Inode {
	in, err := m.Lookup(context.Background(), parent, name)
	if err != nil {
		t.Fatal(err)
	}
	return in
}

func report(t *testing.T, label string, res *Result) {
	var lines []string
	for _, f := range res.Files {
		lines = append(lines, fmt.Sprintf("  %-28s ino=%-3d %6d B  %d/%d chunks  %-18s %s",
			f.Path, f.Ino, f.Size, f.Found, f.Expected, f.NameSource, strings.Join(f.Notes, "; ")))
	}
	sort.Strings(lines)
	t.Logf("rebuild (%s): %+v\n%s", label, res.Stats, strings.Join(lines, "\n"))
}

func TestMapRsync(t *testing.T) {
	cases := map[string]string{
		"/home/a/.report.pdf.Ab12Cd": "/home/a/report.pdf",
		"/home/a/report.pdf":         "/home/a/report.pdf",
		"/home/a/.~tmp~/x.txt":       "/home/a/x.txt",
		"/home/a/.bashrc":            "/home/a/.bashrc",        // a real dotfile stays
		"/home/a/.config.tar.ab":     "/home/a/.config.tar.ab", // 2-char suffix: not rsync
		"/home/a/..x.Q1w2E3":         "/home/a/.x",             // rsync temp of a dotfile
	}
	for in, want := range cases {
		if got, _ := MapRsync(in); got != want {
			t.Errorf("MapRsync(%q) = %q, want %q", in, got, want)
		}
	}
}
