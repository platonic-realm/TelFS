package restore

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"telfs/internal/chunk"
	"telfs/internal/compress"
	"telfs/internal/meta"
	"telfs/internal/trash"
	"telfs/internal/wire"
)

// fakeChannel serves documents like the channel and counts every call.
type fakeChannel struct {
	mu     sync.Mutex
	docs   map[int64][]byte
	whole  map[int64]int
	ranges map[int64][][2]int64
	fail   func(msg int64) error
}

func newFakeChannel() *fakeChannel {
	return &fakeChannel{docs: map[int64][]byte{}, whole: map[int64]int{}, ranges: map[int64][][2]int64{}}
}

func (f *fakeChannel) Fetch(_ context.Context, msg int64) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.whole[msg]++
	if f.fail != nil {
		if err := f.fail(msg); err != nil {
			return nil, err
		}
	}
	d, ok := f.docs[msg]
	if !ok {
		return nil, fmt.Errorf("message %d is empty (deleted?)", msg)
	}
	return append([]byte(nil), d...), nil
}

func (f *fakeChannel) FetchRange(_ context.Context, msg, off, n int64) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ranges[msg] = append(f.ranges[msg], [2]int64{off, n})
	if f.fail != nil {
		if err := f.fail(msg); err != nil {
			return nil, err
		}
	}
	d, ok := f.docs[msg]
	if !ok {
		return nil, fmt.Errorf("message %d is empty (deleted?)", msg)
	}
	if off < 0 || off+n > int64(len(d)) {
		return nil, fmt.Errorf("range [%d,+%d) outside message %d (%d bytes)", off, n, msg, len(d))
	}
	return append([]byte(nil), d[off:off+n]...), nil
}

func (f *fakeChannel) calls() (whole, ranged int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, n := range f.whole {
		whole += n
	}
	for _, rs := range f.ranges {
		ranged += len(rs)
	}
	return
}

func (f *fakeChannel) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.whole = map[int64]int{}
	f.ranges = map[int64][][2]int64{}
}

// fixture is a small format-2 filesystem: its DB, codec and channel.
type fixture struct {
	t     *testing.T
	ctx   context.Context
	m     *meta.Store
	codec *chunk.Format2Codec
	ch    *fakeChannel
	want  map[string][]byte // fs path → content of every regular file
	pack  []byte            // the pack being filled
	packM int64
}

const testCS = 64 << 10 // chunk size of the fixture's files

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	m, err := meta.Open(filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	if err := m.PutKV(ctx, meta.KVFormat, []byte("2")); err != nil {
		t.Fatal(err)
	}
	fsUUID, err := m.FSUUID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dek := make([]byte, 32)
	rand.Read(dek)
	keys, err := wire.DeriveKeys(dek, fsUUID, nil)
	if err != nil {
		t.Fatal(err)
	}
	codec, err := chunk.NewFormat2Codec(keys, compress.None{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{t: t, ctx: ctx, m: m, codec: codec, ch: newFakeChannel(), want: map[string][]byte{}, packM: 100}
}

func (f *fixture) dir(parent int64, name string, mode uint32, mtime int64) int64 {
	f.t.Helper()
	ino, err := f.m.CreateChild(f.ctx, parent, name, meta.Inode{Kind: meta.KindDir, Mode: 0o40000 | mode, Mtime: mtime})
	if err != nil {
		f.t.Fatal(err)
	}
	return ino
}

func (f *fixture) seal(data []byte) ([]byte, []byte) {
	f.t.Helper()
	h := f.codec.Hash(data)
	w, _, err := f.codec.Encode(0, 0, data, h)
	if err != nil {
		f.t.Fatal(err)
	}
	return w, h
}

// packedFile adds a small file whose one chunk goes into the open pack.
func (f *fixture) packedFile(parent int64, fsPath string, data []byte, mode uint32, mtime int64) int64 {
	f.t.Helper()
	ino, err := f.m.CreateChild(f.ctx, parent, filepath.Base(fsPath), meta.Inode{Kind: meta.KindFile, Mode: 0o100000 | mode,
		Size: int64(len(data)), Mtime: mtime, ChunkSize: testCS})
	if err != nil {
		f.t.Fatal(err)
	}
	w, h := f.seal(data)
	if err := f.m.PutChunk(f.ctx, meta.Chunk{Ino: ino, Idx: 0, TGMessageID: f.packM, Size: int32(len(data)),
		PackOffset: int64(len(f.pack)), PackSize: int32(len(w)), Hash: h}); err != nil {
		f.t.Fatal(err)
	}
	f.pack = append(f.pack, w...)
	f.want[fsPath] = data
	return ino
}

// closePack posts the pack (with a tail, like the encrypted index).
func (f *fixture) closePack() {
	f.ch.docs[f.packM] = append(f.pack, bytes.Repeat([]byte{0xEE}, 777)...)
	f.pack = nil
	f.packM++
}

// directFile adds a file whose chunks are direct messages from msg on;
// skip lists chunk indexes left without a row.
func (f *fixture) directFile(parent int64, fsPath string, data []byte, msg int64, sparse bool, skip ...int32) int64 {
	f.t.Helper()
	ino, err := f.m.CreateChild(f.ctx, parent, filepath.Base(fsPath), meta.Inode{Kind: meta.KindFile, Mode: 0o100644,
		Size: int64(len(data)), Mtime: 1_600_000_000, ChunkSize: testCS, Sparse: sparse})
	if err != nil {
		f.t.Fatal(err)
	}
	for idx := int32(0); int64(idx)*testCS < int64(len(data)); idx++ {
		if contains(skip, idx) {
			continue
		}
		part := data[int64(idx)*testCS : min(int64(idx+1)*testCS, int64(len(data)))]
		w, h := f.seal(part)
		f.ch.docs[msg] = w
		if err := f.m.PutChunk(f.ctx, meta.Chunk{Ino: ino, Idx: idx, TGMessageID: msg, Size: int32(len(part)), Hash: h}); err != nil {
			f.t.Fatal(err)
		}
		msg++
	}
	f.want[fsPath] = data
	return ino
}

func contains(s []int32, v int32) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

// standardTree builds the tree most tests restore:
//
//	/docs (0750)        small files f00..f39 in ONE pack, ro.txt (0400),
//	                    empty, a hard link of f00, a symlink to f01
//	/docs/sub (0555)    deep.txt (packed)
//	/big.bin            2.5 chunks, direct messages 200..202
//	/sparse.bin         3 chunks, only chunk 1 stored (a hole each side)
//	/.trash/old.txt     the rm safety net — not restored with "/"
func standardTree(t *testing.T) *fixture {
	f := newFixture(t)
	docs := f.dir(meta.RootIno, "docs", 0o750, 1_500_000_000)
	for i := 0; i < 40; i++ {
		ino := f.packedFile(docs, fmt.Sprintf("/docs/f%02d", i), randBytes(10+i*7), 0o644, 1_500_000_100+int64(i))
		if i == 0 {
			if err := f.m.SetXattr(f.ctx, ino, "user.color", []byte("blue")); err != nil {
				t.Fatal(err)
			}
			if err := f.m.Link(f.ctx, docs, "f00-link", ino); err != nil {
				t.Fatal(err)
			}
		}
	}
	f.packedFile(docs, "/docs/ro.txt", []byte("read only\n"), 0o400, 1_500_000_200)
	if _, err := f.m.CreateChild(f.ctx, docs, "empty", meta.Inode{Kind: meta.KindFile, Mode: 0o100640, Mtime: 1_500_000_300, ChunkSize: testCS}); err != nil {
		t.Fatal(err)
	}
	f.want["/docs/empty"] = []byte{}
	if _, err := f.m.CreateChild(f.ctx, docs, "sym", meta.Inode{Kind: meta.KindSymlink, Mode: 0o120777, SymlinkTarget: "f01", Mtime: 1_500_000_400}); err != nil {
		t.Fatal(err)
	}
	sub := f.dir(docs, "sub", 0o555, 1_500_000_500)
	f.packedFile(sub, "/docs/sub/deep.txt", []byte("deep\n"), 0o644, 1_500_000_600)
	f.closePack()

	f.directFile(meta.RootIno, "/big.bin", randBytes(2*testCS+testCS/2), 200, false)
	sparse := make([]byte, 3*testCS)
	copy(sparse[testCS:], randBytes(testCS))
	f.directFile(meta.RootIno, "/sparse.bin", sparse, 300, true, 0, 2)

	tr := f.dir(meta.RootIno, trash.DirName, 0o700, 1)
	f.packedFile(tr, "/.trash/old.txt", []byte("deleted"), 0o644, 1)
	f.closePack()
	return f
}

func (f *fixture) run(src, dest string, mod func(*Options)) *Result {
	f.t.Helper()
	plan, err := BuildPlan(f.ctx, f.m, src)
	if err != nil {
		f.t.Fatal(err)
	}
	opts := Options{Dest: dest, Codec: f.codec, Download: f.ch, Workers: 3}
	if mod != nil {
		mod(&opts)
	}
	res, err := Run(f.ctx, plan, opts)
	if err != nil {
		f.t.Fatal(err)
	}
	return res
}

// destDir is a temporary directory the test's cleanup can remove after
// a restore made read-only directories in it.
func destDir(t *testing.T) string {
	t.Helper()
	d := t.TempDir()
	t.Cleanup(func() {
		filepath.WalkDir(d, func(p string, e os.DirEntry, err error) error {
			if err == nil && e.IsDir() {
				os.Chmod(p, 0o700)
			}
			return nil
		})
	})
	return d
}

func mtimeOf(t *testing.T, p string) int64 {
	t.Helper()
	var st unix.Stat_t
	if err := unix.Lstat(p, &st); err != nil {
		t.Fatal(err)
	}
	return st.Mtim.Sec
}

// The whole tree of "/" comes back: content, empty file, sparse holes,
// modes, mtimes, xattrs, symlink, hard link; /.trash stays out; every
// message is downloaded exactly once — the pack as ONE ranged read over
// just its extents (not its index tail), direct chunks whole.
func TestRestoreTree(t *testing.T) {
	f := standardTree(t)
	dest := filepath.Join(destDir(t), "out")
	res := f.run("/", dest, nil)
	if len(res.Problems) != 0 {
		t.Fatalf("problems: %+v", res.Problems)
	}
	for p, want := range f.want {
		if strings.HasPrefix(p, "/.trash") {
			if _, err := os.Lstat(filepath.Join(dest, p)); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("%s restored from the trash", p)
			}
			continue
		}
		got, err := os.ReadFile(filepath.Join(dest, p))
		if err != nil || !bytes.Equal(got, want) {
			t.Errorf("%s: content differs (%d vs %d bytes, %v)", p, len(got), len(want), err)
		}
	}
	// One download per message.
	for msg := range f.ch.docs {
		w, r := f.ch.whole[msg], len(f.ch.ranges[msg])
		switch {
		case msg == 100: // the pack of /docs
			if w != 0 || r != 1 {
				t.Errorf("pack %d: %d whole + %d ranged downloads, want 1 ranged", msg, w, r)
			} else if rg := f.ch.ranges[msg][0]; rg[0] != 0 || rg[1] != int64(len(f.ch.docs[msg])-777) {
				t.Errorf("pack range %v, want [0,+%d): the extents only", rg, len(f.ch.docs[msg])-777)
			}
		case msg == 101: // the trash's pack
			if w+r != 0 {
				t.Errorf("the trash's pack was downloaded")
			}
		default:
			if w != 1 || r != 0 {
				t.Errorf("message %d: %d whole + %d ranged downloads, want 1 whole", msg, w, r)
			}
		}
	}
	if res.Messages != 5 || res.Verified != 45 || res.Files != 45 || res.Links != 1 || res.Symlinks != 1 || res.Dirs != 3 {
		t.Errorf("result %+v", res)
	}

	// Modes and mtimes.
	for p, want := range map[string]os.FileMode{"docs": 0o750, "docs/sub": 0o555, "docs/f00": 0o644, "docs/ro.txt": 0o400, "docs/empty": 0o640} {
		fi, err := os.Lstat(filepath.Join(dest, p))
		if err != nil || fi.Mode().Perm() != want {
			t.Errorf("%s: mode %v, want %v (%v)", p, fi.Mode().Perm(), want, err)
		}
	}
	for p, want := range map[string]int64{"docs": 1_500_000_000, "docs/sub": 1_500_000_500, "docs/f05": 1_500_000_105,
		"docs/sym": 1_500_000_400, "docs/empty": 1_500_000_300, "big.bin": 1_600_000_000} {
		if got := mtimeOf(t, filepath.Join(dest, p)); got != want {
			t.Errorf("%s: mtime %d, want %d", p, got, want)
		}
	}
	// Symlink, hard link, xattr, sparse holes.
	if tgt, err := os.Readlink(filepath.Join(dest, "docs/sym")); err != nil || tgt != "f01" {
		t.Errorf("symlink: %q %v", tgt, err)
	}
	a, _ := os.Stat(filepath.Join(dest, "docs/f00"))
	b, _ := os.Stat(filepath.Join(dest, "docs/f00-link"))
	if a == nil || b == nil || !os.SameFile(a, b) {
		t.Error("f00-link is not a hard link of f00")
	}
	buf := make([]byte, 16)
	if n, err := unix.Getxattr(filepath.Join(dest, "docs/f00"), "user.color", buf); err != nil {
		if !errors.Is(err, unix.ENOTSUP) {
			t.Errorf("xattr: %v", err)
		}
	} else if string(buf[:n]) != "blue" {
		t.Errorf("xattr = %q", buf[:n])
	}
	var st syscall.Stat_t
	if err := syscall.Stat(filepath.Join(dest, "sparse.bin"), &st); err == nil && st.Blocks*512 >= 3*testCS {
		t.Logf("sparse.bin occupies %d bytes (holes not preserved by this filesystem)", st.Blocks*512)
	}
}

// A second run finds everything in DEST and downloads nothing; after a
// byte of one chunk is damaged and a file deleted, only the messages
// holding what is wrong are fetched again — the damaged chunk alone, not
// the rest of its file.
func TestRestoreResume(t *testing.T) {
	f := standardTree(t)
	dest := filepath.Join(destDir(t), "out")
	f.run("/", dest, nil)
	f.ch.reset()

	res := f.run("/", dest, nil)
	if w, r := f.ch.calls(); w+r != 0 || len(res.Problems) != 0 || res.Identical != 45 {
		t.Fatalf("second run: %d whole + %d ranged downloads, identical %d, problems %v", w, r, res.Identical, res.Problems)
	}
	// The finished first run left docs/sub 0555 and ro.txt 0400.
	big := filepath.Join(dest, "big.bin")
	fh, err := os.OpenFile(big, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	fh.WriteAt([]byte{0x55}, testCS+10) // chunk 1 (message 201)
	fh.Close()
	os.Chmod(filepath.Join(dest, "docs/sub"), 0o755)
	os.Remove(filepath.Join(dest, "docs/sub/deep.txt"))
	os.Chmod(filepath.Join(dest, "docs/sub"), 0o555)
	f.ch.reset()

	res = f.run("/", dest, nil)
	if len(res.Problems) != 0 {
		t.Fatalf("problems: %v", res.Problems)
	}
	if f.ch.whole[201] != 1 || len(f.ch.whole) != 1 {
		t.Errorf("whole downloads %v, want message 201 only", f.ch.whole)
	}
	if len(f.ch.ranges) != 1 || len(f.ch.ranges[100]) != 1 {
		t.Errorf("ranged downloads %v, want one of the pack", f.ch.ranges)
	}
	if res.Identical != 43 || res.ChunksPresent != 2 {
		t.Errorf("identical %d, chunks present %d; want 43, 2", res.Identical, res.ChunksPresent)
	}
	for _, p := range []string{"/big.bin", "/docs/sub/deep.txt"} {
		got, _ := os.ReadFile(filepath.Join(dest, p))
		if !bytes.Equal(got, f.want[p]) {
			t.Errorf("%s not repaired", p)
		}
	}
	if fi, _ := os.Lstat(filepath.Join(dest, "docs/sub")); fi.Mode().Perm() != 0o555 {
		t.Errorf("docs/sub mode %v after the re-run", fi.Mode().Perm())
	}
}

// corruptingCodec damages the plaintext of one chunk AFTER it decrypted
// and authenticated — as a bad disk or a bug on the write path would.
type corruptingCodec struct {
	Codec
	hash []byte
}

func (c corruptingCodec) Decode(ino int64, idx int32, hash, w []byte) ([]byte, error) {
	b, err := c.Codec.Decode(ino, idx, hash, w)
	if err == nil && bytes.Equal(hash, c.hash) && len(b) > 0 {
		b = append([]byte(nil), b...)
		b[len(b)/2] ^= 0x01
	}
	return b, err
}

// Verification re-reads DEST: a byte that landed wrong is an integrity
// problem naming the file (and its other name).
func TestRestoreVerifyCatchesCorruption(t *testing.T) {
	f := standardTree(t)
	rows, err := f.m.ListChunks(f.ctx, lookupIno(t, f, "/docs/f00"))
	if err != nil {
		t.Fatal(err)
	}
	res := f.run("/docs", filepath.Join(destDir(t), "out"), func(o *Options) {
		o.Codec = corruptingCodec{Codec: f.codec, hash: rows[0].Hash}
	})
	got := problemPaths(res, KindIntegrity)
	want := []string{"/docs/f00", "/docs/f00-link"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("integrity problems %v, want %v (all: %+v)", got, want, res.Problems)
	}
	if !strings.Contains(res.Problems[0].Detail, "differs from its keyed hash") {
		t.Errorf("detail: %q", res.Problems[0].Detail)
	}
}

func lookupIno(t *testing.T, f *fixture, p string) int64 {
	t.Helper()
	ino := meta.RootIno
	for _, n := range strings.Split(strings.TrimPrefix(p, "/"), "/") {
		in, err := f.m.Lookup(f.ctx, ino, n)
		if err != nil {
			t.Fatal(err)
		}
		ino = in.Ino
	}
	return ino
}

func problemPaths(res *Result, kind string) []string {
	var out []string
	for _, p := range res.Problems {
		if p.Kind == kind {
			out = append(out, p.Path)
		}
	}
	sort.Strings(out)
	return out
}

// Damage on the channel or in the DB is an integrity problem: a chunk
// row without its message, a message whose bytes do not decrypt, a
// non-sparse file with a chunk missing from the DB. The rest restores.
func TestRestoreIntegrityProblems(t *testing.T) {
	f := standardTree(t)
	f.directFile(meta.RootIno, "/holey.bin", randBytes(2*testCS), 400, false, 1) // chunk 1 has no row
	f.directFile(meta.RootIno, "/gone.bin", randBytes(100), 500, false)
	delete(f.ch.docs, 500)
	f.directFile(meta.RootIno, "/damaged.bin", randBytes(100), 600, false)
	f.ch.docs[600][5] ^= 0xFF
	dest := filepath.Join(destDir(t), "out")
	res := f.run("/", dest, nil)
	got := problemPaths(res, KindIntegrity)
	if strings.Join(got, ",") != "/damaged.bin,/gone.bin,/holey.bin" || len(res.Problems) != 3 {
		t.Fatalf("problems %+v", res.Problems)
	}
	if got, _ := os.ReadFile(filepath.Join(dest, "big.bin")); !bytes.Equal(got, f.want["/big.bin"]) {
		t.Error("big.bin not restored next to the damaged files")
	}
	// The chunk that IS there was written and checked.
	got0, _ := os.ReadFile(filepath.Join(dest, "holey.bin"))
	if len(got0) != 2*testCS || !bytes.Equal(got0[:testCS], f.want["/holey.bin"][:testCS]) {
		t.Error("holey.bin: its present chunk was not restored")
	}
	// A live restore whose mount is still uploading holey.bin: its
	// missing chunk is pending, not lost; the damage stays integrity.
	holey := lookupIno(t, f, "/holey.bin")
	asked := 0
	res = f.run("/", dest, func(o *Options) {
		o.InFlight = func(_ context.Context, files []*Entry) map[int64]string {
			asked++
			if len(files) != 45+3 {
				t.Errorf("InFlight asked about %d files, want every restored one (48)", len(files))
			}
			return map[int64]string{holey: "its uploads were open in the mount"}
		}
	})
	if asked != 1 {
		t.Errorf("InFlight asked %d times, want once", asked)
	}
	if got := problemPaths(res, KindPending); strings.Join(got, ",") != "/holey.bin" {
		t.Errorf("pending %v (all %+v)", got, res.Problems)
	}
	if got := problemPaths(res, KindIntegrity); strings.Join(got, ",") != "/damaged.bin,/gone.bin" {
		t.Errorf("integrity %v", got)
	}
}

type netErr struct{}

func (netErr) Error() string { return "connection reset" }

// Telegram failing after the retries stops the restore (a re-run
// resumes): the failed message's files and those never tried are
// Telegram problems, not integrity ones.
func TestRestoreTelegramFailure(t *testing.T) {
	f := standardTree(t)
	attempts := 0
	f.ch.fail = func(msg int64) error {
		if msg == 100 {
			attempts++
			return netErr{}
		}
		return nil
	}
	res := f.run("/", filepath.Join(destDir(t), "out"), func(o *Options) {
		o.Workers = 1
		o.Attempts = 3
		o.Backoff = time.Millisecond
		o.Retriable = func(err error) bool { var n netErr; return errors.As(err, &n) }
	})
	if attempts != 3 {
		t.Errorf("attempts %d, want 3", attempts)
	}
	if res.Has(KindIntegrity) || !res.Has(KindTelegram) {
		t.Fatalf("problems %+v", res.Problems)
	}
	// Message 100 is first: everything after it was not tried.
	if w, _ := f.ch.calls(); w != 0 {
		t.Errorf("%d whole downloads after Telegram failed", w)
	}
	if n := len(problemPaths(res, KindTelegram)); n != 45 { // 42 files + the hard link, big.bin, sparse.bin
		t.Errorf("%d telegram problems, want 45", n)
	}
}

// SRC can be any directory (its contents become DEST) or a single file
// (restored into DEST under its name); a missing SRC is an error; DEST
// entries in the way of a directory are reported, not removed.
func TestRestoreSubtreesAndConflicts(t *testing.T) {
	f := standardTree(t)
	dest := filepath.Join(destDir(t), "sub")
	if res := f.run("/docs/sub", dest, nil); len(res.Problems) != 0 {
		t.Fatal(res.Problems)
	}
	if got, _ := os.ReadFile(filepath.Join(dest, "deep.txt")); string(got) != "deep\n" {
		t.Errorf("deep.txt = %q", got)
	}
	one := destDir(t)
	if res := f.run("/docs/f03", one, nil); len(res.Problems) != 0 {
		t.Fatal(res.Problems)
	}
	if got, _ := os.ReadFile(filepath.Join(one, "f03")); !bytes.Equal(got, f.want["/docs/f03"]) {
		t.Error("single file not restored into DEST")
	}
	for _, src := range []string{"/nope", "/docs/f03/x", "docs"} {
		if _, err := BuildPlan(f.ctx, f.m, src); err == nil {
			t.Errorf("SRC %q: no error", src)
		}
	}
	// A file where a directory goes, a directory where a file goes.
	conflict := destDir(t)
	os.WriteFile(filepath.Join(conflict, "sub"), []byte("x"), 0o644)
	os.Mkdir(filepath.Join(conflict, "f07"), 0o755)
	res := f.run("/docs", conflict, nil)
	got := problemPaths(res, KindLocal)
	if strings.Join(got, ",") != "/docs/f07,/docs/sub,/docs/sub/deep.txt" || res.Has(KindIntegrity) {
		t.Errorf("local problems %v (all %+v)", got, res.Problems)
	}
	if b, _ := os.ReadFile(filepath.Join(conflict, "sub")); string(b) != "x" {
		t.Error("the file in the way was changed")
	}
}

// A DB name that is not a single file name is reported, never followed
// out of DEST.
func TestRestoreUnsafeName(t *testing.T) {
	f := newFixture(t)
	f.packedFile(meta.RootIno, "/ok", []byte("ok"), 0o644, 1)
	f.closePack()
	// A damaged or crafted DB: "../escape" as a dirent name.
	ino, err := f.m.CreateInode(f.ctx, meta.Inode{Kind: meta.KindFile, Mode: 0o100644, ChunkSize: testCS})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.m.DB().Exec(`INSERT INTO dirents(parent_ino, name, child_ino) VALUES (?,?,?)`, meta.RootIno, "../escape", ino); err != nil {
		t.Fatal(err)
	}
	base := destDir(t)
	res := f.run("/", filepath.Join(base, "out"), nil)
	if got := problemPaths(res, KindIntegrity); len(got) != 1 || got[0] != "/../escape" {
		t.Errorf("problems %+v", res.Problems)
	}
	if _, err := os.Lstat(filepath.Join(base, "escape")); !errors.Is(err, os.ErrNotExist) {
		t.Error("restore wrote outside DEST")
	}
}
