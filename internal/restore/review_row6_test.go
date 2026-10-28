package restore

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"telfs/internal/meta"
)

// A sparse file restored over a same-size file DEST already holds (a
// non-sparse version of it): its holes must come back as zeros, not as
// the old bytes — and the restore must not call it identical.
func TestRestoreSparseOverExistingFile(t *testing.T) {
	f := newFixture(t)
	full := randBytes(3 * testCS)
	ino := f.directFile(meta.RootIno, "/img.raw", full, 700, false)
	dest := destDir(t)
	if res := f.run("/", dest, nil); len(res.Problems) != 0 {
		t.Fatal(res.Problems)
	}
	// Truncated to 0 and grown back with only chunk 2 written: chunk 2
	// keeps its row (same bytes), chunks 0 and 1 are holes.
	f.m.DeleteChunk(f.ctx, ino, 0)
	f.m.DeleteChunk(f.ctx, ino, 1)
	if err := f.m.SetInodeSparse(f.ctx, ino); err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 3*testCS)
	copy(want[2*testCS:], full[2*testCS:])
	res := f.run("/", dest, nil)
	if len(res.Problems) != 0 || res.Identical != 0 {
		t.Fatalf("problems %+v, identical %d (the old bytes in its holes make it differ)", res.Problems, res.Identical)
	}
	if got, _ := os.ReadFile(filepath.Join(dest, "img.raw")); !bytes.Equal(got, want) {
		t.Fatal("the holes kept the old version's bytes")
	}
	// Now it is identical: nothing downloaded again.
	f.ch.reset()
	if res := f.run("/", dest, nil); len(res.Problems) != 0 || res.Identical != 1 {
		t.Fatalf("re-run: %+v identical %d", res.Problems, res.Identical)
	}
	if w, r := f.ch.calls(); w+r != 0 {
		t.Fatalf("re-run downloaded %d+%d", w, r)
	}
}

// uncovered: holes and the tails of short rows, merged.
func TestUncovered(t *testing.T) {
	in := meta.Inode{Size: 3*testCS + 100, ChunkSize: testCS, Sparse: true}
	e := &Entry{Inode: in, Chunks: []meta.Chunk{{Idx: 1, Size: testCS / 2}, {Idx: 3, Size: 100}}}
	got := uncovered(e)
	want := [][2]int64{{0, testCS}, {testCS + testCS/2, 3 * testCS}}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("uncovered %v, want %v", got, want)
	}
	full := &Entry{Inode: meta.Inode{Size: 2 * testCS, ChunkSize: testCS}, Chunks: []meta.Chunk{{Idx: 0, Size: testCS}, {Idx: 1, Size: testCS}}}
	if got := uncovered(full); len(got) != 0 {
		t.Fatalf("a whole file: %v", got)
	}
}

// zeros reads only data regions and finds a non-zero byte in a hole.
func TestZeros(t *testing.T) {
	p := filepath.Join(t.TempDir(), "f")
	if err := createFile(p, 8<<20); err != nil {
		t.Fatal(err)
	}
	if ok, err := zerosIn(p, [][2]int64{{0, 8 << 20}}); err != nil || !ok {
		t.Fatalf("a fresh sparse file: %v %v", ok, err)
	}
	if err := writeAt(p, []byte{0, 0, 7}, 5<<20); err != nil {
		t.Fatal(err)
	}
	if ok, _ := zerosIn(p, [][2]int64{{0, 4 << 20}, {6 << 20, 8 << 20}}); !ok {
		t.Fatal("ranges without the byte")
	}
	if ok, _ := zerosIn(p, [][2]int64{{0, 4 << 20}, {5 << 20, 5<<20 + 3}}); ok {
		t.Fatal("the byte at 5 MiB + 2 was not seen")
	}
	if ok, _ := zerosIn(p, [][2]int64{{8 << 20, 9 << 20}}); ok {
		t.Fatal("a range past the end must not count as zeros")
	}
}

// Verification checks the holes too: a byte that lands in one is an
// integrity problem.
func TestRestoreVerifyChecksHoles(t *testing.T) {
	f := newFixture(t)
	data := make([]byte, 3*testCS)
	copy(data[testCS:], randBytes(testCS))
	f.directFile(meta.RootIno, "/sparse.bin", data, 800, true, 0, 2)
	dest := destDir(t)
	plan, err := BuildPlan(f.ctx, f.m, "/")
	if err != nil {
		t.Fatal(err)
	}
	r := &runner{opts: Options{Dest: dest, Codec: f.codec, Download: f.ch, Workers: 1, Attempts: 1, Backoff: time.Millisecond, Progress: time.Minute}, plan: plan,
		files: map[int]*fileRun{}, created: make([]bool, len(plan.Entries)), linksOf: map[int][]int{}, problems: map[int][]Problem{}}
	res := &Result{}
	if err := r.structure(f.ctx, res); err != nil {
		t.Fatal(err)
	}
	if err := r.download(f.ctx); err != nil {
		t.Fatal(err)
	}
	writeAt(filepath.Join(dest, "sparse.bin"), []byte{1}, 2*testCS+5) // a byte in the last hole
	if err := r.verify(f.ctx, res); err != nil {
		t.Fatal(err)
	}
	ps := r.collect()
	if len(ps) != 1 || ps[0].Kind != KindIntegrity || !strings.Contains(ps[0].Detail, "not zero") {
		t.Fatalf("problems %+v", ps)
	}
}

// Two names that were one inode in DEST (an earlier restore) but are two
// files now come back as two files with their own attributes.
func TestRestoreSplitsHardLinkedDest(t *testing.T) {
	f := newFixture(t)
	content := []byte("same bytes\n")
	ino := f.packedFile(meta.RootIno, "/a", content, 0o644, 1_600_000_000)
	if err := f.m.Link(f.ctx, meta.RootIno, "b", ino); err != nil {
		t.Fatal(err)
	}
	f.closePack()
	dest := destDir(t)
	if res := f.run("/", dest, nil); len(res.Problems) != 0 {
		t.Fatal(res.Problems)
	}
	sameFile := func() bool {
		a, _ := os.Stat(filepath.Join(dest, "a"))
		b, _ := os.Stat(filepath.Join(dest, "b"))
		return a != nil && b != nil && os.SameFile(a, b)
	}
	if !sameFile() {
		t.Fatal("first restore: a and b must be one inode")
	}
	// rm b; cp a b — b is now its own file (other mode and time).
	if err := f.m.Unlink(f.ctx, meta.RootIno, "b"); err != nil {
		t.Fatal(err)
	}
	f.packedFile(meta.RootIno, "/b", content, 0o600, 1_700_000_000)
	f.closePack()
	res := f.run("/", dest, nil)
	if len(res.Problems) != 0 {
		t.Fatal(res.Problems)
	}
	if sameFile() {
		t.Fatal("a and b are separate files now; DEST keeps them linked")
	}
	for name, want := range map[string]struct {
		mode  os.FileMode
		mtime int64
	}{"a": {0o644, 1_600_000_000}, "b": {0o600, 1_700_000_000}} {
		fi, err := os.Stat(filepath.Join(dest, name))
		if err != nil || fi.Mode().Perm() != want.mode || mtimeOf(t, filepath.Join(dest, name)) != want.mtime {
			t.Errorf("%s: mode %v mtime %d, want %v %d", name, fi.Mode().Perm(), mtimeOf(t, filepath.Join(dest, name)), want.mode, want.mtime)
		}
		if got, _ := os.ReadFile(filepath.Join(dest, name)); !bytes.Equal(got, content) {
			t.Errorf("%s content", name)
		}
	}
	// A planned hard link that DEST already has is kept as it is.
	var st syscall.Stat_t
	syscall.Stat(filepath.Join(dest, "a"), &st)
	if st.Nlink != 1 {
		t.Errorf("a has %d names", st.Nlink)
	}
}

// A live restore: a whole file the mount had not finished uploading (in
// place rewrite: old rows under the new size and time) is pending, not
// "OK".
func TestRestorePendingWholeFile(t *testing.T) {
	f := newFixture(t)
	ino := f.directFile(meta.RootIno, "/db.img", randBytes(2*testCS), 900, false)
	f.directFile(meta.RootIno, "/other", randBytes(100), 950, false)
	res := f.run("/", destDir(t), func(o *Options) {
		o.InFlight = func(_ context.Context, files []*Entry) map[int64]string {
			return map[int64]string{ino: "its uploads were open in the mount"}
		}
	})
	if got := problemPaths(res, KindPending); len(got) != 1 || got[0] != "/db.img" || res.Has(KindIntegrity) {
		t.Fatalf("problems %+v", res.Problems)
	}
}
