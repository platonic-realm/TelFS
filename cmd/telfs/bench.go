package main

// telfs bench — micro-benchmark a directory through normal file
// syscalls. Pointed at a TelFS mountpoint it measures the full
// kernel → FUSE → daemon → SQLite/chunk stack; pointed at a local
// directory it produces a baseline for comparison. The scenarios map
// onto the FS's distinct cost centers: kernel-side metadata caching
// (stat-hot / stat-deep / stat-miss), dirent scans (readdir), the
// small-file packer path (create / read-small / unlink), and the
// streaming chunk pipeline (seq-write / seq-read / rand-read).
//
// Everything happens under a fresh <dir>/.telfs-bench.<pid>/ workdir,
// removed afterwards unless --keep. Fixtures are built untimed, so
// each scenario also works standalone via --scenarios.

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"telfs/internal/config"
)

// benchEnv carries the resolved knobs + workdir shared by all scenarios.
type benchEnv struct {
	work      string
	files     int   // small-file count (create / read-small / readdir / unlink)
	smallSize int64 // bytes per small file
	bigSize   int64 // bytes of the streaming test file
	bs        int64 // block size for seq-write / seq-read
	iters     int   // op count for metadata scenarios
	jobs      int   // parallel workers (metadata + small-file + rand-read)
	randOps   int   // op count for rand-read
	randBS    int64 // bytes per rand-read op
	fsync     bool  // fsync before close in create / seq-write
}

func (e *benchEnv) smallDir() string      { return filepath.Join(e.work, "small") }
func (e *benchEnv) smallPath(i int) string { return filepath.Join(e.smallDir(), fmt.Sprintf("f-%05d", i)) }
func (e *benchEnv) bigPath() string       { return filepath.Join(e.work, "big") }

// benchResult is one scenario's outcome. lats arrives sorted ascending.
type benchResult struct {
	name  string
	ops   int
	bytes int64 // 0 → no throughput column
	wall  time.Duration
	lats  []time.Duration
}

type benchScenario struct {
	name string
	desc string
	run  func(context.Context, *benchEnv) (benchResult, error)
}

// benchScenarios is the registry, in the order "all" runs them:
// small-file fixtures are built first and consumed (unlink) last, so
// intermediate scenarios can reuse them.
var benchScenarios = []benchScenario{
	{"create", "write+close N small files (packer / metadata write path)", benchCreate},
	{"read-small", "read back the N small files", benchReadSmall},
	{"readdir", "list the N-entry directory repeatedly", benchReaddir},
	{"stat-hot", "stat the same file repeatedly (attr cache)", benchStatHot},
	{"stat-deep", "stat through an 8-deep dir chain (entry cache)", benchStatDeep},
	{"stat-miss", "stat nonexistent names, compiler-include style (negative cache)", benchStatMiss},
	{"seq-write", "stream one big file in --bs blocks (writeback / chunk pipeline)", benchSeqWrite},
	{"seq-read", "stream it back in --bs blocks", benchSeqRead},
	{"rand-read", "random --rand-bs reads within the big file", benchRandRead},
	{"unlink", "delete the N small files", benchUnlink},
}

func cmdBench(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	scenarios := fs.String("scenarios", "all", "comma-separated scenario list, or \"all\"")
	files := fs.Int("files", 200, "small-file count")
	smallSize := fs.String("small-size", "4KiB", "bytes per small file")
	size := fs.String("size", "64MiB", "streaming test file size")
	bs := fs.String("bs", "1MiB", "block size for seq-write/seq-read")
	iters := fs.Int("iters", 5000, "op count for stat/readdir scenarios")
	jobs := fs.Int("jobs", 1, "parallel workers (like make -j) for metadata, small-file and rand-read scenarios")
	randOps := fs.Int("rand-ops", 512, "op count for rand-read")
	randBS := fs.String("rand-bs", "64KiB", "bytes per rand-read op")
	fsyncF := fs.Bool("fsync", false, "fsync before close in create/seq-write (on TelFS: wait for the Telegram upload round trip)")
	keep := fs.Bool("keep", false, "keep the bench workdir instead of deleting it")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: telfs bench [flags] <dir>")
		fmt.Fprintln(os.Stderr, "\nScenarios:")
		for _, s := range benchScenarios {
			fmt.Fprintf(os.Stderr, "  %-11s %s\n", s.name, s.desc)
		}
		fmt.Fprintln(os.Stderr, "\nFlags:")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return fmt.Errorf("bench: exactly one target directory required")
	}
	dir := fs.Arg(0)
	st, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("bench: %w", err)
	}
	if !st.IsDir() {
		return fmt.Errorf("bench: %s is not a directory", dir)
	}

	env := &benchEnv{
		files:   *files,
		iters:   *iters,
		jobs:    *jobs,
		randOps: *randOps,
		fsync:   *fsyncF,
	}
	for _, p := range []struct {
		dst  *int64
		flag string
		val  string
	}{
		{&env.smallSize, "--small-size", *smallSize},
		{&env.bigSize, "--size", *size},
		{&env.bs, "--bs", *bs},
		{&env.randBS, "--rand-bs", *randBS},
	} {
		n, perr := config.ParseHumanBytes(p.val)
		if perr != nil {
			return fmt.Errorf("bench: %s: %w", p.flag, perr)
		}
		if n <= 0 {
			return fmt.Errorf("bench: %s must be positive", p.flag)
		}
		*p.dst = n
	}
	if env.files < 1 || env.iters < 1 || env.jobs < 1 || env.randOps < 1 {
		return fmt.Errorf("bench: --files/--iters/--jobs/--rand-ops must be >= 1")
	}
	if env.randBS > env.bigSize {
		return fmt.Errorf("bench: --rand-bs (%d) must not exceed --size (%d)", env.randBS, env.bigSize)
	}

	selected, err := selectBenchScenarios(*scenarios)
	if err != nil {
		return err
	}

	env.work = filepath.Join(dir, fmt.Sprintf(".telfs-bench.%d", os.Getpid()))
	if err := os.MkdirAll(env.work, 0o755); err != nil {
		return fmt.Errorf("bench: workdir: %w", err)
	}
	if !*keep {
		defer os.RemoveAll(env.work)
	}

	fmt.Printf("bench: dir=%s jobs=%d files=%d small=%s size=%s bs=%s iters=%d fsync=%v\n\n",
		dir, env.jobs, env.files, humanBytes(env.smallSize), humanBytes(env.bigSize), humanBytes(env.bs), env.iters, env.fsync)
	fmt.Printf("%-11s %8s %10s %12s %9s %9s %9s %9s %12s\n",
		"scenario", "ops", "wall", "ops/s", "p50", "p95", "p99", "max", "throughput")
	for _, s := range selected {
		res, err := s.run(ctx, env)
		if err != nil {
			return fmt.Errorf("bench %s: %w", s.name, err)
		}
		printBenchRow(res)
	}
	fmt.Println("\nnotes: on a TelFS mount with trash enabled, deleted bench files land in /.trash until TTL expiry.")
	fmt.Println("       seq-read/rand-read after seq-write in the same run are served from the page cache; remount for cold-read numbers.")
	return nil
}

// selectBenchScenarios resolves a --scenarios spec against the registry,
// preserving registry order for "all" and user order otherwise.
func selectBenchScenarios(spec string) ([]benchScenario, error) {
	if strings.TrimSpace(spec) == "all" {
		return benchScenarios, nil
	}
	var out []benchScenario
	for _, name := range strings.Split(spec, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		found := false
		for _, s := range benchScenarios {
			if s.name == name {
				out = append(out, s)
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("bench: unknown scenario %q (see telfs bench --help)", name)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("bench: no scenarios selected")
	}
	return out, nil
}

func printBenchRow(r benchResult) {
	opsRate := "-"
	thrpt := "-"
	if r.wall > 0 {
		opsRate = fmt.Sprintf("%.0f/s", float64(r.ops)/r.wall.Seconds())
		if r.bytes > 0 {
			thrpt = humanBytes(int64(float64(r.bytes)/r.wall.Seconds())) + "/s"
		}
	}
	fmt.Printf("%-11s %8d %10s %12s %9s %9s %9s %9s %12s\n",
		r.name, r.ops, fmtBenchDur(r.wall), opsRate,
		fmtBenchDur(benchPct(r.lats, 0.50)), fmtBenchDur(benchPct(r.lats, 0.95)),
		fmtBenchDur(benchPct(r.lats, 0.99)), fmtBenchDur(benchPct(r.lats, 1.0)), thrpt)
}

// benchPct returns the q-quantile of an ascending-sorted latency slice.
func benchPct(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(float64(len(sorted)-1)*q)]
}

// fmtBenchDur renders a duration at a precision proportional to its
// magnitude, so columns stay narrow across µs-to-minutes ranges.
func fmtBenchDur(d time.Duration) string {
	switch {
	case d >= 10*time.Second:
		return d.Round(100 * time.Millisecond).String()
	case d >= time.Second:
		return d.Round(10 * time.Millisecond).String()
	case d >= time.Millisecond:
		return d.Round(10 * time.Microsecond).String()
	case d >= 10*time.Microsecond:
		return d.Round(time.Microsecond).String()
	default:
		return d.Round(10 * time.Nanosecond).String()
	}
}

// runBenchOps drives op(i) for i in [0, ops) across env.jobs workers
// (dynamic dispatch, so slow ops don't leave a static shard lagging),
// recording per-op latency. Returned lats are sorted ascending.
func runBenchOps(ctx context.Context, jobs, ops int, op func(i int) error) (time.Duration, []time.Duration, error) {
	if jobs > ops {
		jobs = ops
	}
	var (
		next    atomic.Int64
		wg      sync.WaitGroup
		mu      sync.Mutex
		firstEr error
	)
	perWorker := make([][]time.Duration, jobs)
	start := time.Now()
	for w := 0; w < jobs; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			lats := make([]time.Duration, 0, ops/jobs+1)
			for {
				i := int(next.Add(1) - 1)
				if i >= ops {
					break
				}
				if err := ctx.Err(); err != nil {
					mu.Lock()
					if firstEr == nil {
						firstEr = err
					}
					mu.Unlock()
					break
				}
				t0 := time.Now()
				if err := op(i); err != nil {
					mu.Lock()
					if firstEr == nil {
						firstEr = fmt.Errorf("op %d: %w", i, err)
					}
					mu.Unlock()
					break
				}
				lats = append(lats, time.Since(t0))
			}
			perWorker[w] = lats
		}(w)
	}
	wg.Wait()
	wall := time.Since(start)
	if firstEr != nil {
		return 0, nil, firstEr
	}
	var merged []time.Duration
	for _, l := range perWorker {
		merged = append(merged, l...)
	}
	sort.Slice(merged, func(a, b int) bool { return merged[a] < merged[b] })
	return wall, merged, nil
}

// benchBuf returns n deterministic pseudo-random bytes. Random (not
// zero) so compression codecs can't flatter the numbers; deterministic
// so runs are comparable.
func benchBuf(n int64) []byte {
	buf := make([]byte, n)
	rand.New(rand.NewSource(1)).Read(buf)
	return buf
}

// stampBuf makes a block globally unique by overwriting its first 8
// bytes with id — defeats chunk-level dedup (convergent encryption)
// without the cost of generating fresh random data per block.
func stampBuf(b []byte, id uint64) {
	if len(b) >= 8 {
		binary.LittleEndian.PutUint64(b, id)
	}
}

// writeBenchFile writes buf to path and closes it, optionally fsyncing
// first (which on TelFS waits for the chunk-upload round trip).
func writeBenchFile(path string, buf []byte, fsync bool) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return err
	}
	if fsync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	return f.Close()
}

// --- small-file scenarios -------------------------------------------

// createSmallOps is the shared engine for benchCreate and the untimed
// fixture builds in read-small/readdir/unlink.
func createSmallOps(ctx context.Context, env *benchEnv, fsync bool) (time.Duration, []time.Duration, error) {
	if err := os.MkdirAll(env.smallDir(), 0o755); err != nil {
		return 0, nil, err
	}
	// Per-worker buffers via a pool: stampBuf mutates the buffer, so
	// sharing one across jobs would race.
	pool := sync.Pool{New: func() any { b := benchBuf(env.smallSize); return &b }}
	return runBenchOps(ctx, env.jobs, env.files, func(i int) error {
		bp := pool.Get().(*[]byte)
		defer pool.Put(bp)
		stampBuf(*bp, uint64(i))
		return writeBenchFile(env.smallPath(i), *bp, fsync)
	})
}

// ensureSmallFixture builds the small-file set untimed (and without
// fsync) if a scenario that needs it runs standalone.
func ensureSmallFixture(ctx context.Context, env *benchEnv) error {
	if _, err := os.Stat(env.smallPath(env.files - 1)); err == nil {
		return nil
	}
	_, _, err := createSmallOps(ctx, env, false)
	return err
}

func benchCreate(ctx context.Context, env *benchEnv) (benchResult, error) {
	wall, lats, err := createSmallOps(ctx, env, env.fsync)
	if err != nil {
		return benchResult{}, err
	}
	return benchResult{name: "create", ops: env.files, bytes: env.smallSize * int64(env.files), wall: wall, lats: lats}, nil
}

func benchReadSmall(ctx context.Context, env *benchEnv) (benchResult, error) {
	if err := ensureSmallFixture(ctx, env); err != nil {
		return benchResult{}, err
	}
	wall, lats, err := runBenchOps(ctx, env.jobs, env.files, func(i int) error {
		b, err := os.ReadFile(env.smallPath(i))
		if err != nil {
			return err
		}
		if int64(len(b)) != env.smallSize {
			return fmt.Errorf("short read: %d of %d bytes", len(b), env.smallSize)
		}
		return nil
	})
	if err != nil {
		return benchResult{}, err
	}
	return benchResult{name: "read-small", ops: env.files, bytes: env.smallSize * int64(env.files), wall: wall, lats: lats}, nil
}

func benchReaddir(ctx context.Context, env *benchEnv) (benchResult, error) {
	if err := ensureSmallFixture(ctx, env); err != nil {
		return benchResult{}, err
	}
	wall, lats, err := runBenchOps(ctx, env.jobs, env.iters, func(int) error {
		ents, err := os.ReadDir(env.smallDir())
		if err != nil {
			return err
		}
		if len(ents) < env.files {
			return fmt.Errorf("short listing: %d of %d entries", len(ents), env.files)
		}
		return nil
	})
	if err != nil {
		return benchResult{}, err
	}
	return benchResult{name: "readdir", ops: env.iters, wall: wall, lats: lats}, nil
}

func benchUnlink(ctx context.Context, env *benchEnv) (benchResult, error) {
	if err := ensureSmallFixture(ctx, env); err != nil {
		return benchResult{}, err
	}
	wall, lats, err := runBenchOps(ctx, env.jobs, env.files, func(i int) error {
		return os.Remove(env.smallPath(i))
	})
	if err != nil {
		return benchResult{}, err
	}
	return benchResult{name: "unlink", ops: env.files, wall: wall, lats: lats}, nil
}

// --- stat scenarios -------------------------------------------------

func benchStatHot(ctx context.Context, env *benchEnv) (benchResult, error) {
	hot := filepath.Join(env.work, "hot")
	if err := writeBenchFile(hot, benchBuf(64), false); err != nil {
		return benchResult{}, err
	}
	wall, lats, err := runBenchOps(ctx, env.jobs, env.iters, func(int) error {
		_, err := os.Stat(hot)
		return err
	})
	if err != nil {
		return benchResult{}, err
	}
	return benchResult{name: "stat-hot", ops: env.iters, wall: wall, lats: lats}, nil
}

func benchStatDeep(ctx context.Context, env *benchEnv) (benchResult, error) {
	// 8 path components below the workdir: every uncached stat costs
	// one LOOKUP per component, which is exactly what EntryTimeout
	// amortizes.
	deepDir := env.work
	for i := 0; i < 8; i++ {
		deepDir = filepath.Join(deepDir, fmt.Sprintf("d%d", i))
	}
	if err := os.MkdirAll(deepDir, 0o755); err != nil {
		return benchResult{}, err
	}
	leaf := filepath.Join(deepDir, "leaf")
	if err := writeBenchFile(leaf, benchBuf(64), false); err != nil {
		return benchResult{}, err
	}
	wall, lats, err := runBenchOps(ctx, env.jobs, env.iters, func(int) error {
		_, err := os.Stat(leaf)
		return err
	})
	if err != nil {
		return benchResult{}, err
	}
	return benchResult{name: "stat-deep", ops: env.iters, wall: wall, lats: lats}, nil
}

func benchStatMiss(ctx context.Context, env *benchEnv) (benchResult, error) {
	// Model a compiler probing #include search dirs: 8 dirs, the same
	// missing name cycled through them. Without NegativeTimeout every
	// probe is a fresh FUSE round trip; with it only the first miss
	// per dir per cache window reaches the daemon.
	incDirs := make([]string, 8)
	for i := range incDirs {
		incDirs[i] = filepath.Join(env.work, "inc", fmt.Sprintf("inc%d", i))
		if err := os.MkdirAll(incDirs[i], 0o755); err != nil {
			return benchResult{}, err
		}
	}
	wall, lats, err := runBenchOps(ctx, env.jobs, env.iters, func(i int) error {
		_, err := os.Stat(filepath.Join(incDirs[i%len(incDirs)], "missing.h"))
		if err == nil {
			return fmt.Errorf("expected ENOENT, file exists")
		}
		if !os.IsNotExist(err) {
			return err
		}
		return nil
	})
	if err != nil {
		return benchResult{}, err
	}
	return benchResult{name: "stat-miss", ops: env.iters, wall: wall, lats: lats}, nil
}

// --- streaming scenarios --------------------------------------------

// seqWriteOps streams the big file; also the untimed fixture build for
// seq-read/rand-read run standalone.
func seqWriteOps(ctx context.Context, env *benchEnv, fsync bool) (time.Duration, []time.Duration, error) {
	f, err := os.OpenFile(env.bigPath(), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return 0, nil, err
	}
	defer f.Close()
	buf := benchBuf(env.bs)
	nblocks := int((env.bigSize + env.bs - 1) / env.bs)
	lats := make([]time.Duration, 0, nblocks)
	start := time.Now()
	var written int64
	for i := 0; i < nblocks; i++ {
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}
		b := buf
		if rem := env.bigSize - written; rem < env.bs {
			b = buf[:rem]
		}
		stampBuf(b, uint64(i))
		t0 := time.Now()
		if _, err := f.Write(b); err != nil {
			return 0, nil, err
		}
		lats = append(lats, time.Since(t0))
		written += int64(len(b))
	}
	if fsync {
		if err := f.Sync(); err != nil {
			return 0, nil, err
		}
	}
	if err := f.Close(); err != nil {
		return 0, nil, err
	}
	wall := time.Since(start) // includes fsync + close (flush dispatch)
	sort.Slice(lats, func(a, b int) bool { return lats[a] < lats[b] })
	return wall, lats, nil
}

func ensureBigFixture(ctx context.Context, env *benchEnv) error {
	if st, err := os.Stat(env.bigPath()); err == nil && st.Size() == env.bigSize {
		return nil
	}
	_, _, err := seqWriteOps(ctx, env, false)
	return err
}

func benchSeqWrite(ctx context.Context, env *benchEnv) (benchResult, error) {
	wall, lats, err := seqWriteOps(ctx, env, env.fsync)
	if err != nil {
		return benchResult{}, err
	}
	return benchResult{name: "seq-write", ops: len(lats), bytes: env.bigSize, wall: wall, lats: lats}, nil
}

func benchSeqRead(ctx context.Context, env *benchEnv) (benchResult, error) {
	if err := ensureBigFixture(ctx, env); err != nil {
		return benchResult{}, err
	}
	f, err := os.Open(env.bigPath())
	if err != nil {
		return benchResult{}, err
	}
	defer f.Close()
	buf := make([]byte, env.bs)
	var lats []time.Duration
	var total int64
	start := time.Now()
	for {
		if err := ctx.Err(); err != nil {
			return benchResult{}, err
		}
		t0 := time.Now()
		n, err := f.Read(buf)
		if n > 0 {
			lats = append(lats, time.Since(t0))
			total += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return benchResult{}, err
		}
	}
	wall := time.Since(start)
	if total != env.bigSize {
		return benchResult{}, fmt.Errorf("short read: %d of %d bytes", total, env.bigSize)
	}
	sort.Slice(lats, func(a, b int) bool { return lats[a] < lats[b] })
	return benchResult{name: "seq-read", ops: len(lats), bytes: total, wall: wall, lats: lats}, nil
}

func benchRandRead(ctx context.Context, env *benchEnv) (benchResult, error) {
	if err := ensureBigFixture(ctx, env); err != nil {
		return benchResult{}, err
	}
	f, err := os.Open(env.bigPath())
	if err != nil {
		return benchResult{}, err
	}
	defer f.Close()
	// Precomputed deterministic offsets: same access pattern every run.
	rnd := rand.New(rand.NewSource(2))
	offs := make([]int64, env.randOps)
	for i := range offs {
		offs[i] = rnd.Int63n(env.bigSize - env.randBS + 1)
	}
	pool := sync.Pool{New: func() any { b := make([]byte, env.randBS); return &b }}
	wall, lats, err := runBenchOps(ctx, env.jobs, env.randOps, func(i int) error {
		bp := pool.Get().(*[]byte)
		defer pool.Put(bp)
		n, err := f.ReadAt(*bp, offs[i])
		if err != nil && err != io.EOF {
			return err
		}
		if int64(n) != env.randBS {
			return fmt.Errorf("short read at %d: %d of %d bytes", offs[i], n, env.randBS)
		}
		return nil
	})
	if err != nil {
		return benchResult{}, err
	}
	return benchResult{name: "rand-read", ops: env.randOps, bytes: env.randBS * int64(env.randOps), wall: wall, lats: lats}, nil
}
