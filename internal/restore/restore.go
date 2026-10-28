package restore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"telfs/internal/meta"
)

// Downloader fetches channel documents. The restore asks for each
// message once: Fetch for a direct chunk's whole document, FetchRange
// for the part of a pack its extents need.
type Downloader interface {
	Fetch(ctx context.Context, msgID int64) ([]byte, error)
	FetchRange(ctx context.Context, msgID, off, length int64) ([]byte, error)
}

// Codec turns a chunk's wire bytes into plaintext and computes keyed
// content hashes — chunk.WireCodec of a format-2 filesystem.
type Codec interface {
	Decode(ino int64, idx int32, hash, wire []byte) ([]byte, error)
	Hash(plaintext []byte) []byte
}

// Problem kinds, in the order the exit code weighs them.
const (
	// KindIntegrity: the filesystem's data does not match its metadata
	// (missing or short chunks, a chunk that does not decrypt, a message
	// that is gone, a restored file whose bytes differ) — exit 7.
	KindIntegrity = "integrity"
	// KindTelegram: Telegram or the channel failed after the retries;
	// re-running the restore resumes — exit 6.
	KindTelegram = "telegram"
	// KindPending: a live restore read a file whose data the mount had
	// not uploaded yet (missing or short chunks still in flight) — `telfs
	// sync --wait`, then restore again — exit 4.
	KindPending = "pending"
	// KindLocal: writing DEST failed (space, permissions, an entry in
	// the way) — exit 1.
	KindLocal = "local"
)

// weight orders problem kinds: the weightiest names a file's problem
// (and decides the exit code).
func weight(kind string) int {
	switch kind {
	case KindIntegrity:
		return 4
	case KindTelegram:
		return 3
	case KindPending:
		return 2
	}
	return 1
}

// Problem is one file the restore could not bring back exactly.
type Problem struct {
	Kind   string
	Path   string // inside the filesystem
	Detail string
}

// Options configure Run.
type Options struct {
	Dest     string
	Codec    Codec
	Download Downloader
	// Workers is the number of messages downloaded at once (default 4).
	Workers int
	// Retriable says whether a download error is worth another try
	// (network, Telegram 5xx); nil = never retry.
	Retriable func(error) bool
	// Telegram says whether a download error (after the retries) is a
	// failure of Telegram or the channel — the restore stops and a re-run
	// resumes — rather than a message the data needs being gone or
	// damaged (an integrity problem). nil = Retriable.
	Telegram func(error) bool
	// Attempts per message for retriable errors (default 5) and the
	// first back-off (default 2 s, doubled per attempt).
	Attempts int
	Backoff  time.Duration
	// InFlight says whether a file the DB shows with missing or short
	// chunks is still being uploaded by the mount (a restore of the live
	// filesystem while it writes) rather than damaged; nil = never (a
	// version is complete by construction).
	InFlight func(context.Context, *Entry) bool
	// Chown restores owners (only root can; default: when running as
	// root).
	Chown bool
	// Logf prints progress; nil = quiet.
	Logf func(format string, a ...any)
	// Progress is how often a long download reports (default 10 s).
	Progress time.Duration
}

// Result is what Run did.
type Result struct {
	Dirs, Files, Symlinks, Links int
	// Identical files were already in DEST (size and every chunk's
	// keyed hash): not downloaded or written again.
	Identical int
	// ChunksPresent are chunks of other files already right in DEST.
	ChunksPresent int
	Messages      int
	Downloaded    int64 // bytes fetched from the channel
	Written       int64 // bytes written into DEST
	Verified      int   // files re-read and checked after writing
	Stray         int   // chunk rows past the end of their file
	Problems      []Problem
}

// Has reports whether any problem is of kind k.
func (r *Result) Has(kind string) bool {
	for _, p := range r.Problems {
		if p.Kind == kind {
			return true
		}
	}
	return false
}

// fileRun is the per-file state of a data-holding regular file.
type fileRun struct {
	present []bool   // per Entry.Chunks: already right in DEST
	want    [][]byte // per Entry.Chunks: the keyed hash DEST must show (nil = the row's)
	skip    bool     // identical in DEST
}

type runner struct {
	opts    Options
	plan    *Plan
	files   map[int]*fileRun // entry index → state
	created []bool           // per entry: exists in DEST as planned

	mu       sync.Mutex
	problems map[int][]Problem // entry index → problems (first decides the kind)
	extra    []Problem         // problems not tied to an entry

	messages   atomic.Int64
	downloaded atomic.Int64
	written    atomic.Int64
}

// Run restores plan into opts.Dest. The error is for a restore that
// could not run at all (or was canceled); per-file failures are in
// Result.Problems.
func Run(ctx context.Context, plan *Plan, opts Options) (*Result, error) {
	if opts.Workers <= 0 {
		opts.Workers = 4
	}
	if opts.Attempts <= 0 {
		opts.Attempts = 5
	}
	if opts.Backoff <= 0 {
		opts.Backoff = 2 * time.Second
	}
	if opts.Progress <= 0 {
		opts.Progress = 10 * time.Second
	}
	if opts.Codec == nil || opts.Download == nil {
		return nil, errors.New("restore: no codec or downloader")
	}
	r := &runner{opts: opts, plan: plan, files: map[int]*fileRun{}, created: make([]bool, len(plan.Entries)),
		problems: map[int][]Problem{}, extra: append([]Problem(nil), plan.Problems...)}
	res := &Result{Stray: plan.Stray}
	res.Dirs, res.Files, res.Symlinks, res.Links, _ = plan.Totals()

	if err := r.structure(ctx, res); err != nil {
		return nil, err
	}
	if err := r.download(ctx); err != nil {
		return nil, err
	}
	if err := r.verify(ctx, res); err != nil {
		return nil, err
	}
	r.attributes()
	res.Messages = int(r.messages.Load())
	res.Downloaded = r.downloaded.Load()
	res.Written = r.written.Load()
	res.Problems = r.collect()
	return res, nil
}

func (r *runner) logf(format string, a ...any) {
	if r.opts.Logf != nil {
		r.opts.Logf(format, a...)
	}
}

func (r *runner) dest(e *Entry) string {
	if e.Rel == "" {
		return r.opts.Dest
	}
	return filepath.Join(r.opts.Dest, filepath.FromSlash(e.Rel))
}

func (r *runner) problem(i int, kind, format string, a ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.problems[i] = append(r.problems[i], Problem{Kind: kind, Path: r.plan.Entries[i].Path, Detail: fmt.Sprintf(format, a...)})
}

func (r *runner) failed(i int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.problems[i]) > 0
}

// collect returns one problem per file (its first, with a count of the
// others), in tree order, after those found while planning.
func (r *runner) collect() []Problem {
	out := append([]Problem(nil), r.extra...)
	idx := make([]int, 0, len(r.problems))
	for i := range r.problems {
		idx = append(idx, i)
	}
	sort.Ints(idx)
	links := map[int][]int{} // first name → its other names
	for i, e := range r.plan.Entries {
		if e.LinkOf >= 0 {
			links[e.LinkOf] = append(links[e.LinkOf], i)
		}
	}
	for _, i := range idx {
		ps := r.problems[i]
		p := ps[0]
		// The weightiest kind among the file's problems names it.
		for _, q := range ps {
			if weight(q.Kind) > weight(p.Kind) {
				p.Kind = q.Kind
			}
		}
		if len(ps) > 1 {
			p.Detail += fmt.Sprintf(" (+%d more)", len(ps)-1)
		}
		out = append(out, p)
		// The file's other names show the same bytes.
		for _, l := range links[i] {
			if _, own := r.problems[l]; !own {
				out = append(out, Problem{Kind: p.Kind, Path: r.plan.Entries[l].Path, Detail: "hard link of " + p.Path + ": " + p.Detail})
			}
		}
	}
	return out
}

// ---- 1. the tree ----

// structure creates DEST's directories, symlinks, (empty, full-size)
// files and hard links, and finds what is already right.
func (r *runner) structure(ctx context.Context, res *Result) error {
	if err := os.MkdirAll(r.opts.Dest, 0o700); err != nil {
		return fmt.Errorf("restore: DEST: %w", err)
	}
	r.logf("Creating the tree in %s (%d entries)…", r.opts.Dest, len(r.plan.Entries))
	defer func() {
		if res.Identical > 0 || res.ChunksPresent > 0 {
			r.logf("  %d file(s) already identical in DEST, %d chunk(s) of other files already right", res.Identical, res.ChunksPresent)
		}
	}()
	for i := range r.plan.Entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		e := &r.plan.Entries[i]
		p := r.dest(e)
		if i > 0 && !r.parentReady(e) {
			r.problem(i, KindLocal, "its directory could not be created")
			continue
		}
		var err error
		switch {
		case e.LinkOf >= 0:
			err = r.makeLink(e, p)
		case e.Inode.Kind == meta.KindDir:
			err = makeDir(p)
		case e.Inode.Kind == meta.KindSymlink:
			err = makeSymlink(e.Inode.SymlinkTarget, p)
		default:
			err = r.makeFile(i, e, p, res)
		}
		if err != nil {
			r.problem(i, KindLocal, "%v", err)
			continue
		}
		r.created[i] = true
	}
	return nil
}

// parentReady reports whether e's directory exists as planned.
func (r *runner) parentReady(e *Entry) bool {
	dir := filepath.Dir(r.dest(e))
	if filepath.Clean(dir) == filepath.Clean(r.opts.Dest) {
		return true
	}
	fi, err := os.Lstat(dir)
	return err == nil && fi.IsDir()
}

// makeDir creates a directory or takes an existing one, writable by its
// owner until the attributes pass sets the final mode.
func makeDir(p string) error {
	fi, err := os.Lstat(p)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return os.Mkdir(p, 0o700)
	case err != nil:
		return err
	case !fi.IsDir():
		return fmt.Errorf("%s exists and is not a directory", p)
	}
	return os.Chmod(p, fi.Mode().Perm()|0o700)
}

func makeSymlink(target, p string) error {
	fi, err := os.Lstat(p)
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return err
	case fi.IsDir():
		return fmt.Errorf("%s exists and is a directory", p)
	case fi.Mode()&os.ModeSymlink != 0:
		if t, err := os.Readlink(p); err == nil && t == target {
			return nil
		}
		fallthrough
	default:
		if err := os.Remove(p); err != nil {
			return err
		}
	}
	return os.Symlink(target, p)
}

func (r *runner) makeLink(e *Entry, p string) error {
	if !r.created[e.LinkOf] {
		return fmt.Errorf("its first name %s could not be restored", r.plan.Entries[e.LinkOf].Path)
	}
	primary := r.dest(&r.plan.Entries[e.LinkOf])
	pfi, err := os.Lstat(primary)
	if err != nil {
		return err
	}
	fi, err := os.Lstat(p)
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return err
	case os.SameFile(fi, pfi):
		return nil
	case fi.IsDir():
		return fmt.Errorf("%s exists and is a directory", p)
	default:
		if err := os.Remove(p); err != nil {
			return err
		}
	}
	return os.Link(primary, p)
}

// makeFile creates a regular file at its full size (the chunks are
// written into it later), or keeps an existing one of the right size:
// identical → skipped; else, if no other name shares it, only its wrong
// chunks are downloaded again.
func (r *runner) makeFile(i int, e *Entry, p string, res *Result) error {
	fr := &fileRun{present: make([]bool, len(e.Chunks)), want: make([][]byte, len(e.Chunks))}
	r.files[i] = fr
	fi, err := os.Lstat(p)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return createFile(p, e.Inode.Size)
	case err != nil:
		return err
	case fi.IsDir():
		return fmt.Errorf("%s exists and is a directory", p)
	case fi.Mode().IsRegular() && fi.Size() == e.Inode.Size:
		n, err := r.checkExisting(e, p, fr)
		if err != nil {
			break // unreadable: write it anew
		}
		if n == len(e.Chunks) && len(e.Missing) == 0 && len(e.Short) == 0 {
			fr.skip = true
			res.Identical++
			return nil
		}
		if st, ok := fi.Sys().(*syscall.Stat_t); ok && st.Nlink == 1 {
			res.ChunksPresent += n
			return os.Chmod(p, fi.Mode().Perm()|0o600)
		}
		// Shared with another name in DEST: writing into it would change
		// that one too. Write it anew.
		for j := range fr.present {
			fr.present[j] = false
		}
	}
	if err := os.Remove(p); err != nil {
		return err
	}
	return createFile(p, e.Inode.Size)
}

func createFile(p string, size int64) error {
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	if err := f.Truncate(size); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// checkExisting marks the chunks an existing file already holds and
// returns how many. A chunk longer than its slot (a stale row past a
// shrink) cannot be checked against its hash and counts as absent.
func (r *runner) checkExisting(e *Entry, p string, fr *fileRun) (int, error) {
	f, err := openRead(p)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var buf []byte
	n := 0
	for j, c := range e.Chunks {
		if int64(c.Size) > slotLen(e.Inode, c.Idx) {
			continue
		}
		ok, err := r.chunkMatches(f, e, c, c.Hash, &buf)
		if err != nil {
			return 0, err
		}
		if ok {
			fr.present[j] = true
			n++
		}
	}
	return n, nil
}

// openRead opens a restored file without following a symlink, making it
// readable by its owner first when a finished restore left it 0000.
func openRead(p string) (*os.File, error) {
	f, err := os.OpenFile(p, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if errors.Is(err, os.ErrPermission) {
		if fi, serr := os.Lstat(p); serr == nil && fi.Mode().IsRegular() {
			if os.Chmod(p, fi.Mode().Perm()|0o600) == nil {
				return os.OpenFile(p, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
			}
		}
	}
	return f, err
}

// chunkMatches reads chunk c's bytes of the file and compares their
// keyed hash with want.
func (r *runner) chunkMatches(f *os.File, e *Entry, c meta.Chunk, want []byte, buf *[]byte) (bool, error) {
	n := int64(c.Size)
	if s := slotLen(e.Inode, c.Idx); n > s {
		n = s
	}
	if int64(cap(*buf)) < n {
		*buf = make([]byte, n)
	}
	b := (*buf)[:n]
	if n > 0 {
		if _, err := f.ReadAt(b, int64(c.Idx)*e.Inode.ChunkSize); errors.Is(err, io.EOF) {
			return false, nil // the file is shorter than the chunk's place
		} else if err != nil {
			return false, err
		}
	}
	return len(want) > 0 && bytes.Equal(r.opts.Codec.Hash(b), want), nil
}

// ---- 2. the data ----

type target struct {
	entry int
	chunk int // index into Entry.Chunks
}

type job struct {
	msg     int64
	targets []target
}

// download fetches every message the remaining chunks need, once, over
// the worker pool, in message order (≈ upload order).
func (r *runner) download(ctx context.Context) error {
	byMsg := map[int64][]target{}
	for i := range r.plan.Entries {
		fr := r.files[i]
		if fr == nil || fr.skip || !r.created[i] {
			continue
		}
		for j, c := range r.plan.Entries[i].Chunks {
			if !fr.present[j] {
				byMsg[c.TGMessageID] = append(byMsg[c.TGMessageID], target{i, j})
			}
		}
	}
	jobs := make([]job, 0, len(byMsg))
	for msg, ts := range byMsg {
		jobs = append(jobs, job{msg, ts})
	}
	sort.Slice(jobs, func(a, b int) bool { return jobs[a].msg < jobs[b].msg })
	if len(jobs) == 0 {
		return nil
	}
	r.logf("Downloading %d message(s) with %d worker(s)…", len(jobs), r.opts.Workers)

	ch := make(chan job)
	var wg sync.WaitGroup
	var tgFailed atomic.Pointer[error]
	for w := 0; w < r.opts.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var buf []byte
			for j := range ch {
				if p := tgFailed.Load(); p != nil {
					for _, t := range j.targets {
						r.problem(t.entry, KindTelegram, "not downloaded: Telegram failed (%v)", *p)
					}
					continue
				}
				if err := r.doJob(ctx, j, &buf); err != nil {
					tgFailed.CompareAndSwap(nil, &err)
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(r.opts.Progress)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				r.logf("  …%d/%d messages, %s downloaded", r.messages.Load(), len(jobs), human(r.downloaded.Load()))
			}
		}
	}()
	var err error
feed:
	for _, j := range jobs {
		select {
		case ch <- j:
		case <-ctx.Done():
			err = ctx.Err()
			break feed
		}
	}
	close(ch)
	wg.Wait()
	close(done)
	if err == nil {
		err = ctx.Err()
	}
	return err
}

// doJob downloads one message and writes every chunk it holds that the
// restore needs. It returns an error only when Telegram failed for good
// (the remaining messages are then not tried: re-run to resume).
func (r *runner) doJob(ctx context.Context, j job, buf *[]byte) error {
	rows := make([]meta.Chunk, len(j.targets))
	packed := true
	var start, end int64 = -1, 0
	for k, t := range j.targets {
		c := r.plan.Entries[t.entry].Chunks[t.chunk]
		rows[k] = c
		if c.PackSize <= 0 {
			packed = false
			continue
		}
		if start < 0 || c.PackOffset < start {
			start = c.PackOffset
		}
		if e := c.PackOffset + int64(c.PackSize); e > end {
			end = e
		}
	}
	var doc []byte
	var base int64
	var err error
	if packed {
		base = start
		doc, err = r.fetch(ctx, func() ([]byte, error) { return r.opts.Download.FetchRange(ctx, j.msg, start, end-start) })
		if err == nil && int64(len(doc)) < end-start {
			err = fmt.Errorf("short read: %d of %d bytes", len(doc), end-start)
		}
	} else {
		doc, err = r.fetch(ctx, func() ([]byte, error) { return r.opts.Download.Fetch(ctx, j.msg) })
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil // canceled: download() reports it
		}
		kind := KindIntegrity
		if r.isTelegram(err) {
			kind = KindTelegram
		}
		for _, t := range j.targets {
			r.problem(t.entry, kind, "chunk %d: message %d: %v", r.plan.Entries[t.entry].Chunks[t.chunk].Idx, j.msg, err)
		}
		if kind == KindTelegram {
			return err
		}
		return nil
	}
	r.messages.Add(1)
	r.downloaded.Add(int64(len(doc)))

	type decoded struct {
		data []byte
		err  error
	}
	seen := map[string]decoded{} // dedup: one decode per distinct wire
	for k, t := range j.targets {
		c := rows[k]
		e := &r.plan.Entries[t.entry]
		wire := doc
		if c.PackSize > 0 {
			rel := c.PackOffset - base
			if rel < 0 || rel+int64(c.PackSize) > int64(len(doc)) {
				r.problem(t.entry, KindIntegrity, "chunk %d: extent [%d,+%d) outside message %d", c.Idx, c.PackOffset, c.PackSize, j.msg)
				continue
			}
			wire = doc[rel : rel+int64(c.PackSize)]
		}
		key := fmt.Sprintf("%d/%d/%x", c.PackOffset, c.PackSize, c.Hash)
		d, ok := seen[key]
		if !ok {
			d.data, d.err = r.opts.Codec.Decode(c.Ino, c.Idx, c.Hash, wire)
			seen[key] = d
		}
		if d.err != nil {
			r.problem(t.entry, KindIntegrity, "chunk %d (message %d): %v", c.Idx, j.msg, d.err)
			continue
		}
		if len(d.data) != int(c.Size) {
			r.problem(t.entry, KindIntegrity, "chunk %d (message %d): %d bytes, the DB says %d", c.Idx, j.msg, len(d.data), c.Size)
			continue
		}
		data := d.data
		if s := slotLen(e.Inode, c.Idx); int64(len(data)) > s {
			// A stale row longer than its slot: only its head is the
			// file's; verification checks that head's own hash.
			data = data[:s]
			r.files[t.entry].want[t.chunk] = r.opts.Codec.Hash(data)
		}
		if err := writeAt(r.dest(e), data, int64(c.Idx)*e.Inode.ChunkSize); err != nil {
			r.problem(t.entry, KindLocal, "chunk %d: %v", c.Idx, err)
			continue
		}
		r.written.Add(int64(len(data)))
	}
	return nil
}

func (r *runner) isTelegram(err error) bool {
	if r.opts.Telegram != nil {
		return r.opts.Telegram(err)
	}
	return r.opts.Retriable != nil && r.opts.Retriable(err)
}

// fetch runs f, retrying retriable errors with a doubling back-off.
func (r *runner) fetch(ctx context.Context, f func() ([]byte, error)) ([]byte, error) {
	delay := r.opts.Backoff
	for attempt := 1; ; attempt++ {
		b, err := f()
		if err == nil {
			return b, nil
		}
		if ctx.Err() != nil || r.opts.Retriable == nil || !r.opts.Retriable(err) || attempt >= r.opts.Attempts {
			return nil, err
		}
		r.logf("  download failed (%v) — retrying in %s", err, delay)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
	}
}

func writeAt(p string, data []byte, off int64) error {
	f, err := os.OpenFile(p, os.O_WRONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	if _, err := f.WriteAt(data, off); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// ---- 3. verification ----

// verify re-reads every file this run wrote and checks each chunk's
// keyed hash (addition 4). Files already identical were read and
// checked by structure(); lost or short chunks are integrity problems.
func (r *runner) verify(ctx context.Context, res *Result) error {
	var todo []int
	for i := range r.plan.Entries {
		e := &r.plan.Entries[i]
		if r.files[i] == nil {
			continue
		}
		// A file whose data could not be written is not read back: its
		// problem is already recorded. Lost chunks don't stop the check
		// of the chunks that are there.
		writeFailed := r.failed(i)
		if len(e.Missing) > 0 || len(e.Short) > 0 {
			kind, hint := KindIntegrity, ""
			if r.opts.InFlight != nil && r.opts.InFlight(ctx, e) {
				kind, hint = KindPending, " — not uploaded yet when the DB was read (the mount is still writing it): `telfs sync --wait`, then restore again"
			}
			for _, idx := range e.Missing {
				r.problem(i, kind, "chunk %d: missing from the DB%s", idx, hint)
			}
			for _, idx := range e.Short {
				r.problem(i, kind, "chunk %d: shorter than its place in the file%s", idx, hint)
			}
		}
		if r.files[i].skip || !r.created[i] || writeFailed {
			continue
		}
		todo = append(todo, i)
	}
	if len(todo) == 0 {
		return nil
	}
	r.logf("Verifying %d restored file(s)…", len(todo))
	ch := make(chan int)
	var wg sync.WaitGroup
	var verified atomic.Int64
	for w := 0; w < r.opts.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var buf []byte
			for i := range ch {
				if r.verifyFile(i, &buf) {
					verified.Add(1)
				}
			}
		}()
	}
	var err error
feed:
	for _, i := range todo {
		select {
		case ch <- i:
		case <-ctx.Done():
			err = ctx.Err()
			break feed
		}
	}
	close(ch)
	wg.Wait()
	res.Verified = int(verified.Load())
	return err
}

func (r *runner) verifyFile(i int, buf *[]byte) bool {
	e := &r.plan.Entries[i]
	fr := r.files[i]
	f, err := openRead(r.dest(e))
	if err != nil {
		r.problem(i, KindIntegrity, "verification: %v", err)
		return false
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil || !fi.Mode().IsRegular() || fi.Size() != e.Inode.Size {
		r.problem(i, KindIntegrity, "verification: the restored file is not a regular file of %d bytes", e.Inode.Size)
		return false
	}
	ok := true
	for j, c := range e.Chunks {
		want := fr.want[j]
		if want == nil {
			want = c.Hash
		}
		match, err := r.chunkMatches(f, e, c, want, buf)
		if err != nil {
			r.problem(i, KindIntegrity, "verification: chunk %d: %v", c.Idx, err)
			return false
		}
		if !match {
			r.problem(i, KindIntegrity, "verification: chunk %d differs from its keyed hash", c.Idx)
			ok = false
		}
	}
	return ok
}

// ---- 4. attributes ----

// attributes sets xattrs, owners, modes and mtimes, children before
// their directory (creating a child changes the directory's mtime; a
// read-only directory must stay writable until its children are done).
func (r *runner) attributes() {
	for i := len(r.plan.Entries) - 1; i >= 0; i-- {
		e := &r.plan.Entries[i]
		if !r.created[i] || e.LinkOf >= 0 {
			continue // a hard link shares its first name's inode
		}
		p := r.dest(e)
		in := e.Inode
		symlink := in.Kind == meta.KindSymlink
		if !symlink {
			for _, x := range e.Xattrs {
				if err := unix.Setxattr(p, x.Name, x.Value, 0); err != nil {
					r.problem(i, KindLocal, "xattr %s: %v", x.Name, err)
				}
			}
		}
		if r.opts.Chown {
			if err := os.Lchown(p, int(in.UID), int(in.GID)); err != nil {
				r.problem(i, KindLocal, "owner: %v", err)
			}
		}
		if !symlink {
			if err := unix.Chmod(p, in.Mode&0o7777); err != nil {
				r.problem(i, KindLocal, "mode: %v", err)
			}
		}
		ts := []unix.Timespec{unix.NsecToTimespec(in.Mtime * 1e9), unix.NsecToTimespec(in.Mtime * 1e9)}
		if err := unix.UtimesNanoAt(unix.AT_FDCWD, p, ts, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			r.problem(i, KindLocal, "mtime: %v", err)
		}
	}
}

func human(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
