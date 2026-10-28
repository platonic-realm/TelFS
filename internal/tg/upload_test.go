package tg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/session"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"telfs/internal/config"
)

// The byte limiter holds a rate over time: concurrent callers together get
// the rate (plus the one-part burst), not the rate each.
func TestByteLimiterRateConcurrent(t *testing.T) {
	const bps = 2_000_000
	b := NewByteLimiter(bps)
	if b.Rate() != bps {
		t.Fatalf("Rate() = %d", b.Rate())
	}
	const callers, parts, part = 4, 8, 128 << 10
	total := callers * parts * part
	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < parts; j++ {
				if err := b.WaitN(context.Background(), part); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	want := time.Duration(float64(total-uploadPartMax) / bps * float64(time.Second))
	if elapsed < want-100*time.Millisecond || elapsed > want+3*time.Second {
		t.Fatalf("%d bytes at %d B/s took %v, want ~%v", total, bps, elapsed, want)
	}
}

// More than one part's worth at once is split, not refused.
func TestByteLimiterLargeRequest(t *testing.T) {
	b := NewByteLimiter(4_000_000)
	start := time.Now()
	if err := b.WaitN(context.Background(), 3*uploadPartMax); err != nil {
		t.Fatal(err)
	}
	want := time.Duration(float64(2*uploadPartMax) / 4_000_000 * float64(time.Second))
	if elapsed := time.Since(start); elapsed < want-50*time.Millisecond {
		t.Fatalf("1.5 MiB at 4 MB/s took %v, want ≥ %v", elapsed, want)
	}
}

// 0 (and a nil limiter) = no cap.
func TestByteLimiterUnlimited(t *testing.T) {
	for _, b := range []*ByteLimiter{NewByteLimiter(0), nil} {
		start := time.Now()
		for i := 0; i < 100; i++ {
			if err := b.WaitN(context.Background(), 1<<30); err != nil {
				t.Fatal(err)
			}
		}
		if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
			t.Fatalf("unlimited limiter waited %v", elapsed)
		}
		if b.Rate() != 0 {
			t.Fatalf("Rate() = %d, want 0", b.Rate())
		}
		var calls int
		h := b.Handle(invokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error { calls++; return nil }))
		if err := h(context.Background(), &tg.UploadSaveBigFilePartRequest{Bytes: make([]byte, uploadPartMax)}, nil); err != nil || calls != 1 {
			t.Fatalf("pass-through: %v, %d calls", err, calls)
		}
	}
}

// Only upload parts are charged; a canceled wait returns the context's error.
func TestByteLimiterChargesOnlyParts(t *testing.T) {
	b := NewByteLimiter(640 << 10)
	next := invokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error { return nil })
	h := b.Handle(next)
	ctx := context.Background()
	if err := b.WaitN(ctx, uploadPartMax); err != nil { // empty the bucket
		t.Fatal(err)
	}
	start := time.Now()
	if err := h(ctx, &tg.MessagesSendMediaRequest{Message: strings.Repeat("x", 4096)}, nil); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("a non-part RPC waited %v", elapsed)
	}
	start = time.Now()
	if err := h(ctx, &tg.UploadSaveFilePartRequest{Bytes: make([]byte, 64<<10)}, nil); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Fatalf("a 64 KiB part at 640 KiB/s with an empty bucket waited only %v", elapsed)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := h(cctx, &tg.UploadSaveBigFilePartRequest{Bytes: make([]byte, uploadPartMax)}, nil); err == nil {
		t.Fatal("a canceled wait returned nil")
	}
}

type invokeFunc func(ctx context.Context, input bin.Encoder, output bin.Decoder) error

func (f invokeFunc) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	return f(ctx, input, output)
}

// fakeTelegram answers upload parts and sendMedia, counts them, and records
// how many parts were in flight at once.
type fakeTelegram struct {
	mu        sync.Mutex
	methods   []string
	inFlight  atomic.Int32
	maxFlight atomic.Int32
	partDelay time.Duration
	// floods: the first len(floods) part calls answer with these errors.
	floods []error
	// started / done: part requests in the order they started / ended;
	// media: the uploaded file of each sendMedia.
	started, done []tg.UploadSaveBigFilePartRequest
	media         []tg.InputFileClass
}

func (f *fakeTelegram) Invoke(_ context.Context, input bin.Encoder, output bin.Decoder) error {
	f.mu.Lock()
	f.methods = append(f.methods, requestName(input))
	var flood error
	if _, ok := input.(*tg.UploadSaveBigFilePartRequest); ok && len(f.floods) > 0 {
		flood, f.floods = f.floods[0], f.floods[1:]
	}
	f.mu.Unlock()
	if flood != nil {
		return flood
	}
	switch r := input.(type) {
	case *tg.UploadSaveBigFilePartRequest:
		f.mu.Lock()
		f.started = append(f.started, *r)
		f.mu.Unlock()
		n := f.inFlight.Add(1)
		for {
			m := f.maxFlight.Load()
			if n <= m || f.maxFlight.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(f.partDelay)
		f.inFlight.Add(-1)
		f.mu.Lock()
		f.done = append(f.done, *r)
		f.mu.Unlock()
		output.(*tg.BoolBox).Bool = &tg.BoolTrue{}
		return nil
	case *tg.MessagesSendMediaRequest:
		f.mu.Lock()
		f.media = append(f.media, r.Media.(*tg.InputMediaUploadedDocument).File)
		f.mu.Unlock()
		output.(*tg.UpdatesBox).Updates = &tg.UpdateShortSentMessage{ID: 77}
		return nil
	}
	return fmt.Errorf("fake: unexpected %T", input)
}

func (f *fakeTelegram) count(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, m := range f.methods {
		if m == method {
			n++
		}
	}
	return n
}

func uploadTestSession(threads int) *Session {
	cfg := &config.Config{Channel: config.ChannelConfig{ID: 1, AccessHash: 2}}
	return &Session{cfg: cfg, tracker: NewTracker(16), uploadThreads: threads}
}

// With an upload pool bound, the parts go through it, parts_per_upload at
// a time; the message is sent on the main connection.
func TestUploadDocumentPartsGoThroughPool(t *testing.T) {
	mainTG := &fakeTelegram{}
	poolTG := &fakeTelegram{partDelay: 20 * time.Millisecond}
	s := uploadTestSession(2)
	s.swapAPI(tg.NewClient(mainTG))
	s.swapUploadAPI(tg.NewClient(poolTG), nil)
	body := bytes.Repeat([]byte{1}, 6*uploadPartMax+100) // 7 parts
	id, err := s.UploadDocument(context.Background(), bytes.NewReader(body), "blob", "c")
	if err != nil || id != 77 {
		t.Fatalf("UploadDocument = %d, %v", id, err)
	}
	if n := poolTG.count("UploadSaveBigFilePartRequest"); n != 7 {
		t.Fatalf("pool saw %d parts, want 7 (%v)", n, poolTG.methods)
	}
	if n := poolTG.count("MessagesSendMediaRequest"); n != 0 {
		t.Fatalf("sendMedia went through the pool")
	}
	if n := mainTG.count("MessagesSendMediaRequest"); n != 1 || len(mainTG.methods) != 1 {
		t.Fatalf("main connection saw %v, want one sendMedia", mainTG.methods)
	}
	if m := poolTG.maxFlight.Load(); m != 2 {
		t.Fatalf("%d parts in flight at most, want 2 (parts_per_upload)", m)
	}
	checkStreamedUpload(t, poolTG.started, poolTG.done, mainTG.media[0], len(body))

	// The cycle ends: the pool is unbound, uploads fall back to the main
	// connection, one part at a time (as before v1).
	s.swapUploadAPI(nil, nil)
	mainTG.partDelay = 5 * time.Millisecond
	if _, err := s.UploadDocument(context.Background(), bytes.NewReader(body), "blob", "c"); err != nil {
		t.Fatal(err)
	}
	if n := mainTG.count("UploadSaveBigFilePartRequest"); n != 7 {
		t.Fatalf("main connection saw %d parts, want 7", n)
	}
	if m := mainTG.maxFlight.Load(); m != 1 {
		t.Fatalf("%d parts in flight on the main connection, want 1", m)
	}
	if n := poolTG.count("UploadSaveBigFilePartRequest"); n != 7 {
		t.Fatalf("an unbound pool got parts")
	}
	checkStreamedUpload(t, mainTG.started, mainTG.done, mainTG.media[1], len(body))

	// A body of exactly whole parts, and one smaller than a part.
	for _, size := range []int{2 * uploadPartMax, 10} {
		f := &fakeTelegram{}
		s.swapAPI(tg.NewClient(f))
		s.swapUploadAPI(tg.NewClient(f), nil)
		if _, err := s.UploadDocument(context.Background(), bytes.NewReader(make([]byte, size)), "blob", "c"); err != nil {
			t.Fatal(err)
		}
		checkStreamedUpload(t, f.started, f.done, f.media[0], size)
	}
}

// checkStreamedUpload: the parts of one upload are the wire format TelFS
// has always sent (gotd's streamed upload): 512 KiB saveBigFilePart parts
// in order of index, file_total_parts -1 on all but the last, which
// carries the total and starts only after every other part was saved; the
// file handed to sendMedia names the same id and part count.
func checkStreamedUpload(t *testing.T, started, done []tg.UploadSaveBigFilePartRequest, file tg.InputFileClass, size int) {
	t.Helper()
	n := (size + uploadPartMax - 1) / uploadPartMax
	if len(started) != n || len(done) != n {
		t.Fatalf("%d/%d parts, want %d", len(started), len(done), n)
	}
	seen := map[int]bool{}
	total := 0
	for i, p := range started {
		last := p.FilePart == n-1
		want := -1
		if last {
			want = n
		}
		if p.FileTotalParts != want {
			t.Errorf("part %d: file_total_parts %d, want %d", p.FilePart, p.FileTotalParts, want)
		}
		if last && i != n-1 {
			t.Errorf("the last part started as #%d of %d", i+1, n)
		}
		if !last && len(p.Bytes) != uploadPartMax {
			t.Errorf("part %d: %d bytes", p.FilePart, len(p.Bytes))
		}
		if p.FileID != started[0].FileID {
			t.Errorf("part %d: another file id", p.FilePart)
		}
		seen[p.FilePart] = true
		total += len(p.Bytes)
	}
	if len(seen) != n || total != size {
		t.Fatalf("parts %v carry %d bytes, want %d parts / %d bytes", seen, total, n, size)
	}
	if done[n-1].FilePart != n-1 {
		t.Errorf("the last part finished before another one")
	}
	big, ok := file.(*tg.InputFileBig)
	if !ok || big.ID != started[0].FileID || big.Parts != n || big.Name != "blob" {
		t.Fatalf("sendMedia file %#v, want InputFileBig{id %d, %d parts}", file, started[0].FileID, n)
	}
}

// gotd's Pool invoker skips the client's middlewares; TelFS wraps it in the
// same chain, so an upload through the pool is tracked, its flood waits
// (FLOOD_WAIT and FLOOD_PREMIUM_WAIT) are slept through, and its bytes are
// charged to the byte-rate cap.
func TestUploadPoolInvokerRunsTheChain(t *testing.T) {
	c := &Client{tracker: NewTracker(16), rateLimit: NewRateLimiter(0, 0), byteLimit: NewByteLimiter(0)}
	premium := tgerr.New(420, "FLOOD_PREMIUM_WAIT_1")
	fake := &fakeTelegram{floods: []error{premium}}
	pool := &uploadPool{inv: fakeCloser{fake}}
	api := c.wrapUploadInvoker(pool)

	stderr := captureStderr(t)
	start := time.Now()
	ok, err := api.UploadSaveBigFilePart(context.Background(), &tg.UploadSaveBigFilePartRequest{FileID: 1, FileTotalParts: 1, Bytes: make([]byte, 1000)})
	out := stderr()
	if err != nil || !ok {
		t.Fatalf("part: %v, %v", ok, err)
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Fatalf("FLOOD_PREMIUM_WAIT_1 not waited out (%v)", elapsed)
	}
	if n := fake.count("UploadSaveBigFilePartRequest"); n != 2 {
		t.Fatalf("pool saw %d attempts, want 2 (flood, retry)", n)
	}
	if !strings.Contains(out, "[flood-wait] UploadSaveBigFilePartRequest: FLOOD_PREMIUM_WAIT") {
		t.Fatalf("stderr %q: the premium flood wait is not named", out)
	}
	snap := c.tracker.Snapshot()
	var mc MethodCount
	for _, m := range snap.PerMethod {
		if m.Method == "UploadSaveBigFilePartRequest" {
			mc = m
		}
	}
	if mc.OK != 1 || mc.FloodWait != 1 || snap.TotalBytesTx < 2000 {
		t.Fatalf("tracker did not see the pool's RPCs: %+v, tx %d", mc, snap.TotalBytesTx)
	}

	// The byte-rate cap is in the pool's chain too.
	c.byteLimit = NewByteLimiter(1 << 20)
	api = c.wrapUploadInvoker(pool)
	start = time.Now()
	for i := 0; i < 2; i++ {
		if _, err := api.UploadSaveBigFilePart(context.Background(), &tg.UploadSaveBigFilePartRequest{FileID: 2, FilePart: i, FileTotalParts: 2, Bytes: make([]byte, uploadPartMax)}); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(start); elapsed < 400*time.Millisecond {
		t.Fatalf("two 512 KiB parts at 1 MiB/s took %v: the byte cap is not in the pool's chain", elapsed)
	}

	// A closed pool answers ErrNotConnected, which uploads retry.
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = api.UploadSaveBigFilePart(context.Background(), &tg.UploadSaveBigFilePartRequest{Bytes: []byte{1}})
	if !errors.Is(err, ErrNotConnected) || !isRetriableUploadErr(context.Background(), err) {
		t.Fatalf("closed pool: %v", err)
	}
}

// The main connection and the pool get the same chain, in the same order.
func TestMiddlewareChainOrder(t *testing.T) {
	c := &Client{tracker: NewTracker(4), rateLimit: NewRateLimiter(0, 0), byteLimit: NewByteLimiter(5_000_000)}
	mws := c.middlewares()
	if len(mws) != 4 {
		t.Fatalf("%d middlewares", len(mws))
	}
	if mws[0] != c.rateLimit || mws[1] != c.byteLimit {
		t.Fatalf("limiters out of order: %T %T", mws[0], mws[1])
	}
	if _, ok := mws[2].(*FloodWaiter); !ok {
		t.Fatalf("mws[2] = %T, want the flood waiter", mws[2])
	}
	if tm, ok := mws[3].(*TrackingMiddleware); !ok || tm.Tracker != c.tracker {
		t.Fatalf("mws[3] = %T, want the tracker (innermost: it records raw flood answers)", mws[3])
	}
}

type fakeCloser struct{ tg.Invoker }

func (fakeCloser) Close() error { return nil }

// captureStderr redirects os.Stderr until the returned func is called,
// which restores it and returns what was written.
func captureStderr(t *testing.T) func() string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stderr
	os.Stderr = w
	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	return func() string {
		os.Stderr = old
		w.Close()
		return <-done
	}
}

// New reads [upload]: the byte cap, the pool size and the parts per
// upload reach the client and its sessions; a bad value is refused.
func TestNewClientUploadSettings(t *testing.T) {
	cfg := &config.Config{APIID: 1, APIHash: "h"}
	c, err := New(cfg, &session.StorageMemory{})
	if err != nil {
		t.Fatal(err)
	}
	if u := c.UploadSettings(); u.Connections != 8 || u.PartsPerUpload != 2 || c.byteLimit.Rate() != 5_000_000 {
		t.Fatalf("defaults: %+v, cap %d", u, c.byteLimit.Rate())
	}
	if s := c.NewSessionManager().Session(); s.uploadThreads != 2 {
		t.Fatalf("session threads %d", s.uploadThreads)
	}
	parts := 0
	cfg.Upload.PartsPerUpload = &parts
	if _, err := New(cfg, &session.StorageMemory{}); err == nil || !strings.Contains(err.Error(), "[upload]") {
		t.Fatalf("bad [upload]: %v", err)
	}
}

// A pool that won't close does not hold up the end of a connection cycle.
func TestClosePoolBounded(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	stderr := captureStderr(t)
	start := time.Now()
	closePoolBounded(closerFunc(func() error { <-block; return nil }), 50*time.Millisecond)
	out := stderr()
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("waited %v", elapsed)
	}
	if !strings.Contains(out, "still closing") {
		t.Fatalf("stderr %q", out)
	}
	closePoolBounded(closerFunc(func() error { return nil }), time.Minute) // returns at once
}

type closerFunc func() error

func (f closerFunc) Close() error { return f() }
