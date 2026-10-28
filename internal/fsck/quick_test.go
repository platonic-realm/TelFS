package fsck

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"telfs/internal/meta"
	"telfs/internal/tg"
)

// fakeChannel is the channel side: message id → document (HasDoc false =
// a message without one); ids not in docs are deleted. It records every
// request's ids.
type fakeChannel struct {
	mu    sync.Mutex
	docs  map[int]tg.MessageDoc
	calls [][]int
	fail  []error // returned by the next requests, in order
}

func (f *fakeChannel) MessageDocs(_ context.Context, ids []int) (map[int]tg.MessageDoc, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, append([]int(nil), ids...))
	if len(f.fail) > 0 {
		err := f.fail[0]
		f.fail = f.fail[1:]
		if err != nil {
			return nil, err
		}
	}
	out := map[int]tg.MessageDoc{}
	for _, id := range ids {
		if d, ok := f.docs[id]; ok {
			out[id] = d
		}
	}
	return out, nil
}

func newStore(t *testing.T) *meta.Store {
	t.Helper()
	m, err := meta.Open(filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

func mkfile(t *testing.T, m *meta.Store, parent int64, name string) int64 {
	t.Helper()
	ino, err := m.CreateChild(context.Background(), parent, name, meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	if err != nil {
		t.Fatal(err)
	}
	return ino
}

func put(t *testing.T, m *meta.Store, c meta.Chunk) {
	t.Helper()
	if err := m.PutChunk(context.Background(), c); err != nil {
		t.Fatal(err)
	}
}

// 250 referenced messages are asked about in three requests of at most
// 100 ids, in id order, each id exactly once; a store whose count is a
// multiple of the batch needs no extra request.
func TestQuickBatches(t *testing.T) {
	ctx := context.Background()
	m := newStore(t)
	ch := &fakeChannel{docs: map[int]tg.MessageDoc{}}
	for i := 0; i < 250; i++ {
		ino := mkfile(t, m, meta.RootIno, "f"+string(rune('a'+i%26))+string(rune('a'+i/26)))
		id := 1000 + 3*i
		put(t, m, meta.Chunk{Ino: ino, Idx: 0, TGMessageID: int64(id), Size: 10})
		ch.docs[id] = tg.MessageDoc{HasDoc: true, Size: 64}
	}
	rep, err := Quick(ctx, m, ch, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK() || rep.MessagesChecked != 250 || rep.RowsChecked != 250 {
		t.Fatalf("report %+v", rep)
	}
	if len(ch.calls) != 3 || len(ch.calls[0]) != 100 || len(ch.calls[1]) != 100 || len(ch.calls[2]) != 50 {
		t.Fatalf("requests of %v ids, want 100, 100, 50", lens(ch.calls))
	}
	var all []int
	for _, c := range ch.calls {
		all = append(all, c...)
	}
	if !sort.IntsAreSorted(all) || all[0] != 1000 || all[249] != 1000+3*249 {
		t.Fatalf("ids not asked once each in order: first %d last %d", all[0], all[len(all)-1])
	}

	// Smaller batches: 250 = 5 full pages = 5 requests, none extra.
	ch.calls = nil
	if rep, err := Quick(ctx, m, ch, Options{Batch: 50}); err != nil || rep.MessagesChecked != 250 || len(ch.calls) != 5 {
		t.Fatalf("batch 50: %v, %d requests, %+v", err, len(ch.calls), rep)
	}
	ch.calls = nil
	if _, err := Quick(ctx, m, ch, Options{Batch: 500}); err != nil || len(ch.calls) != 3 {
		t.Fatalf("a batch above 100 is capped at 100: %v, %v", err, lens(ch.calls))
	}
}

func lens(calls [][]int) []int {
	var out []int
	for _, c := range calls {
		out = append(out, len(c))
	}
	return out
}

// Every way a reference can go wrong, beside ones that are fine.
func TestQuickClassifies(t *testing.T) {
	ctx := context.Background()
	m := newStore(t)
	dir, err := m.CreateChild(ctx, meta.RootIno, "docs", meta.Inode{Kind: meta.KindDir, Mode: 0o40755})
	if err != nil {
		t.Fatal(err)
	}
	big := mkfile(t, m, dir, "big")
	gone := mkfile(t, m, dir, "gone")
	twin := mkfile(t, m, meta.RootIno, "twin")
	text := mkfile(t, m, meta.RootIno, "text")
	p1 := mkfile(t, m, dir, "p1")
	p2 := mkfile(t, m, dir, "p2")
	p3 := mkfile(t, m, dir, "p3")
	q1 := mkfile(t, m, meta.RootIno, "q1")
	bad := mkfile(t, m, meta.RootIno, "bad")

	put(t, m, meta.Chunk{Ino: big, Idx: 0, TGMessageID: 10, Size: 4096})                            // fine
	put(t, m, meta.Chunk{Ino: gone, Idx: 0, TGMessageID: 11, Size: 4096})                           // deleted
	put(t, m, meta.Chunk{Ino: twin, Idx: 0, TGMessageID: 11, Size: 4096})                           // deleted, a dedup twin
	put(t, m, meta.Chunk{Ino: text, Idx: 0, TGMessageID: 12, Size: 4096})                           // no document
	put(t, m, meta.Chunk{Ino: p1, Idx: 0, TGMessageID: 13, Size: 50, PackOffset: 0, PackSize: 100}) // fits
	put(t, m, meta.Chunk{Ino: p2, Idx: 0, TGMessageID: 13, Size: 50, PackOffset: 100, PackSize: 100})
	put(t, m, meta.Chunk{Ino: p3, Idx: 0, TGMessageID: 13, Size: 50, PackOffset: 200, PackSize: 100}) // past the end
	put(t, m, meta.Chunk{Ino: q1, Idx: 0, TGMessageID: 14, Size: 50, PackOffset: 0, PackSize: 300})   // fits exactly
	put(t, m, meta.Chunk{Ino: bad, Idx: 0, TGMessageID: 0, Size: 10})                                 // invalid id

	ch := &fakeChannel{docs: map[int]tg.MessageDoc{
		10: {HasDoc: true, Size: 5000},
		12: {},
		13: {HasDoc: true, Size: 250},
		14: {HasDoc: true, Size: 300},
	}}
	rep, err := Quick(ctx, m, ch, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.MessagesChecked != 6 || rep.RowsChecked != 9 {
		t.Fatalf("checked %d messages, %d rows; want 6, 9", rep.MessagesChecked, rep.RowsChecked)
	}
	if len(ch.calls) != 1 || len(ch.calls[0]) != 5 {
		t.Fatalf("requests %v: the invalid id must not be asked", ch.calls)
	}
	wantMissing := []Missing{
		{MsgID: 0, Rows: 1, ExamplePath: "/bad", ExampleIno: bad, Reason: ReasonBadID},
		{MsgID: 11, Rows: 2, ExamplePath: "/docs/gone", ExampleIno: gone, Reason: ReasonDeleted},
		{MsgID: 12, Rows: 1, ExamplePath: "/text", ExampleIno: text, Reason: ReasonNoDoc},
	}
	if len(rep.Missing) != len(wantMissing) {
		t.Fatalf("missing %+v, want %+v", rep.Missing, wantMissing)
	}
	for i := range wantMissing {
		if rep.Missing[i] != wantMissing[i] {
			t.Errorf("missing[%d] %+v, want %+v", i, rep.Missing[i], wantMissing[i])
		}
	}
	wantSmall := TooSmall{MsgID: 13, Rows: 1, ExamplePath: "/docs/p3", ExampleIno: p3, DocSize: 250, Need: 300}
	if len(rep.TooSmall) != 1 || rep.TooSmall[0] != wantSmall {
		t.Fatalf("too small %+v, want %+v", rep.TooSmall, wantSmall)
	}
	if rep.OK() || rep.RowsAffected() != 5 {
		t.Fatalf("OK %v, rows affected %d (want 5)", rep.OK(), rep.RowsAffected())
	}
}

// An empty chunk_map asks the channel nothing; the lists are empty, not
// null.
func TestQuickEmpty(t *testing.T) {
	ch := &fakeChannel{}
	rep, err := Quick(context.Background(), newStore(t), ch, Options{})
	if err != nil || !rep.OK() || rep.MessagesChecked != 0 || len(ch.calls) != 0 || rep.Missing == nil || rep.TooSmall == nil {
		t.Fatalf("%+v %v, %d requests", rep, err, len(ch.calls))
	}
}

// A retriable failure is asked again with doubling waits; a failure that
// is not, or one that outlasts the attempts, is ErrChannel; a canceled
// context is not.
func TestQuickRetries(t *testing.T) {
	ctx := context.Background()
	m := newStore(t)
	put(t, m, meta.Chunk{Ino: mkfile(t, m, meta.RootIno, "a"), Idx: 0, TGMessageID: 7, Size: 10})
	transient := errors.New("connection reset")
	refused := errors.New("CHANNEL_PRIVATE")
	retriable := func(err error) bool { return errors.Is(err, transient) }
	var waits []time.Duration
	sleep := func(_ context.Context, d time.Duration) error { waits = append(waits, d); return nil }

	ch := &fakeChannel{docs: map[int]tg.MessageDoc{7: {HasDoc: true, Size: 10}}, fail: []error{transient, transient}}
	rep, err := Quick(ctx, m, ch, Options{Retriable: retriable, Sleep: sleep})
	if err != nil || !rep.OK() || len(ch.calls) != 3 {
		t.Fatalf("after two transient failures: %v, %d requests", err, len(ch.calls))
	}
	if len(waits) != 2 || waits[0] != time.Second || waits[1] != 2*time.Second {
		t.Fatalf("waits %v, want [1s 2s]", waits)
	}

	ch = &fakeChannel{fail: []error{refused}}
	if _, err := Quick(ctx, m, ch, Options{Retriable: retriable, Sleep: sleep}); !errors.Is(err, ErrChannel) || !errors.Is(err, refused) || len(ch.calls) != 1 {
		t.Fatalf("refused: %v after %d requests, want ErrChannel at once", err, len(ch.calls))
	}

	ch = &fakeChannel{fail: []error{transient, transient, transient}}
	if _, err := Quick(ctx, m, ch, Options{Retriable: retriable, Sleep: sleep, Attempts: 3}); !errors.Is(err, ErrChannel) || len(ch.calls) != 3 {
		t.Fatalf("out of attempts: %v after %d requests", err, len(ch.calls))
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := Quick(cctx, m, &fakeChannel{}, Options{}); !errors.Is(err, context.Canceled) || errors.Is(err, ErrChannel) {
		t.Fatalf("canceled: %v", err)
	}
}
