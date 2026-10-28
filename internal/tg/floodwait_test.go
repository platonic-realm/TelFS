package tg

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"telfs/internal/metrics"
)

// fakeInvoker counts how many times it was called and can be
// configured to fail with a FLOOD_WAIT first N times.
type fakeInvoker struct {
	calls int
	wait  []time.Duration // wait[i] is the FLOOD_WAIT to return on call i; nil = no error
	err   error           // final non-flood error to return after walking wait
}

func (f *fakeInvoker) Invoke(_ context.Context, _ bin.Encoder, _ bin.Decoder) error {
	idx := f.calls
	f.calls++
	if idx < len(f.wait) {
		d := f.wait[idx]
		// gotd's tgerr.Error with Type "FLOOD_WAIT" + ms argument is
		// what AsFloodWait parses. Use a constructor for a fake error.
		return &tgerr.Error{
			Type:     "FLOOD_WAIT",
			Argument: int(d / time.Second),
			Message:  "FLOOD_WAIT_X",
		}
	}
	return f.err
}

// invokerAdapter wraps fakeInvoker so it satisfies tg.Invoker.
type invokerAdapter struct{ f *fakeInvoker }

func (a *invokerAdapter) Invoke(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
	return a.f.Invoke(ctx, in, out)
}

var _ tg.Invoker = (*invokerAdapter)(nil)

// TestFloodWaiterShortWaitSleeps: a 1-second FLOOD_WAIT should be
// honored — the middleware retries the next call after sleeping.
func TestFloodWaiterShortWaitSleeps(t *testing.T) {
	fi := &fakeInvoker{wait: []time.Duration{1 * time.Second}}
	w := &FloodWaiter{max: MaxFloodWait}
	start := time.Now()
	if err := w.Handle(&invokerAdapter{f: fi})(context.Background(), nil, nil); err != nil {
		t.Fatalf("expected retry to succeed, got %v", err)
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Fatalf("did not sleep through the wait (elapsed %v)", elapsed)
	}
	if fi.calls != 2 {
		t.Fatalf("expected 2 calls (1 fail, 1 succeed), got %d", fi.calls)
	}
}

// TestFloodWaiterOverCap: a wait that exceeds the configured cap is
// returned as an error (not silently honored), so the caller can
// surface it as a real failure instead of hanging.
func TestFloodWaiterOverCap(t *testing.T) {
	fi := &fakeInvoker{wait: []time.Duration{10 * time.Second}}
	w := &FloodWaiter{max: 5 * time.Second}
	err := w.Handle(&invokerAdapter{f: fi})(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("expected over-cap wait to error, got nil")
	}
	if fi.calls != 1 {
		t.Fatalf("expected exactly 1 call before bailing, got %d", fi.calls)
	}
}

// TestFloodWaiterContextCancel: ctx.Done firing during the sleep
// unwinds with ctx.Err, not a retry-loop error.
func TestFloodWaiterContextCancel(t *testing.T) {
	fi := &fakeInvoker{wait: []time.Duration{60 * time.Second}} // long enough to outlast the test
	w := &FloodWaiter{max: MaxFloodWait}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- w.Handle(&invokerAdapter{f: fi})(ctx, nil, nil)
	}()
	// Give the middleware a moment to enter sleep.
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("middleware did not unwind on ctx cancel within 2 s")
	}
}

// TestFloodWaiterPassesThroughNonFlood: a regular error (not a
// FLOOD_WAIT) returns unchanged; the middleware does NOT retry.
func TestFloodWaiterPassesThroughNonFlood(t *testing.T) {
	target := errors.New("not a flood wait")
	fi := &fakeInvoker{err: target}
	w := &FloodWaiter{max: MaxFloodWait}
	err := w.Handle(&invokerAdapter{f: fi})(context.Background(), nil, nil)
	if !errors.Is(err, target) {
		t.Fatalf("expected wrapped target, got %v", err)
	}
	if fi.calls != 1 {
		t.Fatalf("expected exactly 1 call for non-retryable error, got %d", fi.calls)
	}
}

// Every flood wait is named on stderr, even a short one.
func TestFloodWaiterNamesEveryWait(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stderr
	os.Stderr = w
	fi := &fakeInvoker{wait: []time.Duration{1 * time.Second}}
	fw := &FloodWaiter{max: MaxFloodWait}
	err = fw.Handle(&invokerAdapter{f: fi})(context.Background(), &tg.UploadGetFileRequest{}, nil)
	os.Stderr = old
	w.Close()
	out, _ := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if want := "[flood-wait] UploadGetFileRequest: Telegram asked to wait 1s\n"; string(out) != want {
		t.Fatalf("stderr %q, want %q", out, want)
	}
}

// FLOOD_PREMIUM_WAIT_X (Telegram's upload limit for accounts without
// Premium) is a flood wait like FLOOD_WAIT_X — honoured for X seconds —
// but classified, counted and logged on its own.
func TestClassifyFloodPremium(t *testing.T) {
	for _, c := range []struct {
		err  error
		kind string
		d    time.Duration
	}{
		{tgerr.New(420, "FLOOD_WAIT_7"), tgerr.ErrFloodWait, 7 * time.Second},
		{tgerr.New(420, "FLOOD_PREMIUM_WAIT_5"), tgerr.ErrPremiumFloodWait, 5 * time.Second},
		{fmt.Errorf("upload part: %w", tgerr.New(420, "FLOOD_PREMIUM_WAIT_3")), tgerr.ErrPremiumFloodWait, 3 * time.Second},
	} {
		kind, d, ok := classifyFlood(c.err)
		if !ok || kind != c.kind || d != c.d {
			t.Errorf("%v: %q %v %v, want %q %v", c.err, kind, d, ok, c.kind, c.d)
		}
		if d2, ok := asFloodWait(c.err); !ok || d2 != c.d {
			t.Errorf("tracker's asFloodWait(%v) = %v %v", c.err, d2, ok)
		}
	}
	for _, err := range []error{nil, errors.New("FLOOD"), tgerr.New(400, "CHANNEL_INVALID")} {
		if _, _, ok := classifyFlood(err); ok {
			t.Errorf("%v classified as a flood wait", err)
		}
	}
}

func TestFloodWaiterPremiumWaitNamedDistinctly(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stderr
	os.Stderr = w
	before := metrics.FloodPremiumWaits.Load()
	calls := 0
	next := invokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error {
		calls++
		if calls == 1 {
			return tgerr.New(420, "FLOOD_PREMIUM_WAIT_1")
		}
		return nil
	})
	start := time.Now()
	err = NewFloodWaiter().Handle(next)(context.Background(), &tg.UploadSaveFilePartRequest{}, nil)
	elapsed := time.Since(start)
	os.Stderr = old
	w.Close()
	out, _ := io.ReadAll(r)
	if err != nil || calls != 2 {
		t.Fatalf("err %v after %d calls, want a retry that succeeds", err, calls)
	}
	if elapsed < 900*time.Millisecond {
		t.Fatalf("waited %v, want the 1 s Telegram asked for", elapsed)
	}
	want := "[flood-wait] UploadSaveFilePartRequest: FLOOD_PREMIUM_WAIT — Telegram's upload limit for accounts without Premium; waiting 1s\n"
	if string(out) != want {
		t.Fatalf("stderr %q, want %q", out, want)
	}
	if metrics.FloodPremiumWaits.Load() != before+1 {
		t.Fatal("telfs_flood_premium_waits_total not counted")
	}
}
