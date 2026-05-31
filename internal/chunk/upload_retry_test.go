package chunk

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

// flakyUploader fails the first failFirst attempts with errToReturn,
// then succeeds. Used to prove uploadWithRetry rides through a
// transient error string.
type flakyUploader struct {
	failFirst    int32 // attempts to fail before letting the next one succeed
	calls        atomic.Int32
	errToReturn  error
	returnedID   int
}

func (f *flakyUploader) UploadDocument(_ context.Context, _ io.Reader, _, _ string) (int, error) {
	n := f.calls.Add(1)
	if n <= f.failFirst {
		return 0, f.errToReturn
	}
	return f.returnedID, nil
}

// TestUploadWithRetrySucceedsAfterTransient: the first 3 calls fail
// (transient TG-style error), the 4th succeeds. Caller sees nil error
// and the eventual msgID — same shape as a clean first-try success.
func TestUploadWithRetrySucceedsAfterTransient(t *testing.T) {
	u := &flakyUploader{
		failFirst:   3,
		errToReturn: errors.New("FLOOD_WAIT (1)"),
		returnedID:  42,
	}
	got, err := uploadWithRetry(context.Background(), u, []byte("hello"), "test", "")
	if err != nil {
		t.Fatalf("uploadWithRetry: want nil err, got %v", err)
	}
	if got != 42 {
		t.Fatalf("uploadWithRetry: want msgID 42, got %d", got)
	}
	if u.calls.Load() != 4 {
		t.Fatalf("UploadDocument called %d times, want 4", u.calls.Load())
	}
}

// TestUploadWithRetryGivesUp: when every attempt fails the wrapper
// returns the last error wrapped with attempt count. Caller can then
// surface to userspace.
func TestUploadWithRetryGivesUp(t *testing.T) {
	u := &flakyUploader{
		failFirst:   100, // more than maxAttempts (5)
		errToReturn: errors.New("network unreachable"),
	}
	_, err := uploadWithRetry(context.Background(), u, []byte("hi"), "t", "")
	if err == nil {
		t.Fatal("want error after exhausted retries, got nil")
	}
	if !errors.Is(err, u.errToReturn) {
		t.Fatalf("want wrapped %v, got %v", u.errToReturn, err)
	}
	if u.calls.Load() != 5 {
		t.Fatalf("UploadDocument called %d times, want exactly 5 (maxAttempts)", u.calls.Load())
	}
}

// TestUploadWithRetryHonorsCtxCancel: ctx cancellation between
// attempts stops retrying immediately. Sleeping in the backoff would
// otherwise burn cycles after the daemon has decided to shut down.
func TestUploadWithRetryHonorsCtxCancel(t *testing.T) {
	u := &flakyUploader{
		failFirst:   100,
		errToReturn: errors.New("blip"),
	}
	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after the first attempt returns; the next iteration of
	// the retry loop should see ctx.Err() and bail.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := uploadWithRetry(ctx, u, []byte("hi"), "t", "")
	elapsed := time.Since(start)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("uploadWithRetry kept retrying for %s after ctx cancel; want quick exit", elapsed)
	}
}
