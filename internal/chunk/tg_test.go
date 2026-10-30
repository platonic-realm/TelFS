package chunk

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// fakeDocs downloads a document of `parts` writes, `gap` apart; a negative
// parts never answers (Telegram unreachable).
type fakeDocs struct {
	parts int
	gap   time.Duration
}

func (f fakeDocs) DownloadDocument(ctx context.Context, msgID int, w io.Writer) (int64, error) {
	return f.DownloadDocumentRange(ctx, msgID, w, 0, 0)
}

func (f fakeDocs) DownloadDocumentRange(ctx context.Context, _ int, w io.Writer, _, _ int64) (int64, error) {
	if f.parts < 0 {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	var n int64
	for i := 0; i < f.parts; i++ {
		select {
		case <-ctx.Done():
			return n, ctx.Err()
		case <-time.After(f.gap):
		}
		k, _ := w.Write([]byte("part"))
		n += int64(k)
	}
	return n, nil
}

// Offline: the read fails with ErrReadStalled once nothing moved for the
// offline timeout — and the error is no cancellation (that would be EINTR,
// which the kernel restarts for ever).
func TestFetchFailsWhenTelegramIsUnreachable(t *testing.T) {
	f := &TGFetcher{OfflineTimeout: 80 * time.Millisecond, dl: fakeDocs{parts: -1}}
	start := time.Now()
	_, err := f.Fetch(context.Background(), Key{Ino: 1}, 7)
	if !errors.Is(err, ErrReadStalled) {
		t.Fatalf("err = %v, want ErrReadStalled", err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("a stalled read must not look like a cancellation (EINTR)")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("took %s", d)
	}
	if _, err := f.FetchRange(context.Background(), Key{Ino: 1}, 7, 0, 10); !errors.Is(err, ErrReadStalled) {
		t.Fatalf("ranged: %v", err)
	}
}

// A slow download that keeps moving is never cut off, however long it
// takes in total.
func TestFetchSlowButProgressingIsNotCut(t *testing.T) {
	f := &TGFetcher{OfflineTimeout: 80 * time.Millisecond, dl: fakeDocs{parts: 8, gap: 40 * time.Millisecond}}
	b, err := f.Fetch(context.Background(), Key{Ino: 1}, 7)
	if err != nil || len(b) != 32 {
		t.Fatalf("%d bytes, %v", len(b), err)
	}
}

// The caller's own cancellation stays a cancellation; 0 = no limit.
func TestFetchCallerCancelAndNoLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	f := &TGFetcher{OfflineTimeout: time.Hour, dl: fakeDocs{parts: -1}}
	if _, err := f.Fetch(ctx, Key{Ino: 1}, 7); !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrReadStalled) {
		t.Fatalf("caller timeout: %v", err)
	}
	f = &TGFetcher{dl: fakeDocs{parts: 2, gap: 10 * time.Millisecond}}
	if b, err := f.Fetch(context.Background(), Key{Ino: 1}, 7); err != nil || len(b) != 8 {
		t.Fatalf("no limit: %d %v", len(b), err)
	}
}
