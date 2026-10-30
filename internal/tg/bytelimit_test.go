package tg

import (
	"context"
	"testing"
	"time"
)

// The cap changes while running (v1.1 I3): lowered, it slows the next
// parts; 0 lets everything through at once.
func TestByteLimiterSetRate(t *testing.T) {
	b := NewByteLimiter(0)
	start := time.Now()
	if err := b.WaitN(context.Background(), 8<<20); err != nil || time.Since(start) > 100*time.Millisecond || b.Rate() != 0 {
		t.Fatalf("no cap: %v after %s", err, time.Since(start))
	}
	b.SetRate(uploadPartMax)                     // one part per second
	b.WaitN(context.Background(), uploadPartMax) // the burst
	start = time.Now()
	if err := b.WaitN(context.Background(), uploadPartMax/2); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 300*time.Millisecond {
		t.Fatalf("half a part went through in %s at one part per second", d)
	}
	b.SetRate(0)
	start = time.Now()
	if err := b.WaitN(context.Background(), 8<<20); err != nil || time.Since(start) > 100*time.Millisecond || b.Rate() != 0 {
		t.Fatalf("cap removed: %v after %s", err, time.Since(start))
	}
}
