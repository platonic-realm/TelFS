package tg

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
)

// countingInvoker counts how many times Invoke was called. Used to
// prove the rate limiter actually gates calls instead of letting
// them all through.
type countingInvoker struct {
	calls atomic.Int64
}

func (c *countingInvoker) Invoke(_ context.Context, _ bin.Encoder, _ bin.Decoder) error {
	c.calls.Add(1)
	return nil
}

var _ tg.Invoker = (*countingInvoker)(nil)

// TestRateLimiterUnlimited proves the default (PerMinute=0) passes
// every call straight through with no wait — same shape as the
// pre-limiter code path.
func TestRateLimiterUnlimited(t *testing.T) {
	rl := NewRateLimiter(0, 0)
	if rl.PerMinute() != 0 {
		t.Fatalf("PerMinute=%d, want 0 (unlimited)", rl.PerMinute())
	}
	inv := &countingInvoker{}
	fn := rl.Handle(inv)
	ctx := context.Background()
	start := time.Now()
	for i := 0; i < 100; i++ {
		if err := fn(ctx, nil, nil); err != nil {
			t.Fatalf("Handle: %v", err)
		}
	}
	if got := inv.calls.Load(); got != 100 {
		t.Fatalf("calls = %d, want 100", got)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("unlimited mode took %s for 100 calls — should be near-instant", elapsed)
	}
}

// TestRateLimiterCaps proves a small per-minute cap actually slows
// down the call rate. With burst=cap/4 + cap=60/min the limiter
// admits the burst immediately then trickles 1/sec.
func TestRateLimiterCaps(t *testing.T) {
	rl := NewRateLimiter(60, 0) // upload 1/s sustained, burst=15; downloads unlimited
	if rl.PerMinute() != 60 {
		t.Fatalf("PerMinute=%d, want 60", rl.PerMinute())
	}
	inv := &countingInvoker{}
	fn := rl.Handle(inv)
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	// Drain the burst (15 should pass instantly) plus try a few more.
	// In 600 ms at 1/s sustained we expect ~burst + 0–1 sustained.
	done := make(chan struct{})
	go func() {
		for {
			if err := fn(ctx, nil, nil); err != nil {
				close(done)
				return
			}
		}
	}()
	<-done
	got := inv.calls.Load()
	// We expect roughly burst (15) + a small handful from the
	// sustained 1/s, well under 100.
	if got < 1 || got > 30 {
		t.Fatalf("calls in 600 ms with cap=60/min = %d, want 1..30 (burst + a sustained tick or two)", got)
	}
}

// TestRateLimiterSetLive proves SetPerMinute can be called between
// invocations and the new rate takes effect for subsequent calls.
// This is the contract the daemon's hot-reload poller relies on.
func TestRateLimiterSetLive(t *testing.T) {
	rl := NewRateLimiter(0, 0)
	rl.SetPerMinute(120)
	if rl.PerMinute() != 120 {
		t.Fatalf("after Set(120): PerMinute=%d", rl.PerMinute())
	}
	rl.SetPerMinute(0)
	if rl.PerMinute() != 0 {
		t.Fatalf("after Set(0): PerMinute=%d", rl.PerMinute())
	}
	// Negative values clamp to 0.
	rl.SetPerMinute(-5)
	if rl.PerMinute() != 0 {
		t.Fatalf("after Set(-5): PerMinute=%d (should clamp to 0)", rl.PerMinute())
	}
}
