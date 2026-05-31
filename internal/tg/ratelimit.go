package tg

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"golang.org/x/time/rate"
)

// RateLimiter is a telegram.Middleware that caps outbound MTProto
// requests at a per-minute budget. Sits ABOVE FloodWaiter and
// Tracking so the limiter gate is the user-controllable knob:
// requests that get queued at the limiter are NOT yet recorded as
// "in flight", which keeps the /telegram tab's in-flight count
// meaningful (it counts requests actually riding an MTProto round
// trip, not requests waiting to be admitted).
//
// SetPerMinute can be called concurrently from a config-poller
// goroutine to make CLI/web changes propagate without remount. 0
// means unlimited (the historical default); the limiter holds
// rate.Inf in that case and Wait returns immediately.
type RateLimiter struct {
	mu      sync.Mutex
	limiter *rate.Limiter
	// current is the most recently-applied per-minute value, atomic
	// so a non-locking read for the status page is cheap.
	current atomic.Int32
}

// NewRateLimiter constructs a limiter starting at perMinute. Pass
// 0 for unlimited (the default).
func NewRateLimiter(perMinute int) *RateLimiter {
	r := &RateLimiter{
		limiter: rate.NewLimiter(rate.Inf, 1),
	}
	r.SetPerMinute(perMinute)
	return r
}

// SetPerMinute applies a new per-minute cap. 0 (or negative) means
// unlimited — the limiter is set to rate.Inf so Wait short-circuits
// without burning a token slot. Safe to call concurrently. The
// burst is set proportionally to the per-minute rate (up to 32) so
// short bursts pass through cleanly without sustained throughput
// staying above the cap.
func (r *RateLimiter) SetPerMinute(n int) {
	if n < 0 {
		n = 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if n == 0 {
		r.limiter.SetLimit(rate.Inf)
		r.limiter.SetBurst(1)
		r.current.Store(0)
		return
	}
	r.limiter.SetLimit(rate.Limit(float64(n) / 60.0))
	burst := n / 4
	if burst < 1 {
		burst = 1
	}
	if burst > 32 {
		burst = 32
	}
	r.limiter.SetBurst(burst)
	r.current.Store(int32(n))
}

// PerMinute returns the currently-configured cap; 0 means
// unlimited. Safe to call concurrently.
func (r *RateLimiter) PerMinute() int { return int(r.current.Load()) }

// Handle implements telegram.Middleware. Blocks at Wait until a
// token is available, then defers to the next middleware.
func (r *RateLimiter) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		if err := r.limiter.Wait(ctx); err != nil {
			return err
		}
		return next.Invoke(ctx, input, output)
	}
}
