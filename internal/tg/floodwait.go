package tg

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"telfs/internal/metrics"
)

// MaxFloodWait is the upper bound this daemon will honor on a single
// FLOOD_WAIT response. Telegram will sometimes ask for very long waits
// (~tens of minutes) after a burst; gotd's stock SimpleWaiter caps at
// 2 min and bounces anything bigger, which our journal poster then
// retries in a 5 s tight loop forever (seen in the wild). 30 minutes
// is the longest cooldown observed in practice; past that we surface
// the error rather than block a goroutine indefinitely.
const MaxFloodWait = 30 * time.Minute

// FloodWaitLogInterval is how often a long sleep emits a progress
// line. Without this, a 10-minute wait looks indistinguishable from
// a wedged client.
const FloodWaitLogInterval = 30 * time.Second

// FloodWaiter is a telegram.Middleware that handles FLOOD_WAIT_N by
// sleeping N seconds and retrying, bounded by MaxFloodWait per attempt.
// Replaces gotd/contrib's SimpleWaiter with WithMaxWait(2 * time.Minute):
// the 2-minute cap was a stand-in default, not a deliberate floor.
//
// Progress is logged once per FloodWaitLogInterval so a long sleep is
// visible in the daemon log. Retries are unbounded — TelFS's failure
// modes for "too many retries" are worse than "long wait completed
// eventually."
type FloodWaiter struct {
	max time.Duration
}

// NewFloodWaiter returns a middleware that tolerates waits up to
// MaxFloodWait. Zero or negative `max` means use MaxFloodWait.
func NewFloodWaiter() *FloodWaiter {
	return &FloodWaiter{max: MaxFloodWait}
}

// Handle implements telegram.Middleware.
func (w *FloodWaiter) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		for {
			err := next.Invoke(ctx, input, output)
			if err == nil {
				return nil
			}
			d, ok := tgerr.AsFloodWait(err)
			if !ok {
				return err
			}
			if d <= 0 {
				d = time.Second
			}
			if d > w.max {
				return fmt.Errorf("flood wait %v exceeds cap %v: %w", d, w.max, err)
			}
			metrics.FloodWaits.Inc()
			if err := sleepWithProgress(ctx, d); err != nil {
				return err
			}
		}
	}
}

// sleepWithProgress sleeps for d, logging remaining time every
// FloodWaitLogInterval for waits longer than that. Returns ctx.Err()
// if ctx fires first.
func sleepWithProgress(ctx context.Context, d time.Duration) error {
	deadline := time.Now().Add(d)
	if d <= FloodWaitLogInterval {
		select {
		case <-time.After(d):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	fmt.Fprintf(os.Stderr, "[flood-wait] sleeping %v\n", d.Round(time.Second))
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}
		step := FloodWaitLogInterval
		if remaining < step {
			step = remaining
		}
		select {
		case <-time.After(step):
			r := time.Until(deadline).Round(time.Second)
			if r > 0 {
				fmt.Fprintf(os.Stderr, "[flood-wait] %v remaining\n", r)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
