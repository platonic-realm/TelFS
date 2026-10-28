package tg

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"telfs/internal/metrics"
)

// MaxFloodWait is the upper bound this daemon will honor on a single
// FLOOD_WAIT response. Telegram will sometimes ask for very long waits
// (tens of minutes, occasionally ~1 hour) after a bulk operation —
// notably after `telfs gc` reaps thousands of messages. gotd's stock
// SimpleWaiter caps at 2 min and bounces anything bigger, which our
// journal poster then retries in a 5 s tight loop forever (seen in
// the wild). 60 minutes covers post-bulk-gc cooldowns without daemon
// intervention; past that something is structurally wrong (account
// restriction, channel ban) and the retry loop won't fix it.
const MaxFloodWait = 60 * time.Minute

// FloodWaitLogInterval is how often a long sleep emits a progress
// line. Without this, a 10-minute wait looks indistinguishable from
// a wedged client.
const FloodWaitLogInterval = 30 * time.Second

// FloodWaiter is a telegram.Middleware that handles FLOOD_WAIT_N and
// FLOOD_PREMIUM_WAIT_N by sleeping N seconds and retrying, bounded by
// MaxFloodWait per attempt.
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
			kind, d, ok := classifyFlood(err)
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
			// Every flood wait is named, however short: an operator
			// watching a long run must be able to tell "Telegram asked us
			// to slow down" from a slow network (short waits used to be
			// slept through silently). FLOOD_PREMIUM_WAIT gets its own
			// line: it is not a per-method flood but Telegram's upload
			// speed limit for accounts without Premium, and the remedy
			// differs (a lower [upload] max_rate / connections, or Premium).
			if kind == tgerr.ErrPremiumFloodWait {
				metrics.FloodPremiumWaits.Inc()
				fmt.Fprintf(os.Stderr, "[flood-wait] %s: FLOOD_PREMIUM_WAIT — Telegram's upload limit for accounts without Premium; waiting %v\n", methodName(input), d.Round(time.Second))
			} else {
				fmt.Fprintf(os.Stderr, "[flood-wait] %s: Telegram asked to wait %v\n", methodName(input), d.Round(time.Second))
			}
			if err := sleepWithProgress(ctx, d); err != nil {
				return err
			}
		}
	}
}

// classifyFlood reports whether err is one of Telegram's flood throttles —
// FLOOD_WAIT_X or FLOOD_PREMIUM_WAIT_X — which one (tgerr.ErrFloodWait or
// tgerr.ErrPremiumFloodWait) and the wait it asks for. FLOOD_PREMIUM_WAIT
// is checked by its own type, not only through tgerr.AsFloodWait (whose
// list holds it in gotd v0.144): a gotd release that changed that list must
// not turn Telegram's upload limit into failed uploads.
func classifyFlood(err error) (kind string, d time.Duration, ok bool) {
	if err == nil {
		return "", 0, false
	}
	if e, ok := tgerr.AsType(err, tgerr.ErrPremiumFloodWait); ok {
		return tgerr.ErrPremiumFloodWait, time.Duration(e.Argument) * time.Second, true
	}
	if d, ok := tgerr.AsFloodWait(err); ok {
		return tgerr.ErrFloodWait, d, true
	}
	return "", 0, false
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

// methodName is the RPC's type name without the package ("UploadGetFileRequest").
func methodName(input bin.Encoder) string {
	n := fmt.Sprintf("%T", input)
	if i := strings.LastIndexByte(n, '.'); i >= 0 {
		n = n[i+1:]
	}
	return n
}
