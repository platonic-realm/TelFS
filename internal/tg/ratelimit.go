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
// Two lanes — upload (everything by default) and download (just
// upload.getFile) — share neither tokens nor mutex, so a heavy packer
// flush cannot starve reads that go through the same middleware
// chain. That starvation was a real failure mode visible in the
// kernel-untar incident: 256 SaveBigFilePart RPCs per blob held the
// limiter while menuconfig's Kconfig downloads piled up behind them.
//
// Set{Upload,Download}PerMinute can be called concurrently from a
// config-poller goroutine to make CLI/web changes propagate without
// remount. 0 means unlimited on that lane (the download default);
// the limiter holds rate.Inf in that case and Wait returns
// immediately.
type RateLimiter struct {
	mu    sync.Mutex
	up    *rate.Limiter
	down  *rate.Limiter
	upN   atomic.Int32
	downN atomic.Int32
}

// NewRateLimiter constructs a limiter with the given starting
// per-minute caps. Pass 0 for unlimited.
//
// Single-arg call sites are common (upload-only throttle) — pass
// (perMinute, 0).
func NewRateLimiter(uploadPerMinute, downloadPerMinute int) *RateLimiter {
	r := &RateLimiter{
		up:   rate.NewLimiter(rate.Inf, 1),
		down: rate.NewLimiter(rate.Inf, 1),
	}
	r.SetUploadPerMinute(uploadPerMinute)
	r.SetDownloadPerMinute(downloadPerMinute)
	return r
}

// SetUploadPerMinute applies a new per-minute cap on the upload lane.
// 0 (or negative) means unlimited. Burst is set proportionally so
// short bursts pass through cleanly without sustained throughput
// staying above the cap. Safe to call concurrently.
func (r *RateLimiter) SetUploadPerMinute(n int) {
	applyLimit(&r.mu, r.up, &r.upN, n)
}

// SetDownloadPerMinute applies a new per-minute cap on the download
// lane (upload.getFile). 0 means unlimited (the safe default — TG
// rarely FLOOD_WAITs downloads).
func (r *RateLimiter) SetDownloadPerMinute(n int) {
	applyLimit(&r.mu, r.down, &r.downN, n)
}

// applyLimit is the shared SetXPerMinute body. Lives free-standing so
// both lane setters share the burst-tuning and atomic-store logic.
func applyLimit(mu *sync.Mutex, lim *rate.Limiter, current *atomic.Int32, n int) {
	if n < 0 {
		n = 0
	}
	mu.Lock()
	defer mu.Unlock()
	if n == 0 {
		lim.SetLimit(rate.Inf)
		lim.SetBurst(1)
		current.Store(0)
		return
	}
	lim.SetLimit(rate.Limit(float64(n) / 60.0))
	burst := n / 4
	if burst < 1 {
		burst = 1
	}
	if burst > 32 {
		burst = 32
	}
	lim.SetBurst(burst)
	current.Store(int32(n))
}

// UploadPerMinute / DownloadPerMinute return the currently-configured
// caps; 0 means unlimited. Both are safe to call concurrently.
func (r *RateLimiter) UploadPerMinute() int   { return int(r.upN.Load()) }
func (r *RateLimiter) DownloadPerMinute() int { return int(r.downN.Load()) }

// PerMinute is the legacy single-cap accessor — returns the upload
// cap, preserving behavior for callers that haven't been migrated.
// Prefer UploadPerMinute / DownloadPerMinute for new code.
func (r *RateLimiter) PerMinute() int { return r.UploadPerMinute() }

// SetPerMinute is the legacy single-cap setter — sets the upload
// cap. Download lane is unaffected.
func (r *RateLimiter) SetPerMinute(n int) { r.SetUploadPerMinute(n) }

// Handle implements telegram.Middleware. Routes each request to the
// upload or download lane based on the input type, then blocks at
// Wait until that lane has a token.
//
// Lane policy: only upload.getFile (and the CDN-routed variant
// if/when used) counts as a download — everything else is upload-
// shaped (SaveFilePart, SaveBigFilePart, SendMedia, channel scans,
// journal posts). Misrouting a download to the upload lane is the
// conservative failure mode: at worst it shares the upload budget
// (current behavior), never the other direction.
func (r *RateLimiter) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		lim := r.up
		if isDownloadRequest(input) {
			lim = r.down
		}
		if err := lim.Wait(ctx); err != nil {
			return err
		}
		return next.Invoke(ctx, input, output)
	}
}

// isDownloadRequest reports whether input is a download-shaped RPC
// for rate-limit lane selection. Keep the list narrow on purpose —
// the cost of a false positive (upload misrouted to the download
// lane) is to bypass the operator's intended throttle, which is
// worse than the cost of a false negative (download throttled
// alongside uploads, the historical behavior).
func isDownloadRequest(input bin.Encoder) bool {
	switch input.(type) {
	case *tg.UploadGetFileRequest:
		return true
	}
	return false
}
