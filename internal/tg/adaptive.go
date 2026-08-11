package tg

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"
)

// Adaptive rate-limit controller. Observes the Tracker for
// FLOOD_WAIT / FLOOD_PREMIUM_WAIT events and adjusts the upload
// lane's per-minute cap using a classic AIMD (additive increase /
// multiplicative decrease) loop:
//
//   - On EACH tick, snapshot the tracker's total FLOOD_WAIT count.
//   - If it grew since the last tick → halve the current cap
//     (floored at AdaptiveMinCap). Reset the quiet-window timer.
//   - If no FLOOD_WAITs have been seen for AdaptiveQuietBeforeRaise
//     AND there's been active upload traffic → add AdaptiveStep
//     to the cap (capped at AdaptiveMaxCap).
//   - Otherwise hold the current value.
//
// When the operator pins an explicit cap (via `telfs ratelimit set
// <N>` or the web form with N > 0), adaptive control suspends for
// that lane — pins win. Setting the pin back to 0 resumes adaptive
// control from a fresh starting cap.
//
// Downloads typically don't trigger FLOOD_WAIT in practice, so the
// download lane is left to the operator (unlimited by default, pinned
// only on explicit set). The controller still tracks pin state for
// surfacing in /telegram, but doesn't modulate it.
type AdaptiveController struct {
	rl      *RateLimiter
	tracker *Tracker
	tick    time.Duration

	mu             sync.Mutex
	pinnedUpload   int   // 0 = adaptive, >0 = operator-pinned to this value
	pinnedDownload int   // 0 = unlimited (or adaptive — we don't adapt downloads today)
	adaptiveUp     int   // current adaptive upload cap; only meaningful when pinnedUpload==0
	lastFloodTotal int64 // sum across upload-lane MethodCount.FloodWait at previous tick
	lastOKTotal    int64 // sum across upload-lane MethodCount.OK at previous tick
	lastFloodAt    time.Time
	lastAdjustAt   time.Time
	lastAction     string // human-readable reason for the last tick's decision
	// manual disables adaptive control entirely: pins apply raw
	// (including 0 = unlimited, the pre-adaptive historical
	// semantics). The escape hatch for operators on fat links who
	// would otherwise be capped at AdaptiveMaxCap forever.
	manual bool
}

// Adaptive controller tunables. Bounds + step size picked to be
// useful across both casual home connections and high-throughput
// dedicated boxes; small enough that one bad decision doesn't move
// the budget far from a reasonable region.
const (
	// AdaptiveStartingCap is the conservative initial cap when
	// adaptive control resumes (fresh start or operator clearing
	// the pin). 120 is safe for non-premium TG accounts; the
	// controller raises from here once it has observed enough quiet
	// to be confident.
	AdaptiveStartingCap      = 120
	AdaptiveMinCap           = 30              // floor — below this, throughput is too painful
	AdaptiveMaxCap           = 1200            // ceiling — above this, TG starts complaining anyway
	AdaptiveStep             = 30              // additive increase per tick during quiet periods
	AdaptiveQuietBeforeRaise = 5 * time.Minute // window without FLOOD_WAIT before we raise
	AdaptiveDefaultTick      = 30 * time.Second
)

// NewAdaptiveController constructs the controller. rl and tracker
// must be non-nil. Construct the controller, register it with the
// rate-limit poller (so pins propagate), then start Run in a
// goroutine.
func NewAdaptiveController(rl *RateLimiter, tracker *Tracker) *AdaptiveController {
	// Seed lastFloodAt to "now" so the controller behaves as if it
	// just observed a flood. This forces a warmup of
	// AdaptiveQuietBeforeRaise before the first additive increase,
	// instead of the previous bug where a zero lastFloodAt always
	// computed quiet-since == "year 1 → now" and raised every tick
	// from startup until either a flood-wait landed or the ceiling
	// was hit.
	return &AdaptiveController{
		rl:          rl,
		tracker:     tracker,
		tick:        AdaptiveDefaultTick,
		adaptiveUp:  AdaptiveStartingCap,
		lastFloodAt: time.Now(),
	}
}

// SetPinnedUpload pushes the operator's desired upload cap. n > 0
// pins (suspends adaptive control for the upload lane) and applies
// the value directly. n == 0 resumes adaptive control from
// AdaptiveStartingCap — unless manual mode is on, where 0 means
// UNLIMITED (the historical semantics). Safe to call from any
// goroutine.
func (c *AdaptiveController) SetPinnedUpload(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pinnedUpload = n
	if n > 0 {
		c.rl.SetUploadPerMinute(n)
		c.lastAction = fmt.Sprintf("pinned by operator to %d req/min", n)
		c.lastAdjustAt = time.Now()
		return
	}
	if c.manual {
		c.rl.SetUploadPerMinute(0)
		c.lastAction = "manual mode: upload unlimited"
		c.lastAdjustAt = time.Now()
		return
	}
	// Operator cleared. Resume adaptive from a fresh starting cap.
	c.adaptiveUp = AdaptiveStartingCap
	c.rl.SetUploadPerMinute(c.adaptiveUp)
	c.lastAction = fmt.Sprintf("adaptive resumed at %d req/min", c.adaptiveUp)
	c.lastAdjustAt = time.Now()
}

// SetManualMode toggles manual mode. Manual = the controller never
// adjusts anything; the operator's pin applies raw and 0 means
// unlimited. Switching back to adaptive resumes from the conservative
// starting cap (when no pin is set).
func (c *AdaptiveController) SetManualMode(on bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.manual == on {
		return
	}
	c.manual = on
	if on {
		c.rl.SetUploadPerMinute(c.pinnedUpload) // 0 → unlimited
		c.lastAction = "manual mode: adaptive control disabled"
	} else if c.pinnedUpload == 0 {
		c.adaptiveUp = AdaptiveStartingCap
		c.rl.SetUploadPerMinute(c.adaptiveUp)
		c.lastAction = fmt.Sprintf("adaptive resumed at %d req/min", c.adaptiveUp)
	}
	c.lastAdjustAt = time.Now()
}

// SetPinnedDownload mirrors SetPinnedUpload for the download lane.
// We don't adapt download today (TG rarely throttles downloads), so
// this just records the pin state and applies the raw value.
func (c *AdaptiveController) SetPinnedDownload(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pinnedDownload = n
	c.rl.SetDownloadPerMinute(n) // n=0 → unlimited, applied via the lane's existing semantics
}

// Run drives the AIMD loop until ctx is canceled. Idempotent;
// safe to call once per process.
func (c *AdaptiveController) Run(ctx context.Context) {
	// Apply the starting cap so the daemon starts with a sane
	// conservative value (instead of unlimited, where the first
	// burst triggers FLOOD_WAIT). If the operator's pinned value is
	// loaded first (poller runs at startup), that wins via
	// SetPinnedUpload — this only sets adaptiveUp's initial bias.
	// Manual mode (also loaded by the poller) means "hands off".
	c.mu.Lock()
	if c.pinnedUpload == 0 && !c.manual {
		c.rl.SetUploadPerMinute(c.adaptiveUp)
	}
	c.mu.Unlock()

	t := time.NewTicker(c.tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.step()
		}
	}
}

// isDownloadMethodName reports whether a tracker method name belongs
// to the download lane (upload.getFile and its CDN variant). Their
// flood events must not modulate the UPLOAD cap — only the upload
// lane's own behavior should.
func isDownloadMethodName(name string) bool {
	return name == "UploadGetFileRequest" || name == "UploadGetCdnFileRequest"
}

// step is one AIMD iteration. Exposed at package level so tests can
// drive it deterministically without sleeping.
func (c *AdaptiveController) step() {
	snap := c.tracker.Snapshot()
	// Aggregate the UPLOAD lane only: a download-side flood storm
	// halving the upload cap punishes the wrong lane (downloads stay
	// unlimited while uploads crawl to the floor).
	var floodTotal, okTotal int64
	for _, m := range snap.PerMethod {
		if isDownloadMethodName(m.Method) {
			continue
		}
		floodTotal += m.FloodWait
		okTotal += m.OK
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	floodDelta := floodTotal - c.lastFloodTotal
	c.lastFloodTotal = floodTotal
	okDelta := okTotal - c.lastOKTotal
	c.lastOKTotal = okTotal

	// Manual mode / pinned: respect operator's choice. Still track
	// flood history so /telegram's view stays meaningful.
	if c.manual || c.pinnedUpload > 0 {
		if floodDelta > 0 {
			c.lastFloodAt = time.Now()
		}
		return
	}

	now := time.Now()
	if floodDelta > 0 {
		// Multiplicative decrease.
		c.lastFloodAt = now
		newCap := c.adaptiveUp / 2
		if newCap < AdaptiveMinCap {
			newCap = AdaptiveMinCap
		}
		if newCap == c.adaptiveUp {
			c.lastAction = fmt.Sprintf("hold at floor %d req/min (flood +%d)", c.adaptiveUp, floodDelta)
			c.lastAdjustAt = now
			return
		}
		c.lastAction = fmt.Sprintf("lowered %d → %d req/min (flood +%d)", c.adaptiveUp, newCap, floodDelta)
		c.lastAdjustAt = now
		c.adaptiveUp = newCap
		c.rl.SetUploadPerMinute(newCap)
		fmt.Fprintf(os.Stderr, "[ratelimit] adaptive: upload → %d req/min (FLOOD_WAIT +%d in last %v)\n", newCap, floodDelta, c.tick)
		return
	}

	// No new floods. Decide whether to raise.
	if now.Sub(c.lastFloodAt) < AdaptiveQuietBeforeRaise {
		remaining := (AdaptiveQuietBeforeRaise - now.Sub(c.lastFloodAt)).Round(time.Second)
		c.lastAction = fmt.Sprintf("hold at %d req/min (waiting %v for quiet window)", c.adaptiveUp, remaining)
		return
	}
	// Raise only when the upload lane actually MOVED this tick. The
	// old gate (cumulative TotalOK != 0) was satisfied forever after
	// the first RPC, so an idle overnight mount climbed to the
	// ceiling and the morning's first burst slammed into FLOOD_WAIT
	// from max rate. "Quiet + active traffic" is what justifies
	// probing for more headroom; "quiet because idle" justifies
	// nothing.
	if okDelta == 0 {
		c.lastAction = fmt.Sprintf("hold at %d req/min (no upload traffic this tick)", c.adaptiveUp)
		return
	}
	newCap := c.adaptiveUp + AdaptiveStep
	if newCap > AdaptiveMaxCap {
		newCap = AdaptiveMaxCap
	}
	if newCap == c.adaptiveUp {
		c.lastAction = fmt.Sprintf("hold at ceiling %d req/min", c.adaptiveUp)
		return
	}
	quietFor := now.Sub(c.lastFloodAt).Round(time.Second)
	c.lastAction = fmt.Sprintf("raised %d → %d req/min (quiet for %v)", c.adaptiveUp, newCap, quietFor)
	c.lastAdjustAt = now
	c.adaptiveUp = newCap
	c.rl.SetUploadPerMinute(newCap)
	fmt.Fprintf(os.Stderr, "[ratelimit] adaptive: upload → %d req/min (quiet >%v)\n", newCap, AdaptiveQuietBeforeRaise)
}

// AdaptiveSnapshot is the state the /telegram JSON exposes about the
// controller. Read-only; callers don't get the live mutex.
type AdaptiveSnapshot struct {
	Manual              bool   `json:"manual"` // manual mode: no adaptive control, 0 = unlimited
	UploadPinned        bool   `json:"upload_pinned"`
	UploadEffective     int    `json:"upload_effective"`
	UploadAdaptiveLevel int    `json:"upload_adaptive_level"` // current adaptive cap (even when pinned, for transparency)
	DownloadPinned      bool   `json:"download_pinned"`
	DownloadEffective   int    `json:"download_effective"`
	LastFloodAgoSec     int64  `json:"last_flood_ago_sec"`  // -1 if never observed
	LastAdjustAgoSec    int64  `json:"last_adjust_ago_sec"` // -1 if no adjustment yet
	LastAction          string `json:"last_action"`
}

// Snapshot returns the controller's current state for surfacing.
func (c *AdaptiveController) Snapshot() AdaptiveSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	floodAgo := int64(-1)
	if !c.lastFloodAt.IsZero() {
		floodAgo = int64(time.Since(c.lastFloodAt).Seconds())
	}
	adjAgo := int64(-1)
	if !c.lastAdjustAt.IsZero() {
		adjAgo = int64(time.Since(c.lastAdjustAt).Seconds())
	}
	effectiveUp := c.adaptiveUp
	if c.pinnedUpload > 0 {
		effectiveUp = c.pinnedUpload
	}
	if c.manual {
		effectiveUp = c.pinnedUpload // 0 = unlimited
	}
	return AdaptiveSnapshot{
		Manual:              c.manual,
		UploadPinned:        c.pinnedUpload > 0,
		UploadEffective:     effectiveUp,
		UploadAdaptiveLevel: c.adaptiveUp,
		DownloadPinned:      c.pinnedDownload > 0,
		DownloadEffective:   c.pinnedDownload,
		LastFloodAgoSec:     floodAgo,
		LastAdjustAgoSec:    adjAgo,
		LastAction:          c.lastAction,
	}
}
