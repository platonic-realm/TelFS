package tg

import (
	"testing"
	"time"

	"github.com/gotd/td/tgerr"
)

// TestAsFloodWaitRecognizesGotdErrors: gotd's real error type prints
// as "rpc error code 420: FLOOD_WAIT (23)" — no "FLOOD_WAIT_23"
// substring — so the parser MUST go through tgerr.AsFloodWait. This
// was the bug that made the tracker blind to every real flood event.
func TestAsFloodWaitRecognizesGotdErrors(t *testing.T) {
	cases := []struct {
		err  error
		want time.Duration
	}{
		{tgerr.New(420, "FLOOD_WAIT_23"), 23 * time.Second},
		{tgerr.New(420, "FLOOD_PREMIUM_WAIT_7"), 7 * time.Second},
	}
	for _, c := range cases {
		d, ok := asFloodWait(c.err)
		if !ok || d != c.want {
			t.Errorf("asFloodWait(%v) = (%v, %v), want (%v, true)", c.err, d, ok, c.want)
		}
	}
	// String fallback for wrapped/foreign errors still works.
	if d, ok := asFloodWait(errString("some wrapper: FLOOD_WAIT_11 (caused by ...)")); !ok || d != 11*time.Second {
		t.Errorf("string fallback broken: got (%v, %v)", d, ok)
	}
	if _, ok := asFloodWait(errString("AUTH_KEY_UNREGISTERED")); ok {
		t.Error("non-flood error misclassified as flood")
	}
}

type errString string

func (e errString) Error() string { return string(e) }

// endRPC simulates one completed RPC through the tracker.
func endRPC(tr *Tracker, method string, err error, flood time.Duration) {
	id := tr.Begin(method, 100)
	tr.End(id, err, flood, 0)
}

func newTestController() (*AdaptiveController, *RateLimiter, *Tracker) {
	rl := NewRateLimiter(0, 0)
	tr := NewTracker(16)
	c := NewAdaptiveController(rl, tr)
	// Pretend the flood-quiet warmup has long passed so raise
	// decisions depend only on the traffic gate.
	c.lastFloodAt = time.Now().Add(-2 * AdaptiveQuietBeforeRaise)
	return c, rl, tr
}

// TestAdaptiveNoRaiseWhileIdle: cumulative traffic from the past must
// not justify raises during idle ticks — otherwise an idle overnight
// mount climbs to the ceiling and the morning burst starts at max
// rate straight into a flood wall.
func TestAdaptiveNoRaiseWhileIdle(t *testing.T) {
	c, rl, tr := newTestController()
	// Historic traffic: some successful uploads, then silence.
	endRPC(tr, "MessagesSendMediaRequest", nil, 0)
	endRPC(tr, "MessagesSendMediaRequest", nil, 0)
	c.step() // consumes the ok-delta (and may raise once — traffic was real)
	after := rl.UploadPerMinute()
	for i := 0; i < 10; i++ {
		c.step() // idle ticks: no new RPCs
	}
	if got := rl.UploadPerMinute(); got != after {
		t.Fatalf("cap climbed from %d to %d during idle ticks", after, got)
	}
}

// TestAdaptiveRaisesWithActiveQuietTraffic: quiet + active traffic
// raises by AdaptiveStep per tick.
func TestAdaptiveRaisesWithActiveQuietTraffic(t *testing.T) {
	c, rl, tr := newTestController()
	before := c.Snapshot().UploadAdaptiveLevel
	endRPC(tr, "MessagesSendMediaRequest", nil, 0)
	c.step()
	if got := rl.UploadPerMinute(); got != before+AdaptiveStep {
		t.Fatalf("cap = %d, want %d after active quiet tick", got, before+AdaptiveStep)
	}
}

// TestAdaptiveHalvesOnFlood: an upload-lane flood halves the cap.
func TestAdaptiveHalvesOnFlood(t *testing.T) {
	c, rl, tr := newTestController()
	before := c.Snapshot().UploadAdaptiveLevel
	endRPC(tr, "MessagesSendMediaRequest", tgerr.New(420, "FLOOD_WAIT_30"), 30*time.Second)
	c.step()
	want := before / 2
	if want < AdaptiveMinCap {
		want = AdaptiveMinCap
	}
	if got := rl.UploadPerMinute(); got != want {
		t.Fatalf("cap = %d after flood, want %d", got, want)
	}
}

// TestAdaptiveIgnoresDownloadLaneFloods: a flood on upload.getFile
// (download lane) must not halve the UPLOAD cap.
func TestAdaptiveIgnoresDownloadLaneFloods(t *testing.T) {
	c, rl, tr := newTestController()
	before := rl.UploadPerMinute()
	endRPC(tr, "UploadGetFileRequest", tgerr.New(420, "FLOOD_WAIT_30"), 30*time.Second)
	c.step()
	if got := rl.UploadPerMinute(); got != before {
		t.Fatalf("upload cap moved from %d to %d on a DOWNLOAD-lane flood", before, got)
	}
}

// TestAdaptiveManualMode: manual + pin 0 = unlimited; step never
// adjusts; leaving manual resumes adaptive from the starting cap.
func TestAdaptiveManualMode(t *testing.T) {
	c, rl, tr := newTestController()
	c.SetManualMode(true)
	c.SetPinnedUpload(0)
	if got := rl.UploadPerMinute(); got != 0 {
		t.Fatalf("manual+clear should be unlimited (0), got %d", got)
	}
	endRPC(tr, "MessagesSendMediaRequest", tgerr.New(420, "FLOOD_WAIT_30"), 30*time.Second)
	c.step()
	if got := rl.UploadPerMinute(); got != 0 {
		t.Fatalf("manual mode adjusted the cap to %d", got)
	}
	if !c.Snapshot().Manual {
		t.Fatal("snapshot does not report manual mode")
	}
	c.SetManualMode(false)
	if got := rl.UploadPerMinute(); got != AdaptiveStartingCap {
		t.Fatalf("leaving manual should resume adaptive at %d, got %d", AdaptiveStartingCap, got)
	}
}
