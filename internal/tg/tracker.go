package tg

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
)

// Request records one MTProto RPC the daemon has issued. The Tracker
// keeps the most recent N of these in a ring buffer so the web UI
// can show what the daemon is doing without enabling gotd's full
// debug logging.
type Request struct {
	ID        int64         // monotonically increasing within a Tracker
	Method    string        // gotd type name without package prefix (e.g. "UploadGetFileRequest")
	Started   time.Time     // when Invoke returned to the middleware
	Duration  time.Duration // populated on completion
	Bytes     int           // wire bytes of the request encoder (best-effort)
	Err       string        // empty on success
	InFlight  bool          // true between Started and completion
	FloodWait time.Duration // non-zero if Telegram returned FLOOD_WAIT
}

// Tracker is a thread-safe ring buffer of recent RPCs plus aggregate
// connection-health state (last successful RPC, count of FLOOD_WAITs,
// in-flight count). One Tracker is shared by the gotd middleware
// chain and the web /telegram handler.
type Tracker struct {
	mu       sync.Mutex
	ring     []Request
	head     int       // next write position
	cap      int       // ring capacity
	seq      int64     // last issued ID
	inFlight int       // currently outstanding RPCs
	lastOK   time.Time // wall time of last successful Invoke
	lastErr  time.Time // wall time of last errored Invoke
	lastErrText string
}

// NewTracker returns a tracker that retains the last `capacity`
// requests. Older entries get overwritten as new ones come in.
func NewTracker(capacity int) *Tracker {
	if capacity <= 0 {
		capacity = 64
	}
	return &Tracker{
		ring: make([]Request, capacity),
		cap:  capacity,
	}
}

// Begin records the start of an Invoke and returns the assigned ID.
// Pair with End once the RPC finishes (success or failure).
func (t *Tracker) Begin(method string, bytes int) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.seq++
	t.inFlight++
	r := Request{
		ID:       t.seq,
		Method:   method,
		Started:  time.Now(),
		Bytes:    bytes,
		InFlight: true,
	}
	t.ring[t.head] = r
	t.head = (t.head + 1) % t.cap
	return r.ID
}

// End updates the previously-Begun request with completion info. The
// linear scan is fine because the ring is small (default 64).
func (t *Tracker) End(id int64, err error, floodWait time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.inFlight--
	if t.inFlight < 0 {
		t.inFlight = 0
	}
	now := time.Now()
	for i := range t.ring {
		if t.ring[i].ID == id {
			t.ring[i].Duration = now.Sub(t.ring[i].Started)
			t.ring[i].InFlight = false
			t.ring[i].FloodWait = floodWait
			if err != nil {
				t.ring[i].Err = err.Error()
				t.lastErr = now
				t.lastErrText = err.Error()
			} else {
				t.lastOK = now
			}
			return
		}
	}
}

// Snapshot returns the current ring contents in chronological order
// (oldest → newest) plus aggregate stats. Safe to call from any
// goroutine, including while RPCs are in flight.
type Snapshot struct {
	Requests     []Request
	InFlight     int
	LastSuccess  time.Time
	LastError    time.Time
	LastErrorMsg string
}

func (t *Tracker) Snapshot() Snapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]Request, 0, t.cap)
	for i := 0; i < t.cap; i++ {
		idx := (t.head + i) % t.cap
		if t.ring[idx].ID != 0 {
			out = append(out, t.ring[idx])
		}
	}
	return Snapshot{
		Requests:     out,
		InFlight:     t.inFlight,
		LastSuccess:  t.lastOK,
		LastError:    t.lastErr,
		LastErrorMsg: t.lastErrText,
	}
}

// TrackingMiddleware wraps gotd's invoker chain to feed the Tracker
// on every RPC. Sits BELOW the FloodWaiter so we record FLOOD_WAIT
// values as the underlying Invoke sees them (not after the waiter
// silently swallowed them).
type TrackingMiddleware struct {
	Tracker *Tracker
}

// Handle implements telegram.Middleware.
func (m *TrackingMiddleware) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		method := requestName(input)
		bytes := encodedSize(input)
		id := m.Tracker.Begin(method, bytes)
		err := next.Invoke(ctx, input, output)
		var fw time.Duration
		if d, ok := asFloodWait(err); ok {
			fw = d
		}
		m.Tracker.End(id, err, fw)
		return err
	}
}

// requestName returns a short type name like "UploadGetFileRequest"
// so the UI doesn't have to render the full reflect package path.
func requestName(v interface{}) string {
	t := reflect.TypeOf(v)
	if t == nil {
		return "<nil>"
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}

// encodedSize sniffs the size of the encoded request by writing into
// a throwaway buffer. Used for the wire-bytes column. Errors are
// silently treated as 0 — this is a diagnostic, not load-bearing.
func encodedSize(v bin.Encoder) int {
	if v == nil {
		return 0
	}
	b := &bin.Buffer{}
	if err := v.Encode(b); err != nil {
		return 0
	}
	return b.Len()
}

// asFloodWait is a copy of tgerr.AsFloodWait that we can call without
// circular imports against our FloodWaiter. Returns (duration, true)
// if the error is a FLOOD_WAIT_X.
func asFloodWait(err error) (time.Duration, bool) {
	if err == nil {
		return 0, false
	}
	if fwErr, ok := err.(interface{ FloodWait() (int, bool) }); ok {
		if secs, ok := fwErr.FloodWait(); ok {
			return time.Duration(secs) * time.Second, true
		}
	}
	// Fall back to a string match for older gotd versions or wrapped errors.
	s := err.Error()
	for i := 0; i+10 < len(s); i++ {
		if s[i:i+11] == "FLOOD_WAIT_" {
			n := 0
			j := i + 11
			for j < len(s) && s[j] >= '0' && s[j] <= '9' {
				n = n*10 + int(s[j]-'0')
				j++
			}
			if n > 0 {
				return time.Duration(n) * time.Second, true
			}
		}
	}
	return 0, false
}

// String returns a human-readable summary, useful for log lines.
func (r Request) String() string {
	if r.InFlight {
		return fmt.Sprintf("%s (in flight, %v elapsed)", r.Method, time.Since(r.Started).Round(time.Millisecond))
	}
	tag := "ok"
	if r.Err != "" {
		tag = "ERR " + r.Err
	}
	return fmt.Sprintf("%s %s %v %dB", r.Method, tag, r.Duration.Round(time.Millisecond), r.Bytes)
}
