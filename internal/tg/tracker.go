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
//
// Bytes is the per-row wire-bytes column shown in /telegram: for
// upload-shaped RPCs (UploadSaveFilePartRequest, ...) it's the
// encoded request size, which contains the payload; for download-
// shaped RPCs (UploadGetFileRequest) it's the response payload size
// the server returned. Either way it represents "bytes that crossed
// the wire for this RPC," matching what an operator expects to see.
type Request struct {
	ID        int64         // monotonically increasing within a Tracker
	Method    string        // gotd type name without package prefix (e.g. "UploadGetFileRequest")
	Started   time.Time     // when Invoke returned to the middleware
	Duration  time.Duration // populated on completion
	Bytes     int           // tx for uploads, rx for downloads (whichever is the payload-bearing direction)
	Err       string        // empty on success
	InFlight  bool          // true between Started and completion
	FloodWait time.Duration // non-zero if Telegram returned FLOOD_WAIT
}

// MethodCount aggregates totals for one RPC method since daemon
// start. Surfaced via /telegram so the web can render "uploaded
// 8 blobs by SendMedia + 512 wire parts by SaveFilePart" rather
// than just the latest 128 entries.
type MethodCount struct {
	Method    string
	OK        int64 // successful invocations
	Err       int64 // failed invocations (any non-nil error)
	FloodWait int64 // FLOOD_WAIT responses (subset of Err — gets retried)
	Bytes     int64 // payload-direction bytes per Request.Bytes semantics (tx for upload methods, rx for download methods)
}

// Tracker is a thread-safe ring buffer of recent RPCs plus aggregate
// connection-health state (last successful RPC, count of FLOOD_WAITs,
// in-flight count, per-method totals). One Tracker is shared by the
// gotd middleware chain and the web /telegram handler.
type Tracker struct {
	mu         sync.Mutex
	ring       []Request
	head       int       // next write position
	cap        int       // ring capacity
	seq        int64     // last issued ID
	totalOK    int64     // count of successful Invokes since start
	totalErr   int64     // count of errored Invokes since start
	totalBytes int64     // sum of request+response payload bytes since start (tx+rx)
	totalTx    int64     // upload-direction payload (encoded request bytes for upload methods)
	totalRx    int64     // download-direction payload (response .Bytes for download methods)
	byMethod   map[string]*MethodCount
	inFlight   int       // currently outstanding RPCs
	lastOK     time.Time // wall time of last successful Invoke
	lastErr    time.Time // wall time of last errored Invoke
	lastErrText string
}

// NewTracker returns a tracker that retains the last `capacity`
// requests. Older entries get overwritten as new ones come in.
func NewTracker(capacity int) *Tracker {
	if capacity <= 0 {
		capacity = 64
	}
	return &Tracker{
		ring:     make([]Request, capacity),
		cap:      capacity,
		byMethod: make(map[string]*MethodCount),
	}
}

// Begin records the start of an Invoke and returns the assigned ID.
// Pair with End once the RPC finishes (success or failure). txBytes is
// the encoded request size — populated for ALL methods; the End call
// may later override Request.Bytes with rx for download-shaped methods.
func (t *Tracker) Begin(method string, txBytes int) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.seq++
	t.inFlight++
	r := Request{
		ID:       t.seq,
		Method:   method,
		Started:  time.Now(),
		Bytes:    txBytes,
		InFlight: true,
	}
	t.ring[t.head] = r
	t.head = (t.head + 1) % t.cap
	return r.ID
}

// End updates the previously-Begun request with completion info.
// rxBytes is the response payload size for download-shaped methods
// (zero for upload methods where the request encoder already carries
// the payload). The linear scan is fine because the ring is small
// (default 64).
func (t *Tracker) End(id int64, err error, floodWait time.Duration, rxBytes int) {
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
			method := t.ring[i].Method
			tx := int64(t.ring[i].Bytes) // recorded as tx at Begin time
			rx := int64(rxBytes)
			// Per-row display: for downloads the response payload is
			// the meaningful number, not the tiny request encoder.
			// Pick whichever side actually moved bytes.
			if rx > 0 && rx > tx {
				t.ring[i].Bytes = int(rx)
			}
			mc := t.byMethod[method]
			if mc == nil {
				mc = &MethodCount{Method: method}
				t.byMethod[method] = mc
			}
			// Per-method aggregate matches the display semantics:
			// downloads contribute rx, uploads contribute tx.
			payload := tx
			if rx > 0 && rx > tx {
				payload = rx
			}
			mc.Bytes += payload
			t.totalBytes += payload
			t.totalTx += tx
			t.totalRx += rx
			if err != nil {
				t.ring[i].Err = err.Error()
				t.lastErr = now
				t.lastErrText = err.Error()
				t.totalErr++
				mc.Err++
				if floodWait > 0 {
					mc.FloodWait++
				}
			} else {
				t.lastOK = now
				t.totalOK++
				mc.OK++
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
	// Aggregate totals since daemon start.
	TotalOK      int64
	TotalErr     int64
	TotalBytes   int64 // payload-direction sum across all RPCs (tx for uploads + rx for downloads)
	TotalBytesTx int64 // upload-direction wire bytes
	TotalBytesRx int64 // download-direction wire bytes
	PerMethod    []MethodCount // sorted by total count desc
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
	perMethod := make([]MethodCount, 0, len(t.byMethod))
	for _, mc := range t.byMethod {
		perMethod = append(perMethod, *mc)
	}
	// Sort by total count (OK+Err) desc — most-used methods first.
	for i := 1; i < len(perMethod); i++ {
		for j := i; j > 0; j-- {
			ai := perMethod[j].OK + perMethod[j].Err
			aj := perMethod[j-1].OK + perMethod[j-1].Err
			if ai > aj {
				perMethod[j], perMethod[j-1] = perMethod[j-1], perMethod[j]
				continue
			}
			break
		}
	}
	return Snapshot{
		Requests:     out,
		InFlight:     t.inFlight,
		LastSuccess:  t.lastOK,
		LastError:    t.lastErr,
		LastErrorMsg: t.lastErrText,
		TotalOK:      t.totalOK,
		TotalErr:     t.totalErr,
		TotalBytes:   t.totalBytes,
		TotalBytesTx: t.totalTx,
		TotalBytesRx: t.totalRx,
		PerMethod:    perMethod,
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
		txBytes := encodedSize(input)
		id := m.Tracker.Begin(method, txBytes)
		err := next.Invoke(ctx, input, output)
		var fw time.Duration
		if d, ok := asFloodWait(err); ok {
			fw = d
		}
		// Sniff the response payload for download-shaped RPCs. The
		// gotd output decoder for upload.getFile is *tg.UploadFile,
		// which carries .Bytes — the actual chunk data. Without this,
		// download traffic is invisible because the encoded REQUEST is
		// tiny (file location + offset + limit, ~64 B). Same for the
		// CDN-routed variant. Other download shapes (web.getFile,
		// upload.getCDNFile, etc.) can be added here if we ever use
		// them.
		var rxBytes int
		if err == nil {
			rxBytes = responsePayloadSize(output)
		}
		m.Tracker.End(id, err, fw, rxBytes)
		return err
	}
}

// responsePayloadSize extracts the byte-length of the payload portion
// of a known download response. Returns 0 for unknown / non-download
// shapes — those are inherently "no rx accounting needed" cases (the
// request encoder already contains the payload, or there's no
// content). Worth keeping the type-assert list small: each entry has
// to be a download shape we actually issue.
func responsePayloadSize(output bin.Decoder) int {
	if output == nil {
		return 0
	}
	if uf, ok := output.(*tg.UploadFile); ok {
		return len(uf.Bytes)
	}
	return 0
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
