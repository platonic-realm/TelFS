package tg

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
)

// KVLifetimeStats is the meta_kv key holding the tracker's persisted
// lifetime totals (TrackerState JSON). Lives in the profile's
// db.sqlite, so the numbers survive daemon restarts — and, riding the
// snapshot blob, even a channel recovery.
const KVLifetimeStats = "tg_lifetime_stats"

// TrackerState is the persistable slice of a Tracker: lifetime
// aggregate counters, no ring/no in-flight state. The daemon seeds a
// fresh Tracker from it at startup and writes it back periodically,
// so /telegram's totals are continuous instead of resetting to zero
// on every restart.
type TrackerState struct {
	TotalOK     int64                  `json:"total_ok"`
	TotalErr    int64                  `json:"total_err"`
	TotalBytes  int64                  `json:"total_bytes"`
	TotalTx     int64                  `json:"total_bytes_tx"`
	TotalRx     int64                  `json:"total_bytes_rx"`
	PerMethod   map[string]MethodCount `json:"per_method"`
	UpdatedUnix int64                  `json:"updated_unix"`
}

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
	mu   sync.Mutex
	ring []Request
	head int   // next write position
	cap  int   // ring capacity
	seq  int64 // last issued ID
	// pending tracks in-flight RPCs by ID, independent of the ring:
	// the ring is a bounded DISPLAY of recent requests and its slots
	// get overwritten under load, but accounting (totals, per-method,
	// flood counts) must never lose an entry — the adaptive
	// controller and the stall watchdog read those numbers exactly
	// when load is highest.
	pending     map[int64]pendingRPC
	totalOK     int64 // count of successful Invokes since start
	totalErr    int64 // count of errored Invokes since start
	totalBytes  int64 // sum of request+response payload bytes since start (tx+rx)
	totalTx     int64 // upload-direction payload (encoded request bytes for upload methods)
	totalRx     int64 // download-direction payload (response .Bytes for download methods)
	byMethod    map[string]*MethodCount
	inFlight    int       // currently outstanding RPCs
	lastOK      time.Time // wall time of last successful Invoke
	lastErr     time.Time // wall time of last errored Invoke
	floodUntil  time.Time // latest (now + FLOOD_WAIT duration) observed — the server-mandated quiet period
	lastErrText string
}

// pendingRPC is the accounting-critical slice of an in-flight RPC.
type pendingRPC struct {
	method  string
	started time.Time
	tx      int
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
		pending:  make(map[int64]pendingRPC),
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
	t.pending[r.ID] = pendingRPC{method: method, started: r.Started, tx: txBytes}
	t.ring[t.head] = r
	t.head = (t.head + 1) % t.cap
	return r.ID
}

// End updates the previously-Begun request with completion info.
// rxBytes is the response payload size for download-shaped methods
// (zero for upload methods where the request encoder already carries
// the payload).
//
// Accounting reads the pending map, NOT the ring: under load (> cap
// Begins while an RPC is in flight) the ring slot gets overwritten,
// and the old ring-scan-only implementation silently dropped that
// RPC's OK/Err/flood/bytes — degrading the adaptive controller's and
// watchdog's inputs exactly when load peaked. The ring scan below is
// display-only best effort.
func (t *Tracker) End(id int64, err error, floodWait time.Duration, rxBytes int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.inFlight--
	if t.inFlight < 0 {
		t.inFlight = 0
	}
	now := time.Now()
	p, tracked := t.pending[id]
	if !tracked {
		return // End without Begin — nothing to account
	}
	delete(t.pending, id)

	tx := int64(p.tx)
	rx := int64(rxBytes)
	mc := t.byMethod[p.method]
	if mc == nil {
		mc = &MethodCount{Method: p.method}
		t.byMethod[p.method] = mc
	}
	// Per-method aggregate: downloads contribute rx, uploads tx —
	// whichever side actually moved the payload.
	payload := tx
	if rx > 0 && rx > tx {
		payload = rx
	}
	mc.Bytes += payload
	t.totalBytes += payload
	t.totalTx += tx
	t.totalRx += rx
	if err != nil {
		t.lastErr = now
		t.lastErrText = err.Error()
		t.totalErr++
		mc.Err++
		if floodWait > 0 {
			mc.FloodWait++
			if until := now.Add(floodWait); until.After(t.floodUntil) {
				t.floodUntil = until
			}
		}
	} else {
		t.lastOK = now
		t.totalOK++
		mc.OK++
	}

	// Display: update the ring row if it hasn't been overwritten.
	for i := range t.ring {
		if t.ring[i].ID == id {
			t.ring[i].Duration = now.Sub(p.started)
			t.ring[i].InFlight = false
			t.ring[i].FloodWait = floodWait
			if rx > 0 && rx > tx {
				t.ring[i].Bytes = int(rx)
			}
			if err != nil {
				t.ring[i].Err = err.Error()
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
	// FloodWaitUntil is the end of the most recent server-mandated
	// FLOOD_WAIT quiet period (zero if none observed). While now <
	// FloodWaitUntil, an absence of successful RPCs is EXPECTED — the
	// stall watchdog must not read it as a wedged connection.
	FloodWaitUntil time.Time
	// Aggregate totals since daemon start.
	TotalOK      int64
	TotalErr     int64
	TotalBytes   int64         // payload-direction sum across all RPCs (tx for uploads + rx for downloads)
	TotalBytesTx int64         // upload-direction wire bytes
	TotalBytesRx int64         // download-direction wire bytes
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
		Requests:       out,
		InFlight:       t.inFlight,
		LastSuccess:    t.lastOK,
		LastError:      t.lastErr,
		LastErrorMsg:   t.lastErrText,
		FloodWaitUntil: t.floodUntil,
		TotalOK:        t.totalOK,
		TotalErr:       t.totalErr,
		TotalBytes:     t.totalBytes,
		TotalBytesTx:   t.totalTx,
		TotalBytesRx:   t.totalRx,
		PerMethod:      perMethod,
	}
}

// MarshalState serializes the tracker's lifetime totals for
// persistence. Because SeedState ADDS the stored baseline into the
// live counters, marshaling the live counters later yields the full
// lifetime sum — a plain overwrite of the stored value is always
// correct.
func (t *Tracker) MarshalState() ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := TrackerState{
		TotalOK:     t.totalOK,
		TotalErr:    t.totalErr,
		TotalBytes:  t.totalBytes,
		TotalTx:     t.totalTx,
		TotalRx:     t.totalRx,
		PerMethod:   make(map[string]MethodCount, len(t.byMethod)),
		UpdatedUnix: time.Now().Unix(),
	}
	for name, mc := range t.byMethod {
		st.PerMethod[name] = *mc
	}
	return json.Marshal(st)
}

// SeedState merges previously-persisted lifetime totals into the
// tracker's counters. Call once at daemon startup, before traffic.
// Watchdog and adaptive-ratelimit logic compare DELTAS of these
// counters, so a shifted baseline doesn't affect them.
func (t *Tracker) SeedState(data []byte) error {
	var st TrackerState
	if err := json.Unmarshal(data, &st); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.totalOK += st.TotalOK
	t.totalErr += st.TotalErr
	t.totalBytes += st.TotalBytes
	t.totalTx += st.TotalTx
	t.totalRx += st.TotalRx
	for name, mc := range st.PerMethod {
		cur := t.byMethod[name]
		if cur == nil {
			c := mc
			c.Method = name
			t.byMethod[name] = &c
			continue
		}
		cur.OK += mc.OK
		cur.Err += mc.Err
		cur.FloodWait += mc.FloodWait
		cur.Bytes += mc.Bytes
	}
	return nil
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
	// gotd's generated raw calls pass BOXED decoders through Invoke
	// (*tg.UploadFileBox wrapping an UploadFileClass), never the bare
	// *tg.UploadFile — matching only the bare type meant download rx
	// bytes always counted as zero. Handle both shapes.
	switch v := output.(type) {
	case *tg.UploadFile:
		return len(v.Bytes)
	case *tg.UploadFileBox:
		if uf, ok := v.File.(*tg.UploadFile); ok {
			return len(uf.Bytes)
		}
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

// asFloodWait classifies err as a Telegram flood throttle and returns
// the mandated sleep. The AUTHORITATIVE probe is classifyFlood (gotd's typed
// error: tgerr FLOOD_WAIT / FLOOD_PREMIUM_WAIT) —
// gotd's own error type prints as "rpc error code 420: FLOOD_WAIT
// (23)" (type and argument SEPARATED), so no substring scan for
// "FLOOD_WAIT_N" ever matches it; the previous implementation relied
// on exactly that scan plus a FloodWait() interface *tgerr.Error
// doesn't have, which meant the tracker never counted a single real
// flood error and the adaptive controller's decrease path was dead
// code. The string scans below remain only as a fallback for wrapped
// or foreign error strings.
func asFloodWait(err error) (time.Duration, bool) {
	if err == nil {
		return 0, false
	}
	if _, d, ok := classifyFlood(err); ok {
		return d, true
	}
	// Fall back to a string match for older gotd versions or wrapped errors.
	// Match BOTH variants TG emits:
	//   - FLOOD_WAIT_N        (classic per-account throttle)
	//   - FLOOD_PREMIUM_WAIT_N (non-premium account hitting a tier limit)
	// Both signal "back off, sleep N seconds." Earlier code only caught
	// FLOOD_WAIT_, so the controller couldn't see free-account throttling
	// and would happily raise the cap into a wall.
	s := err.Error()
	if d, ok := scanFloodMarker(s, "FLOOD_WAIT_"); ok {
		return d, true
	}
	if d, ok := scanFloodMarker(s, "FLOOD_PREMIUM_WAIT_"); ok {
		return d, true
	}
	return 0, false
}

// scanFloodMarker searches s for marker followed by a decimal number
// of seconds. Returns (N seconds, true) on the first match.
func scanFloodMarker(s, marker string) (time.Duration, bool) {
	for i := 0; i+len(marker) < len(s); i++ {
		if s[i:i+len(marker)] != marker {
			continue
		}
		n := 0
		j := i + len(marker)
		for j < len(s) && s[j] >= '0' && s[j] <= '9' {
			n = n*10 + int(s[j]-'0')
			j++
		}
		if n > 0 {
			return time.Duration(n) * time.Second, true
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
