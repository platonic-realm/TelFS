package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"telfs/internal/chunk"
	"telfs/internal/config"
	"telfs/internal/control"
	"telfs/internal/fsck"
	"telfs/internal/meta"
	"telfs/internal/mountctl"
	"telfs/internal/snapshot"
	"telfs/internal/tg"
	"telfs/internal/wire"
)

// Exit codes of the v1 interface (spec v1 §3) beyond 0/1/2/3.
var (
	errNotSynced  = errors.New("not synced")                          // exit 4
	errNotMounted = errors.New("not mounted (no running daemon)")     // exit 5
	errTelegram   = errors.New("Telegram or the channel unreachable") // exit 6
	errIntegrity  = errors.New("integrity problem")                   // exit 7
)

// statusV1 is `telfs status --json`, schema 1 (spec v1 §3). Within schema 1
// fields are only ever added. Live-only fields are null when not mounted.
type statusV1 struct {
	Schema       int              `json:"schema"`
	TelfsVersion string           `json:"telfs_version"`
	Profile      string           `json:"profile"`
	FSUUID       string           `json:"fs_uuid"`
	Format       int              `json:"format"`
	Mounted      bool             `json:"mounted"`
	ReadOnly     *bool            `json:"read_only"`
	VersionMount *string          `json:"version_mount"`
	Mountpoint   *string          `json:"mountpoint"`
	PID          *int             `json:"pid"`
	LogPath      *string          `json:"log_path"`
	Link         *statusLink      `json:"link"`
	Sync         statusSync       `json:"sync"`
	Snapshot     statusSnapshot   `json:"snapshot"`
	Versions     statusVersions   `json:"versions"`
	Upload       *statusUpload    `json:"upload"`
	Staging      statusStaging    `json:"staging"`
	KeyHeader    *statusKeyHeader `json:"key_header"`
	CryptoUses   *statusUses      `json:"crypto_uses"`
	LastError    *statusError     `json:"last_error"`
}

type statusLink struct {
	State              string `json:"state"` // ok | busy | down | fault
	LastSuccessUnix    int64  `json:"last_success_unix"`
	FloodWaitUntilUnix int64  `json:"flood_wait_until_unix"`
	// HealthySinceUnix is the first successful RPC after the last error
	// (v1.1 I8); 0 while none has succeeded since.
	HealthySinceUnix int64 `json:"healthy_since_unix"`
}

type statusSync struct {
	Synced            bool  `json:"synced"`
	PendingUploads    int   `json:"pending_uploads"`
	PendingBytes      int64 `json:"pending_bytes"`
	JournalUnposted   int64 `json:"journal_unposted"`
	OldestPendingAgeS int64 `json:"oldest_pending_age_s"`
	// SnapshotDue says why a snapshot is owed before the channel holds
	// the filesystem (format 2: after `snapshot restore` whose own
	// snapshot failed, a rebuild from captions, a setting change).
	SnapshotDue string `json:"snapshot_due,omitempty"`
}

type statusSnapshot struct {
	LastMsgID       int   `json:"last_msg_id"`
	LastUnix        int64 `json:"last_unix"`
	JournalOpsSince int64 `json:"journal_ops_since"`
}

type statusVersions struct {
	Count  int            `json:"count"`
	Newest *statusVersion `json:"newest"`
}

type statusVersion struct {
	Name        string `json:"name"`
	CreatedUnix int64  `json:"created_unix"`
}

type statusUpload struct {
	Connections int `json:"connections"`
	// RateCapBytesPerS is the cap in effect — `telfs upload limit` changes
	// it while the daemon runs (v1.1 I3); 0 = no cap.
	RateCapBytesPerS int64 `json:"rate_cap_bytes_per_s"`
	RateBytesPerS    int64 `json:"rate_bytes_per_s"`
	// RateCapConfigBytesPerS is [upload] max_rate: the cap at the next start.
	RateCapConfigBytesPerS int64 `json:"rate_cap_config_bytes_per_s"`
}

type statusStaging struct {
	Dir          string `json:"dir"`
	PendingBytes int64  `json:"pending_bytes"`
	CapBytes     int64  `json:"cap_bytes"`
	Blocked      bool   `json:"blocked"`
}

type statusKeyHeader struct {
	PinnedOK    bool  `json:"pinned_ok"`
	CheckedUnix int64 `json:"checked_unix"`
}

type statusUses struct {
	Chunk   uint64 `json:"chunk"`
	Meta    uint64 `json:"meta"`
	Caption uint64 `json:"caption"`
	Warn    bool   `json:"warn"`
}

type statusError struct {
	Text string `json:"text"`
	Unix int64  `json:"unix"`
	// RecoveredUnix is when an RPC first succeeded after this error; null
	// while the link has not recovered from it (v1.1 I8: monitoring used
	// to read an 8-hour-old timeout as current).
	RecoveredUnix *int64 `json:"recovered_unix"`
}

// usesView turns per-key counts into the status block.
func usesView(u usesState) *statusUses {
	warn := u.Chunk >= 1<<31 || u.Meta >= 1<<31 || u.Caption >= 1<<31
	return &statusUses{Chunk: u.Chunk, Meta: u.Meta, Caption: u.Caption, Warn: warn}
}

// fillFromDB fills the fields every status shares, from the local DB.
func fillFromDB(ctx context.Context, st *statusV1, m *meta.Store) {
	st.FSUUID, _ = m.FSUUID(ctx)
	st.Format, _ = m.Format(ctx)
	if v, err := m.GetKV(ctx, snapshot.KVCurrentMsgID); err == nil {
		st.Snapshot.LastMsgID, _ = strconv.Atoi(string(v))
	}
	if v, err := m.GetKV(ctx, snapshot.KVLastSnapUnix); err == nil {
		st.Snapshot.LastUnix, _ = strconv.ParseInt(string(v), 10, 64)
	}
	if last, err := m.LastJournalSeq(ctx); err == nil {
		var snapSeq int64
		if v, err := m.GetKV(ctx, snapshot.KVCurrentJournalSeq); err == nil {
			snapSeq, _ = strconv.ParseInt(string(v), 10, 64)
		}
		if last > snapSeq {
			st.Snapshot.JournalOpsSince = last - snapSeq
		}
	}
	if vs, err := snapshot.LoadVersions(ctx, m); err == nil {
		st.Versions = versionsStatus(vs)
	}
	if kc, ok := loadKeyCheck(ctx, m); ok {
		st.KeyHeader = &statusKeyHeader{PinnedOK: kc.OK, CheckedUnix: kc.TS}
	}
	if b, err := m.GetKV(ctx, meta.KVUses); err == nil {
		var u usesState
		if json.Unmarshal(b, &u) == nil {
			st.CryptoUses = usesView(u)
		}
	}
}

// versionsStatus is the status's versions block: how many, and the
// newest by creation time.
func versionsStatus(vs []wire.AnchorVersion) statusVersions {
	out := statusVersions{Count: len(vs)}
	for i := range vs {
		if out.Newest == nil || vs[i].Created >= out.Newest.CreatedUnix {
			out.Newest = &statusVersion{Name: vs[i].Name, CreatedUnix: vs[i].Created}
		}
	}
	return out
}

// daemonCtl is the running mount's control-socket service (spec v1 §3).
type daemonCtl struct {
	cfg        *config.Config
	mountpoint string
	readOnly   bool
	format     int
	meta       *meta.Store
	sess       *tg.Session
	pool       *chunk.UploadPool
	poster     *snapshot.Poster
	snapMgr    *snapshot.Manager
	counters   *wire.Counters
	telegram   http.HandlerFunc // the existing /telegram JSON (web console)
	cache      http.HandlerFunc // the existing /cache JSON (web console)

	// Versions (format 2, live read-write mounts): the daemon's context
	// (a version post runs on it, not on a request's), the filesystem's
	// keys and the channel the version is posted to.
	ctx  context.Context
	keys *wire.Keys
	ch   snapshot.Channel
	// version is the version a version mount serves ("" for the live
	// filesystem's mount).
	version string

	// upLimit is the process's upload byte-rate cap (nil: none known).
	upLimit *tg.ByteLimiter

	rateMu  sync.Mutex
	samples []rateSample

	syncMu   sync.Mutex
	syncNext uint64
	syncToks map[uint64]*syncTok

	verMu   sync.Mutex
	verNext uint64
	verJobs map[uint64]*versionJob

	// fsckMu: one quick check at a time. msgs, when set, replaces sess
	// as the channel the quick check asks (tests).
	fsckMu sync.Mutex
	msgs   fsck.Messages
}

type rateSample struct {
	at time.Time
	tx int64
}

type syncTok struct {
	reg          uint64 // uploads registered before the sync was asked for
	journal      int64  // journal seq to be posted, fixed once those uploads landed
	journalFixed bool
	created      time.Time
}

// sampleRate records the upload byte counter every 10 s (for the rate in
// the status) until ctx ends.
func (d *daemonCtl) sampleRate(ctx context.Context) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		snap := d.sess.Tracker().Snapshot()
		d.rateMu.Lock()
		d.samples = append(d.samples, rateSample{time.Now(), snap.TotalBytesTx})
		if len(d.samples) > 7 {
			d.samples = d.samples[len(d.samples)-7:]
		}
		d.rateMu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (d *daemonCtl) rate() int64 {
	d.rateMu.Lock()
	defer d.rateMu.Unlock()
	if len(d.samples) < 2 {
		return 0
	}
	a, b := d.samples[0], d.samples[len(d.samples)-1]
	if dt := b.at.Sub(a.at).Seconds(); dt > 0 {
		return int64(float64(b.tx-a.tx) / dt)
	}
	return 0
}

// queueOpen reports open uploads, staged bytes and the oldest open upload.
func (d *daemonCtl) queueOpen() (open int, oldest time.Time, staged, capBytes int64, blocked bool) {
	if d.pool == nil {
		return
	}
	open, oldest = d.pool.OpenStats()
	if d.pool.Disk != nil {
		staged, capBytes, blocked = d.pool.Disk.Stats()
	}
	return
}

func (d *daemonCtl) status(ctx context.Context) statusV1 {
	now := time.Now()
	ro := d.readOnly
	mp := d.mountpoint
	pid := os.Getpid()
	st := statusV1{Schema: 1, TelfsVersion: version, Profile: d.cfg.ProfileName(), Mounted: true,
		ReadOnly: &ro, Mountpoint: &mp, PID: &pid}
	if lp := ownLogPath(); lp != "" {
		st.LogPath = &lp
	}
	if d.version != "" {
		v := d.version
		st.VersionMount = &v
	}
	fillFromDB(ctx, &st, d.meta)
	if d.counters != nil {
		st.CryptoUses = usesView(usesState{Chunk: d.counters.Chunk.Load(), Meta: d.counters.Meta.Load(), Caption: d.counters.Caption.Load()})
	}
	open, oldest, staged, capBytes, blocked := d.queueOpen()
	var unposted int64
	if d.version == "" { // a version's copy has no journal of its own to post
		unposted, _ = d.meta.UnpostedUpTo(ctx, 1<<62)
	}
	st.Sync = statusSync{PendingUploads: open, PendingBytes: staged, JournalUnposted: unposted}
	if !oldest.IsZero() {
		st.Sync.OldestPendingAgeS = int64(now.Sub(oldest).Seconds())
	}
	// Format 2 journals every change, chunk placements included, so an
	// empty queue + a posted journal = the channel holds everything,
	// unless a snapshot is owed; a format-1 filesystem also needs a
	// snapshot covering the DB.
	snapOK := false
	if d.format == 2 {
		if !d.readOnly {
			st.Sync.SnapshotDue = snapshot.Owed(ctx, d.meta)
		}
		snapOK = st.Sync.SnapshotDue == ""
	} else {
		snapOK = d.snapMgr != nil && d.snapMgr.Synced(ctx)
	}
	st.Sync.Synced = d.readOnly || (open == 0 && unposted == 0 && snapOK)
	st.Staging = statusStaging{Dir: d.cfg.StagingDir(), PendingBytes: staged, CapBytes: capBytes, Blocked: blocked}
	if d.version != "" {
		// A version mount stages nothing; the key check and the use
		// counts in its copy are as old as the version.
		st.Staging = statusStaging{}
		st.KeyHeader, st.CryptoUses = nil, nil
	}
	snap := d.sess.Tracker().Snapshot()
	link := statusLink{State: "ok", LastSuccessUnix: unixOrZero(snap.LastSuccess), FloodWaitUntilUnix: unixOrZero(snap.FloodWaitUntil),
		HealthySinceUnix: unixOrZero(snap.HealthySince)}
	switch {
	case !d.sess.Connected():
		link.State = "down"
	case !snap.LastError.IsZero() && now.Sub(snap.LastError) < 30*time.Second:
		link.State = "fault"
	case snap.FloodWaitUntil.After(now),
		open > 0 && !snap.LastSuccess.IsZero() && now.Sub(snap.LastSuccess) > time.Minute:
		link.State = "busy"
	}
	st.Link = &link
	st.Upload = uploadStatus(d.cfg, d.rate(), d.upLimit)
	if !snap.LastError.IsZero() {
		st.LastError = lastErrorStatus(snap)
	}
	return st
}

// lastErrorStatus is the status's last_error: the tracker's last error and,
// once an RPC has succeeded after it, when.
func lastErrorStatus(snap tg.Snapshot) *statusError {
	e := &statusError{Text: snap.LastErrorMsg, Unix: snap.LastError.Unix()}
	if snap.HealthySince.After(snap.LastError) {
		r := snap.HealthySince.Unix()
		e.RecoveredUnix = &r
	}
	return e
}

// uploadStatus is the status's upload block: the configured [upload] pool
// size (a read-only mount opens no upload connections), the byte-rate cap
// in effect (live, when the daemon's limiter is known) and the configured
// one, and the measured upload rate.
func uploadStatus(cfg *config.Config, rate int64, live *tg.ByteLimiter) *statusUpload {
	us, _ := cfg.UploadSettings()
	st := &statusUpload{Connections: us.Connections, RateCapBytesPerS: us.MaxRate, RateBytesPerS: rate, RateCapConfigBytesPerS: us.MaxRate}
	if live != nil {
		st.RateCapBytesPerS = live.Rate()
	}
	return st
}

// uploadLimitReply is GET/POST /upload/limit.
type uploadLimitReply struct {
	RateCapBytesPerS       int64  `json:"rate_cap_bytes_per_s"`
	RateCapConfigBytesPerS int64  `json:"rate_cap_config_bytes_per_s"`
	Error                  string `json:"error,omitempty"`
}

// uploadLimit is GET /upload/limit (the cap in effect) and POST
// /upload/limit?rate=RATE (change it until the daemon stops — [upload]
// max_rate is the cap again at the next start). RATE as in config.toml:
// "3MB/s", "800KiB/s", "0" = no cap.
func (d *daemonCtl) uploadLimit(w http.ResponseWriter, r *http.Request) {
	us, _ := d.cfg.UploadSettings()
	rep := uploadLimitReply{RateCapConfigBytesPerS: us.MaxRate}
	if d.upLimit == nil || d.readOnly {
		rep.Error = "this mount uploads nothing (read-only)"
		writeJSON(w, http.StatusConflict, rep)
		return
	}
	if r.Method == http.MethodPost {
		n, err := config.ParseByteRate(r.URL.Query().Get("rate"))
		if err == nil && n != 0 && n < config.MinUploadMaxRate {
			err = fmt.Errorf("%q: below the 64 KiB/s floor (\"0\" = no cap)", r.URL.Query().Get("rate"))
		}
		if err != nil {
			rep.RateCapBytesPerS = d.upLimit.Rate()
			rep.Error = "upload limit: " + err.Error()
			writeJSON(w, http.StatusBadRequest, rep)
			return
		}
		d.upLimit.SetRate(n)
		fmt.Fprintf(os.Stderr, "[upload] byte-rate cap set to %s over the control socket (until the daemon stops)\n", rateText(n))
	}
	rep.RateCapBytesPerS = d.upLimit.Rate()
	writeJSON(w, http.StatusOK, rep)
}

// rateText renders a cap: "5.0 MB/s", or "no cap".
func rateText(n int64) string {
	if n <= 0 {
		return "no cap"
	}
	return fmt.Sprintf("%.1f MB/s", float64(n)/1e6)
}

// cmdUpload is `telfs upload limit [RATE]`: show or change the running
// daemon's upload cap (no secret; exit 5 when not mounted).
func cmdUpload(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "limit" || len(args) > 2 {
		return errors.New("usage: telfs upload limit [RATE]   (RATE e.g. 3MB/s, 800KiB/s, 0 = no cap; until the daemon stops)")
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	method, path := http.MethodGet, "/upload/limit"
	if len(args) == 2 {
		method, path = http.MethodPost, "/upload/limit?rate="+url.QueryEscape(args[1])
	}
	req, err := http.NewRequestWithContext(ctx, method, control.BaseURL+path, nil)
	if err != nil {
		return err
	}
	resp, err := ctlClient(cfg, 10*time.Second).Do(req)
	if err != nil {
		return fmt.Errorf("%w: profile %s: %v", errNotMounted, cfg.ProfileName(), err)
	}
	defer resp.Body.Close()
	var rep uploadLimitReply
	if err := json.NewDecoder(resp.Body).Decode(&rep); err != nil {
		return fmt.Errorf("upload limit: %w (an older daemon without the route? restart it with this binary)", err)
	}
	if rep.Error != "" {
		return errors.New(rep.Error)
	}
	fmt.Printf("upload cap: %s (config.toml [upload] max_rate: %s)\n", rateText(rep.RateCapBytesPerS), rateText(rep.RateCapConfigBytesPerS))
	return nil
}

// ownLogPath is where this daemon's stdout goes, when that is a regular
// file (the web console shows adopted mounts' logs from it).
func ownLogPath() string {
	if target, err := os.Readlink("/proc/self/fd/1"); err == nil && strings.HasPrefix(target, "/") && !strings.HasPrefix(target, "/dev/") {
		if st, err := os.Stat(target); err == nil && st.Mode().IsRegular() {
			return target
		}
	}
	return ""
}

func unixOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

// openUploads is the body of GET /uploads/open.
type openUploads struct {
	Inos []int64 `json:"inos"`
}

// syncReply is the body of /sync and /sync/wait.
type syncReply struct {
	Token           uint64 `json:"token,omitempty"`
	Synced          bool   `json:"synced"`
	Phase           string `json:"phase,omitempty"` // uploads | journal | snapshot
	PendingUploads  int    `json:"pending_uploads"`
	JournalUnposted int64  `json:"journal_unposted"`
	SnapshotDue     string `json:"snapshot_due,omitempty"`
	Error           string `json:"error,omitempty"`
}

// syncCheck advances a sync token: first every upload registered before it
// must have landed, then the journal — up to the seq at that moment, which
// includes those uploads' chunk placements — must be posted.
func (d *daemonCtl) syncCheck(ctx context.Context, t *syncTok) syncReply {
	if d.readOnly {
		return syncReply{Synced: true}
	}
	if d.pool != nil {
		if open := d.pool.OpenUpTo(t.reg); open > 0 {
			return syncReply{Phase: "uploads", PendingUploads: open}
		}
	}
	d.syncMu.Lock()
	if !t.journalFixed {
		t.journal, _ = d.meta.LastJournalSeq(ctx)
		t.journalFixed = true
	}
	target := t.journal
	d.syncMu.Unlock()
	n, err := d.meta.UnpostedUpTo(ctx, target)
	if err != nil {
		return syncReply{Error: err.Error()}
	}
	if n > 0 {
		if d.poster != nil {
			d.poster.Kick()
		}
		return syncReply{Phase: "journal", JournalUnposted: n}
	}
	// A snapshot owed (format 2): the channel's snapshot chain does not
	// describe this DB until the policy loop has posted one.
	if d.format == 2 {
		if why := snapshot.Owed(ctx, d.meta); why != "" {
			return syncReply{Phase: "snapshot", SnapshotDue: why}
		}
	}
	return syncReply{Synced: true}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (d *daemonCtl) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, d.status(r.Context()))
	})
	// GET /sync: synced right now (everything, not just a barrier).
	mux.HandleFunc("GET /sync", func(w http.ResponseWriter, r *http.Request) {
		st := d.status(r.Context())
		writeJSON(w, http.StatusOK, syncReply{Synced: st.Sync.Synced, PendingUploads: st.Sync.PendingUploads, JournalUnposted: st.Sync.JournalUnposted, SnapshotDue: st.Sync.SnapshotDue})
	})
	mux.HandleFunc("POST /sync/begin", func(w http.ResponseWriter, r *http.Request) {
		if d.format != 2 {
			writeJSON(w, http.StatusConflict, syncReply{Error: "telfs sync needs a format-2 filesystem (format 1 journals no chunk placements)"})
			return
		}
		var reg uint64
		if d.pool != nil {
			reg = d.pool.LastRegistered()
		}
		d.syncMu.Lock()
		if d.syncToks == nil {
			d.syncToks = map[uint64]*syncTok{}
		}
		for id, t := range d.syncToks {
			if time.Since(t.created) > time.Hour {
				delete(d.syncToks, id)
			}
		}
		d.syncNext++
		id := d.syncNext
		d.syncToks[id] = &syncTok{reg: reg, created: time.Now()}
		d.syncMu.Unlock()
		if d.poster != nil {
			d.poster.Kick()
		}
		writeJSON(w, http.StatusOK, syncReply{Token: id})
	})
	// GET /sync/wait?token=N&max=30s: long-poll until synced or max.
	mux.HandleFunc("GET /sync/wait", func(w http.ResponseWriter, r *http.Request) {
		id, _ := strconv.ParseUint(r.URL.Query().Get("token"), 10, 64)
		maxWait, err := time.ParseDuration(r.URL.Query().Get("max"))
		if err != nil || maxWait <= 0 || maxWait > 5*time.Minute {
			maxWait = 30 * time.Second
		}
		d.syncMu.Lock()
		t := d.syncToks[id]
		d.syncMu.Unlock()
		if t == nil {
			writeJSON(w, http.StatusNotFound, syncReply{Error: "unknown or expired sync token"})
			return
		}
		deadline := time.Now().Add(maxWait)
		for {
			rep := d.syncCheck(r.Context(), t)
			rep.Token = id
			if rep.Synced || rep.Error != "" || time.Now().After(deadline) {
				if rep.Synced {
					d.syncMu.Lock()
					delete(d.syncToks, id)
					d.syncMu.Unlock()
				}
				writeJSON(w, http.StatusOK, rep)
				return
			}
			select {
			case <-r.Context().Done():
				return
			case <-time.After(250 * time.Millisecond):
			}
		}
	})
	// POST /stop: orderly shutdown, exactly as SIGTERM.
	mux.HandleFunc("POST /stop", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusAccepted, map[string]int{"pid": os.Getpid()})
		go func() {
			time.Sleep(100 * time.Millisecond)
			_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		}()
	})
	// GET /uploads/open: the inodes with uploads in flight (`telfs
	// restore` of the live filesystem: their data in the DB may not be
	// their current content).
	mux.HandleFunc("GET /upload/limit", d.uploadLimit)
	mux.HandleFunc("POST /upload/limit", d.uploadLimit)
	mux.HandleFunc("GET /uploads/open", func(w http.ResponseWriter, _ *http.Request) {
		rep := openUploads{Inos: []int64{}}
		if d.pool != nil && d.pool.Pending != nil {
			rep.Inos = append(rep.Inos, d.pool.Pending.Inos()...)
		}
		writeJSON(w, http.StatusOK, rep)
	})
	if d.version == "" {
		d.versionsRoutes(mux)
		mux.HandleFunc("POST /fsck/quick", d.fsckQuick)
	}
	if d.telegram != nil {
		mux.HandleFunc("GET /telegram", d.telegram)
	}
	if d.cache != nil {
		mux.HandleFunc("GET /cache", d.cache)
	}
	return mux
}

// ctlClient is the control-socket client of the active profile.
func ctlClient(cfg *config.Config, timeout time.Duration) *http.Client {
	return control.Client(control.Path(cfg.DataDir), timeout)
}

// offlineStatus is the schema-1 status from local state alone — no daemon,
// no secret (the monitoring unit runs without one).
func offlineStatus(ctx context.Context, cfg *config.Config) (statusV1, error) {
	st := statusV1{Schema: 1, TelfsVersion: version, Profile: cfg.ProfileName()}
	if _, pid, mp, ok := profileMount(cfg.ProfileName()); ok {
		st.Mounted = true // mounted, but its daemon did not answer
		if pid > 0 {
			st.PID = &pid
		}
		st.Mountpoint = &mp
	}
	stageMax, _, _ := cfg.StagingLimits()
	st.Staging = statusStaging{Dir: cfg.StagingDir(), CapBytes: stageMax}
	files, bytes := 0, int64(0)
	if ents, err := os.ReadDir(cfg.StagingDir()); err == nil {
		for _, e := range ents {
			if fi, err := e.Info(); err == nil && !e.IsDir() {
				files++
				bytes += fi.Size()
			}
		}
	}
	st.Staging.PendingBytes = bytes
	st.Sync.PendingUploads, st.Sync.PendingBytes = files, bytes
	if _, err := os.Stat(cfg.DBPath()); err != nil {
		return st, nil // no local DB: nothing more to say
	}
	m, err := meta.Open(cfg.DBPath())
	if err != nil {
		return st, err
	}
	defer m.Close()
	fillFromDB(ctx, &st, m)
	st.Sync.JournalUnposted, _ = m.UnpostedUpTo(ctx, 1<<62)
	snapOK := st.Snapshot.JournalOpsSince == 0
	if st.Format == 2 {
		st.Sync.SnapshotDue = snapshot.Owed(ctx, m)
		snapOK = st.Sync.SnapshotDue == ""
	}
	st.Sync.Synced = files == 0 && st.Sync.JournalUnposted == 0 && snapOK
	return st, nil
}

// profileMount finds the live mount of a profile (pid 0 when the daemon
// could not be found); version mounts are not it.
func profileMount(profile string) (mountctl.Mount, int, string, bool) {
	for _, m := range mountctl.ScanMounts() {
		if mountctl.VersionOf(m) != "" {
			continue
		}
		if p, ok := mountctl.ProfileOf(m); ok && p == profile {
			pid, _ := mountctl.FindDaemonPid(m.Point)
			return m, pid, m.Point, true
		}
	}
	return mountctl.Mount{}, 0, "", false
}

// statusJSON prints `telfs status --json`: the daemon's view when it runs,
// local state otherwise. Exit 0 either way (1 only if the profile can't be
// read). With version (`--version NAME`): the status of that version's
// mount, from its daemon — exit 5 when it is not mounted.
func statusJSON(ctx context.Context, cfg *config.Config, version string) error {
	var st statusV1
	sock := control.Path(cfg.DataDir)
	if version != "" {
		if err := snapshot.ValidateVersionName(version); err != nil {
			return err
		}
		sock = control.VersionPath(cfg.DataDir, version)
	}
	resp, err := control.Client(sock, 5*time.Second).Get(control.BaseURL + "/status")
	if err == nil {
		defer resp.Body.Close()
		err = json.NewDecoder(resp.Body).Decode(&st)
	}
	if err != nil && version != "" {
		return fmt.Errorf("%w: version %s of profile %s: %v", errNotMounted, version, cfg.ProfileName(), err)
	}
	if err != nil {
		if st, err = offlineStatus(ctx, cfg); err != nil {
			return err
		}
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(st)
}

// cmdSync is `telfs sync [--wait] [--timeout D]` (spec v1 §3).
func cmdSync(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	wait := fs.Bool("wait", false, "block until everything written through the mount before this call is on the channel")
	timeout := fs.Duration("timeout", 0, "with --wait: give up after this long (exit 4); 0 = as long as it takes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	get := func(c *http.Client, method, path string) (syncReply, error) {
		var rep syncReply
		req, err := http.NewRequestWithContext(ctx, method, control.BaseURL+path, nil)
		if err != nil {
			return rep, err
		}
		resp, err := c.Do(req)
		if err != nil {
			return rep, fmt.Errorf("%w: profile %s: %v", errNotMounted, cfg.ProfileName(), err)
		}
		defer resp.Body.Close()
		if err := json.NewDecoder(resp.Body).Decode(&rep); err != nil {
			return rep, err
		}
		if rep.Error != "" {
			return rep, errors.New(rep.Error)
		}
		return rep, nil
	}
	short := ctlClient(cfg, 10*time.Second)
	if !*wait {
		rep, err := get(short, http.MethodGet, "/sync")
		if err != nil {
			return err
		}
		if !rep.Synced {
			if rep.SnapshotDue != "" {
				return fmt.Errorf("%w: %d upload(s) pending, %d journal op(s) unposted; %s", errNotSynced, rep.PendingUploads, rep.JournalUnposted, rep.SnapshotDue)
			}
			return fmt.Errorf("%w: %d upload(s) pending, %d journal op(s) unposted", errNotSynced, rep.PendingUploads, rep.JournalUnposted)
		}
		fmt.Println("synced")
		return nil
	}
	begin, err := get(short, http.MethodPost, "/sync/begin")
	if err != nil {
		return err
	}
	var deadline time.Time
	if *timeout > 0 {
		deadline = time.Now().Add(*timeout)
	}
	long := ctlClient(cfg, 0)
	for {
		poll := 30 * time.Second
		if !deadline.IsZero() {
			left := time.Until(deadline)
			if left <= 0 {
				return fmt.Errorf("%w after %s", errNotSynced, *timeout)
			}
			if left < poll {
				poll = left
			}
		}
		rep, err := get(long, http.MethodGet, fmt.Sprintf("/sync/wait?token=%d&max=%s", begin.Token, poll))
		if err != nil {
			return err
		}
		if rep.Synced {
			fmt.Println("synced")
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}
