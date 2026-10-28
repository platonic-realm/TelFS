package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
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
}

type statusSync struct {
	Synced            bool  `json:"synced"`
	PendingUploads    int   `json:"pending_uploads"`
	PendingBytes      int64 `json:"pending_bytes"`
	JournalUnposted   int64 `json:"journal_unposted"`
	OldestPendingAgeS int64 `json:"oldest_pending_age_s"`
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
	Connections      int   `json:"connections"`
	RateCapBytesPerS int64 `json:"rate_cap_bytes_per_s"`
	RateBytesPerS    int64 `json:"rate_bytes_per_s"`
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
	// empty queue + a posted journal = the channel holds everything; a
	// format-1 filesystem also needs a snapshot covering the DB.
	st.Sync.Synced = d.readOnly || (open == 0 && unposted == 0 &&
		(d.format == 2 || (d.snapMgr != nil && d.snapMgr.Synced(ctx))))
	st.Staging = statusStaging{Dir: d.cfg.StagingDir(), PendingBytes: staged, CapBytes: capBytes, Blocked: blocked}
	if d.version != "" {
		// A version mount stages nothing; the key check and the use
		// counts in its copy are as old as the version.
		st.Staging = statusStaging{}
		st.KeyHeader, st.CryptoUses = nil, nil
	}
	snap := d.sess.Tracker().Snapshot()
	link := statusLink{State: "ok", LastSuccessUnix: unixOrZero(snap.LastSuccess), FloodWaitUntilUnix: unixOrZero(snap.FloodWaitUntil)}
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
	st.Upload = uploadStatus(d.cfg, d.rate())
	if !snap.LastError.IsZero() {
		st.LastError = &statusError{Text: snap.LastErrorMsg, Unix: snap.LastError.Unix()}
	}
	return st
}

// uploadStatus is the status's upload block: the configured [upload] pool
// size and byte-rate cap (checked when the mount started; a read-only
// mount opens no upload connections) and the measured upload rate.
func uploadStatus(cfg *config.Config, rate int64) *statusUpload {
	us, _ := cfg.UploadSettings()
	return &statusUpload{Connections: us.Connections, RateCapBytesPerS: us.MaxRate, RateBytesPerS: rate}
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

// syncReply is the body of /sync and /sync/wait.
type syncReply struct {
	Token           uint64 `json:"token,omitempty"`
	Synced          bool   `json:"synced"`
	Phase           string `json:"phase,omitempty"` // uploads | journal
	PendingUploads  int    `json:"pending_uploads"`
	JournalUnposted int64  `json:"journal_unposted"`
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
		writeJSON(w, http.StatusOK, syncReply{Synced: st.Sync.Synced, PendingUploads: st.Sync.PendingUploads, JournalUnposted: st.Sync.JournalUnposted})
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
	st.Sync.Synced = files == 0 && st.Sync.JournalUnposted == 0 && (st.Format == 2 || st.Snapshot.JournalOpsSince == 0)
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
