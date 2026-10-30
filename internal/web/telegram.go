package web

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	neturl "net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"telfs/internal/config"
	"telfs/internal/control"
	"telfs/internal/meta"
	"telfs/internal/snapshot"
	"telfs/internal/tg"
)

// tgState mirrors the JSON shape served by the mount daemon's
// /telegram endpoint (cmd/telfs/main.go:telegramJSONHandler). The
// web process runs separately from the mount daemon, so we have to
// fetch over HTTP — there's no shared memory between the two.
type tgState struct {
	ChannelID             int64       `json:"channel_id"`
	ChannelTitle          string      `json:"channel_title"`
	InFlight              int         `json:"in_flight"`
	TotalOK               int64       `json:"total_ok"`
	TotalErr              int64       `json:"total_err"`
	TotalBytes            int64       `json:"total_bytes"`
	TotalBytesTx          int64       `json:"total_bytes_tx"`
	TotalBytesRx          int64       `json:"total_bytes_rx"`
	LastSuccessUnix       int64       `json:"last_success_unix"`
	LastSuccessAgeSeconds int64       `json:"last_success_age_seconds"`
	LastErrorUnix         int64       `json:"last_error_unix"`
	LastErrorAgeSeconds   int64       `json:"last_error_age_seconds"`
	LastErrorMessage      string      `json:"last_error_message"`
	PerMethod             []tgMethod  `json:"per_method"`
	Requests              []tgRequest `json:"requests"`
	Queue                 tgQueue     `json:"queue"`
	Sync                  *tgSync     `json:"sync,omitempty"`
	Adaptive              *tgAdaptive `json:"adaptive,omitempty"`
}

// tgAdaptive mirrors the daemon's AdaptiveSnapshot — drives the
// "auto N req/min, raised 2 min ago" rendering on the Telegram tab.
type tgAdaptive struct {
	Manual              bool   `json:"manual"`
	UploadPinned        bool   `json:"upload_pinned"`
	UploadEffective     int    `json:"upload_effective"`
	UploadAdaptiveLevel int    `json:"upload_adaptive_level"`
	DownloadPinned      bool   `json:"download_pinned"`
	DownloadEffective   int    `json:"download_effective"`
	LastFloodAgoSec     int64  `json:"last_flood_ago_sec"`
	LastAdjustAgoSec    int64  `json:"last_adjust_ago_sec"`
	LastAction          string `json:"last_action"`
}

// tgQueue is the upload-side backlog the daemon surfaces. There's no
// download-side queue to track: the chunk Reader fetches synchronously
// on cache miss; there's no separate "download backlog" because the
// kernel's FUSE driver is the one queuing reads, and we serve them
// inline. So the queue panel is exclusively an upload-side view.
type tgQueue struct {
	// PackerChunks + PackerBytes: chunks waiting in the in-memory
	// packer buffer for the next flush. PackerTargetBytes is the
	// trigger threshold; bytes >= target wakes the flusher.
	PackerChunks      int   `json:"packer_chunks"`
	PackerBytes       int64 `json:"packer_bytes"`
	PackerTargetBytes int64 `json:"packer_target_bytes"`
	// PendingWiresCount + Bytes: wires durably staged on disk in the
	// staging directory ([staging] dir, default cache/pending/). Each
	// is a chunk written through the mount whose upload has not landed
	// yet. Survives daemon crash.
	PendingWiresCount int   `json:"pending_wires_count"`
	PendingWiresBytes int64 `json:"pending_wires_bytes"`
	// The disk-backed staging queue (spec v1, row 9): its directory, the
	// staged bytes, the cap ([staging] max_bytes; 0 = no cap, or a
	// daemon without the queue), whether a writer is waiting on the cap
	// or the free-space floor, and the staged direct uploads waiting for
	// / holding an upload worker. Daemons older than v1 send none of it.
	StagingDir          string `json:"staging_dir"`
	StagingBytes        int64  `json:"staging_bytes"`
	StagingCapBytes     int64  `json:"staging_cap_bytes"`
	StagingBlocked      bool   `json:"staging_blocked"`
	StagedDirectWaiting int64  `json:"staged_direct_waiting"`
	StagedDirectRunning int32  `json:"staged_direct_running"`
	// PoolInFlight / PoolCap: how many concurrency slots in the
	// UploadPool are currently held vs the cap.
	PoolInFlight int `json:"pool_in_flight"`
	PoolCap      int `json:"pool_cap"`
	// UploadsTotal: uploads anywhere between dispatch and chunk_map
	// commit (includes goroutines queued for a sem slot, which
	// PoolInFlight misses under async dispatch).
	UploadsTotal int64 `json:"uploads_total"`
	// JournalUnposted: rows in the local journal table not yet
	// posted to the channel.
	JournalUnposted int `json:"journal_unposted"`
}

// stagingWarnRatio: above this share of the staging cap the link pill
// turns yellow (writes do not wait yet, but uploads are behind).
const stagingWarnRatio = 0.8

// StagingLevel classifies the staging queue for the pill and the meter:
// "err" while writes wait for uploads (the cap or the free-space floor is
// reached), "warn" above 80 % of the cap, "" otherwise. A large backlog
// below the cap is normal during a first backup, so the number of staged
// wires plays no part.
func (q tgQueue) StagingLevel() string {
	switch {
	case q.StagingBlocked:
		return "err"
	case q.StagingCapBytes > 0 && float64(q.StagingBytes) > stagingWarnRatio*float64(q.StagingCapBytes):
		return "warn"
	}
	return ""
}

// StagingPct is the staged bytes as a share of the cap, 0..100 (0 when
// there is no cap).
func (q tgQueue) StagingPct() float64 {
	if q.StagingCapBytes <= 0 {
		return 0
	}
	p := float64(q.StagingBytes) / float64(q.StagingCapBytes) * 100
	return max(0, min(p, 100))
}

// stagingSummary is "N wire(s), X of the Y cap" for tooltips.
func (q tgQueue) stagingSummary() string {
	b := q.StagingBytes
	if b == 0 {
		b = q.PendingWiresBytes // a daemon without the queue fields
	}
	s := fmt.Sprintf("%d wire(s), %s staged", q.PendingWiresCount, humanBytesTpl(b))
	if q.StagingCapBytes > 0 {
		s += fmt.Sprintf(" of the %s cap (%.0f %%)", humanBytesTpl(q.StagingCapBytes), q.StagingPct())
	}
	return s
}

// tgSync is the daemon's channel-sync verdict. ChannelComplete means
// the channel alone can recover the FS on fresh hardware: upload
// queue drained AND a snapshot covering the current DB posted
// (chunk_map isn't journaled, so queue-empty alone leaves chunk
// mappings local-only until a snapshot lands).
type tgSync struct {
	QueueEmpty      bool `json:"queue_empty"`
	SnapshotClean   bool `json:"snapshot_clean"`
	ChannelComplete bool `json:"channel_complete"`
}

type tgMethod struct {
	Method    string `json:"method"`
	OK        int64  `json:"ok"`
	Err       int64  `json:"err"`
	FloodWait int64  `json:"flood_wait"`
	Bytes     int64  `json:"bytes"`
}

type tgRequest struct {
	ID          int64  `json:"id"`
	Method      string `json:"method"`
	StartedUnix int64  `json:"started_unix"`
	DurationMS  int64  `json:"duration_ms"`
	// Bytes is int64 (not int) so the {{humanBytes .Bytes}} template
	// call compiles — Go's html/template enforces the function-arg
	// type strictly, and a mismatch crashes the template mid-row.
	// We previously shipped this as int, which caused the /telegram
	// page to be cut off before the rate-limit form was rendered.
	Bytes            int64  `json:"bytes"`
	InFlight         bool   `json:"in_flight"`
	Err              string `json:"err"`
	FloodWaitSeconds int64  `json:"flood_wait_seconds"`
}

type tgPageData struct {
	pageBase
	// Profile is the profile this page shows (?profile=, default the
	// selected one); Profiles lists every profile for the tabs.
	Profile  string
	Profiles []string
	// Upload is the daemon's upload block (status --json): rate, cap in
	// effect and configured cap (full page, daemon online).
	Upload       *uploadView
	Source       string // URL we fetched from (for the help text)
	Online       bool   // did the fetch succeed
	FetchError   string // explanation if Online is false
	State        tgState
	StallSeconds int64 // derived: how long since last success
	NowUnix      int64

	// Persisted: the daemon is offline but lifetime totals were
	// loaded from the profile's meta DB (the daemon lands them there
	// every minute + at shutdown), so the numbers stay visible
	// instead of vanishing. PersistedAgeSec is how stale they are.
	Persisted       bool
	PersistedAgeSec int64

	// RateLimitPerMin is the per-FS UPLOAD requests/min cap, read
	// fresh from meta_kv on every request so CLI edits show up
	// immediately. Zero means unlimited. -1 signals "couldn't
	// read meta_kv" so the template can flag that.
	RateLimitPerMin int
	// DownloadRateLimitPerMin mirrors the upload cap above but for
	// upload.getFile RPCs only. -1 on read failure, 0 = unlimited,
	// >0 = explicit cap.
	DownloadRateLimitPerMin int

	// Channel-sync lamp, computed by classifySync from the daemon's
	// sync verdict when online, or from local profile state when
	// offline (journal drained + no staged wires + journal seq
	// covered by the last snapshot ⇒ the channel was complete as of
	// the last unmount). Rendered in the masthead next to the link
	// pill and as a row on the /telegram tab.
	SyncKind  string // pill class: ok | warn | muted
	SyncLabel string // terse lamp caption
	SyncTip   string // hover/title explanation

	// Offline verdict inputs (daemon unreachable): whether local
	// state was readable at all, and what it said.
	OfflineSyncKnown    bool
	OfflineSyncComplete bool
}

// classifySync fills the SyncKind/Label/Tip lamp from whichever
// evidence is available. Ordering: online daemon verdicts first
// (live truth), then the offline approximation, then unknown.
func classifySync(d *tgPageData) {
	switch {
	case d.Online && d.State.Sync == nil:
		d.SyncKind, d.SyncLabel = "muted", "sync n/a"
		d.SyncTip = "Daemon predates the sync report — restart the mount with the current binary."
	case d.Online && d.State.Sync.ChannelComplete:
		d.SyncKind, d.SyncLabel = "ok", "synced"
		d.SyncTip = "Channel holds everything — recoverable without this machine."
	case d.Online && d.State.Sync.QueueEmpty:
		d.SyncKind, d.SyncLabel = "warn", "snap due"
		d.SyncTip = "Uploads drained; waiting for a snapshot to cover the latest chunk maps (automatic within ~30 s; unmount also takes one)."
	case d.Online:
		q := d.State.Queue
		d.SyncKind, d.SyncLabel = "warn", "uploading"
		d.SyncTip = fmt.Sprintf("Backlog: %d upload(s) in flight, %d wire(s) staged, %d packer chunk(s), %d journal op(s) unposted.",
			q.UploadsTotal, q.PendingWiresCount, q.PackerChunks, q.JournalUnposted)
	case d.OfflineSyncKnown && d.OfflineSyncComplete:
		d.SyncKind, d.SyncLabel = "ok", "synced"
		d.SyncTip = "As of the last unmount: queue drained and the snapshot covers the journal. Channel is a complete copy."
	case d.OfflineSyncKnown:
		d.SyncKind, d.SyncLabel = "warn", "not synced"
		d.SyncTip = "Local state holds changes the channel may lack (staged wires, unposted journal, or an uncovered snapshot window). Mount to let it drain."
	default:
		d.SyncKind, d.SyncLabel = "muted", "sync ?"
		d.SyncTip = "No daemon and no readable local state — channel-sync unknown."
	}
}

// loadOfflineSync computes the daemon-offline sync approximation from
// the profile's local state. Channel-complete offline ⇔ no unposted
// journal rows AND no staged wires AND the journal seq is covered by
// the last posted snapshot (an empty journal counts as covered: the
// snapshot cadence prunes posted rows ≤ its seq, so a fully-drained,
// fully-snapshotted FS ends with an empty table). chunk_map rows are
// only ever written with the daemon up, so "clean at last shutdown"
// carries over to now.
func loadOfflineSync(ctx context.Context, cfg *config.Config, data *tgPageData) {
	mstore, err := openMetaRead(cfg)
	if err != nil {
		return
	}
	defer mstore.Close()
	st, err := mstore.Stats(ctx)
	if err != nil {
		return
	}
	seq, err := mstore.LastJournalSeq(ctx)
	if err != nil {
		return
	}
	var snapSeq int64 = -1
	if raw, err := mstore.GetKV(ctx, snapshot.KVCurrentJournalSeq); err == nil {
		if v, perr := strconv.ParseInt(string(raw), 10, 64); perr == nil {
			snapSeq = v
		}
	}
	pendingClean := true
	pendingDir := cfg.StagingDir()
	if ents, err := os.ReadDir(pendingDir); err == nil {
		for _, e := range ents {
			if !e.IsDir() {
				pendingClean = false
				break
			}
		}
	}
	data.OfflineSyncKnown = true
	data.OfflineSyncComplete = st.JournalUnposted == 0 && pendingClean &&
		snapSeq >= 0 && (seq == 0 || seq == snapSeq)
	if f, err := mstore.Format(ctx); err == nil && f == 2 {
		// Format 2 journals every change, chunk placements included, and
		// snapshots only rarely: the posted journal is the whole story.
		data.OfflineSyncComplete = st.JournalUnposted == 0 && pendingClean
	}
}

// telegramPage renders the connection-state + request-log view of one
// profile (?profile=NAME, default the selected one). Data comes from that
// profile's mount daemon, over its control socket (a `source=` URL reads a
// daemon started with --metrics-addr instead).
func (s *Server) telegramPage(w http.ResponseWriter, r *http.Request) {
	data := s.fetchTelegramState(w, r)
	// Parse the partial alongside the page so {{template
	// "telegram_panel" .}} in telegram.html resolves. The partial
	// defines a uniquely-named block (NOT "content"), so this
	// doesn't collide with telegram.html's own content define.
	s.renderTemplateWith(w, "telegram.html", data, "templates/partials/telegram.html")
}

// telegramPartial is the HTMX-polled inner fragment. Same data
// fetch as telegramPage but uses fetchTelegramStatePartial which
// SKIPS basePage — so the 2s poll doesn't rotate the CSRF cookie
// (which would break the still-rendered Apply form) and doesn't
// CONSUME the flash message (which would be silently eaten before
// the user sees it).
func (s *Server) telegramPartial(w http.ResponseWriter, r *http.Request) {
	data := s.fetchTelegramStatePartial(r)
	s.renderPartialBlock(w, "partials/telegram.html", "telegram_panel", data)
}

// fetchTelegramStatePartial is the htmx-poll variant: same network
// fetch + parse as fetchTelegramState, but skips basePage so the
// poll doesn't consume the flash or rotate CSRF. The partial
// template doesn't reference any pageBase fields anyway.
func (s *Server) fetchTelegramStatePartial(r *http.Request) tgPageData {
	cfg, name, err := requestProfile(r)
	data := tgPageData{Profile: name, NowUnix: time.Now().Unix()}
	fillTGState(r.Context(), &data, cfg, err, sourceFor(r))
	return data
}

// sourceFor is where the daemon state comes from: the profile's control
// socket, or a loopback `source=` URL (a daemon with --metrics-addr).
func sourceFor(r *http.Request) string {
	if v, ok := sanitizeSourceURL(r.URL.Query().Get("source")); ok {
		return v
	}
	return controlSource
}

// fillTGState fetches one profile's daemon state, falls back to its local
// state when no daemon answers, and sets the sync lamp.
func fillTGState(ctx context.Context, data *tgPageData, cfg *config.Config, cfgErr error, url string) {
	data.Source = url
	if cfgErr != nil {
		data.FetchError = cfgErr.Error()
		classifySync(data)
		return
	}
	populateTGState(data, cfg, url)
	if !data.Online {
		loadPersistedTGStats(ctx, cfg, data)
		loadOfflineSync(ctx, cfg, data)
	}
	classifySync(data)
}

// requestProfile is the profile a page shows: ?profile=NAME (or the form
// field) when it names an existing profile, else the selected one.
// Monitoring is per profile; the selection is only the default. A missing
// profile is an error, never created (LoadFromDir would make its dir).
func requestProfile(r *http.Request) (*config.Config, string, error) {
	name := strings.TrimSpace(r.FormValue("profile"))
	if name == "" {
		name = profileOf(r)
	}
	if name == "" {
		return nil, "", errNoProfile
	}
	if err := config.ValidateProfileName(name); err != nil {
		return nil, name, err
	}
	dir, err := config.ProfileDir(name)
	if err != nil {
		return nil, name, err
	}
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		return nil, name, fmt.Errorf("no profile %q", name)
	}
	cfg, err := config.LoadFromDir(dir)
	return cfg, name, err
}

// loadPersistedTGStats fills the totals from the profile DB's
// persisted TrackerState when the daemon is unreachable — the stats
// stay consistent across daemon restarts and outages instead of
// disappearing with the process.
func loadPersistedTGStats(ctx context.Context, cfg *config.Config, data *tgPageData) {
	mstore, err := openMetaRead(cfg)
	if err != nil {
		return
	}
	defer mstore.Close()
	b, err := mstore.GetKV(ctx, tg.KVLifetimeStats)
	if err != nil {
		return
	}
	var st tg.TrackerState
	if err := json.Unmarshal(b, &st); err != nil {
		return
	}
	data.Persisted = true
	if st.UpdatedUnix > 0 {
		data.PersistedAgeSec = data.NowUnix - st.UpdatedUnix
	}
	data.State.TotalOK = st.TotalOK
	data.State.TotalErr = st.TotalErr
	data.State.TotalBytes = st.TotalBytes
	data.State.TotalBytesTx = st.TotalTx
	data.State.TotalBytesRx = st.TotalRx
	methods := make([]tgMethod, 0, len(st.PerMethod))
	for name, mc := range st.PerMethod {
		methods = append(methods, tgMethod{Method: name, OK: mc.OK, Err: mc.Err, FloodWait: mc.FloodWait, Bytes: mc.Bytes})
	}
	// Most-used first, mirroring the daemon's ordering.
	sort.Slice(methods, func(i, j int) bool {
		return methods[i].OK+methods[i].Err > methods[j].OK+methods[j].Err
	})
	data.State.PerMethod = methods
}

// sanitizeSourceURL validates the user-supplied metrics source
// override. Only plain-http URLs on a LOOPBACK host are accepted —
// the override exists so an operator can point the page at a daemon
// on a non-default local port, not to make the web server fetch
// arbitrary URLs (classic SSRF: internal services, cloud metadata
// endpoints) on behalf of whoever crafts a link.
func sanitizeSourceURL(v string) (string, bool) {
	if v == "" {
		return "", false
	}
	u, err := neturl.Parse(v)
	if err != nil || u.Scheme != "http" {
		return "", false
	}
	host := u.Hostname()
	if host != "localhost" {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return "", false
		}
	}
	return u.String(), true
}

// fetchTelegramState pulls + parses the daemon's /telegram JSON for
// the full-page render (CSRF + flash needed for the form).
func (s *Server) fetchTelegramState(w http.ResponseWriter, r *http.Request) tgPageData {
	cfg, name, err := requestProfile(r)
	data := tgPageData{
		pageBase:                s.basePage(w, r),
		Profile:                 name,
		Profiles:                listProfileNames(),
		NowUnix:                 time.Now().Unix(),
		RateLimitPerMin:         -1,
		DownloadRateLimitPerMin: -1,
	}
	if err == nil {
		data.RateLimitPerMin = readRateLimitFromMeta(r.Context(), cfg)
		data.DownloadRateLimitPerMin = readDownloadRateLimitFromMeta(r.Context(), cfg)
	}
	fillTGState(r.Context(), &data, cfg, err, sourceFor(r))
	if data.Online && cfg != nil {
		if live := fetchLiveStatus(cfg); live != nil {
			data.Upload = live.Upload
		}
	}
	return data
}

// uploadView is status --json's upload block.
type uploadView struct {
	Connections            int   `json:"connections"`
	RateCapBytesPerS       int64 `json:"rate_cap_bytes_per_s"`
	RateBytesPerS          int64 `json:"rate_bytes_per_s"`
	RateCapConfigBytesPerS int64 `json:"rate_cap_config_bytes_per_s"`
}

// telegramSetUploadLimit changes the shown profile's upload cap while its
// daemon runs (control socket POST /upload/limit, v1.1 I3).
func (s *Server) telegramSetUploadLimit(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	cfg, name, err := requestProfile(r)
	back := "/telegram"
	if name != "" {
		back += "?profile=" + neturl.QueryEscape(name)
	}
	if err != nil {
		SetFlash(w, "error", "profile: "+err.Error())
		http.Redirect(w, r, back, http.StatusSeeOther)
		return
	}
	resp, err := control.Client(control.Path(cfg.DataDir), 5*time.Second).Post(
		control.BaseURL+"/upload/limit?rate="+neturl.QueryEscape(strings.TrimSpace(r.FormValue("rate"))), "", nil)
	if err != nil {
		SetFlash(w, "error", "the mount of "+name+" does not answer: "+err.Error())
		http.Redirect(w, r, back, http.StatusSeeOther)
		return
	}
	defer resp.Body.Close()
	var rep struct {
		Cap   int64  `json:"rate_cap_bytes_per_s"`
		Error string `json:"error"`
	}
	json.NewDecoder(resp.Body).Decode(&rep)
	switch {
	case rep.Error != "":
		SetFlash(w, "error", rep.Error)
	case resp.StatusCode != http.StatusOK:
		SetFlash(w, "error", fmt.Sprintf("the mount answered %d (an older telfs without the route? restart it)", resp.StatusCode))
	case rep.Cap == 0:
		SetFlash(w, "ok", "upload cap removed until the mount stops")
	default:
		SetFlash(w, "ok", "upload cap set to "+rateText(rep.Cap)+" until the mount stops")
	}
	http.Redirect(w, r, back, http.StatusSeeOther)
}

// populateTGState does the actual HTTP fetch + JSON parse + reverse-
// order. Pure function so both fetchTelegramState (full page) and
// fetchTelegramStatePartial (htmx fragment) can share the network
// step without dragging in basePage's CSRF/flash side effects.
func populateTGState(data *tgPageData, cfg *config.Config, url string) {
	var resp *http.Response
	var err error
	if url == controlSource {
		resp, err = daemonGetFor(cfg, "/telegram")
	} else {
		resp, err = httpClient.Get(url)
	}
	if err != nil {
		data.Online = false
		data.FetchError = err.Error()
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		data.Online = false
		data.FetchError = fmt.Sprintf("HTTP %d from daemon", resp.StatusCode)
		return
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		data.Online = false
		data.FetchError = "read body: " + err.Error()
		return
	}
	if err := json.Unmarshal(body, &data.State); err != nil {
		data.Online = false
		data.FetchError = "parse json: " + err.Error()
		return
	}
	data.Online = true
	if data.State.LastSuccessAgeSeconds > 0 && data.State.InFlight > 0 {
		data.StallSeconds = data.State.LastSuccessAgeSeconds
	}
	// Reverse the requests slice so the most recent appears first.
	// The daemon serves them oldest-first (ring-buffer insertion
	// order); flipping in the web layer keeps the daemon API stable.
	reqs := data.State.Requests
	for i, j := 0, len(reqs)-1; i < j; i, j = i+1, j-1 {
		reqs[i], reqs[j] = reqs[j], reqs[i]
	}
}

// httpClient has a short timeout — the daemon's /telegram endpoint
// is local and trivial; if it doesn't respond in 2 s, the daemon
// itself is wedged and the user needs to know.
var httpClient = &http.Client{Timeout: 2 * time.Second}

// controlSource is the default daemon source: the profile's owner-only
// control socket (spec v1 §3). A `source=` URL (loopback only) still reads
// a daemon started with --metrics-addr over TCP.
const controlSource = "the profile's control socket"

// daemonGetFor fetches path from cfg's profile's running daemon.
func daemonGetFor(cfg *config.Config, path string) (*http.Response, error) {
	return control.Client(control.Path(cfg.DataDir), 2*time.Second).Get(control.BaseURL + path)
}

// openMetaExisting opens the active profile's meta DB ONLY if the
// file already exists. meta.Open bootstraps schema + a fresh fs_uuid
// on a missing file, and a read-only page doing that forecloses
// cold-mount recovery: cmdMount sees a db.sqlite and skips
// tryRecover, then mounts an empty FS and posts an empty snapshot.
// Every web-side read path must go through this.
func openMetaExisting() (*meta.Store, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}
	return openMetaExistingFor(cfg)
}

// openMetaExistingFor is openMetaExisting for a named profile's config
// (writes: the rate-limit form).
func openMetaExistingFor(cfg *config.Config) (*meta.Store, error) {
	if _, err := os.Stat(cfg.DBPath()); err != nil {
		return nil, fmt.Errorf("metadata DB not found at %s", cfg.DBPath())
	}
	return meta.Open(cfg.DBPath())
}

// openMetaRead opens a profile's existing meta DB for reading only: no
// schema work, no writes, safe next to the profile's running daemon (the
// monitoring paths read every profile every few seconds). Falls back to a
// normal open where SQLite refuses a read-only one.
func openMetaRead(cfg *config.Config) (*meta.Store, error) {
	if _, err := os.Stat(cfg.DBPath()); err != nil {
		return nil, fmt.Errorf("metadata DB not found at %s", cfg.DBPath())
	}
	if m, err := meta.OpenReadOnly(cfg.DBPath(), false); err == nil {
		return m, nil
	}
	return meta.Open(cfg.DBPath())
}

// readRateLimitFromMeta opens the active profile's meta DB and
// returns the current TG UPLOAD rate-limit value. Returns -1 if the
// meta DB can't be read (no active profile, DB missing, schema not
// yet initialized) so the template can flag the "unknown" case
// distinctly from "explicitly unlimited" (0).
func readRateLimitFromMeta(ctx context.Context, cfg *config.Config) int {
	mstore, err := openMetaRead(cfg)
	if err != nil {
		return -1
	}
	defer mstore.Close()
	n, err := mstore.TGRateLimitPerMin(ctx)
	if err != nil {
		return -1
	}
	return n
}

// readDownloadRateLimitFromMeta mirrors readRateLimitFromMeta but
// for the download-direction lane.
func readDownloadRateLimitFromMeta(ctx context.Context, cfg *config.Config) int {
	mstore, err := openMetaRead(cfg)
	if err != nil {
		return -1
	}
	defer mstore.Close()
	n, err := mstore.TGDownloadRateLimitPerMin(ctx)
	if err != nil {
		return -1
	}
	return n
}

// telegramSetRateLimit accepts the upload + download per-minute
// caps from the form on /telegram and writes them to meta_kv. The
// daemon's poller picks them up within ~5 s — no remount needed.
// Empty / 0 / "clear" disable each lane independently.
//
// Back-compat: the form previously named the field "per_minute"
// (single cap). We accept both "upload_per_minute" (new) and
// "per_minute" (legacy) for the upload lane so external callers
// hitting the endpoint with the old field name continue to work.
func (s *Server) telegramSetRateLimit(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	// The form carries the profile its page showed: the limits are that
	// profile's, whichever profile is selected.
	cfg, name, cfgErr := requestProfile(r)
	back := "/telegram"
	if name != "" {
		back += "?profile=" + neturl.QueryEscape(name)
	}
	parseN := func(field string) (int, bool, error) {
		raw := strings.TrimSpace(r.FormValue(field))
		if raw == "" {
			return 0, false, nil
		}
		if raw == "clear" {
			return 0, true, nil
		}
		n, err := strconv.Atoi(raw)
		return n, true, err
	}
	upN, upSet, upErr := parseN("upload_per_minute")
	if !upSet && upErr == nil {
		// Legacy field name.
		upN, upSet, upErr = parseN("per_minute")
	}
	downN, downSet, downErr := parseN("download_per_minute")
	if upErr != nil {
		SetFlash(w, "error", "upload rate limit: "+upErr.Error())
		http.Redirect(w, r, back, http.StatusSeeOther)
		return
	}
	if downErr != nil {
		SetFlash(w, "error", "download rate limit: "+downErr.Error())
		http.Redirect(w, r, back, http.StatusSeeOther)
		return
	}
	if cfgErr != nil {
		SetFlash(w, "error", "profile: "+cfgErr.Error())
		http.Redirect(w, r, back, http.StatusSeeOther)
		return
	}
	mstore, err := openMetaExistingFor(cfg)
	if err != nil {
		SetFlash(w, "error", "meta: "+err.Error())
		http.Redirect(w, r, back, http.StatusSeeOther)
		return
	}
	defer mstore.Close()
	var bits []string
	if upSet {
		if err := mstore.SetTGRateLimitPerMin(r.Context(), upN); err != nil {
			SetFlash(w, "error", "set upload rate limit: "+err.Error())
			http.Redirect(w, r, back, http.StatusSeeOther)
			return
		}
		if upN == 0 {
			bits = append(bits, "upload cleared")
		} else {
			bits = append(bits, fmt.Sprintf("upload %d req/min", upN))
		}
	}
	if downSet {
		if err := mstore.SetTGDownloadRateLimitPerMin(r.Context(), downN); err != nil {
			SetFlash(w, "error", "set download rate limit: "+err.Error())
			http.Redirect(w, r, back, http.StatusSeeOther)
			return
		}
		if downN == 0 {
			bits = append(bits, "download cleared")
		} else {
			bits = append(bits, fmt.Sprintf("download %d req/min", downN))
		}
	}
	if len(bits) > 0 {
		// Kept only in meta_kv (no journal op): a format-2 filesystem
		// owes a snapshot so the change reaches the channel soon.
		if err := snapshot.MarkDue(r.Context(), mstore, "rate-limit settings changed"); err != nil {
			SetFlash(w, "error", "applied, but marking a snapshot owed failed: "+err.Error())
			http.Redirect(w, r, back, http.StatusSeeOther)
			return
		}
	}
	if len(bits) == 0 {
		SetFlash(w, "ok", "no changes — both fields were empty")
	} else {
		SetFlash(w, "ok", "applied: "+strings.Join(bits, ", ")+" — daemon picks it up within 5 s")
	}
	http.Redirect(w, r, back, http.StatusSeeOther)
}
