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
	"telfs/internal/meta"
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
	// PendingWiresCount + Bytes: wires durably staged on disk under
	// cache/pending/. Each represents a dispatched chunk whose
	// chunk_map row hasn't landed yet. Survives daemon crash.
	PendingWiresCount int   `json:"pending_wires_count"`
	PendingWiresBytes int64 `json:"pending_wires_bytes"`
	// PoolInFlight / PoolCap: how many concurrency slots in the
	// UploadPool are currently held vs the cap.
	PoolInFlight int `json:"pool_in_flight"`
	PoolCap      int `json:"pool_cap"`
	// JournalUnposted: rows in the local journal table not yet
	// posted to the channel.
	JournalUnposted int `json:"journal_unposted"`
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
}

// telegramPage renders the connection-state + request-log view. Data
// comes from the mount daemon's /telegram HTTP endpoint
// (--metrics-addr), so the daemon must be running with metrics
// enabled. We default to 127.0.0.1:9090 — the same default as
// `telfs mount --metrics-addr`. Configurable via the
// TELFS_METRICS_URL env var the daemon's wrapper might set.
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
	url := "http://127.0.0.1:9090/telegram"
	if v, ok := sanitizeSourceURL(r.URL.Query().Get("source")); ok {
		url = v
	}
	data := tgPageData{
		Source:  url,
		NowUnix: time.Now().Unix(),
	}
	populateTGState(&data, url)
	if !data.Online {
		loadPersistedTGStats(r.Context(), &data)
	}
	return data
}

// loadPersistedTGStats fills the totals from the profile DB's
// persisted TrackerState when the daemon is unreachable — the stats
// stay consistent across daemon restarts and outages instead of
// disappearing with the process.
func loadPersistedTGStats(ctx context.Context, data *tgPageData) {
	mstore, err := openMetaExisting()
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
	url := "http://127.0.0.1:9090/telegram"
	if v, ok := sanitizeSourceURL(r.URL.Query().Get("source")); ok {
		url = v
	}
	data := tgPageData{
		pageBase:                s.basePage(w, r),
		Source:                  url,
		NowUnix:                 time.Now().Unix(),
		RateLimitPerMin:         readRateLimitFromMeta(r.Context()),
		DownloadRateLimitPerMin: readDownloadRateLimitFromMeta(r.Context()),
	}
	populateTGState(&data, url)
	if !data.Online {
		loadPersistedTGStats(r.Context(), &data)
	}
	return data
}

// populateTGState does the actual HTTP fetch + JSON parse + reverse-
// order. Pure function so both fetchTelegramState (full page) and
// fetchTelegramStatePartial (htmx fragment) can share the network
// step without dragging in basePage's CSRF/flash side effects.
func populateTGState(data *tgPageData, url string) {
	resp, err := httpClient.Get(url)
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
	if _, err := os.Stat(cfg.DBPath()); err != nil {
		return nil, fmt.Errorf("metadata DB not found at %s", cfg.DBPath())
	}
	return meta.Open(cfg.DBPath())
}

// readRateLimitFromMeta opens the active profile's meta DB and
// returns the current TG UPLOAD rate-limit value. Returns -1 if the
// meta DB can't be read (no active profile, DB missing, schema not
// yet initialized) so the template can flag the "unknown" case
// distinctly from "explicitly unlimited" (0).
func readRateLimitFromMeta(ctx context.Context) int {
	mstore, err := openMetaExisting()
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
func readDownloadRateLimitFromMeta(ctx context.Context) int {
	mstore, err := openMetaExisting()
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
		http.Redirect(w, r, "/telegram", http.StatusSeeOther)
		return
	}
	if downErr != nil {
		SetFlash(w, "error", "download rate limit: "+downErr.Error())
		http.Redirect(w, r, "/telegram", http.StatusSeeOther)
		return
	}
	mstore, err := openMetaExisting()
	if err != nil {
		SetFlash(w, "error", "meta: "+err.Error())
		http.Redirect(w, r, "/telegram", http.StatusSeeOther)
		return
	}
	defer mstore.Close()
	var bits []string
	if upSet {
		if err := mstore.SetTGRateLimitPerMin(r.Context(), upN); err != nil {
			SetFlash(w, "error", "set upload rate limit: "+err.Error())
			http.Redirect(w, r, "/telegram", http.StatusSeeOther)
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
			http.Redirect(w, r, "/telegram", http.StatusSeeOther)
			return
		}
		if downN == 0 {
			bits = append(bits, "download cleared")
		} else {
			bits = append(bits, fmt.Sprintf("download %d req/min", downN))
		}
	}
	if len(bits) == 0 {
		SetFlash(w, "ok", "no changes — both fields were empty")
	} else {
		SetFlash(w, "ok", "applied: "+strings.Join(bits, ", ")+" — daemon picks it up within 5 s")
	}
	http.Redirect(w, r, "/telegram", http.StatusSeeOther)
}
