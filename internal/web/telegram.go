package web

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"telfs/internal/config"
	"telfs/internal/meta"
)

// tgState mirrors the JSON shape served by the mount daemon's
// /telegram endpoint (cmd/telfs/main.go:telegramJSONHandler). The
// web process runs separately from the mount daemon, so we have to
// fetch over HTTP — there's no shared memory between the two.
type tgState struct {
	ChannelID             int64        `json:"channel_id"`
	ChannelTitle          string       `json:"channel_title"`
	InFlight              int          `json:"in_flight"`
	TotalOK               int64        `json:"total_ok"`
	TotalErr              int64        `json:"total_err"`
	TotalBytes            int64        `json:"total_bytes"`
	LastSuccessUnix       int64        `json:"last_success_unix"`
	LastSuccessAgeSeconds int64        `json:"last_success_age_seconds"`
	LastErrorUnix         int64        `json:"last_error_unix"`
	LastErrorAgeSeconds   int64        `json:"last_error_age_seconds"`
	LastErrorMessage      string       `json:"last_error_message"`
	PerMethod             []tgMethod   `json:"per_method"`
	Requests              []tgRequest  `json:"requests"`
}

type tgMethod struct {
	Method    string `json:"method"`
	OK        int64  `json:"ok"`
	Err       int64  `json:"err"`
	FloodWait int64  `json:"flood_wait"`
	Bytes     int64  `json:"bytes"`
}

type tgRequest struct {
	ID               int64  `json:"id"`
	Method           string `json:"method"`
	StartedUnix      int64  `json:"started_unix"`
	DurationMS       int64  `json:"duration_ms"`
	Bytes            int    `json:"bytes"`
	InFlight         bool   `json:"in_flight"`
	Err              string `json:"err"`
	FloodWaitSeconds int    `json:"flood_wait_seconds"`
}

type tgPageData struct {
	pageBase
	Source       string // URL we fetched from (for the help text)
	Online       bool   // did the fetch succeed
	FetchError   string // explanation if Online is false
	State        tgState
	StallSeconds int64 // derived: how long since last success
	NowUnix      int64

	// RateLimitPerMin is the per-FS requests/min cap, read fresh
	// from meta_kv on every request so CLI edits show up
	// immediately. Zero means unlimited. -1 signals "couldn't
	// read meta_kv" so the template can flag that.
	RateLimitPerMin int
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
	if v := r.URL.Query().Get("source"); v != "" {
		url = v
	}
	data := tgPageData{
		Source:  url,
		NowUnix: time.Now().Unix(),
	}
	populateTGState(&data, url)
	return data
}

// fetchTelegramState pulls + parses the daemon's /telegram JSON for
// the full-page render (CSRF + flash needed for the form).
func (s *Server) fetchTelegramState(w http.ResponseWriter, r *http.Request) tgPageData {
	url := "http://127.0.0.1:9090/telegram"
	if v := r.URL.Query().Get("source"); v != "" {
		url = v
	}
	data := tgPageData{
		pageBase:        s.basePage(w, r),
		Source:          url,
		NowUnix:         time.Now().Unix(),
		RateLimitPerMin: readRateLimitFromMeta(r.Context()),
	}
	populateTGState(&data, url)
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

// readRateLimitFromMeta opens the active profile's meta DB and
// returns the current TG rate-limit value. Returns -1 if the meta
// DB can't be read (no active profile, DB missing, schema not yet
// initialized) so the template can flag the "unknown" case
// distinctly from "explicitly unlimited" (0).
func readRateLimitFromMeta(ctx context.Context) int {
	cfg, err := config.Load()
	if err != nil {
		return -1
	}
	mstore, err := meta.Open(cfg.DBPath())
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

// telegramSetRateLimit accepts the per-minute cap from the form on
// /telegram and writes it to meta_kv. The daemon's poller picks it
// up within ~5 s — no remount needed. Empty / 0 / "clear" all
// disable the limit.
func (s *Server) telegramSetRateLimit(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	raw := strings.TrimSpace(r.FormValue("per_minute"))
	if raw == "" || raw == "clear" {
		raw = "0"
	}
	n, perr := strconv.Atoi(raw)
	if perr != nil {
		SetFlash(w, "error", "rate limit: "+perr.Error())
		http.Redirect(w, r, "/telegram", http.StatusSeeOther)
		return
	}
	cfg, err := config.Load()
	if err != nil {
		SetFlash(w, "error", "config: "+err.Error())
		http.Redirect(w, r, "/telegram", http.StatusSeeOther)
		return
	}
	mstore, err := meta.Open(cfg.DBPath())
	if err != nil {
		SetFlash(w, "error", "meta.Open: "+err.Error())
		http.Redirect(w, r, "/telegram", http.StatusSeeOther)
		return
	}
	defer mstore.Close()
	if err := mstore.SetTGRateLimitPerMin(r.Context(), n); err != nil {
		SetFlash(w, "error", "set rate limit: "+err.Error())
		http.Redirect(w, r, "/telegram", http.StatusSeeOther)
		return
	}
	if n == 0 {
		SetFlash(w, "ok", "rate limit cleared (unlimited) — daemon picks it up within 5 s")
	} else {
		SetFlash(w, "ok", fmt.Sprintf("rate limit set to %d requests/min — daemon picks it up within 5 s", n))
	}
	http.Redirect(w, r, "/telegram", http.StatusSeeOther)
}
