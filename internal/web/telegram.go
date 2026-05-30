package web

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// tgState mirrors the JSON shape served by the mount daemon's
// /telegram endpoint (cmd/telfs/main.go:telegramJSONHandler). The
// web process runs separately from the mount daemon, so we have to
// fetch over HTTP — there's no shared memory between the two.
type tgState struct {
	InFlight              int       `json:"in_flight"`
	LastSuccessUnix       int64     `json:"last_success_unix"`
	LastSuccessAgeSeconds int64     `json:"last_success_age_seconds"`
	LastErrorUnix         int64     `json:"last_error_unix"`
	LastErrorAgeSeconds   int64     `json:"last_error_age_seconds"`
	LastErrorMessage      string    `json:"last_error_message"`
	Requests              []tgRequest `json:"requests"`
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
}

// telegramPage renders the connection-state + request-log view. Data
// comes from the mount daemon's /telegram HTTP endpoint
// (--metrics-addr), so the daemon must be running with metrics
// enabled. We default to 127.0.0.1:9090 — the same default as
// `telfs mount --metrics-addr`. Configurable via the
// TELFS_METRICS_URL env var the daemon's wrapper might set.
func (s *Server) telegramPage(w http.ResponseWriter, r *http.Request) {
	data := s.fetchTelegramState(w, r)
	s.renderTemplate(w, "telegram.html", data)
}

// telegramPartial is the HTMX-polled inner fragment. Same data
// fetch as telegramPage; renders only the partial so hx-swap can
// replace the section without a full page navigation.
func (s *Server) telegramPartial(w http.ResponseWriter, r *http.Request) {
	data := s.fetchTelegramState(w, r)
	s.renderPartial(w, "partials/telegram.html", data)
}

// fetchTelegramState pulls + parses the daemon's /telegram JSON for
// rendering by either the page or the partial.
func (s *Server) fetchTelegramState(w http.ResponseWriter, r *http.Request) tgPageData {
	url := "http://127.0.0.1:9090/telegram"
	if v := r.URL.Query().Get("source"); v != "" {
		url = v
	}
	data := tgPageData{
		pageBase: s.basePage(w, r),
		Source:   url,
		NowUnix:  time.Now().Unix(),
	}
	resp, err := httpClient.Get(url)
	if err != nil {
		data.Online = false
		data.FetchError = err.Error()
		return data
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		data.Online = false
		data.FetchError = fmt.Sprintf("HTTP %d from daemon", resp.StatusCode)
		return data
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		data.Online = false
		data.FetchError = "read body: " + err.Error()
		return data
	}
	if err := json.Unmarshal(body, &data.State); err != nil {
		data.Online = false
		data.FetchError = "parse json: " + err.Error()
		return data
	}
	data.Online = true
	if data.State.LastSuccessAgeSeconds > 0 && data.State.InFlight > 0 {
		data.StallSeconds = data.State.LastSuccessAgeSeconds
	}
	return data
}

// httpClient has a short timeout — the daemon's /telegram endpoint
// is local and trivial; if it doesn't respond in 2 s, the daemon
// itself is wedged and the user needs to know.
var httpClient = &http.Client{Timeout: 2 * time.Second}
