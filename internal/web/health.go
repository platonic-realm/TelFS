package web

import (
	"fmt"
	"net/http"
	"strings"
)

// healthPartial renders the small one-line pill that lives in the
// header on every page. Polled every 5 s via HTMX. Reads from the
// running daemon's /telegram JSON (no daemon → red pill with a hint
// to start the mount).
//
// Status rules — keep them simple so an operator can predict what
// turns the pill yellow vs red without reading code:
//
//	🟢 healthy — daemon online, last error > 5 min ago, no FLOOD_WAIT
//	             in the last 5 min, pending wires < 100.
//	🟡 busy    — queue > 0 with last-success-age > 60 s, OR FLOOD_WAIT
//	             observed in the last 5 min, OR pending wires 100–999.
//	🔴 issue   — daemon offline, last error < 30 s, OR pending wires
//	             ≥ 1000 (the disk-leak signal — that many wires
//	             accumulating means the FK-violation cleanup isn't
//	             keeping up).
func (s *Server) healthPartial(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	state := s.fetchTelegramStatePartial(r)
	kind, label, tip := classifyHealth(state)
	fmt.Fprintf(w,
		`<span id="health-pill" class="pill %s" title="%s" hx-get="/partials/health" hx-trigger="every 5s" hx-swap="outerHTML">%s</span>`,
		kind, htmlAttr(tip), label,
	)
}

func classifyHealth(state tgPageData) (kind, label, tip string) {
	// Labels are terse panel-lamp captions; the colored dot itself is
	// drawn by CSS (.pill::before), so no emoji here.
	if !state.Online {
		return "err", "link down", "Daemon /telegram endpoint unreachable. Start a mount with --metrics-addr."
	}
	q := state.State.Queue
	// Red conditions first — they win over yellow.
	if state.State.LastErrorAgeSeconds > 0 && state.State.LastErrorAgeSeconds < 30 {
		return "err", "link fault", fmt.Sprintf("Last error %ds ago: %s", state.State.LastErrorAgeSeconds, state.State.LastErrorMessage)
	}
	if q.PendingWiresCount >= 1000 {
		return "err", "backlog", fmt.Sprintf("%d wires staged on disk — FK-violation cleanup may be lagging. Check the daemon log.", q.PendingWiresCount)
	}
	// Yellow conditions.
	var yellowReasons []string
	if state.StallSeconds > 60 && (q.PackerChunks > 0 || q.PoolInFlight > 0) {
		yellowReasons = append(yellowReasons, fmt.Sprintf("no upload progress for %ds with queue non-empty", state.StallSeconds))
	}
	if hasFloodWaitInLast5Min(state) {
		yellowReasons = append(yellowReasons, "FLOOD_WAIT observed in the last 5 min")
	}
	if q.PendingWiresCount >= 100 && q.PendingWiresCount < 1000 {
		yellowReasons = append(yellowReasons, fmt.Sprintf("%d wires staged", q.PendingWiresCount))
	}
	if len(yellowReasons) > 0 {
		return "warn", "link busy", strings.Join(yellowReasons, " · ")
	}
	// Green — fall through.
	parts := []string{"Daemon online"}
	if state.State.LastSuccessAgeSeconds > 0 {
		parts = append(parts, fmt.Sprintf("last RPC %ds ago", state.State.LastSuccessAgeSeconds))
	}
	if q.PendingWiresCount > 0 {
		parts = append(parts, fmt.Sprintf("%d wire(s) staged (draining)", q.PendingWiresCount))
	}
	return "ok", "link ok", strings.Join(parts, " · ")
}

// hasFloodWaitInLast5Min scans the Recent RPCs ring for any flood-
// wait entries newer than 5 min. The tracker keeps the last 128 RPCs
// by default — under a busy upload that's a sub-minute window, but
// we only need ONE positive to switch the pill.
func hasFloodWaitInLast5Min(state tgPageData) bool {
	cutoff := state.NowUnix - 300
	for _, req := range state.State.Requests {
		if req.FloodWaitSeconds > 0 && req.StartedUnix >= cutoff {
			return true
		}
	}
	return false
}

// htmlAttr escapes a string for inclusion in an HTML attribute value
// that's wrapped in double quotes. We're not running it through the
// template engine (the pill is hand-rolled fmt.Fprintf), so do this
// minimal escape inline.
func htmlAttr(s string) string {
	r := strings.NewReplacer(
		"&", "&amp;",
		"\"", "&quot;",
		"<", "&lt;",
		">", "&gt;",
	)
	return r.Replace(s)
}
