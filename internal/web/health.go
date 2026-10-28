package web

import (
	"fmt"
	"net/http"
	"strings"
)

// healthPartial renders the small one-line pill that lives in the
// header on every page. Polled every 5 s via HTMX. Reads from the
// running daemon's /telegram JSON over the profile's control socket
// (no daemon → red pill).
//
// Status rules — keep them simple so an operator can predict what
// turns the pill yellow vs red without reading code:
//
//	green  "link ok"     daemon online and none of the below.
//	yellow "link busy"   no upload progress for > 60 s with work queued,
//	                     OR a FLOOD_WAIT in the last 5 min, OR the staging
//	                     directory holds more than 80 % of its cap.
//	red    "link down"   no daemon answers.
//	red    "link fault"  last error < 30 s ago.
//	red    "backlog"     writes are blocked: the staging cap ([staging]
//	                     max_bytes) or the staging filesystem's free-space
//	                     floor ([staging] min_free) is reached, so writes
//	                     wait for uploads.
//
// A large staged backlog by itself is normal (a first backup writes far
// faster than it uploads; the disk-backed staging queue exists for that),
// so the wire COUNT never colours the pill — only how close the staged
// BYTES are to the cap, and whether writes actually wait.
func (s *Server) healthPartial(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	state := s.fetchTelegramStatePartial(r)
	kind, label, tip := classifyHealth(state)
	// Two lamps in one swap target: link health + channel sync. The
	// sync lamp answers "is the channel a complete copy — could I
	// lose this machine right now?" (see classifySync); green means
	// yes, including the offline "as of last unmount" case.
	fmt.Fprintf(w,
		`<span id="health-pill" class="pill-pair" hx-get="/partials/health" hx-trigger="every 5s" hx-swap="outerHTML">`+
			`<span class="pill %s" title="%s">%s</span>`+
			`<span class="pill %s" title="%s">%s</span>`+
			`</span>`,
		kind, htmlAttr(tip), label,
		state.SyncKind, htmlAttr(state.SyncTip), state.SyncLabel,
	)
}

func classifyHealth(state tgPageData) (kind, label, tip string) {
	// Labels are terse panel-lamp captions; the colored dot itself is
	// drawn by CSS (.pill::before), so no emoji here.
	if !state.Online {
		return "err", "link down", "No running mount for this profile answers on its control socket."
	}
	q := state.State.Queue
	// Red conditions first — they win over yellow.
	if state.State.LastErrorAgeSeconds > 0 && state.State.LastErrorAgeSeconds < 30 {
		return "err", "link fault", fmt.Sprintf("Last error %ds ago: %s", state.State.LastErrorAgeSeconds, state.State.LastErrorMessage)
	}
	if q.StagingBlocked {
		return "err", "backlog", "Writes are waiting for uploads: " + q.stagingSummary() +
			" — the staging cap ([staging] max_bytes) or the staging filesystem's free-space floor ([staging] min_free) is reached; writes resume as uploads land."
	}
	// Yellow conditions.
	var yellowReasons []string
	if state.StallSeconds > 60 && (q.PackerChunks > 0 || q.PoolInFlight > 0) {
		yellowReasons = append(yellowReasons, fmt.Sprintf("no upload progress for %ds with queue non-empty", state.StallSeconds))
	}
	if hasFloodWaitInLast5Min(state) {
		yellowReasons = append(yellowReasons, "FLOOD_WAIT observed in the last 5 min")
	}
	if q.StagingLevel() == "warn" {
		yellowReasons = append(yellowReasons, "staging nearly full: "+q.stagingSummary()+" — uploads are behind the writes")
	}
	if len(yellowReasons) > 0 {
		return "warn", "link busy", strings.Join(yellowReasons, " · ")
	}
	// Green — fall through.
	parts := []string{"Daemon online"}
	if state.State.LastSuccessAgeSeconds > 0 {
		parts = append(parts, fmt.Sprintf("last RPC %ds ago", state.State.LastSuccessAgeSeconds))
	}
	if q.PendingWiresCount > 0 || q.StagingBytes > 0 {
		parts = append(parts, q.stagingSummary()+" (draining)")
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
