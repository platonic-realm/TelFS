package web

import (
	"fmt"
	"html/template"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// healthPartial renders the masthead lamps on every page: one pair per
// profile that has a filesystem or a running daemon — every profile, not
// only the selected one, because several run at once (S9: backup and
// cloud). Polled every 5 s via HTMX. Each pair reads its own daemon's
// /telegram JSON over that profile's control socket (no daemon → red).
//
// Status rules — keep them simple so an operator can predict what
// turns the pill yellow vs red without reading code:
//
//	green  "link ok"     daemon online and none of the below.
//	yellow "link busy"   no upload progress for > 60 s with work queued,
//	                     OR a FLOOD_WAIT in the last 5 min.
//	yellow "backlog"     the link is fine but the staging directory holds
//	                     more than 80 % of its cap: uploads are behind the
//	                     writes (the lamp names the backlog, not the link).
//	red    "link down"   no daemon answers.
//	red    "link fault"  last error < 30 s ago.
//	red    "writes wait" writes are blocked: the staging cap ([staging]
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
	_, _ = io.WriteString(w, renderLamps(lampProfiles(monitorProfiles(r.Context(), monLamps, profileOf(r)))))
}

// renderLamps is the masthead fragment: per profile its name and two
// lamps — link health, and channel sync ("is the channel a complete copy —
// could I lose this machine right now?", see classifySync) — linking to
// that profile's Telegram page.
func renderLamps(profiles []profileMonitor) string {
	var b strings.Builder
	b.WriteString(`<span id="health-pill" class="lamp-sets" hx-get="/partials/health" hx-trigger="every 5s" hx-swap="outerHTML">`)
	if len(profiles) == 0 {
		b.WriteString(`<span class="pill muted" title="No profile yet — Setup → Guided wizard.">no profile</span>`)
	}
	for _, p := range profiles {
		cls := "lamp-set"
		if p.Selected {
			cls += " selected"
		}
		fmt.Fprintf(&b, `<a class="%s" href="/telegram?profile=%s" title="%s">`+
			`<span class="lamp-name">%s</span>`+
			`<span class="pill %s" title="%s">%s</span>`+
			`<span class="pill %s" title="%s">%s</span>`+
			`</a>`,
			cls, url.QueryEscape(p.Name), htmlAttr("Profile "+p.Name+" — open its Telegram page"),
			template.HTMLEscapeString(p.Name),
			p.LinkKind, htmlAttr(p.LinkTip), p.LinkLabel,
			p.SyncKind, htmlAttr(p.SyncTip), p.SyncLabel)
	}
	b.WriteString(`</span>`)
	return b.String()
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
		return "err", "writes wait", "Writes are waiting for uploads: " + q.stagingSummary() +
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
	stagingFull := q.StagingLevel() == "warn"
	if stagingFull {
		yellowReasons = append(yellowReasons, "staging nearly full: "+q.stagingSummary()+" — uploads are behind the writes")
	}
	if len(yellowReasons) > 0 {
		if stagingFull && len(yellowReasons) == 1 {
			return "warn", "backlog", yellowReasons[0]
		}
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
