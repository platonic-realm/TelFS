package web

import (
	"bytes"
	"html/template"
	"strings"
	"testing"
)

// The link pill follows the staging cap, not the number of staged wires:
// a first backup stages hundreds of thousands of wires on purpose.
func TestClassifyHealthStaging(t *testing.T) {
	const capB = 50_000_000_000
	online := func(q tgQueue) tgPageData {
		return tgPageData{Online: true, State: tgState{Queue: q}}
	}
	cases := []struct {
		name      string
		data      tgPageData
		kind      string
		label     string
		tipSubstr string
	}{
		{"offline", tgPageData{}, "err", "link down", "control socket"},
		{"idle", online(tgQueue{StagingCapBytes: capB}), "ok", "link ok", "Daemon online"},
		{"large backlog well below the cap", online(tgQueue{PendingWiresCount: 250_000, PendingWiresBytes: 10_000_000_000,
			StagingBytes: 10_000_000_000, StagingCapBytes: capB}), "ok", "link ok", "250000 wire(s)"},
		{"exactly 80 % is not above it", online(tgQueue{PendingWiresCount: 9, StagingBytes: 40_000_000_000, StagingCapBytes: capB}),
			"ok", "link ok", "(80 %)"},
		{"above 80 % of the cap", online(tgQueue{PendingWiresCount: 9, StagingBytes: 41_000_000_000, StagingCapBytes: capB}),
			"warn", "link busy", "staging nearly full"},
		{"writes blocked", online(tgQueue{PendingWiresCount: 9, StagingBytes: capB, StagingCapBytes: capB, StagingBlocked: true}),
			"err", "backlog", "Writes are waiting"},
		{"blocked on the free-space floor without a cap", online(tgQueue{PendingWiresCount: 1, StagingBytes: 4 << 20, StagingBlocked: true}),
			"err", "backlog", "min_free"},
		{"no cap reported (old daemon / in-memory pipeline)", online(tgQueue{PendingWiresCount: 5000, PendingWiresBytes: 1 << 30}),
			"ok", "link ok", "5000 wire(s), 1 GiB staged (draining)"},
		{"a recent error still wins", tgPageData{Online: true, State: tgState{LastErrorAgeSeconds: 3, LastErrorMessage: "boom",
			Queue: tgQueue{StagingBlocked: true}}}, "err", "link fault", "boom"},
	}
	for _, c := range cases {
		kind, label, tip := classifyHealth(c.data)
		if kind != c.kind || label != c.label || !strings.Contains(tip, c.tipSubstr) {
			t.Errorf("%s: got (%q, %q, %q), want (%q, %q, …%q…)", c.name, kind, label, tip, c.kind, c.label, c.tipSubstr)
		}
	}
}

func TestStagingLevelAndPct(t *testing.T) {
	q := tgQueue{StagingBytes: 60, StagingCapBytes: 100}
	if q.StagingLevel() != "" || q.StagingPct() != 60 {
		t.Errorf("60 %%: level %q pct %v", q.StagingLevel(), q.StagingPct())
	}
	q.StagingBytes = 150 // a single wire larger than the cap may pass the gate
	if q.StagingLevel() != "warn" || q.StagingPct() != 100 {
		t.Errorf("over the cap: level %q pct %v", q.StagingLevel(), q.StagingPct())
	}
	if (tgQueue{StagingBytes: 1 << 40}).StagingPct() != 0 {
		t.Error("no cap: the meter must stay empty")
	}
}

// The Telegram page shows the staging directory, bytes and cap when the
// daemon reports them, and no longer tells anyone to use 127.0.0.1:9090.
func TestTelegramPanelStaging(t *testing.T) {
	tm, err := template.New("").Funcs(tmplFuncs()).ParseFS(templatesFS, "templates/partials/telegram.html")
	if err != nil {
		t.Fatal(err)
	}
	exec := func(d tgPageData) string {
		var b bytes.Buffer
		if err := tm.ExecuteTemplate(&b, "telegram_panel", d); err != nil {
			t.Fatal(err)
		}
		return b.String()
	}
	out := exec(tgPageData{FetchError: "dial unix: no such file"})
	if strings.Contains(out, "9090") || strings.Contains(out, "metrics-addr") {
		t.Error("offline panel still points at the old TCP default")
	}
	if !strings.Contains(out, "control socket") {
		t.Error("offline panel should explain the control socket")
	}

	q := tgQueue{PendingWiresCount: 3, PendingWiresBytes: 45 << 30, StagingBytes: 45 << 30, StagingCapBytes: 50 << 30,
		StagingDir: "/srv/telfs-staging", StagedDirectWaiting: 2, StagedDirectRunning: 4}
	out = exec(tgPageData{Online: true, State: tgState{Queue: q}})
	for _, want := range []string{"/srv/telfs-staging", "45 GiB", "50 GiB", `qbar-fill warn`, "4 running, 2 queued", "over 80 %"} {
		if !strings.Contains(out, want) {
			t.Errorf("staging row: %q missing", want)
		}
	}
	q.StagingBlocked = true
	out = exec(tgPageData{Online: true, State: tgState{Queue: q}})
	if !strings.Contains(out, `qbar-fill err`) || !strings.Contains(out, "writes are waiting") {
		t.Error("blocked staging must show a red meter and say writes wait")
	}
	// A daemon without the queue fields: no meter, still the wire count.
	out = exec(tgPageData{Online: true, State: tgState{Queue: tgQueue{PendingWiresCount: 7, PendingWiresBytes: 1 << 20}}})
	if strings.Contains(out, "of the staging cap") || !strings.Contains(out, "<strong>7</strong> wire(s)") {
		t.Error("without a cap the row shows the count and no meter")
	}
}

// The full Telegram page (layout + page + partial) renders with queue data.
func TestTelegramPageRendersWithStaging(t *testing.T) {
	t.Setenv("TELFS_PROFILE", "webtest") // the masthead names the active profile; never read the real one
	q := tgQueue{PendingWiresCount: 1, PendingWiresBytes: 1 << 30, StagingBytes: 1 << 30, StagingCapBytes: 2 << 30, StagingDir: "/x"}
	out := render(t, "telegram.html", tgPageData{pageBase: pageBase{CSRF: "c"}, Online: true, State: tgState{Queue: q}},
		"templates/partials/telegram.html")
	if !strings.Contains(out, "of the staging cap") {
		t.Error("staging meter missing from the full page")
	}
}
