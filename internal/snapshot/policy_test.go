package snapshot

import (
	"testing"
	"time"
)

// The format-2 policy: a snapshot only when the journal has grown by
// EveryOps, or Every has passed AND the journal moved; nothing when the
// journal seq is unchanged (no snapshot at every mount); an anchor alone
// when only the anchor is behind.
func TestPolicyDecide(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	p := Policy{EveryOps: 1000, Every: 24 * time.Hour}
	fresh := PolicyState{HaveSnap: true, SnapMsg: 50, SnapSeq: 100, CurrentSeq: 100,
		LastSnap: now.Add(-time.Hour), HaveJnlAfter: true, AnchorSnap: 50}
	with := func(f func(*PolicyState)) PolicyState {
		s := fresh
		f(&s)
		return s
	}
	for _, tc := range []struct {
		name string
		st   PolicyState
		want Action
	}{
		{"unchanged seq (a mount after a clean stop)", fresh, ActNone},
		{"unchanged seq, last snapshot a week ago", with(func(s *PolicyState) { s.LastSnap = now.Add(-7 * 24 * time.Hour) }), ActNone},
		{"a few ops, recent snapshot", with(func(s *PolicyState) { s.CurrentSeq = 150 }), ActNone},
		{"ops threshold reached", with(func(s *PolicyState) { s.CurrentSeq = 1100 }), ActSnapshotQuiet},
		{"ops just below the threshold", with(func(s *PolicyState) { s.CurrentSeq = 1099 }), ActNone},
		{"time threshold with changes", with(func(s *PolicyState) { s.CurrentSeq = 101; s.LastSnap = now.Add(-25 * time.Hour) }), ActSnapshotQuiet},
		{"time threshold exactly", with(func(s *PolicyState) { s.CurrentSeq = 101; s.LastSnap = now.Add(-24 * time.Hour) }), ActSnapshotQuiet},
		{"changes, last snapshot time unknown", with(func(s *PolicyState) { s.CurrentSeq = 101; s.LastSnap = time.Time{} }), ActSnapshotQuiet},
		{"no snapshot recorded", with(func(s *PolicyState) { s.HaveSnap = false }), ActSnapshot},
		{"snapshot owed (after `snapshot restore`)", with(func(s *PolicyState) { s.Due = "restored from snapshot 7" }), ActSnapshot},
		{"journal behind the snapshot", with(func(s *PolicyState) { s.CurrentSeq = 90 }), ActSnapshot},
		{"pre-v1 snapshot, nothing since: anchor only", with(func(s *PolicyState) { s.HaveJnlAfter = false; s.AnchorSnap = 0 }), ActAnchor},
		{"pre-v1 snapshot, ops since: one snapshot", with(func(s *PolicyState) { s.HaveJnlAfter = false; s.CurrentSeq = 101 }), ActSnapshot},
		{"anchor names an older snapshot", with(func(s *PolicyState) { s.AnchorSnap = 40 }), ActAnchor},
		{"no anchor yet", with(func(s *PolicyState) { s.AnchorSnap = 0 }), ActAnchor},
		{"threshold beats a stale anchor", with(func(s *PolicyState) { s.AnchorSnap = 40; s.CurrentSeq = 5000 }), ActSnapshotQuiet},
		{"version list changed, anchor not re-posted", with(func(s *PolicyState) { s.AnchorDue = true }), ActAnchor},
	} {
		got, why := p.Decide(tc.st, now)
		if got != tc.want {
			t.Errorf("%s: %s (%q), want %s", tc.name, got, why, tc.want)
		}
		if got != ActNone && why == "" {
			t.Errorf("%s: no reason given", tc.name)
		}
	}
}

// The zero Policy is the documented default: 500,000 ops or 24 h.
func TestPolicyDefaults(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	st := PolicyState{HaveSnap: true, SnapMsg: 1, SnapSeq: 0, HaveJnlAfter: true, AnchorSnap: 1, LastSnap: now.Add(-23 * time.Hour)}
	var p Policy
	st.CurrentSeq = DefaultEveryOps - 1
	if a, _ := p.Decide(st, now); a != ActNone {
		t.Fatalf("499,999 ops in 23 h: %s", a)
	}
	st.CurrentSeq = DefaultEveryOps
	if a, _ := p.Decide(st, now); a != ActSnapshotQuiet {
		t.Fatalf("500,000 ops: %s", a)
	}
	st.CurrentSeq = 1
	st.LastSnap = now.Add(-DefaultEvery)
	if a, _ := p.Decide(st, now); a != ActSnapshotQuiet {
		t.Fatalf("1 op in 24 h: %s", a)
	}
	if d := p.withDefaults(); d.MaxQuietWait != time.Hour {
		t.Fatalf("max quiet wait %s, want 1h", d.MaxQuietWait)
	}
}
