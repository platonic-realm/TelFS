package snapshot

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"telfs/internal/meta"
	"telfs/internal/wire"
)

// restore2 is the local half of a recovery: restore the fetched
// snapshot, replay its journal, record what it came from.
func restore2(t *testing.T, f *fs2, got *Fetched2) *meta.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "recovered.sqlite")
	if err := Restore(f.ctx, got.DB, path); err != nil {
		t.Fatal(err)
	}
	m, err := meta.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	for _, e := range got.Journal {
		op, err := meta.DecodeOp(e.Op)
		if err != nil {
			t.Fatal(err)
		}
		if err := m.ReplayOp(f.ctx, op); err != nil {
			t.Fatalf("replay seq %d: %v", e.Seq, err)
		}
	}
	if n := len(got.Journal); n > 0 {
		if err := m.AdvanceJournalSeq(f.ctx, got.Journal[n-1].Seq); err != nil {
			t.Fatal(err)
		}
	}
	if err := RecordRecovered(f.ctx, m, got.Located2); err != nil {
		t.Fatal(err)
	}
	return m
}

func hasFile(t *testing.T, m *meta.Store, name string) bool {
	t.Helper()
	_, err := m.Lookup(context.Background(), meta.RootIno, name)
	if err != nil && !errors.Is(err, meta.ErrNotFound) {
		t.Fatal(err)
	}
	return err == nil
}

// Recovery goes pinned list → anchor → the anchor's snapshot → only the
// journal posted after the anchor's boundary: no history scan. The
// anchor's snapshot is used even when a newer snapshot exists whose
// anchor never got pinned, and a journal batch posted WHILE the snapshot
// uploaded (smaller id than the snapshot, ops the snapshot lacks) is
// replayed — the boundary is the newest id before the snapshot was
// taken, not the snapshot's own id.
func TestFetch2ThroughAnchor(t *testing.T) {
	f := newFS2(t)
	mgr := f.manager()
	mgr.Logger = quietLogger()
	f.mkfile(t, "before")
	f.post(t)
	// The race: during the snapshot's upload a change is journaled and
	// its batch posted, ahead of the snapshot message.
	var raceBatch int
	f.ch.onUpload = func(caption string) {
		if c, err := f.keys.OpenCaption(caption); err != nil || c.K != wire.KindSnapshot {
			t.Errorf("first upload is not the snapshot: %v", err)
		}
		f.mkfile(t, "during-upload")
		raceBatch = f.post(t)
	}
	if err := mgr.Once(f.ctx); err != nil {
		t.Fatal(err)
	}
	snap1 := int(f.kv(t, KVCurrentMsgID))
	if raceBatch == 0 || raceBatch > snap1 {
		t.Fatalf("race batch %d should precede snapshot %d", raceBatch, snap1)
	}
	f.mkfile(t, "after1")
	f.post(t)
	// A newer snapshot whose anchor could not be pinned: the anchor
	// still names snap1.
	f.ch.failPin = true
	if err := mgr.Once(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.ch.failPin = false
	if newer := int(f.kv(t, KVCurrentMsgID)); newer == snap1 {
		t.Fatal("expected a newer snapshot")
	}
	f.mkfile(t, "after2")
	f.post(t)

	pins, _ := f.ch.PinnedMessages(f.ctx)
	scans := f.ch.fullWalks
	var log []string
	got, err := Fetch2(f.ctx, f.ch, f.keys, pins, func(fm string, a ...any) { log = append(log, fmt.Sprintf(fm, a...)) })
	if err != nil {
		t.Fatal(err)
	}
	if got.Anchor == nil || got.SnapMsg != snap1 {
		t.Fatalf("recovered from snapshot %d (anchor %v), want the anchor's %d", got.SnapMsg, got.Anchor, snap1)
	}
	if f.ch.fullWalks != scans {
		t.Fatal("a recovery through the anchor must not scan the channel history")
	}
	if len(f.ch.afterWalks) != 1 || f.ch.afterWalks[0] != got.Anchor.Anchor.JnlAfter || got.JnlAfter >= snap1 {
		t.Fatalf("journal walks %v (boundary %d, snapshot %d)", f.ch.afterWalks, got.JnlAfter, snap1)
	}
	if len(got.Journal) == 0 || got.Journal[0].Seq != got.SnapSeq+1 {
		t.Fatalf("journal must start right after the snapshot's seq %d: %v", got.SnapSeq, got.Journal)
	}
	m := restore2(t, f, got)
	for _, n := range []string{"before", "during-upload", "after1", "after2"} {
		if !hasFile(t, m, n) {
			t.Errorf("%s missing after recovery", n)
		}
	}
	// What the recovered DB records: the snapshot it came from and its
	// anchor; the policy then sees ops since, no snapshot owed now.
	st, err := LoadPolicyState(f.ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	if st.SnapMsg != snap1 || !st.HaveJnlAfter || st.AnchorSnap != snap1 || st.Due != "" || st.CurrentSeq <= st.SnapSeq {
		t.Fatalf("recovered state %+v", st)
	}
	if act, why := (Policy{}).Decide(st, time.Now()); act != ActNone {
		t.Fatalf("right after a recovery: %s (%s), want none", act, why)
	}
	if strings.Join(log, "\n") == "" {
		t.Fatal("no progress lines")
	}
}

// A filesystem from before v1 (no anchor): the newest snapshot and the
// journal are found by scanning, as before.
func TestFetch2WithoutAnchorScans(t *testing.T) {
	f := newFS2(t)
	mgr := f.manager()
	mgr.Logger = quietLogger()
	if err := mgr.Once(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.mkfile(t, "x")
	f.post(t)
	if err := mgr.Once(f.ctx); err != nil {
		t.Fatal(err)
	}
	newest := int(f.kv(t, KVCurrentMsgID))
	f.mkfile(t, "y")
	f.post(t)
	_ = f.ch.DeleteChannelMessages(f.ctx, f.ch.kinds(f.keys, wire.KindAnchor)...)

	pins, _ := f.ch.PinnedMessages(f.ctx)
	got, err := Fetch2(f.ctx, f.ch, f.keys, pins, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Anchor != nil || got.SnapMsg != newest || f.ch.fullWalks == 0 {
		t.Fatalf("snapshot %d (anchor %v, scans %d), want the newest %d by scan", got.SnapMsg, got.Anchor, f.ch.fullWalks, newest)
	}
	m := restore2(t, f, got)
	if !hasFile(t, m, "x") || !hasFile(t, m, "y") {
		t.Fatal("files missing")
	}
	st, _ := LoadPolicyState(f.ctx, m)
	if st.HaveJnlAfter || st.AnchorSnap != 0 {
		t.Fatalf("found by scan: no boundary may be recorded: %+v", st)
	}
	// One op since the scanned snapshot: the mount takes one snapshot
	// (and with it the first anchor).
	if act, _ := (Policy{}).Decide(st, time.Now()); act != ActSnapshot {
		t.Fatalf("after a scan recovery with ops since: %s", act)
	}
}

// An anchor whose snapshot is gone falls back to the scan (said so) and
// is remembered, so the next anchor replaces it.
func TestFetch2AnchorSnapshotGone(t *testing.T) {
	f := newFS2(t)
	mgr := f.manager()
	mgr.Logger = quietLogger()
	if err := mgr.Once(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.ch.failPin = true
	f.mkfile(t, "x")
	f.post(t)
	if err := mgr.Once(f.ctx); err != nil { // newer snapshot, anchor unchanged
		t.Fatal(err)
	}
	newest := int(f.kv(t, KVCurrentMsgID))
	anchor := int(f.kv(t, KVAnchorMsgID))
	_ = f.ch.DeleteChannelMessages(f.ctx, int(f.kv(t, KVAnchorSnapID)))

	pins, _ := f.ch.PinnedMessages(f.ctx)
	var log []string
	got, err := Fetch2(f.ctx, f.ch, f.keys, pins, func(fm string, a ...any) { log = append(log, fmt.Sprintf(fm, a...)) })
	if err != nil {
		t.Fatal(err)
	}
	if got.Anchor != nil || got.SnapMsg != newest || got.StaleAnchor != anchor {
		t.Fatalf("got snapshot %d stale anchor %d, want %d and %d", got.SnapMsg, got.StaleAnchor, newest, anchor)
	}
	if !strings.Contains(strings.Join(log, "\n"), "can't be read") {
		t.Fatalf("the fallback must be said: %q", log)
	}
	m := restore2(t, f, got)
	if v := kvInt(f.ctx, m, KVAnchorMsgID); v != int64(anchor) {
		t.Fatalf("stale anchor %d not recorded (%d)", anchor, v)
	}
}

// A hole in the journal after the anchor's snapshot refuses the replay.
func TestListJournal2AfterRefusesAHole(t *testing.T) {
	f := newFS2(t)
	mgr := f.manager()
	mgr.Logger = quietLogger()
	if err := mgr.Once(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.mkfile(t, "a")
	f.post(t)
	f.mkfile(t, "b")
	lost := f.post(t)
	f.mkfile(t, "c")
	f.post(t)
	_ = f.ch.DeleteChannelMessages(f.ctx, lost)
	pins, _ := f.ch.PinnedMessages(f.ctx)
	if _, err := Fetch2(f.ctx, f.ch, f.keys, pins, nil); err == nil || !strings.Contains(err.Error(), "hole") {
		t.Fatalf("want a refusal naming the hole, got %v", err)
	}
}

// Key messages come from the pinned list; the history scan runs only
// when no anchor is pinned next to them (filesystems from before v1).
func TestFindKeyMessagesPinned(t *testing.T) {
	f := newFS2(t)
	scans := f.ch.fullWalks
	found, err := FindKeyMessages(f.ctx, f.ch)
	if err != nil || len(found) != 1 || !found[0].Pinned {
		t.Fatalf("pre-v1: %+v %v", found, err)
	}
	if f.ch.fullWalks != scans+1 {
		t.Fatal("without an anchor the key-message scan must still run")
	}
	mgr := f.manager()
	mgr.Logger = quietLogger()
	if err := mgr.Once(f.ctx); err != nil {
		t.Fatal(err)
	}
	scans = f.ch.fullWalks
	found, err = FindKeyMessages(f.ctx, f.ch)
	if err != nil || len(found) != 1 || !found[0].Pinned {
		t.Fatalf("with an anchor: %+v %v", found, err)
	}
	if f.ch.fullWalks != scans {
		t.Fatal("with a pinned key message and anchor, finding the key must not scan history")
	}
}
