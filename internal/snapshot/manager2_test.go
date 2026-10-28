package snapshot

import (
	"context"
	"io"
	"log"
	"testing"
	"time"

	"telfs/internal/meta"
	"telfs/internal/wire"
)

func quietLogger() *log.Logger { return log.New(io.Discard, "", 0) }

// Once on format 2: a snapshot, then a pinned anchor naming it (with the
// journal boundary read before the snapshot), recorded locally; the
// local journal it covers is pruned.
func TestOnce2PostsSnapshotAndAnchor(t *testing.T) {
	f := newFS2(t)
	f.mkfile(t, "a")
	f.post(t)
	before, _ := f.ch.LatestMessageID(f.ctx)
	mgr := f.manager()
	mgr.Logger = quietLogger()
	if err := mgr.Once(f.ctx); err != nil {
		t.Fatal(err)
	}
	snaps := f.ch.kinds(f.keys, wire.KindSnapshot)
	anchors := f.ch.kinds(f.keys, wire.KindAnchor)
	if len(snaps) != 1 || len(anchors) != 1 {
		t.Fatalf("snapshots %v anchors %v, want one each", snaps, anchors)
	}
	seq, _ := f.m.LastJournalSeq(f.ctx)
	a := f.anchorOn(t, anchors[0])
	if a.SnapMsg != snaps[0] || a.SnapSeq != seq || a.JnlAfter != before || a.SnapTS == 0 || a.FS != f.keys.FSUUID {
		t.Fatalf("anchor %+v; want snapshot %d seq %d jnl_after %d", a, snaps[0], seq, before)
	}
	if !f.ch.pinnedIDs()[anchors[0]] {
		t.Fatal("the anchor must be pinned")
	}
	if got := f.kv(t, KVAnchorMsgID); got != int64(anchors[0]) {
		t.Fatalf("anchor_msg_id %d, want %d", got, anchors[0])
	}
	if got := f.kv(t, KVAnchorSnapID); got != int64(snaps[0]) {
		t.Fatalf("anchor_snap_id %d, want %d", got, snaps[0])
	}
	st, _ := f.m.Stats(f.ctx)
	if st.JournalRows != 0 {
		t.Fatalf("%d journal rows left; the snapshot covers them all", st.JournalRows)
	}
	// Two pins now: the key message and the anchor.
	pins, _ := f.ch.PinnedMessages(f.ctx)
	if len(pins) != 2 || len(PinnedKeyMessages(pins)) != 1 {
		t.Fatalf("pins %+v: want the key message and the anchor", pins)
	}
}

// A mount (a fresh Manager) with the journal seq unchanged since the
// last snapshot uploads nothing; neither does a quiet daemon.
func TestStep2NothingChangedNoSnapshot(t *testing.T) {
	f := newFS2(t)
	mgr := f.manager()
	mgr.Logger = quietLogger()
	if err := mgr.Once(f.ctx); err != nil {
		t.Fatal(err)
	}
	n := len(f.ch.idsDesc(0))
	for i, now := range []time.Time{time.Now(), time.Now().Add(48 * time.Hour)} {
		mount := f.manager() // a new process
		mount.Logger = quietLogger()
		act, err := mount.step2(f.ctx, now)
		if err != nil || act != ActNone {
			t.Fatalf("step %d: %s %v, want none", i, act, err)
		}
	}
	if got := len(f.ch.idsDesc(0)); got != n {
		t.Fatalf("%d messages posted with nothing changed", got-n)
	}
}

// Ops threshold: below it nothing; at it a snapshot at a quiet moment —
// waiting while uploads are busy, at most MaxQuietWait. Each snapshot's
// anchor replaces the previous one (one anchor pinned).
func TestStep2OpsThresholdPrefersQuiet(t *testing.T) {
	f := newFS2(t)
	quiet := true
	mgr := f.manager()
	mgr.Logger = quietLogger()
	mgr.Policy = Policy{EveryOps: 3, MaxQuietWait: time.Hour}
	mgr.QuiesceCheck = func(context.Context) bool { return quiet }
	if err := mgr.Once(f.ctx); err != nil {
		t.Fatal(err)
	}
	firstAnchor := f.kv(t, KVAnchorMsgID)
	f.mkfile(t, "a")
	f.mkfile(t, "b")
	now := time.Now()
	if act, err := mgr.step2(f.ctx, now); err != nil || act != ActNone {
		t.Fatalf("2 ops: %s %v", act, err)
	}
	f.mkfile(t, "c")
	quiet = false
	if act, err := mgr.step2(f.ctx, now); err != nil || act != ActWait {
		t.Fatalf("3 ops, busy: %s %v, want wait", act, err)
	}
	if act, _ := mgr.step2(f.ctx, now.Add(59*time.Minute)); act != ActWait {
		t.Fatalf("busy for 59 min: %s, want wait", act)
	}
	if act, err := mgr.step2(f.ctx, now.Add(61*time.Minute)); err != nil || act != ActSnapshotQuiet {
		t.Fatalf("busy for 61 min: %s %v, want the snapshot anyway", act, err)
	}
	if n := len(f.ch.kinds(f.keys, wire.KindSnapshot)); n != 2 {
		t.Fatalf("%d snapshots, want 2", n)
	}
	anchors := f.ch.kinds(f.keys, wire.KindAnchor)
	if len(anchors) != 1 || int64(anchors[0]) == firstAnchor || f.ch.pinnedIDs()[int(firstAnchor)] {
		t.Fatalf("anchors %v (first %d): the new one must replace the old", anchors, firstAnchor)
	}
	if a := f.anchorOn(t, anchors[0]); int64(a.SnapMsg) != f.kv(t, KVCurrentMsgID) {
		t.Fatalf("anchor names %d, newest snapshot is %d", a.SnapMsg, f.kv(t, KVCurrentMsgID))
	}

	// Quiet from the start: no waiting.
	quiet = true
	for _, n := range []string{"d", "e", "f"} {
		f.mkfile(t, n)
	}
	if act, err := mgr.step2(f.ctx, now.Add(2*time.Hour)); err != nil || act != ActSnapshotQuiet {
		t.Fatalf("3 ops, quiet: %s %v", act, err)
	}
}

// Time threshold: Every since the last snapshot AND the journal moved.
func TestStep2TimeThreshold(t *testing.T) {
	f := newFS2(t)
	mgr := f.manager()
	mgr.Logger = quietLogger()
	mgr.Policy = Policy{Every: 24 * time.Hour}
	if err := mgr.Once(f.ctx); err != nil {
		t.Fatal(err)
	}
	if act, _ := mgr.step2(f.ctx, time.Now().Add(25*time.Hour)); act != ActNone {
		t.Fatalf("25 h, no change: %s", act)
	}
	f.mkfile(t, "a")
	if act, _ := mgr.step2(f.ctx, time.Now().Add(23*time.Hour)); act != ActNone {
		t.Fatalf("23 h, a change: %s", act)
	}
	if act, err := mgr.step2(f.ctx, time.Now().Add(25*time.Hour)); err != nil || act != ActSnapshotQuiet {
		t.Fatalf("25 h, a change: %s %v", act, err)
	}
}

// A snapshot owed (after `telfs snapshot restore`) is taken at once,
// busy or not, and the debt is cleared.
func TestStep2DueSnapshot(t *testing.T) {
	f := newFS2(t)
	mgr := f.manager()
	mgr.Logger = quietLogger()
	mgr.QuiesceCheck = func(context.Context) bool { return false }
	if err := mgr.Once(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.m.PutKV(f.ctx, KVSnapDue, []byte("restored from snapshot 3")); err != nil {
		t.Fatal(err)
	}
	if act, err := mgr.step2(f.ctx, time.Now()); err != nil || act != ActSnapshot {
		t.Fatalf("due: %s %v", act, err)
	}
	if _, err := f.m.GetKV(f.ctx, KVSnapDue); err != meta.ErrNotFound {
		t.Fatalf("snap_due must be cleared: %v", err)
	}
	if act, _ := mgr.step2(f.ctx, time.Now()); act != ActNone {
		t.Fatalf("after: %s", act)
	}
}

// A filesystem from before v1 (snapshot without a journal boundary, no
// anchor): with nothing journaled since, the daemon posts an anchor for
// that snapshot instead of a new snapshot; with ops since, it takes one.
func TestStep2PreV1Filesystem(t *testing.T) {
	for _, withOps := range []bool{false, true} {
		f := newFS2(t)
		mgr := f.manager()
		mgr.Logger = quietLogger()
		if err := mgr.Once(f.ctx); err != nil {
			t.Fatal(err)
		}
		oldSnap := f.kv(t, KVCurrentMsgID)
		// Make it look like P1: no boundary, no anchor anywhere.
		for _, k := range []string{KVSnapJnlAfter, KVAnchorMsgID, KVAnchorSnapID} {
			_ = f.m.DeleteKV(f.ctx, k)
		}
		_ = f.ch.DeleteChannelMessages(f.ctx, f.ch.kinds(f.keys, wire.KindAnchor)...)
		if withOps {
			f.mkfile(t, "a")
		}
		act, err := mgr.step2(f.ctx, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		anchors := f.ch.kinds(f.keys, wire.KindAnchor)
		if len(anchors) != 1 {
			t.Fatalf("ops=%v: anchors %v, want one", withOps, anchors)
		}
		a := f.anchorOn(t, anchors[0])
		if !withOps {
			if act != ActAnchor || int64(a.SnapMsg) != oldSnap || int64(a.JnlAfter) != oldSnap {
				t.Fatalf("no ops: %s, anchor %+v; want an anchor for snapshot %d bounded by its own id", act, a, oldSnap)
			}
			if n := len(f.ch.kinds(f.keys, wire.KindSnapshot)); n != 1 {
				t.Fatalf("no ops: %d snapshots, want no new one", n)
			}
		} else if act != ActSnapshot || int64(a.SnapMsg) == oldSnap {
			t.Fatalf("ops: %s, anchor %+v; want a new snapshot", act, a)
		}
	}
}

// Retention never deletes the snapshot the posted anchor names, even
// when later anchors could not be posted.
func TestRetentionKeepsAnchoredSnapshot(t *testing.T) {
	f := newFS2(t)
	mgr := f.manager()
	mgr.Logger = quietLogger()
	mgr.Retention = 2
	if err := mgr.Once(f.ctx); err != nil {
		t.Fatal(err)
	}
	anchored := int(f.kv(t, KVCurrentMsgID))
	anchor := int(f.kv(t, KVAnchorMsgID))
	f.ch.failPin = true // every later anchor fails
	var later []int
	for i := 0; i < 3; i++ {
		f.mkfile(t, string(rune('a'+i)))
		if err := mgr.Once(f.ctx); err != nil {
			t.Fatalf("snapshot %d: %v (a failed anchor must not fail the snapshot)", i, err)
		}
		later = append(later, int(f.kv(t, KVCurrentMsgID)))
	}
	if _, ok := f.ch.get(anchored); !ok {
		t.Fatal("retention deleted the snapshot the anchor names")
	}
	if _, ok := f.ch.get(later[0]); ok {
		t.Fatalf("snapshot %d is past retention and not anchored — should be gone", later[0])
	}
	if got := f.ch.kinds(f.keys, wire.KindAnchor); len(got) != 1 || got[0] != anchor {
		t.Fatalf("anchors %v: the unpinned ones must be deleted, the old one kept (want %d)", got, anchor)
	}
	if got := f.kv(t, KVAnchorSnapID); got != int64(anchored) {
		t.Fatalf("anchor_snap_id %d, want %d", got, anchored)
	}
	// The policy re-posts the anchor once pinning works again.
	f.ch.failPin = false
	if act, err := mgr.step2(f.ctx, time.Now()); err != nil || act != ActAnchor {
		t.Fatalf("anchor retry: %s %v", act, err)
	}
	if got := f.kv(t, KVAnchorSnapID); got != int64(later[2]) {
		t.Fatalf("anchor names %d, want the newest snapshot %d", got, later[2])
	}
	if _, ok := f.ch.get(anchor); ok {
		t.Fatal("the replaced anchor must be deleted")
	}
}

// Format 1 keeps its own cadence: Synced still means "a snapshot this
// process took covers the DB" (format 2: always, the journal suffices),
// and Once's skip check is the old one — no policy, no anchor.
func TestFormat1Unchanged(t *testing.T) {
	f := newFS2(t)
	f1 := &Manager{Meta: f.m, Policy: Policy{EveryOps: 1}} // Keys nil: format 1
	if f1.Synced(f.ctx) {
		t.Fatal("format 1: Synced before any snapshot of this process")
	}
	if !f.manager().Synced(f.ctx) {
		t.Fatal("format 2: Synced must not wait for a snapshot")
	}
	// …unless one is owed (a setting only a snapshot carries).
	if err := MarkDue(f.ctx, f.m, "trash settings changed"); err != nil {
		t.Fatal(err)
	}
	if f.manager().Synced(f.ctx) {
		t.Fatal("format 2: not synced while a snapshot is owed")
	}
	if err := f.m.DeleteKV(f.ctx, KVSnapDue); err != nil {
		t.Fatal(err)
	}
	seq, _ := f.m.LastJournalSeq(f.ctx)
	if err := storeCurrentJournalSeq(f.ctx, f.m, seq); err != nil {
		t.Fatal(err)
	}
	f1.setBaseline(f.m.WriteCounter())
	// Nothing changed since: the format-1 skip returns before touching
	// the (nil) session.
	if err := f1.Once(f.ctx); err != nil {
		t.Fatal(err)
	}
	if !f1.Synced(f.ctx) {
		t.Fatal("format 1: Synced after its baseline")
	}
	if n := len(f.ch.kinds(f.keys, wire.KindAnchor)); n != 0 {
		t.Fatal("format 1 posts no anchor")
	}
}
