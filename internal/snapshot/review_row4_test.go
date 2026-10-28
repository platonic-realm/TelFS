package snapshot

import (
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"

	"telfs/internal/meta"
	"telfs/internal/wire"
)

// A snapshot clears the snap_due mark it was taken for, and only that
// one: a setting changed (and marked) while the snapshot was being
// uploaded is not in it, so its mark stays for the next snapshot. The
// record names the snapshot, its seq, time and journal boundary together.
func TestOnce2ClearsOnlyTheMarkItCovers(t *testing.T) {
	f := newFS2(t)
	mgr := f.manager()
	mgr.Logger = quietLogger()
	if err := MarkDue(f.ctx, f.m, "trash settings changed"); err != nil {
		t.Fatal(err)
	}
	if Owed(f.ctx, f.m) == "" {
		t.Fatal("a marked snapshot must be owed")
	}
	if err := mgr.Once(f.ctx); err != nil {
		t.Fatal(err)
	}
	if why := Owed(f.ctx, f.m); why != "" {
		t.Fatalf("the snapshot covers the mark, still owed: %q", why)
	}
	// A change marked while the next snapshot uploads (after it was taken).
	f.mkfile(t, "a")
	f.ch.onUpload = func(string) {
		if err := MarkDue(f.ctx, f.m, "rate-limit settings changed"); err != nil {
			t.Error(err)
		}
	}
	if err := mgr.Once(f.ctx); err != nil {
		t.Fatal(err)
	}
	if why := Owed(f.ctx, f.m); !strings.Contains(why, "rate-limit") {
		t.Fatalf("a mark made after the snapshot was taken must stay, got %q", why)
	}
	snaps := f.ch.kinds(f.keys, wire.KindSnapshot)
	seq, _ := f.m.LastJournalSeq(f.ctx)
	if f.kv(t, KVCurrentMsgID) != int64(snaps[0]) || f.kv(t, KVCurrentJournalSeq) != seq || f.kv(t, KVAnchorSnapSeq) != seq {
		t.Fatalf("record: msg %d seq %d anchor seq %d; want %d, %d, %d", f.kv(t, KVCurrentMsgID),
			f.kv(t, KVCurrentJournalSeq), f.kv(t, KVAnchorSnapSeq), snaps[0], seq, seq)
	}
	// Format 1 is never marked.
	f.m.PutKV(f.ctx, meta.KVFormat, []byte("1"))
	f.m.DeleteKV(f.ctx, KVSnapDue)
	if err := MarkDue(f.ctx, f.m, "x"); err != nil || Owed(f.ctx, f.m) != "" {
		t.Fatalf("format 1 marked: %v %q", err, Owed(f.ctx, f.m))
	}
}

// A failure to download the anchored snapshot (the network, Telegram) is
// returned as it is — a boot mount retries it — instead of falling back
// to a history scan that cannot reach the snapshot at backup scale.
func TestFetch2TransientSnapshotErrorIsReturned(t *testing.T) {
	f := newFS2(t)
	mgr := f.manager()
	mgr.Logger = quietLogger()
	if err := mgr.Once(f.ctx); err != nil {
		t.Fatal(err)
	}
	snap := int(f.kv(t, KVCurrentMsgID))
	f.ch.failDownload = map[int]error{snap: fmt.Errorf("download: %w", syscall.ECONNRESET)}
	scans := f.ch.fullWalks
	pins, _ := f.ch.PinnedMessages(f.ctx)
	_, err := Fetch2(f.ctx, f.ch, f.keys, pins, nil)
	if !errors.Is(err, syscall.ECONNRESET) || errors.Is(err, ErrNoSnapshot2) {
		t.Fatalf("want the network error itself, got %v", err)
	}
	if f.ch.fullWalks != scans {
		t.Fatal("a transient failure must not fall back to scanning history")
	}
	// Reachable again: the anchor's way works.
	f.ch.failDownload = nil
	if got, err := Fetch2(f.ctx, f.ch, f.keys, pins, nil); err != nil || got.Anchor == nil || got.SnapMsg != snap {
		t.Fatalf("after the outage: %+v %v", got, err)
	}
}

// A snapshot that is there but does not decrypt falls back to the scan.
func TestFetch2UndecryptableSnapshotFallsBack(t *testing.T) {
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
	old, _ := f.ch.get(int(f.kv(t, KVAnchorSnapID)))
	old.body = []byte("not a snapshot")
	pins, _ := f.ch.PinnedMessages(f.ctx)
	got, err := Fetch2(f.ctx, f.ch, f.keys, pins, nil)
	if err != nil || got.Anchor != nil || got.SnapMsg != newest {
		t.Fatalf("want the scan's snapshot %d, got %+v %v", newest, got, err)
	}
	if _, err := DownloadSnapshot2(f.ctx, f.ch, f.keys, old.id); !errors.Is(err, ErrSnapshotUnreadable) {
		t.Fatalf("undecryptable: %v", err)
	}
}

// SnapshotAndAnchor fails unless the anchor naming the new snapshot is
// pinned (`telfs snapshot restore` must not leave the old anchor).
func TestSnapshotAndAnchor(t *testing.T) {
	f := newFS2(t)
	mgr := f.manager()
	mgr.Logger = quietLogger()
	f.ch.failPin = true
	if _, err := mgr.SnapshotAndAnchor(f.ctx); err == nil || !strings.Contains(err.Error(), "anchor") {
		t.Fatalf("pin refused: want an anchor error, got %v", err)
	}
	f.ch.failPin = false
	id, err := mgr.SnapshotAndAnchor(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	anchors := f.ch.kinds(f.keys, wire.KindAnchor)
	if len(anchors) != 1 || f.anchorOn(t, anchors[0]).SnapMsg != id {
		t.Fatalf("anchors %v must name snapshot %d", anchors, id)
	}
}

// A recovery through an anchor older than the local DB's journal refuses
// at a deliberate seq gap instead of replaying two states as one (what
// `telfs snapshot restore` and `recover --from-captions` leave).
func TestStaleAnchorRefusesAtTheGap(t *testing.T) {
	f := newFS2(t)
	mgr := f.manager()
	mgr.Logger = quietLogger()
	if err := mgr.Once(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.mkfile(t, "abandoned")
	f.post(t)
	last, _ := f.m.LastJournalSeq(f.ctx)
	if err := f.m.UpdateKV(f.ctx, func(k *meta.KVTx) error { return k.AdvanceJournalSeq(last + 1) }); err != nil {
		t.Fatal(err)
	}
	f.mkfile(t, "new")
	f.post(t)
	pins, _ := f.ch.PinnedMessages(f.ctx)
	if _, err := Fetch2(f.ctx, f.ch, f.keys, pins, nil); err == nil || !strings.Contains(err.Error(), "hole") {
		t.Fatalf("want a refusal at the gap, got %v", err)
	}
}

// RecordRecovered writes the whole record (anchor seq included) at once.
func TestRecordRecoveredAnchorSeq(t *testing.T) {
	f := newFS2(t)
	loc := Located2{SnapMsg: 77, SnapSeq: 12, SnapTS: 5, JnlAfter: 70,
		Anchor: &AnchorFound{MsgID: 80, Anchor: wire.Anchor{Versions: []wire.AnchorVersion{{Name: "v", Msg: 9}}}}}
	if err := MarkDue(f.ctx, f.m, "old mark"); err != nil {
		t.Fatal(err)
	}
	if err := RecordRecovered(f.ctx, f.m, loc); err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]int64{KVCurrentMsgID: 77, KVCurrentJournalSeq: 12, KVLastSnapUnix: 5,
		KVSnapJnlAfter: 70, KVAnchorMsgID: 80, KVAnchorSnapID: 77, KVAnchorSnapSeq: 12} {
		if got := f.kv(t, k); got != want {
			t.Errorf("%s = %d, want %d", k, got, want)
		}
	}
	if Owed(f.ctx, f.m) != "" {
		t.Error("a recovered DB owes no snapshot")
	}
	if vs, _ := LoadVersions(f.ctx, f.m); len(vs) != 1 || vs[0].Name != "v" {
		t.Errorf("registry %+v", vs)
	}
	if ids := loadSnapIDs(f.ctx, f.m); len(ids) != 1 || ids[0] != 77 {
		t.Errorf("snapshot ids %v", ids)
	}
}
