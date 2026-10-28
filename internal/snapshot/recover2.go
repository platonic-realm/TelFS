package snapshot

import (
	"context"
	"errors"
	"fmt"
	"time"

	"telfs/internal/meta"
	"telfs/internal/tg"
	"telfs/internal/wire"
)

// Located2 is the snapshot a format-2 recovery starts from.
type Located2 struct {
	SnapMsg int
	SnapSeq int64
	SnapTS  int64
	// JnlAfter is the anchor's journal boundary (0 when the snapshot was
	// found by scanning).
	JnlAfter int
	// Anchor is the pinned anchor that named the snapshot; nil when the
	// snapshot was found by scanning (a filesystem from before v1, or an
	// anchor whose snapshot could not be read).
	Anchor *AnchorFound
	// StaleAnchor is the pinned anchor whose snapshot could not be read
	// (0: none) — recorded so the next anchor replaces it.
	StaleAnchor int
}

// Fetched2 is what a format-2 recovery reads from the channel.
type Fetched2 struct {
	Located2
	DB      []byte                 // the snapshot: a gzipped SQLite DB
	Journal []tg.JournalBatchEntry // every op after it, ascending
}

// ErrNoSnapshot2: the key header opened, but no snapshot of the
// filesystem was found.
var ErrNoSnapshot2 = errors.New("no snapshot of this filesystem found")

// Fetch2 reads what a recovery needs. With a pinned anchor (filesystems
// since v1): the anchor's snapshot, then only the journal batches posted
// after the anchor's boundary — no history scan. Without one: the newest
// snapshot and the journal by scanning, as before v1. An anchor whose
// snapshot is gone from the channel or does not decrypt falls back to
// the scan (said through logf); a failure to download it (the network,
// Telegram) is returned as it is, so a boot mount waits and retries the
// anchor's way instead of scanning a history it may not reach the
// snapshot in. pins is the channel's pinned list.
func Fetch2(ctx context.Context, ch Channel, keys *wire.Keys, pins []tg.ChannelMessage, logf func(string, ...any)) (*Fetched2, error) {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	anc, err := FindAnchor(ctx, ch, keys, pins)
	if err != nil {
		return nil, err
	}
	stale := 0
	if anc != nil {
		a := anc.Anchor
		logf("Anchor (msg %d): snapshot msg=%d (journal seq %d, %s)", anc.MsgID, a.SnapMsg, a.SnapSeq,
			time.Unix(a.SnapTS, 0).UTC().Format(time.RFC3339))
		db, derr := DownloadSnapshot2(ctx, ch, keys, a.SnapMsg)
		if derr == nil {
			ops, jerr := ListJournal2After(ctx, ch, keys, a.SnapSeq, a.JnlAfter)
			if jerr != nil {
				return nil, fmt.Errorf("journal: %w", jerr)
			}
			return &Fetched2{Located2: Located2{SnapMsg: a.SnapMsg, SnapSeq: a.SnapSeq, SnapTS: a.SnapTS,
				JnlAfter: a.JnlAfter, Anchor: anc}, DB: db, Journal: ops}, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !errors.Is(derr, tg.ErrMessageGone) && !errors.Is(derr, ErrSnapshotUnreadable) {
			return nil, fmt.Errorf("the anchor's snapshot %d: %w", a.SnapMsg, derr)
		}
		logf("warning: the anchor's snapshot %d can't be read (%v) — scanning the channel for the newest snapshot instead", a.SnapMsg, derr)
		stale = anc.MsgID
	}
	snap, err := FindLatestSnapshot2(ctx, ch, keys)
	if err != nil {
		return nil, err
	}
	if snap == nil {
		return nil, fmt.Errorf("%w in the newest %d messages", ErrNoSnapshot2, ScanPages*100)
	}
	logf("Found snapshot msg=%d (journal seq %d, %s)", snap.MsgID, snap.Caption.Seq,
		time.Unix(snap.Caption.TS, 0).UTC().Format(time.RFC3339))
	db, err := DownloadSnapshot2(ctx, ch, keys, snap.MsgID)
	if err != nil {
		return nil, err
	}
	ops, err := ListJournal2(ctx, ch, keys, snap.Caption.Seq)
	if err != nil {
		return nil, fmt.Errorf("journal: %w", err)
	}
	return &Fetched2{Located2: Located2{SnapMsg: snap.MsgID, SnapSeq: snap.Caption.Seq, SnapTS: snap.Caption.TS,
		StaleAnchor: stale}, DB: db, Journal: ops}, nil
}

// RecordRecovered makes a DB just restored from loc (and its journal
// replayed) describe the channel. A snapshot's DB is copied before its
// own record is written, so it names the snapshot BEFORE it, that one's
// anchor and possibly a pending snap_due: without this the policy would
// measure from the wrong snapshot. With an anchor, the version registry
// is the anchor's list (without one, the snapshot's registry stays).
// One transaction: a half-written record would pair one snapshot with
// another's seq.
func RecordRecovered(ctx context.Context, m *meta.Store, loc Located2) error {
	ints := map[string]int64{KVCurrentMsgID: int64(loc.SnapMsg), KVCurrentJournalSeq: loc.SnapSeq}
	if loc.SnapTS > 0 {
		ints[KVLastSnapUnix] = loc.SnapTS
	}
	drop := []string{KVSnapDue, KVAnchorDue}
	var versions []byte
	if loc.Anchor != nil {
		ints[KVSnapJnlAfter] = int64(loc.JnlAfter)
		ints[KVAnchorMsgID] = int64(loc.Anchor.MsgID)
		ints[KVAnchorSnapID] = int64(loc.SnapMsg)
		ints[KVAnchorSnapSeq] = loc.SnapSeq
		// The anchor is re-posted after every version change; the
		// snapshot's own registry is as old as the snapshot.
		b, err := EncodeVersions(loc.Anchor.Anchor.Versions)
		if err != nil {
			return err
		}
		versions = b
	} else {
		// Found by scanning: no boundary known (the policy posts an
		// anchor, or takes one snapshot, at the next mount).
		drop = append(drop, KVSnapJnlAfter, KVAnchorSnapID, KVAnchorSnapSeq)
		if loc.StaleAnchor > 0 {
			ints[KVAnchorMsgID] = int64(loc.StaleAnchor)
		} else {
			drop = append(drop, KVAnchorMsgID)
		}
	}
	return m.UpdateKV(ctx, func(k *meta.KVTx) error {
		for key, v := range ints {
			if err := putInt(k, key, v); err != nil {
				return err
			}
		}
		for _, key := range drop {
			if err := k.Delete(key); err != nil {
				return err
			}
		}
		if versions != nil {
			if err := k.Put(KVVersions, versions); err != nil {
				return err
			}
		}
		return RecordSnapIDTx(k, loc.SnapMsg)
	})
}
