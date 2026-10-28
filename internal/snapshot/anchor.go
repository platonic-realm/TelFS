package snapshot

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"

	"telfs/internal/meta"
	"telfs/internal/tg"
	"telfs/internal/wire"
)

// The anchor (spec v1 §2): a pinned, encrypted document naming the
// newest snapshot, so a recovery finds it through the pinned list
// (messages.search, pinned filter) instead of scanning history. Format 2
// only. The local DB records what the channel's anchor says, so the
// daemon knows when a new one is owed.
const (
	// KVAnchorMsgID is the message id of the anchor this filesystem
	// posted last (replaced = deleted when the next one is pinned).
	KVAnchorMsgID = "anchor_msg_id"
	// KVAnchorSnapID is the snapshot that anchor names. Retention and
	// gc never delete it.
	KVAnchorSnapID = "anchor_snap_id"
	// KVSnapJnlAfter is the newest snapshot's journal boundary (the
	// anchor's jnl_after): the channel's newest message id read just
	// before the snapshot was taken. Missing on snapshots from before v1.
	KVSnapJnlAfter = "snap_jnl_after"
	// KVSnapDue, when set, says the channel's snapshot chain does not
	// describe the local DB (e.g. after `telfs snapshot restore`): the
	// next mount takes a snapshot at once. Its value is the reason.
	KVSnapDue = "snap_due"
)

// AnchorFound is a pinned anchor of this filesystem.
type AnchorFound struct {
	MsgID  int
	Anchor wire.Anchor
}

// pinnedAnchorIDs returns the pinned messages that are anchors of the
// filesystem keys belong to, newest first.
func pinnedAnchorIDs(keys *wire.Keys, pins []tg.ChannelMessage) []int {
	var ids []int
	for _, p := range pins {
		if !p.HasDoc || !wire.LooksLikeCaption(p.RawCaption) {
			continue
		}
		if c, err := keys.OpenCaption(p.RawCaption); err == nil && c.K == wire.KindAnchor {
			ids = append(ids, p.ID)
		}
	}
	sort.Sort(sort.Reverse(sort.IntSlice(ids)))
	return ids
}

// hasPinnedDocCaption reports a pinned document with a format-2 caption:
// on a TelFS channel, an anchor (readable only with the keys).
func hasPinnedDocCaption(pins []tg.ChannelMessage) bool {
	for _, p := range pins {
		if p.HasDoc && wire.LooksLikeCaption(p.RawCaption) {
			return true
		}
	}
	return false
}

// FindAnchor returns the newest pinned anchor of the filesystem keys
// belong to; nil without error when none is pinned (a filesystem from
// before v1). An anchor that is pinned but can't be read is an error —
// a recovery must not silently fall back to an older path.
func FindAnchor(ctx context.Context, ch Channel, keys *wire.Keys, pins []tg.ChannelMessage) (*AnchorFound, error) {
	var errs []error
	for _, id := range pinnedAnchorIDs(keys, pins) {
		var buf bytes.Buffer
		if _, err := ch.DownloadDocument(ctx, id, &buf); err != nil {
			errs = append(errs, fmt.Errorf("download anchor %d: %w", id, err))
			continue
		}
		a, err := keys.OpenAnchor(buf.Bytes())
		if err != nil {
			errs = append(errs, fmt.Errorf("anchor %d: %w", id, err))
			continue
		}
		return &AnchorFound{MsgID: id, Anchor: a}, nil
	}
	return nil, errors.Join(errs...)
}

// kvInt reads an integer meta_kv value (0 when missing or malformed).
func kvInt(ctx context.Context, m *meta.Store, key string) int64 {
	v, err := m.GetKV(ctx, key)
	if err != nil {
		return 0
	}
	n, err := strconv.ParseInt(string(v), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func putKVInt(ctx context.Context, m *meta.Store, key string, n int64) error {
	return m.PutKV(ctx, key, []byte(strconv.FormatInt(n, 10)))
}

type kvInt64 struct {
	key string
	v   int64
}

func putKVInts(ctx context.Context, m *meta.Store, kvs ...kvInt64) error {
	for _, kv := range kvs {
		if err := putKVInt(ctx, m, kv.key, kv.v); err != nil {
			return err
		}
	}
	return nil
}

// anchorFromDB builds the anchor for the snapshot recorded in m.
func anchorFromDB(ctx context.Context, m *meta.Store, fsUUID string) (wire.Anchor, error) {
	a := wire.Anchor{V: wire.AnchorVersionV1, FS: fsUUID, Versions: []wire.AnchorVersion{}}
	a.SnapMsg = int(kvInt(ctx, m, KVCurrentMsgID))
	if a.SnapMsg <= 0 {
		return a, errors.New("anchor: no snapshot recorded")
	}
	if _, err := m.GetKV(ctx, KVSnapJnlAfter); err != nil {
		return a, fmt.Errorf("anchor: snapshot %d has no journal boundary recorded", a.SnapMsg)
	}
	a.JnlAfter = int(kvInt(ctx, m, KVSnapJnlAfter))
	a.SnapSeq = kvInt(ctx, m, KVCurrentJournalSeq)
	a.SnapTS = kvInt(ctx, m, KVLastSnapUnix)
	vs, err := LoadVersions(ctx, m)
	if err != nil {
		return a, err
	}
	if vs != nil {
		a.Versions = vs
	}
	return a, nil
}

// anchorMu serializes PostAnchor in this process: the snapshot policy and
// a version change may both post one, and each deletes the anchors it
// replaces — unserialized, one could delete the other's new anchor, or
// the last anchor posted could carry an older version list.
var anchorMu sync.Mutex

// PostAnchor posts an anchor naming the snapshot recorded in m and
// listing the version registry, pins it
// silently and records it; then it deletes the anchors it replaces: the
// one recorded before and any other pinned anchor of this filesystem
// (left by a crash between pin and record). Deleting a message also
// removes its pin; a delete that fails leaves the old anchor pinned, so
// the next post retries it — a recovery always takes the newest anchor.
// The returned error is about the new anchor only; a failed cleanup is
// reported through logf.
func PostAnchor(ctx context.Context, ch Channel, m *meta.Store, keys *wire.Keys, logf func(string, ...any)) (int, error) {
	anchorMu.Lock()
	defer anchorMu.Unlock()
	a, err := anchorFromDB(ctx, m, keys.FSUUID)
	if err != nil {
		return 0, err
	}
	body, capt, err := keys.SealAnchor(a)
	if err != nil {
		return 0, err
	}
	id, err := ch.UploadDocument(ctx, bytes.NewReader(body), wire.DocName, capt)
	if err != nil {
		return 0, fmt.Errorf("post anchor: %w", err)
	}
	if err := ch.PinMessage(ctx, id); err != nil {
		_ = ch.DeleteChannelMessages(ctx, id) // an unpinned anchor finds nothing
		return 0, fmt.Errorf("pin anchor: %w", err)
	}
	prev := int(kvInt(ctx, m, KVAnchorMsgID))
	if err := putKVInt(ctx, m, KVAnchorMsgID, int64(id)); err != nil {
		return id, err
	}
	if err := putKVInt(ctx, m, KVAnchorSnapID, int64(a.SnapMsg)); err != nil {
		return id, err
	}
	if err := m.DeleteKV(ctx, KVAnchorDue); err != nil && !errors.Is(err, meta.ErrNotFound) {
		return id, err
	}
	old := map[int]bool{}
	if prev > 0 && prev != id {
		old[prev] = true
	}
	if pins, err := ch.PinnedMessages(ctx); err == nil {
		for _, pid := range pinnedAnchorIDs(keys, pins) {
			if pid != id {
				old[pid] = true
			}
		}
	}
	if len(old) > 0 {
		ids := make([]int, 0, len(old))
		for x := range old {
			ids = append(ids, x)
		}
		sort.Ints(ids)
		if err := ch.DeleteChannelMessages(ctx, ids...); err != nil && logf != nil {
			logf("[snapshot] old anchor(s) %v not deleted (retried with the next anchor): %v", ids, err)
		}
	}
	return id, nil
}
