package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"telfs/internal/meta"
	"telfs/internal/tg"
	"telfs/internal/wire"
)

// Channel side of format 2 (spec P1 §4): the key message, snapshots and
// journal batches whose captions are encrypted under the FS's caption
// key. A format-2 message is recognized only by decrypting its caption.

// ScanPages bounds history scans (newest first, 100 messages a page):
// 200 pages = 20,000 messages, as format-1 recovery. Since v1 the
// snapshot is found through the pinned anchor and the key message
// through the pinned list; the scans remain for filesystems from before
// v1 (no anchor yet).
const ScanPages = 200

// KeyFound is a key message located on the channel.
type KeyFound struct {
	MsgID  int
	Header wire.KeyHeader
	JSON   []byte // exact JSON (byte-compared by status/fsck)
	Pinned bool
}

// PinnedKeyMessages returns the key messages among the pinned messages,
// newest first.
func PinnedKeyMessages(pins []tg.ChannelMessage) []KeyFound {
	var out []KeyFound
	for _, p := range pins {
		if p.HasDoc || !wire.IsKeyMessage(p.RawCaption) {
			continue
		}
		if h, js, err := wire.ParseKeyMessage(p.RawCaption); err == nil {
			out = append(out, KeyFound{MsgID: p.ID, Header: h, JSON: js, Pinned: true})
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].MsgID > out[j].MsgID })
	return out
}

// FindKeyMessages returns the channel's key messages: the pinned ones
// first (newest first), then any others found newest-first in the last
// ScanPages pages. The scan is skipped when a key message AND an anchor
// are pinned (a filesystem since v1): finding the filesystem must not
// depend on scanning history.
func FindKeyMessages(ctx context.Context, ch Channel) ([]KeyFound, error) {
	pins, err := ch.PinnedMessages(ctx)
	if err != nil {
		return nil, err
	}
	return FindKeyMessagesIn(ctx, ch, pins)
}

// FindKeyMessagesIn is FindKeyMessages with the pinned list already read.
func FindKeyMessagesIn(ctx context.Context, ch Channel, pins []tg.ChannelMessage) ([]KeyFound, error) {
	out := PinnedKeyMessages(pins)
	if len(out) > 0 && hasPinnedDocCaption(pins) {
		return out, nil
	}
	pinned := map[int]bool{}
	for _, k := range out {
		pinned[k.MsgID] = true
	}
	err := ch.WalkChannelMessages(ctx, ScanPages, func(cm tg.ChannelMessage) error {
		if cm.HasDoc || pinned[cm.ID] || !wire.IsKeyMessage(cm.RawCaption) {
			return nil
		}
		if h, js, perr := wire.ParseKeyMessage(cm.RawCaption); perr == nil {
			out = append(out, KeyFound{MsgID: cm.ID, Header: h, JSON: js})
		}
		return nil
	})
	return out, err
}

// UnlockAny tries the found key messages in order with the secret and
// returns the first that opens: its DEK and fs_uuid.
func UnlockAny(found []KeyFound, secret []byte) (KeyFound, []byte, string, error) {
	var lastErr error = errors.New("no key header on the channel")
	for _, k := range found {
		dek, fs, err := k.Header.Unlock(secret)
		if err == nil {
			return k, dek, fs, nil
		}
		lastErr = err
	}
	return KeyFound{}, nil, "", lastErr
}

// PostKeyMessage posts the key message and pins it.
func PostKeyMessage(ctx context.Context, ch Channel, h wire.KeyHeader) (int, error) {
	id, err := ch.PostMessage(ctx, h.Message())
	if err != nil {
		return 0, fmt.Errorf("post key message: %w", err)
	}
	if err := ch.PinMessage(ctx, id); err != nil {
		return id, fmt.Errorf("pin key message: %w", err)
	}
	return id, nil
}

// Found2 is a format-2 document located by caption.
type Found2 struct {
	MsgID   int
	Caption wire.Caption
}

// FindLatestSnapshot2 scans newest-first for the newest snapshot of the
// filesystem the keys belong to (filesystems without an anchor).
func FindLatestSnapshot2(ctx context.Context, ch Channel, keys *wire.Keys) (*Found2, error) {
	var found *Found2
	err := ch.WalkChannelMessages(ctx, ScanPages, func(cm tg.ChannelMessage) error {
		if !cm.HasDoc || !wire.LooksLikeCaption(cm.RawCaption) {
			return nil
		}
		c, err := keys.OpenCaption(cm.RawCaption)
		if err != nil || c.K != wire.KindSnapshot {
			return nil
		}
		found = &Found2{MsgID: cm.ID, Caption: c}
		return tg.ErrStopWalk
	})
	return found, err
}

// ErrSnapshotUnreadable: a snapshot message was downloaded and does not
// open under the filesystem's keys (damaged, or not a snapshot).
var ErrSnapshotUnreadable = errors.New("the snapshot does not decrypt")

// DownloadSnapshot2 fetches and decrypts a snapshot body (gzipped DB). A
// message that is gone wraps tg.ErrMessageGone, one that does not open
// ErrSnapshotUnreadable; anything else is a failure to fetch it.
func DownloadSnapshot2(ctx context.Context, ch Channel, keys *wire.Keys, msgID int) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := ch.DownloadDocument(ctx, msgID, &buf); err != nil {
		return nil, fmt.Errorf("download snapshot %d: %w", msgID, err)
	}
	db, err := keys.OpenMeta("snap", buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("%w: snapshot %d: %w", ErrSnapshotUnreadable, msgID, err)
	}
	return db, nil
}

// journalBody2 is the plaintext of a format-2 journal batch.
type journalBody2 struct {
	Ops []tg.JournalBatchEntry `json:"ops"`
}

// ListJournal2 returns every journal op with seq > sinceSeq, ascending.
// Like the format-1 scan it must reach the sinceSeq boundary (or the
// start of the channel): a missing prefix would replay later ops onto
// the wrong state, so a scan cut short is an error, never a silent
// truncation. Filesystems without an anchor; ListJournal2After is the
// anchor's way.
func ListJournal2(ctx context.Context, ch Channel, keys *wire.Keys, sinceSeq int64) ([]tg.JournalBatchEntry, error) {
	var out []tg.JournalBatchEntry
	boundary := false
	const pages = 2000 // runaway guard (200k messages), as format 1
	exhausted, err := ch.WalkChannelMessagesExhaust(ctx, pages, func(cm tg.ChannelMessage) error {
		if !cm.HasDoc || !wire.LooksLikeCaption(cm.RawCaption) {
			return nil
		}
		c, err := keys.OpenCaption(cm.RawCaption)
		if err != nil || c.K != wire.KindJournal {
			return nil
		}
		if c.Last <= sinceSeq {
			boundary = true
			return tg.ErrStopWalk
		}
		ops, err := DownloadJournal2(ctx, ch, keys, cm.ID)
		if err != nil {
			return err
		}
		for _, e := range ops {
			if e.Seq > sinceSeq {
				out = append(out, e)
			}
		}
		if c.First <= sinceSeq+1 {
			boundary = true
			return tg.ErrStopWalk
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !boundary && !exhausted {
		return nil, fmt.Errorf("journal scan hit its %d-page cap before reaching seq %d — refusing a truncated replay set", pages, sinceSeq+1)
	}
	return sortDedupe(out), nil
}

// sortDedupe sorts ops by seq and drops repeats (a batch can be
// re-posted after a lost ack).
func sortDedupe(out []tg.JournalBatchEntry) []tg.JournalBatchEntry {
	sort.SliceStable(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	dedup := out[:0]
	var last int64 = -1
	for _, e := range out {
		if e.Seq != last {
			dedup = append(dedup, e)
			last = e.Seq
		}
	}
	return dedup
}

// ListJournal2After returns every journal op with seq > sinceSeq from the
// batches posted after message afterMsgID (an anchor's jnl_after),
// ascending. It reads only the history after that message, never the
// whole channel. The ops must continue the snapshot without a hole: a
// missing batch would replay later ops onto the wrong state.
func ListJournal2After(ctx context.Context, ch Channel, keys *wire.Keys, sinceSeq int64, afterMsgID int) ([]tg.JournalBatchEntry, error) {
	var out []tg.JournalBatchEntry
	err := ch.WalkChannelMessagesAfter(ctx, afterMsgID, func(cm tg.ChannelMessage) error {
		if !cm.HasDoc || !wire.LooksLikeCaption(cm.RawCaption) {
			return nil
		}
		c, err := keys.OpenCaption(cm.RawCaption)
		if err != nil || c.K != wire.KindJournal || c.Last <= sinceSeq {
			return nil
		}
		ops, err := DownloadJournal2(ctx, ch, keys, cm.ID)
		if err != nil {
			return err
		}
		for _, e := range ops {
			if e.Seq > sinceSeq {
				out = append(out, e)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out = sortDedupe(out)
	next := sinceSeq + 1
	for _, e := range out {
		if e.Seq != next {
			return nil, fmt.Errorf("the journal after the snapshot (seq %d) has a hole: seq %d is missing (next found: %d) — refusing a replay with a gap; `telfs recover --from-captions` rebuilds without it", sinceSeq, next, e.Seq)
		}
		next++
	}
	return out, nil
}

// DownloadJournal2 fetches and decrypts one journal batch.
func DownloadJournal2(ctx context.Context, ch Channel, keys *wire.Keys, msgID int) ([]tg.JournalBatchEntry, error) {
	var buf bytes.Buffer
	if _, err := ch.DownloadDocument(ctx, msgID, &buf); err != nil {
		return nil, fmt.Errorf("download journal batch %d: %w", msgID, err)
	}
	sealed, err := keys.OpenMeta("journal", buf.Bytes())
	if err != nil {
		return nil, err
	}
	plain, err := wire.UnpackJournal(sealed)
	if err != nil {
		return nil, fmt.Errorf("journal batch %d: %w", msgID, err)
	}
	var body journalBody2
	if err := json.Unmarshal(plain, &body); err != nil {
		return nil, fmt.Errorf("journal batch %d: %w", msgID, err)
	}
	return body.Ops, nil
}

// UploadJournal2 posts one encrypted journal batch.
func UploadJournal2(ctx context.Context, ch Channel, keys *wire.Keys, ops []tg.JournalBatchEntry) (int, error) {
	if len(ops) == 0 {
		return 0, errors.New("empty journal batch")
	}
	js, err := json.Marshal(journalBody2{Ops: ops})
	if err != nil {
		return 0, err
	}
	packed, err := wire.PackJournal(js)
	if err != nil {
		return 0, err
	}
	body, err := keys.SealMeta("journal", packed)
	if err != nil {
		return 0, err
	}
	capt, err := keys.SealCaption(wire.Caption{K: wire.KindJournal, First: ops[0].Seq, Last: ops[len(ops)-1].Seq,
		N: len(ops), TS: time.Now().Unix()})
	if err != nil {
		return 0, err
	}
	return ch.UploadDocument(ctx, bytes.NewReader(body), wire.DocName, capt)
}

// KVSnapMsgIDs lists (comma-separated, oldest first) the format-2
// snapshot messages this filesystem posted, so retention never needs a
// channel scan.
const KVSnapMsgIDs = "snap_msg_ids"

func loadSnapIDs(ctx context.Context, m *meta.Store) []int {
	v, err := m.GetKV(ctx, KVSnapMsgIDs)
	if err != nil {
		return nil
	}
	return parseSnapIDs(v)
}

func parseSnapIDs(v []byte) []int {
	var out []int
	for _, f := range strings.Split(string(v), ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(f)); err == nil && n > 0 {
			out = append(out, n)
		}
	}
	return out
}

func formatSnapIDs(ids []int) []byte {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.Itoa(id)
	}
	return []byte(strings.Join(parts, ","))
}

func storeSnapIDs(ctx context.Context, m *meta.Store, ids []int) error {
	return m.PutKV(ctx, KVSnapMsgIDs, formatSnapIDs(ids))
}

// RecordSnapID appends a snapshot message id (recovery uses it to note
// the snapshot it restored, which that snapshot can't contain itself).
func RecordSnapID(ctx context.Context, m *meta.Store, id int) error {
	return m.UpdateKV(ctx, func(k *meta.KVTx) error { return RecordSnapIDTx(k, id) })
}

// RecordSnapIDTx is RecordSnapID inside a meta_kv transaction.
func RecordSnapIDTx(k *meta.KVTx, id int) error {
	v, err := k.Get(KVSnapMsgIDs)
	if err != nil && !errors.Is(err, meta.ErrNotFound) {
		return err
	}
	ids := parseSnapIDs(v)
	for _, x := range ids {
		if x == id {
			return nil
		}
	}
	return k.Put(KVSnapMsgIDs, formatSnapIDs(append(ids, id)))
}
