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
// 200 pages = 20,000 messages, as format-1 recovery. P2 replaces the
// scan for snapshots; the key message is found through the pin.
const ScanPages = 200

// KeyFound is a key message located on the channel.
type KeyFound struct {
	MsgID  int
	Header wire.KeyHeader
	JSON   []byte // exact JSON (byte-compared by status/fsck)
	Pinned bool
}

// FindKeyMessages returns the channel's key messages: the pinned one
// first (if it is a key message), then any others found newest-first in
// the last ScanPages pages.
func FindKeyMessages(ctx context.Context, sess *tg.Session) ([]KeyFound, error) {
	var out []KeyFound
	pinned, err := sess.PinnedMessageID(ctx)
	if err != nil {
		return nil, err
	}
	if pinned > 0 {
		if text, terr := sess.GetMessageText(ctx, pinned); terr == nil && wire.IsKeyMessage(text) {
			if h, js, perr := wire.ParseKeyMessage(text); perr == nil {
				out = append(out, KeyFound{MsgID: pinned, Header: h, JSON: js, Pinned: true})
			}
		}
	}
	err = sess.WalkChannelMessages(ctx, ScanPages, func(cm tg.ChannelMessage) error {
		if cm.HasDoc || cm.ID == pinned || !wire.IsKeyMessage(cm.RawCaption) {
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
func PostKeyMessage(ctx context.Context, sess *tg.Session, h wire.KeyHeader) (int, error) {
	id, err := sess.PostMessage(ctx, h.Message())
	if err != nil {
		return 0, fmt.Errorf("post key message: %w", err)
	}
	if err := sess.PinMessage(ctx, id); err != nil {
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
// filesystem the keys belong to.
func FindLatestSnapshot2(ctx context.Context, sess *tg.Session, keys *wire.Keys) (*Found2, error) {
	var found *Found2
	err := sess.WalkChannelMessages(ctx, ScanPages, func(cm tg.ChannelMessage) error {
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

// DownloadSnapshot2 fetches and decrypts a snapshot body (gzipped DB).
func DownloadSnapshot2(ctx context.Context, sess *tg.Session, keys *wire.Keys, msgID int) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := sess.DownloadDocument(ctx, msgID, &buf); err != nil {
		return nil, fmt.Errorf("download snapshot %d: %w", msgID, err)
	}
	return keys.OpenMeta("snap", buf.Bytes())
}

// journalBody2 is the plaintext of a format-2 journal batch.
type journalBody2 struct {
	Ops []tg.JournalBatchEntry `json:"ops"`
}

// ListJournal2 returns every journal op with seq > sinceSeq, ascending.
// Like the format-1 scan it must reach the sinceSeq boundary (or the
// start of the channel): a missing prefix would replay later ops onto
// the wrong state, so a scan cut short is an error, never a silent
// truncation.
func ListJournal2(ctx context.Context, sess *tg.Session, keys *wire.Keys, sinceSeq int64) ([]tg.JournalBatchEntry, error) {
	var out []tg.JournalBatchEntry
	boundary := false
	const pages = 2000 // runaway guard (200k messages), as format 1
	exhausted, err := sess.WalkChannelMessagesExhaust(ctx, pages, func(cm tg.ChannelMessage) error {
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
		ops, err := DownloadJournal2(ctx, sess, keys, cm.ID)
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
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	// Dedupe (a batch can be re-posted after a lost ack).
	dedup := out[:0]
	var last int64 = -1
	for _, e := range out {
		if e.Seq != last {
			dedup = append(dedup, e)
			last = e.Seq
		}
	}
	return dedup, nil
}

// DownloadJournal2 fetches and decrypts one journal batch.
func DownloadJournal2(ctx context.Context, sess *tg.Session, keys *wire.Keys, msgID int) ([]tg.JournalBatchEntry, error) {
	var buf bytes.Buffer
	if _, err := sess.DownloadDocument(ctx, msgID, &buf); err != nil {
		return nil, fmt.Errorf("download journal batch %d: %w", msgID, err)
	}
	plain, err := keys.OpenMeta("journal", buf.Bytes())
	if err != nil {
		return nil, err
	}
	var body journalBody2
	if err := json.Unmarshal(plain, &body); err != nil {
		return nil, fmt.Errorf("journal batch %d: %w", msgID, err)
	}
	return body.Ops, nil
}

// UploadJournal2 posts one encrypted journal batch.
func UploadJournal2(ctx context.Context, sess *tg.Session, keys *wire.Keys, ops []tg.JournalBatchEntry) (int, error) {
	if len(ops) == 0 {
		return 0, errors.New("empty journal batch")
	}
	js, err := json.Marshal(journalBody2{Ops: ops})
	if err != nil {
		return 0, err
	}
	body, err := keys.SealMeta("journal", js)
	if err != nil {
		return 0, err
	}
	capt, err := keys.SealCaption(wire.Caption{K: wire.KindJournal, First: ops[0].Seq, Last: ops[len(ops)-1].Seq,
		N: len(ops), TS: time.Now().Unix()})
	if err != nil {
		return 0, err
	}
	return sess.UploadDocument(ctx, bytes.NewReader(body), wire.DocName, capt)
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
	var out []int
	for _, f := range strings.Split(string(v), ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(f)); err == nil && n > 0 {
			out = append(out, n)
		}
	}
	return out
}

func storeSnapIDs(ctx context.Context, m *meta.Store, ids []int) error {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.Itoa(id)
	}
	return m.PutKV(ctx, KVSnapMsgIDs, []byte(strings.Join(parts, ",")))
}

// RecordSnapID appends a snapshot message id (recovery uses it to note
// the snapshot it restored, which that snapshot can't contain itself).
func RecordSnapID(ctx context.Context, m *meta.Store, id int) error {
	ids := loadSnapIDs(ctx, m)
	for _, x := range ids {
		if x == id {
			return nil
		}
	}
	return storeSnapIDs(ctx, m, append(ids, id))
}

// once2 is Once's format-2 upload + retention.
func (m *Manager) once2(ctx context.Context, snap *Snapshot) (int, error) {
	body, err := m.Keys.SealMeta("snap", snap.Bytes)
	if err != nil {
		return 0, err
	}
	capt, err := m.Keys.SealCaption(wire.Caption{K: wire.KindSnapshot, Seq: snap.JournalSeq,
		TS: time.Now().Unix(), DBSz: int64(len(snap.Bytes))})
	if err != nil {
		return 0, err
	}
	id, err := m.Session.UploadDocument(ctx, bytes.NewReader(body), wire.DocName, capt)
	if err != nil {
		return 0, fmt.Errorf("upload: %w", err)
	}
	return id, nil
}

// pruneRetention2 keeps the newest Retention snapshots from the local
// list and deletes the rest.
func (m *Manager) pruneRetention2(ctx context.Context, justUploaded int) error {
	ids := loadSnapIDs(ctx, m.Meta)
	found := false
	for _, x := range ids {
		if x == justUploaded {
			found = true
		}
	}
	if !found {
		ids = append(ids, justUploaded)
	}
	keep := m.retention()
	var del []int
	if len(ids) > keep {
		del = append(del, ids[:len(ids)-keep]...)
		ids = ids[len(ids)-keep:]
	}
	if err := storeSnapIDs(ctx, m.Meta, ids); err != nil {
		return err
	}
	if len(del) == 0 {
		return nil
	}
	return m.Session.DeleteChannelMessages(ctx, del...)
}
