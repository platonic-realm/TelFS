package snapshot

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"telfs/internal/crypto"
	"telfs/internal/meta"
	"telfs/internal/tg"
	"telfs/internal/wire"
)

// fakeChannel is an in-memory channel: messages get increasing ids, the
// newest-first walks and the min_id walk behave as Telegram's.
type fakeChannel struct {
	mu     sync.Mutex
	msgs   map[int]*fakeMsg
	pinned map[int]bool
	next   int

	failPin  bool
	onUpload func(caption string) // runs once, before the next upload gets its id

	fullWalks  int   // WalkChannelMessages / …Exhaust calls (history scans)
	afterWalks []int // WalkChannelMessagesAfter min ids
}

type fakeMsg struct {
	id      int
	hasDoc  bool
	caption string // caption of a document, text of a text message
	body    []byte
}

func newFakeChannel() *fakeChannel {
	return &fakeChannel{msgs: map[int]*fakeMsg{}, pinned: map[int]bool{}, next: 1000}
}

func (f *fakeChannel) add(m *fakeMsg) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.next++
	m.id = f.next
	f.msgs[m.id] = m
	return m.id
}

func (f *fakeChannel) view(m *fakeMsg) tg.ChannelMessage {
	return tg.ChannelMessage{ID: m.id, HasDoc: m.hasDoc, RawCaption: m.caption, DocSize: int64(len(m.body))}
}

func (f *fakeChannel) idsDesc(minID int) []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	var ids []int
	for id := range f.msgs {
		if id > minID {
			ids = append(ids, id)
		}
	}
	sort.Sort(sort.Reverse(sort.IntSlice(ids)))
	return ids
}

func (f *fakeChannel) get(id int) (*fakeMsg, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.msgs[id]
	return m, ok
}

func (f *fakeChannel) UploadDocument(ctx context.Context, r io.Reader, filename, caption string) (int, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	f.mu.Lock()
	hook := f.onUpload
	f.onUpload = nil
	f.mu.Unlock()
	if hook != nil {
		hook(caption)
	}
	return f.add(&fakeMsg{hasDoc: true, caption: caption, body: body}), nil
}

func (f *fakeChannel) DownloadDocument(ctx context.Context, msgID int, w io.Writer) (int64, error) {
	m, ok := f.get(msgID)
	if !ok || !m.hasDoc {
		return 0, fmt.Errorf("message %d not found in channel", msgID)
	}
	n, err := w.Write(m.body)
	return int64(n), err
}

func (f *fakeChannel) PostMessage(ctx context.Context, text string) (int, error) {
	return f.add(&fakeMsg{caption: text}), nil
}

func (f *fakeChannel) DeleteChannelMessages(ctx context.Context, ids ...int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range ids {
		delete(f.msgs, id)
		delete(f.pinned, id) // a deleted message is no longer pinned
	}
	return nil
}

func (f *fakeChannel) PinMessage(ctx context.Context, msgID int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failPin {
		return errors.New("pin refused (test)")
	}
	if _, ok := f.msgs[msgID]; !ok {
		return fmt.Errorf("message %d not found", msgID)
	}
	f.pinned[msgID] = true
	return nil
}

func (f *fakeChannel) PinnedMessages(ctx context.Context) ([]tg.ChannelMessage, error) {
	var out []tg.ChannelMessage
	for _, id := range f.idsDesc(0) {
		f.mu.Lock()
		m, pinned := f.msgs[id], f.pinned[id]
		f.mu.Unlock()
		if pinned {
			out = append(out, f.view(m))
		}
	}
	return out, nil
}

func (f *fakeChannel) LatestMessageID(ctx context.Context) (int, error) {
	if ids := f.idsDesc(0); len(ids) > 0 {
		return ids[0], nil
	}
	return 0, nil
}

func (f *fakeChannel) WalkChannelMessages(ctx context.Context, pageLimit int, fn func(tg.ChannelMessage) error) error {
	_, err := f.WalkChannelMessagesExhaust(ctx, pageLimit, fn)
	return err
}

func (f *fakeChannel) WalkChannelMessagesExhaust(ctx context.Context, pageLimit int, fn func(tg.ChannelMessage) error) (bool, error) {
	f.mu.Lock()
	f.fullWalks++
	f.mu.Unlock()
	for i, id := range f.idsDesc(0) {
		if i >= pageLimit*100 {
			return false, nil
		}
		m, ok := f.get(id)
		if !ok {
			continue
		}
		if err := fn(f.view(m)); err != nil {
			if err == tg.ErrStopWalk {
				return false, nil
			}
			return false, err
		}
	}
	return true, nil
}

func (f *fakeChannel) WalkChannelMessagesAfter(ctx context.Context, minID int, fn func(tg.ChannelMessage) error) error {
	f.mu.Lock()
	f.afterWalks = append(f.afterWalks, minID)
	f.mu.Unlock()
	for _, id := range f.idsDesc(minID) {
		m, ok := f.get(id)
		if !ok {
			continue
		}
		if err := fn(f.view(m)); err != nil {
			if err == tg.ErrStopWalk {
				return nil
			}
			return err
		}
	}
	return nil
}

// kinds returns the ids of this filesystem's documents of caption kind k.
func (f *fakeChannel) kinds(keys *wire.Keys, k string) []int {
	var out []int
	for _, id := range f.idsDesc(0) {
		m, ok := f.get(id)
		if !ok || !m.hasDoc {
			continue
		}
		if c, err := keys.OpenCaption(m.caption); err == nil && c.K == k {
			out = append(out, id)
		}
	}
	return out
}

func (f *fakeChannel) pinnedIDs() map[int]bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[int]bool{}
	for id := range f.pinned {
		out[id] = true
	}
	return out
}

// fs2 is a format-2 filesystem on a fake channel.
type fs2 struct {
	ctx  context.Context
	m    *meta.Store
	keys *wire.Keys
	ch   *fakeChannel
	dir  string
}

func newFS2(t *testing.T) *fs2 {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	m, err := meta.Open(filepath.Join(dir, "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	if err := m.PutKV(ctx, meta.KVFormat, []byte("2")); err != nil {
		t.Fatal(err)
	}
	m.SetJournalV2(true)
	fsUUID, err := m.FSUUID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dek := make([]byte, 32)
	if _, err := rand.Read(dek); err != nil {
		t.Fatal(err)
	}
	keys, err := wire.DeriveKeys(dek, fsUUID, nil)
	if err != nil {
		t.Fatal(err)
	}
	ch := newFakeChannel()
	h, err := wire.NewKeyHeader([]byte("secret"), dek, fsUUID, crypto.ArgonParams{Time: 1, Memory: 8 * 1024, Threads: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PostKeyMessage(ctx, ch, h); err != nil {
		t.Fatal(err)
	}
	return &fs2{ctx: ctx, m: m, keys: keys, ch: ch, dir: dir}
}

func (f *fs2) manager() *Manager {
	return &Manager{Meta: f.m, Channel: f.ch, Keys: f.keys}
}

// mkfile creates a file (journaled) at the root.
func (f *fs2) mkfile(t *testing.T, name string) {
	t.Helper()
	if _, err := f.m.CreateChild(f.ctx, meta.RootIno, name, meta.Inode{Kind: meta.KindFile, Mode: 0o100644, ChunkSize: 4 << 20}); err != nil {
		t.Fatal(err)
	}
}

// post posts every pending journal op as one batch, as the poster does.
func (f *fs2) post(t *testing.T) int {
	t.Helper()
	pending, err := f.m.PendingJournal(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) == 0 {
		return 0
	}
	entries := make([]tg.JournalBatchEntry, len(pending))
	seqs := make([]int64, len(pending))
	for i, e := range pending {
		entries[i] = tg.JournalBatchEntry{Seq: e.Seq, Op: e.OpJSON}
		seqs[i] = e.Seq
	}
	id, err := UploadJournal2(f.ctx, f.ch, f.keys, entries)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.m.BatchMarkJournalPosted(f.ctx, seqs, 1); err != nil {
		t.Fatal(err)
	}
	return id
}

func (f *fs2) kv(t *testing.T, key string) int64 {
	t.Helper()
	return kvInt(f.ctx, f.m, key)
}

// anchorOn returns the anchor body pinned at id.
func (f *fs2) anchorOn(t *testing.T, id int) wire.Anchor {
	t.Helper()
	m, ok := f.ch.get(id)
	if !ok {
		t.Fatalf("anchor %d is not on the channel", id)
	}
	a, err := f.keys.OpenAnchor(m.body)
	if err != nil {
		t.Fatal(err)
	}
	return a
}
