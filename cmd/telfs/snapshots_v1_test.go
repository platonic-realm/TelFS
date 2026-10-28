package main

import (
	"context"
	"crypto/rand"
	"path/filepath"
	"strings"
	"testing"

	"telfs/internal/crypto"
	"telfs/internal/meta"
	"telfs/internal/tg"
	"telfs/internal/wire"
)

type fakePins []tg.ChannelMessage

func (f fakePins) PinnedMessages(context.Context) ([]tg.ChannelMessage, error) { return f, nil }

// The key check reads every pin: the snapshot anchor pinned next to the
// key message (spec v1) must not make it fail, whichever is newer.
func TestCheckPinnedHeaderWithAnchor(t *testing.T) {
	ctx := context.Background()
	m, err := meta.Open(filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	fs, _ := m.FSUUID(ctx)
	params := crypto.ArgonParams{Time: 1, Memory: 8 * 1024, Threads: 1}
	mk := func() wire.KeyHeader {
		dek := make([]byte, 32)
		rand.Read(dek)
		h, err := wire.NewKeyHeader([]byte("secret"), dek, fs, params)
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	h, other := mk(), mk()
	if err := m.PutKV(ctx, meta.KVKeyHeader, h.JSON()); err != nil {
		t.Fatal(err)
	}
	key := tg.ChannelMessage{ID: 10, RawCaption: h.Message()}
	anchor := tg.ChannelMessage{ID: 99, HasDoc: true, RawCaption: wire.CaptionTag + strings.Repeat("A", wire.CaptionLen-len(wire.CaptionTag))}
	for _, tc := range []struct {
		name string
		pins fakePins
		ok   bool
		want string
	}{
		{"anchor newer than the key message", fakePins{anchor, key}, true, "matches"},
		{"key message only", fakePins{key}, true, "matches"},
		{"anchor only", fakePins{anchor}, false, "none of the 1 pinned"},
		{"nothing pinned", fakePins{}, false, "no message is pinned"},
		{"another header", fakePins{anchor, {ID: 11, RawCaption: other.Message()}}, false, "differs"},
		{"stale and current header pinned", fakePins{anchor, {ID: 11, RawCaption: other.Message()}, key}, true, "pinned key message 10 matches"},
	} {
		kc := checkPinnedHeader(ctx, tc.pins, m)
		if kc.OK != tc.ok || !strings.Contains(kc.Detail, tc.want) {
			t.Errorf("%s: ok=%v %q; want ok=%v containing %q", tc.name, kc.OK, kc.Detail, tc.ok, tc.want)
		}
	}
}
