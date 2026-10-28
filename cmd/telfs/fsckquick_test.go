package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"telfs/internal/config"
	"telfs/internal/fsck"
	"telfs/internal/meta"
	"telfs/internal/tg"
)

// quickChannel is a fake channel for `fsck --quick`: message id →
// document; ids not in docs are deleted; err fails every request.
type quickChannel struct {
	mu    sync.Mutex
	docs  map[int]tg.MessageDoc
	err   error
	calls int
}

func (q *quickChannel) MessageDocs(_ context.Context, ids []int) (map[int]tg.MessageDoc, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.calls++
	if q.err != nil {
		return nil, q.err
	}
	out := map[int]tg.MessageDoc{}
	for _, id := range ids {
		if d, ok := q.docs[id]; ok {
			out[id] = d
		}
	}
	return out, nil
}

func (q *quickChannel) set(id int, d *tg.MessageDoc) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if d == nil {
		delete(q.docs, id)
	} else {
		q.docs[id] = *d
	}
}

// quickFS fills a format-2 DB with 25 files: 20 in their own message
// (ids 100…119) and 5 packed into message 200 (extents of 100 bytes);
// the channel holds all of it.
func quickFS(t *testing.T, m *meta.Store) *quickChannel {
	t.Helper()
	ctx := context.Background()
	m.PutKV(ctx, meta.KVFormat, []byte("2"))
	ch := &quickChannel{docs: map[int]tg.MessageDoc{200: {HasDoc: true, Size: 500}}}
	for i := 0; i < 25; i++ {
		ino, err := m.CreateChild(ctx, meta.RootIno, fmt.Sprintf("f%02d", i), meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
		if err != nil {
			t.Fatal(err)
		}
		c := meta.Chunk{Ino: ino, Idx: 0, TGMessageID: int64(100 + i), Size: 4096}
		if i >= 20 {
			c = meta.Chunk{Ino: ino, Idx: 0, TGMessageID: 200, Size: 80, PackOffset: int64(100 * (i - 20)), PackSize: 100}
		} else {
			ch.docs[100+i] = tg.MessageDoc{HasDoc: true, Size: 4200}
		}
		if err := m.PutChunk(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	return ch
}

// `telfs fsck --quick` through the running daemon (no secret): exit 0
// when everything is there; 7 when a message is gone or too small, with
// every finding in --json; 6 when Telegram fails; format 1 and a second
// concurrent check are refused (exit 1).
func TestFsckQuickViaDaemon(t *testing.T) {
	ctx := context.Background()
	d, m, _, _, _ := versionDaemon(t)
	ch := quickFS(t, m)
	d.msgs = ch
	fsckQuickArgs := []string{"telfs", "fsck", "--quick"}

	if err := dispatch(ctx, fsckQuickArgs); err != nil {
		t.Fatalf("all present: %v (exit %d)", err, exitCode(err))
	}
	if ch.calls != 1 {
		t.Fatalf("21 messages in %d requests, want 1", ch.calls)
	}

	ch.set(105, nil)                                     // deleted
	ch.set(106, &tg.MessageDoc{})                        // no document
	ch.set(200, &tg.MessageDoc{HasDoc: true, Size: 350}) // the last two extents do not fit
	err := dispatch(ctx, append(fsckQuickArgs, "--json"))
	if exitCode(err) != 7 || !strings.Contains(err.Error(), "2 referenced message(s) missing, 1 too small") {
		t.Fatalf("problems: %v (exit %d), want exit 7", err, exitCode(err))
	}
	var out, errOut bytes.Buffer
	err = cmdFsckQuick(ctx, true, &out, &errOut)
	if exitCode(err) != 7 {
		t.Fatalf("problems (direct): %v", err)
	}
	var rep fsck.Report
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("stdout is not one JSON object: %v\n%s", err, out.String())
	}
	if rep.MessagesChecked != 21 || rep.RowsChecked != 25 || len(rep.Missing) != 2 || len(rep.TooSmall) != 1 {
		t.Fatalf("report %+v", rep)
	}
	if m0 := rep.Missing[0]; m0.MsgID != 105 || m0.Reason != fsck.ReasonDeleted || m0.Rows != 1 || m0.ExamplePath != "/f05" {
		t.Fatalf("missing[0] %+v", m0)
	}
	if ts := rep.TooSmall[0]; ts.MsgID != 200 || ts.Rows != 2 || ts.ExamplePath != "/f23" || ts.DocSize != 350 || ts.Need != 500 {
		t.Fatalf("too small %+v", ts)
	}
	for _, want := range []string{"msg 105 (deleted)", "msg 106 (no document)", "msg 200 (document 350 B, extents need 500 B): 2 chunk row(s), e.g. /f23"} {
		if !strings.Contains(errOut.String(), want) {
			t.Errorf("stderr lacks %q:\n%s", want, errOut.String())
		}
	}

	ch.mu.Lock()
	ch.err = errors.New("CHANNEL_PRIVATE")
	ch.mu.Unlock()
	if err := dispatch(ctx, fsckQuickArgs); exitCode(err) != 6 {
		t.Fatalf("Telegram refusing: %v (exit %d), want 6", err, exitCode(err))
	}

	d.fsckMu.Lock()
	err = dispatch(ctx, fsckQuickArgs)
	d.fsckMu.Unlock()
	if err == nil || !strings.Contains(err.Error(), "already running") || exitCode(err) != 1 {
		t.Fatalf("concurrent check: %v", err)
	}

	d.format = 1
	err = dispatch(ctx, fsckQuickArgs)
	if err == nil || !strings.Contains(err.Error(), "format-2") || exitCode(err) != 1 {
		t.Fatalf("format 1: %v", err)
	}
}

// More than 20 findings: stderr names 20 and says how many more; --json
// lists all.
func TestFsckQuickListsFirst20(t *testing.T) {
	ctx := context.Background()
	d, m, _, _, _ := versionDaemon(t)
	ch := quickFS(t, m)
	ch.docs = map[int]tg.MessageDoc{} // the whole channel is gone
	d.msgs = ch
	var out, errOut bytes.Buffer
	err := cmdFsckQuick(ctx, true, &out, &errOut)
	if exitCode(err) != 7 {
		t.Fatalf("exit %d: %v", exitCode(err), err)
	}
	var rep fsck.Report
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil || len(rep.Missing) != 21 {
		t.Fatalf("json: %v, %d missing", err, len(rep.Missing))
	}
	if n := strings.Count(errOut.String(), "  missing "); n != 20 || !strings.Contains(errOut.String(), "… and 1 more") {
		t.Fatalf("stderr names %d:\n%s", n, errOut.String())
	}
}

// Without a daemon the check runs with the command's own session (the
// fake stands in for it): same exit codes; a DB without chunks asks
// neither for the secret nor Telegram; format 1 and a missing DB are
// refused (exit 1).
func TestFsckQuickStandalone(t *testing.T) {
	ctx := context.Background()
	dir := isolate(t, "s")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	args := []string{"telfs", "fsck", "--quick"}
	if err := dispatch(ctx, args); err == nil || !strings.Contains(err.Error(), "no local DB") || exitCode(err) != 1 {
		t.Fatalf("no DB: %v", err)
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Channel.ID, cfg.Channel.AccessHash = 1, 2
	if err := cfg.Save(); err != nil {
		t.Fatal(err)
	}
	m, err := meta.Open(cfg.DBPath())
	if err != nil {
		t.Fatal(err)
	}
	m.PutKV(ctx, meta.KVFormat, []byte("2"))
	connects := 0
	var ch *quickChannel
	var connectErr error
	old := fsckConnect
	t.Cleanup(func() { fsckConnect = old })
	fsckConnect = func(context.Context, *config.Config, io.Writer) (fsck.Messages, func(), error) {
		connects++
		if connectErr != nil {
			return nil, nil, connectErr
		}
		return ch, func() {}, nil
	}
	if err := dispatch(ctx, args); err != nil || connects != 0 {
		t.Fatalf("no chunks: %v, %d connects (want none)", err, connects)
	}

	ch = quickFS(t, m)
	m.Close()
	if err := dispatch(ctx, args); err != nil || connects != 1 || ch.calls != 1 {
		t.Fatalf("all present: %v (exit %d), %d connects, %d requests", err, exitCode(err), connects, ch.calls)
	}
	ch.set(110, nil)
	if err := dispatch(ctx, append(args, "--json")); exitCode(err) != 7 {
		t.Fatalf("one missing: %v (exit %d), want 7", err, exitCode(err))
	}
	ch.err = errors.New("CHANNEL_INVALID")
	if err := dispatch(ctx, args); exitCode(err) != 6 {
		t.Fatalf("Telegram refusing: %v (exit %d), want 6", err, exitCode(err))
	}
	connectErr = fmt.Errorf("%w: no connection within 2m0s", errTelegram)
	if err := dispatch(ctx, args); exitCode(err) != 6 {
		t.Fatalf("no connection: %v (exit %d), want 6", err, exitCode(err))
	}

	m, err = meta.Open(cfg.DBPath())
	if err != nil {
		t.Fatal(err)
	}
	m.DeleteKV(ctx, meta.KVFormat)
	m.Close()
	if err := dispatch(ctx, args); err == nil || !strings.Contains(err.Error(), "format-2") || exitCode(err) != 1 {
		t.Fatalf("format 1: %v", err)
	}
}

// Flag misuse exits 1; plain `telfs fsck` keeps its flags.
func TestFsckQuickUsage(t *testing.T) {
	isolate(t, "u")
	ctx := context.Background()
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"telfs", "fsck", "--json"}, "--json needs --quick"},
		{[]string{"telfs", "fsck", "--quick", "--fix"}, "do not apply to --quick"},
		{[]string{"telfs", "fsck", "--quick", "--stop-after", "3"}, "do not apply to --quick"},
		{[]string{"telfs", "fsck", "--quick", "extra"}, "usage: telfs fsck --quick"},
	} {
		err := dispatch(ctx, tc.args)
		if err == nil || !strings.Contains(err.Error(), tc.want) || exitCode(err) != 1 {
			t.Errorf("%v: %v (exit %d), want %q / exit 1", tc.args, err, exitCode(err), tc.want)
		}
	}
}
