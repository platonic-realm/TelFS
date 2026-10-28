package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/session"

	"telfs/internal/chunk"
	"telfs/internal/config"
	"telfs/internal/control"
	"telfs/internal/meta"
	"telfs/internal/snapshot"
	"telfs/internal/tg"
	"telfs/internal/wire"
)

// isolate points the profile resolution at a fresh directory, so no test
// ever reaches the real ~/.config/telfs (or a real daemon's socket).
func isolate(t *testing.T, profile string) string {
	t.Helper()
	xdg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", xdg)
	t.Setenv("TELFS_PROFILE", profile)
	return filepath.Join(xdg, "telfs", "profiles", profile)
}

// `telfs versions …` usage errors exit 1 before anything else happens;
// `mount --version` needs --readonly and a valid name.
func TestVersionsCLIUsage(t *testing.T) {
	isolate(t, "t")
	ctx := context.Background()
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"telfs", "versions", "create"}, "usage: telfs versions create"},
		{[]string{"telfs", "versions", "create", "a", "b"}, "usage: telfs versions create"},
		{[]string{"telfs", "versions", "create", "a/b"}, "invalid version name"},
		{[]string{"telfs", "versions", "create", "daily", "--timeout", "soon"}, "invalid value"},
		{[]string{"telfs", "versions", "delete"}, "usage: telfs versions delete"},
		{[]string{"telfs", "versions", "delete", ".."}, "invalid version name"},
		{[]string{"telfs", "versions", "list", "extra"}, "usage: telfs versions list"},
		{[]string{"telfs", "mount", "--version", "daily", "/mnt/x"}, "--version needs --readonly"},
		{[]string{"telfs", "mount", "--readonly", "--version", "a b", "/mnt/x"}, "invalid version name"},
		{[]string{"telfs", "mount", "--readonly", "--version", "daily", "--key-header", "f", "/mnt/x"}, "does not apply to --version"},
		{[]string{"telfs", "status", "--version", "daily"}, "--version needs --json"},
	} {
		err := dispatch(ctx, tc.args)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%v: err %v, want %q", tc.args, err, tc.want)
			continue
		}
		if c := exitCode(err); c != 1 {
			t.Errorf("%v: exit %d, want 1", tc.args, c)
		}
	}
}

func TestParseInterleaved(t *testing.T) {
	for _, args := range [][]string{{"daily", "--timeout", "5m"}, {"--timeout", "5m", "daily"}, {"--timeout=5m", "daily"}} {
		fs := flag.NewFlagSet("x", flag.ContinueOnError)
		d := fs.Duration("timeout", 0, "")
		pos, err := parseInterleaved(fs, args)
		if err != nil || len(pos) != 1 || pos[0] != "daily" || *d != 5*time.Minute {
			t.Errorf("%v: pos %v timeout %s err %v", args, pos, *d, err)
		}
	}
}

// fakeChan is an in-memory channel for the daemon's version routes.
type fakeChan struct {
	mu         sync.Mutex
	next       int
	docs       map[int][]byte
	caps       map[int]string
	pinned     map[int]bool
	failUpload bool
}

func newFakeChan() *fakeChan {
	return &fakeChan{next: 100, docs: map[int][]byte{}, caps: map[int]string{}, pinned: map[int]bool{}}
}

func (f *fakeChan) UploadDocument(_ context.Context, r io.Reader, _, caption string) (int, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failUpload {
		return 0, errors.New("FLOOD_WAIT (test)")
	}
	f.next++
	f.docs[f.next], f.caps[f.next] = b, caption
	return f.next, nil
}

func (f *fakeChan) DownloadDocument(_ context.Context, id int, w io.Writer) (int64, error) {
	f.mu.Lock()
	b, ok := f.docs[id]
	f.mu.Unlock()
	if !ok {
		return 0, fmt.Errorf("message %d not found", id)
	}
	n, err := w.Write(b)
	return int64(n), err
}

func (f *fakeChan) PostMessage(context.Context, string) (int, error) {
	return 0, errors.New("not used")
}

func (f *fakeChan) DeleteChannelMessages(_ context.Context, ids ...int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range ids {
		delete(f.docs, id)
		delete(f.caps, id)
		delete(f.pinned, id)
	}
	return nil
}

func (f *fakeChan) PinMessage(_ context.Context, id int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pinned[id] = true
	return nil
}

func (f *fakeChan) PinnedMessages(context.Context) ([]tg.ChannelMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []tg.ChannelMessage
	for id := range f.pinned {
		out = append(out, tg.ChannelMessage{ID: id, HasDoc: true, RawCaption: f.caps[id]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID > out[j].ID })
	return out, nil
}

func (f *fakeChan) LatestMessageID(context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.next, nil
}

func (f *fakeChan) WalkChannelMessages(context.Context, int, func(tg.ChannelMessage) error) error {
	return nil
}

func (f *fakeChan) WalkChannelMessagesExhaust(context.Context, int, func(tg.ChannelMessage) error) (bool, error) {
	return true, nil
}

func (f *fakeChan) WalkChannelMessagesAfter(context.Context, int, func(tg.ChannelMessage) error) error {
	return nil
}

// kinds returns the ids of the documents of caption kind k.
func (f *fakeChan) kinds(keys *wire.Keys, k string) []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []int
	for id, c := range f.caps {
		if cp, err := keys.OpenCaption(c); err == nil && cp.K == k {
			out = append(out, id)
		}
	}
	sort.Ints(out)
	return out
}

// versionDaemon is a live format-2 daemon of profile "t" (isolated)
// serving its control socket, with a snapshot recorded (as every
// filesystem since `telfs init` has, so the anchor can be posted).
func versionDaemon(t *testing.T) (*daemonCtl, *meta.Store, *chunk.UploadPool, *fakeChan, *wire.Keys) {
	t.Helper()
	ctx := context.Background()
	dir := isolate(t, "t")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{DataDir: dir}
	m, err := meta.Open(cfg.DBPath())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	m.PutKV(ctx, meta.KVFormat, []byte("2"))
	m.SetJournalV2(true)
	m.PutKV(ctx, snapshot.KVCurrentMsgID, []byte("5"))
	m.PutKV(ctx, snapshot.KVSnapJnlAfter, []byte("4"))
	fsUUID, _ := m.FSUUID(ctx)
	dek := make([]byte, 32)
	rand.Read(dek)
	keys, err := wire.DeriveKeys(dek, fsUUID, nil)
	if err != nil {
		t.Fatal(err)
	}
	pool := chunk.NewUploadPool(1)
	t.Cleanup(pool.Shutdown)
	ch := newFakeChan()
	d := &daemonCtl{cfg: cfg, format: 2, meta: m, pool: pool, poster: &snapshot.Poster{Meta: m}, keys: keys, ch: ch}
	path := control.Path(dir)
	ln, err := control.Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	sctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go control.Serve(sctx, ln, path, d.handler())
	return d, m, pool, ch, keys
}

// The whole `versions` interface against a daemon over its control
// socket: create (after the sync barrier) → on the channel, in the
// registry and the anchor; a duplicate is refused; a create whose sync
// does not finish in time exits 4 and posts nothing; Telegram failing
// exits 6; delete removes it; no daemon exits 5.
func TestVersionsOverControlSocket(t *testing.T) {
	ctx := context.Background()
	_, m, pool, ch, keys := versionDaemon(t)
	postAll(t, m)

	if err := dispatch(ctx, []string{"telfs", "versions", "create", "daily", "--timeout", "10s"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	vs, _ := snapshot.LoadVersions(ctx, m)
	if len(vs) != 1 || vs[0].Name != "daily" {
		t.Fatalf("registry %+v", vs)
	}
	if got := ch.kinds(keys, wire.KindVersion); len(got) != 1 || got[0] != vs[0].Msg {
		t.Fatalf("version messages %v", got)
	}
	anchors := ch.kinds(keys, wire.KindAnchor)
	if len(anchors) != 1 || !ch.pinned[anchors[0]] {
		t.Fatalf("anchors %v", anchors)
	}
	a, err := keys.OpenAnchor(ch.docs[anchors[0]])
	if err != nil || len(a.Versions) != 1 || a.Versions[0] != vs[0] {
		t.Fatalf("anchor %+v %v", a.Versions, err)
	}

	err = dispatch(ctx, []string{"telfs", "versions", "create", "daily"})
	if err == nil || !strings.Contains(err.Error(), "already exists") || exitCode(err) != 1 {
		t.Fatalf("duplicate: %v (exit %d)", err, exitCode(err))
	}

	// An upload registered before the call never lands: timeout, exit 4,
	// nothing posted.
	open := pool.Register(42)
	n := len(ch.docs)
	err = dispatch(ctx, []string{"telfs", "versions", "create", "--timeout", "400ms", "weekly"})
	if exitCode(err) != 4 {
		t.Fatalf("unsynced create: %v (exit %d), want exit 4", err, exitCode(err))
	}
	open.Release()
	if len(ch.docs) != n {
		t.Fatal("a timed-out create must post nothing")
	}

	ch.failUpload = true
	err = dispatch(ctx, []string{"telfs", "versions", "create", "monthly"})
	if exitCode(err) != 6 {
		t.Fatalf("Telegram failing: %v (exit %d), want 6", err, exitCode(err))
	}
	ch.failUpload = false
	if vs, _ := snapshot.LoadVersions(ctx, m); len(vs) != 1 {
		t.Fatalf("failed creates must not register: %+v", vs)
	}

	if err := dispatch(ctx, []string{"telfs", "versions", "list", "--json"}); err != nil {
		t.Fatalf("list: %v", err)
	}
	if err := dispatch(ctx, []string{"telfs", "versions", "delete", "daily"}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if got := ch.kinds(keys, wire.KindVersion); len(got) != 0 {
		t.Fatalf("version messages after delete: %v", got)
	}
	if vs, _ := snapshot.LoadVersions(ctx, m); len(vs) != 0 {
		t.Fatalf("registry after delete: %+v", vs)
	}
	err = dispatch(ctx, []string{"telfs", "versions", "delete", "daily"})
	if err == nil || exitCode(err) != 1 {
		t.Fatalf("deleting a missing version: %v", err)
	}

	isolate(t, "nodaemon")
	for _, args := range [][]string{{"telfs", "versions", "create", "x"}, {"telfs", "versions", "delete", "x"}} {
		if err := dispatch(ctx, args); exitCode(err) != 5 {
			t.Errorf("%v without a daemon: %v (exit %d), want 5", args, err, exitCode(err))
		}
	}
	if err := dispatch(ctx, []string{"telfs", "versions", "list"}); err == nil || exitCode(err) != 1 {
		t.Errorf("list without a daemon or a DB: %v", err)
	}
}

// A read-only mount and a format-1 filesystem refuse versions.
func TestVersionsRefused(t *testing.T) {
	d, _, _, _, _ := versionDaemon(t)
	d.readOnly = true
	if msg := d.versionsRefusal(); !strings.Contains(msg, "read-only") {
		t.Fatalf("read-only: %q", msg)
	}
	d.readOnly, d.format = false, 1
	if msg := d.versionsRefusal(); !strings.Contains(msg, "format-2") {
		t.Fatalf("format 1: %q", msg)
	}
	err := dispatch(context.Background(), []string{"telfs", "versions", "create", "x"})
	if err == nil || !strings.Contains(err.Error(), "format-2") || exitCode(err) != 1 {
		t.Fatalf("create on format 1: %v", err)
	}
	d.format, d.version = 2, "daily"
	if msg := d.versionsRefusal(); !strings.Contains(msg, "version mount") {
		t.Fatalf("version mount: %q", msg)
	}
}

// `versions list` without a daemon reads the local registry, read-only.
func TestVersionsListLocal(t *testing.T) {
	ctx := context.Background()
	dir := isolate(t, "t")
	cfg := &config.Config{DataDir: dir}
	m, err := meta.Open(cfg.DBPath())
	if err != nil {
		t.Fatal(err)
	}
	m.PutKV(ctx, meta.KVFormat, []byte("2"))
	snapshot.StoreVersions(ctx, m, []wire.AnchorVersion{{Name: "a", Msg: 3, Seq: 1, Created: 10, Size: 4096}, {Name: "b", Msg: 4, Seq: 2, Created: 20}})
	m.Close()
	list, err := localVersions(ctx, cfg)
	if err != nil || len(list.Versions) != 2 || list.Versions[1].Name != "b" || list.Versions[0].MsgID != 3 || list.Versions[0].Mountpoint != nil {
		t.Fatalf("local list %+v %v", list, err)
	}
	if err := dispatch(ctx, []string{"telfs", "versions", "list"}); err != nil {
		t.Fatalf("list: %v", err)
	}
	st, err := offlineStatus(ctx, cfg)
	if err != nil || st.Versions.Count != 2 || st.Versions.Newest == nil || st.Versions.Newest.Name != "b" || st.Versions.Newest.CreatedUnix != 20 {
		t.Fatalf("status versions %+v %v", st.Versions, err)
	}
}

// gc refuses on a filesystem with versions, and never classifies a
// version or the anchor as something it deletes.
func TestGCAndVersions(t *testing.T) {
	ctx := context.Background()
	m, err := meta.Open(filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := gcVersionsRefusal(ctx, m); err != nil {
		t.Fatalf("no versions: %v", err)
	}
	snapshot.StoreVersions(ctx, m, []wire.AnchorVersion{{Name: "daily", Msg: 9, Created: 5}})
	if err := gcVersionsRefusal(ctx, m); err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("with a version: %v", err)
	}
	snapshot.StoreVersions(ctx, m, nil)
	if err := gcVersionsRefusal(ctx, m); err != nil {
		t.Fatalf("all versions deleted: %v", err)
	}

	fsUUID, _ := m.FSUUID(ctx)
	dek := make([]byte, 32)
	rand.Read(dek)
	keys, _ := wire.DeriveKeys(dek, fsUUID, nil)
	_, vcap, _ := keys.SealVersion(wire.AnchorVersion{Name: "daily"}, []byte("db"))
	_, acap, _ := keys.SealAnchor(wire.Anchor{SnapMsg: 3})
	for kind, capt := range map[string]string{wire.KindVersion: vcap, wire.KindAnchor: acap} {
		cm, k := classify2(tg.ChannelMessage{ID: 1, HasDoc: true, RawCaption: capt}, keys)
		if cm.Kind != tg.KindUnknown || k != kind {
			t.Errorf("%s: classified %v (%q) — gc must never touch it", kind, cm.Kind, k)
		}
	}
}

// A version mount's daemon says so in its status; the version's own
// copy reports no staging, key check or use counts.
func TestVersionMountStatus(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := &config.Config{DataDir: dir, APIID: 1, APIHash: "x"}
	m, err := meta.Open(filepath.Join(dir, "v.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	m.PutKV(ctx, meta.KVFormat, []byte("2"))
	client, err := tg.New(cfg, &session.StorageMemory{})
	if err != nil {
		t.Fatal(err)
	}
	d := &daemonCtl{cfg: cfg, mountpoint: "/mnt/v", readOnly: true, format: 2, meta: m,
		sess: client.NewSessionManager().Session(), version: "daily"}
	st := d.status(ctx)
	if st.VersionMount == nil || *st.VersionMount != "daily" || !st.Sync.Synced || st.Staging.Dir != "" || st.KeyHeader != nil {
		t.Fatalf("version mount status %+v", st)
	}
	live := &daemonCtl{cfg: cfg, mountpoint: "/mnt/live", format: 2, meta: m, sess: d.sess}
	if st := live.status(ctx); st.VersionMount != nil {
		t.Fatalf("the live mount names a version: %q", *st.VersionMount)
	}
}

// The version socket lives in the versions directory, short whatever the
// name's length, distinct per version.
func TestVersionSocketPath(t *testing.T) {
	dir := "/var/lib/telfs/.config/telfs/profiles/backup"
	long := control.VersionPath(dir, strings.Repeat("v", 64))
	if !strings.HasPrefix(long, dir+"/versions/") || len(long) > 107 {
		t.Fatalf("socket %q (%d bytes)", long, len(long))
	}
	if control.VersionPath(dir, "a") == control.VersionPath(dir, "b") || control.VersionPath(dir, "a") == control.Path(dir) {
		t.Fatal("sockets must differ per version and from the live mount's")
	}
}

// A version mount reads its registry entry and key header from the live
// DB, read-only (next to a running daemon); one mount per version.
func TestVersionToMount(t *testing.T) {
	ctx := context.Background()
	dir := isolate(t, "t")
	cfg := &config.Config{DataDir: dir}
	if _, _, _, err := versionToMount(ctx, cfg, "daily"); err == nil || !strings.Contains(err.Error(), "local DB") {
		t.Fatalf("no DB: %v", err)
	}
	if _, err := os.Stat(cfg.DBPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("a version mount must never create the live DB")
	}
	m, err := meta.Open(cfg.DBPath())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if _, _, _, err := versionToMount(ctx, cfg, "daily"); !errors.Is(err, snapshot.ErrVersionFormat1) {
		t.Fatalf("format 1: %v", err)
	}
	m.PutKV(ctx, meta.KVFormat, []byte("2"))
	want := wire.AnchorVersion{Name: "daily", Msg: 7, Seq: 3, Created: 99, Size: 4096}
	snapshot.StoreVersions(ctx, m, []wire.AnchorVersion{want})
	if _, _, _, err := versionToMount(ctx, cfg, "weekly"); !errors.Is(err, snapshot.ErrNoVersion) {
		t.Fatalf("unknown version: %v", err)
	}
	live, v, fs, err := versionToMount(ctx, cfg, "daily")
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	if own, _ := m.FSUUID(ctx); v != want || fs != own {
		t.Fatalf("entry %+v fs %q", v, fs)
	}
	if err := live.PutKV(ctx, "x", []byte("y")); err == nil {
		t.Fatal("the live DB must be opened read-only")
	}

	os.MkdirAll(versionsDir(cfg), 0o700)
	l1, err := lockVersion(cfg, "daily")
	if err != nil {
		t.Fatal(err)
	}
	defer l1.Close()
	if _, err := lockVersion(cfg, "daily"); err == nil {
		t.Fatal("a second mount of the same version must be refused")
	}
	l2, err := lockVersion(cfg, "weekly")
	if err != nil {
		t.Fatalf("another version: %v", err)
	}
	l2.Close()
}
