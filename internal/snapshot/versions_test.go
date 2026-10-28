package snapshot

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"telfs/internal/meta"
	"telfs/internal/wire"
)

func TestValidateVersionName(t *testing.T) {
	for _, ok := range []string{"daily", "daily-2026-10-28", "weekly_1", "v1.0", "A", strings.Repeat("x", 64), "..a", "-x"} {
		if err := ValidateVersionName(ok); err != nil {
			t.Errorf("%q: %v", ok, err)
		}
	}
	for _, bad := range []string{"", strings.Repeat("x", 65), "a b", "a/b", "a:b", "a=b", "ä", ".", "..", "x\n"} {
		if err := ValidateVersionName(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestRegistryEncodeDecode(t *testing.T) {
	b, err := EncodeVersions(nil)
	if err != nil || string(b) != "[]" {
		t.Fatalf("empty registry %q %v", b, err)
	}
	in := []wire.AnchorVersion{{Name: "a", Msg: 7, Seq: 3, Created: 1700000000, Size: 4096, SHA256: "ab"}, {Name: "b", Msg: 9, Seq: 5, Created: 1700000100}}
	if b, err = EncodeVersions(in); err != nil {
		t.Fatal(err)
	}
	out, err := DecodeVersions(b)
	if err != nil || len(out) != 2 || out[0] != in[0] || out[1] != in[1] {
		t.Fatalf("round trip %+v %v", out, err)
	}
	if vs, err := DecodeVersions(nil); err != nil || vs != nil {
		t.Fatalf("missing registry %+v %v", vs, err)
	}
	if _, err := DecodeVersions([]byte("{")); err == nil {
		t.Fatal("a damaged registry must be an error")
	}
	f := newFS2(t)
	if vs, err := LoadVersions(f.ctx, f.m); err != nil || vs != nil {
		t.Fatalf("a filesystem without versions: %+v %v", vs, err)
	}
	if err := StoreVersions(f.ctx, f.m, in); err != nil {
		t.Fatal(err)
	}
	if vs, err := LoadVersions(f.ctx, f.m); err != nil || len(vs) != 2 || vs[1].Name != "b" {
		t.Fatalf("stored registry %+v %v", vs, err)
	}
	if v, ok := FindVersion(in, "b"); !ok || v.Msg != 9 {
		t.Fatalf("FindVersion %+v %v", v, ok)
	}
	if _, ok := FindVersion(in, "c"); ok {
		t.Fatal("FindVersion found a missing name")
	}
}

// snapshotted is a format-2 filesystem with a file, a snapshot and its
// anchor, as every filesystem since v1 has after `telfs init`.
func snapshotted(t *testing.T) (*fs2, *Manager) {
	t.Helper()
	f := newFS2(t)
	f.mkfile(t, "a")
	f.post(t)
	mgr := f.manager()
	mgr.Logger = quietLogger()
	if err := mgr.Once(f.ctx); err != nil {
		t.Fatal(err)
	}
	return f, mgr
}

func (f *fs2) newestAnchor(t *testing.T) wire.Anchor {
	t.Helper()
	anchors := f.ch.kinds(f.keys, wire.KindAnchor)
	if len(anchors) != 1 {
		t.Fatalf("%d anchors on the channel, want 1", len(anchors))
	}
	if !f.ch.pinnedIDs()[anchors[0]] {
		t.Fatal("the anchor is not pinned")
	}
	return f.anchorOn(t, anchors[0])
}

// `versions create`: a sealed DB snapshot of kind "v" carrying the name,
// recorded in the registry and listed by a new pinned anchor (the old one
// deleted); the downloaded copy is the DB as it was, read-only.
func TestCreateVersion(t *testing.T) {
	f, _ := snapshotted(t)
	oldAnchor := f.ch.kinds(f.keys, wire.KindAnchor)[0]
	seq, _ := f.m.LastJournalSeq(f.ctx)
	v, warn, err := CreateVersion(f.ctx, f.ch, f.m, f.keys, "daily-1", nil, nil)
	if err != nil || warn != "" {
		t.Fatalf("create: %v (warning %q)", err, warn)
	}
	docs := f.ch.kinds(f.keys, wire.KindVersion)
	if len(docs) != 1 || docs[0] != v.Msg {
		t.Fatalf("version messages %v, want [%d]", docs, v.Msg)
	}
	msg, _ := f.ch.get(v.Msg)
	c, err := f.keys.OpenCaption(msg.caption)
	if err != nil || c.Name != "daily-1" || c.Seq != seq || c.DBSz != v.Size || v.Size <= 0 || len(c.H) != 32 || c.TS != v.Created {
		t.Fatalf("caption %+v %v (version %+v)", c, err, v)
	}
	if back := wire.VersionFromCaption(c, v.Msg); back != v {
		t.Fatalf("caption → entry %+v, want %+v", back, v)
	}
	if _, err := f.keys.OpenMeta("snap", msg.body); err == nil {
		t.Fatal("a version body must not open as a snapshot")
	}
	vs, _ := LoadVersions(f.ctx, f.m)
	if len(vs) != 1 || vs[0] != v {
		t.Fatalf("registry %+v, want [%+v]", vs, v)
	}
	a := f.newestAnchor(t)
	if len(a.Versions) != 1 || a.Versions[0] != v {
		t.Fatalf("anchor versions %+v", a.Versions)
	}
	if _, ok := f.ch.get(oldAnchor); ok {
		t.Fatal("the previous anchor must be deleted")
	}

	// Later changes never reach the version.
	f.mkfile(t, "after")
	path := filepath.Join(t.TempDir(), "versions", "daily-1.sqlite")
	if err := DownloadVersion(f.ctx, f.ch, f.keys, v, path); err != nil {
		t.Fatal(err)
	}
	if !VersionFileMatches(path, v) {
		t.Fatal("the downloaded copy does not match its registry entry")
	}
	vm, err := meta.OpenReadOnly(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()
	if !hasFile(t, vm, "a") || hasFile(t, vm, "after") {
		t.Fatal("the version must hold the tree as it was")
	}
	if fs, _ := vm.FSUUID(f.ctx); fs != f.keys.FSUUID {
		t.Fatalf("version fs %q", fs)
	}
	if _, err := vm.CreateChild(f.ctx, meta.RootIno, "x", meta.Inode{Kind: meta.KindFile, Mode: 0o100644}); err == nil {
		t.Fatal("a version DB must be read-only")
	}
	ents, _ := os.ReadDir(filepath.Dir(path))
	if len(ents) != 1 {
		t.Fatalf("an immutable open must leave no sidecars: %v", ents)
	}
	if !VersionFileMatches(path, v) {
		t.Fatal("reading must not change the copy")
	}
	other := v
	other.SHA256 = strings.Repeat("0", 64)
	if VersionFileMatches(path, other) {
		t.Fatal("a different hash must not match")
	}
	other = v
	other.Size++
	if VersionFileMatches(path, other) {
		t.Fatal("a different size must not match")
	}

	// A name is unique; a bad name posts nothing.
	n := len(f.ch.idsDesc(0))
	if _, _, err := CreateVersion(f.ctx, f.ch, f.m, f.keys, "daily-1", nil, nil); !errors.Is(err, ErrVersionExists) {
		t.Fatalf("duplicate: %v", err)
	}
	if _, _, err := CreateVersion(f.ctx, f.ch, f.m, f.keys, "a/b", nil, nil); err == nil {
		t.Fatal("bad name accepted")
	}
	if got := len(f.ch.idsDesc(0)); got != n {
		t.Fatalf("%d message(s) posted by refused creates", got-n)
	}
}

// `versions delete`: the message, the registry entry and the anchor's
// listing go; the other versions stay.
func TestDeleteVersion(t *testing.T) {
	f, _ := snapshotted(t)
	v1, _, err := CreateVersion(f.ctx, f.ch, f.m, f.keys, "one", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	v2, _, err := CreateVersion(f.ctx, f.ch, f.m, f.keys, "two", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, warn, err := DeleteVersion(f.ctx, f.ch, f.m, f.keys, "one", nil); err != nil || warn != "" {
		t.Fatalf("delete: %v %q", err, warn)
	}
	if _, ok := f.ch.get(v1.Msg); ok {
		t.Fatal("the version message must be deleted")
	}
	if _, ok := f.ch.get(v2.Msg); !ok {
		t.Fatal("the other version must stay")
	}
	vs, _ := LoadVersions(f.ctx, f.m)
	if len(vs) != 1 || vs[0].Name != "two" {
		t.Fatalf("registry %+v", vs)
	}
	if a := f.newestAnchor(t); len(a.Versions) != 1 || a.Versions[0].Name != "two" {
		t.Fatalf("anchor versions %+v", a.Versions)
	}
	if _, _, err := DeleteVersion(f.ctx, f.ch, f.m, f.keys, "one", nil); !errors.Is(err, ErrNoVersion) {
		t.Fatalf("deleting a missing version: %v", err)
	}
}

// A version whose anchor can't be posted is still a version (it is on
// the channel and in the registry); the anchor is marked due and the
// policy loop re-posts it with the version listed.
func TestVersionAnchorRetry(t *testing.T) {
	f, mgr := snapshotted(t)
	f.ch.failPin = true
	v, warn, err := CreateVersion(f.ctx, f.ch, f.m, f.keys, "weekly", nil, nil)
	if err != nil || warn == "" {
		t.Fatalf("create with the pin refused: %v, warning %q", err, warn)
	}
	if _, ok := f.ch.get(v.Msg); !ok {
		t.Fatal("the version must be on the channel")
	}
	st, err := LoadPolicyState(f.ctx, f.m)
	if err != nil || !st.AnchorDue {
		t.Fatalf("anchor due %v %v", st.AnchorDue, err)
	}
	if a := f.newestAnchor(t); len(a.Versions) != 0 {
		t.Fatalf("the old anchor still pinned lists %+v", a.Versions)
	}
	f.ch.failPin = false
	act, err := mgr.step2(f.ctx, time.Now())
	if err != nil || act != ActAnchor {
		t.Fatalf("policy step: %s %v, want anchor", act, err)
	}
	if a := f.newestAnchor(t); len(a.Versions) != 1 || a.Versions[0] != v {
		t.Fatalf("re-posted anchor lists %+v", a.Versions)
	}
	if st, _ := LoadPolicyState(f.ctx, f.m); st.AnchorDue {
		t.Fatal("anchor_due must be cleared by the post")
	}
}

// A recovery takes the registry from the anchor: the snapshot it
// restores is older than the version (its own registry is empty).
func TestRecoveryRestoresVersionRegistry(t *testing.T) {
	f, _ := snapshotted(t)
	v, _, err := CreateVersion(f.ctx, f.ch, f.m, f.keys, "monthly", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	pins, _ := f.ch.PinnedMessages(f.ctx)
	got, err := Fetch2(f.ctx, f.ch, f.keys, pins, nil)
	if err != nil {
		t.Fatal(err)
	}
	m := restore2(t, f, got)
	vs, err := LoadVersions(f.ctx, m)
	if err != nil || len(vs) != 1 || vs[0] != v {
		t.Fatalf("recovered registry %+v %v, want [%+v]", vs, err, v)
	}
}

// Format 1 has no versions.
func TestCreateVersionRefusesFormat1(t *testing.T) {
	f := newFS2(t)
	if err := f.m.PutKV(f.ctx, meta.KVFormat, []byte("1")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := CreateVersion(f.ctx, f.ch, f.m, f.keys, "x", nil, nil); !errors.Is(err, ErrVersionFormat1) {
		t.Fatalf("format 1: %v", err)
	}
}
