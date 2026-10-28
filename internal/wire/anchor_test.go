package wire

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// The anchor seals and opens under the filesystem's keys; its caption is
// an ordinary format-2 caption of kind "a"; another filesystem's keys
// open neither.
func TestAnchorSealOpen(t *testing.T) {
	k, _ := testKeys(t)
	in := Anchor{SnapMsg: 4711, SnapSeq: 123456, SnapTS: 1761650000, JnlAfter: 4700}
	body, capt, err := k.SealAnchor(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(capt) != CaptionLen || !LooksLikeCaption(capt) {
		t.Fatalf("anchor caption is %d chars, want an ordinary %d-char caption", len(capt), CaptionLen)
	}
	c, err := k.OpenCaption(capt)
	if err != nil || c.K != KindAnchor || c.Seq != in.SnapSeq || c.FS != testFS {
		t.Fatalf("caption: %+v %v", c, err)
	}
	if bytes.Contains(body, []byte("snap_msg")) {
		t.Fatal("the anchor body must be encrypted")
	}
	got, err := k.OpenAnchor(body)
	if err != nil {
		t.Fatal(err)
	}
	if got.V != AnchorVersionV1 || got.FS != testFS || got.SnapMsg != 4711 || got.SnapSeq != 123456 ||
		got.SnapTS != 1761650000 || got.JnlAfter != 4700 || got.Versions == nil || len(got.Versions) != 0 {
		t.Fatalf("round trip: %+v", got)
	}

	// The body is a meta seal of kind "anchor": a snapshot or journal
	// body never passes for one, nor the other way round.
	if _, err := k.OpenMeta("snap", body); err == nil {
		t.Fatal("an anchor body must not open as a snapshot")
	}
	snap, _ := k.SealMeta("snap", []byte("{}"))
	if _, err := k.OpenAnchor(snap); err == nil {
		t.Fatal("a snapshot body must not open as an anchor")
	}

	other, _ := testKeys(t) // same fs_uuid, another DEK
	if _, err := other.OpenAnchor(body); err == nil {
		t.Fatal("another key must not open the anchor")
	}
	if _, err := other.OpenCaption(capt); err != ErrNotOurs {
		t.Fatalf("another key must not open the caption: %v", err)
	}
}

// The versions list is always present in the JSON (empty when the
// filesystem has no versions), and an anchor naming no snapshot, another
// filesystem or a newer version is refused.
func TestAnchorChecks(t *testing.T) {
	k, _ := testKeys(t)
	seal := func(a Anchor) []byte {
		js, _ := json.Marshal(a)
		b, err := k.SealMeta("anchor", js)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	body, _, err := k.SealAnchor(Anchor{SnapMsg: 1})
	if err != nil {
		t.Fatal(err)
	}
	js, err := k.OpenMeta("anchor", body)
	if err != nil || !strings.Contains(string(js), `"versions":[]`) {
		t.Fatalf("anchor JSON %s (%v) must carry an empty versions list", js, err)
	}
	for name, a := range map[string]Anchor{
		"no snapshot":   {V: 1, FS: testFS},
		"other fs":      {V: 1, FS: "another-fs", SnapMsg: 5},
		"newer version": {V: 2, FS: testFS, SnapMsg: 5},
	} {
		if _, err := k.OpenAnchor(seal(a)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := k.OpenAnchor(seal(Anchor{V: 1, FS: testFS, SnapMsg: 5, Versions: []AnchorVersion{{Name: "daily", Msg: 9, Seq: 3, Created: 1}}})); err != nil {
		t.Fatalf("a valid anchor with a version: %v", err)
	}
}
