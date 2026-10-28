package wire

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// A version seals under the filesystem's keys: an ordinary caption of
// kind "v" carrying the name, seq, time, DB size and hash; a body that
// opens only as a version.
func TestVersionSealOpen(t *testing.T) {
	k, _ := testKeys(t)
	v := AnchorVersion{Name: strings.Repeat("n", 64), Seq: 42, Created: 1761650000, Size: 8192,
		SHA256: strings.Repeat("ab", 32)}
	gz := []byte("gzipped sqlite")
	body, capt, err := k.SealVersion(v, gz)
	if err != nil {
		t.Fatal(err)
	}
	if len(capt) != CaptionLen || bytes.Contains(body, gz) {
		t.Fatal("the version must be an ordinary sealed caption and an encrypted body")
	}
	c, err := k.OpenCaption(capt)
	if err != nil || c.K != KindVersion {
		t.Fatalf("caption %+v %v", c, err)
	}
	v.Msg = 77
	if got := VersionFromCaption(c, 77); got != v {
		t.Fatalf("caption → %+v, want %+v", got, v)
	}
	if got, err := k.OpenVersion(body); err != nil || !bytes.Equal(got, gz) {
		t.Fatalf("body %q %v", got, err)
	}
	for _, kind := range []string{"snap", "anchor", "journal"} {
		if _, err := k.OpenMeta(kind, body); err == nil {
			t.Fatalf("a version body opened as %s", kind)
		}
	}
	snap, _ := k.SealMeta("snap", gz)
	if _, err := k.OpenVersion(snap); err == nil {
		t.Fatal("a snapshot body must not open as a version")
	}
	if _, _, err := k.SealVersion(AnchorVersion{Name: "x", SHA256: "zz"}, gz); err == nil {
		t.Fatal("a bad hash must be refused")
	}
}

// The anchor carries the versions with their size and hash; entries from
// before those fields existed still parse.
func TestAnchorVersionsRoundTrip(t *testing.T) {
	k, _ := testKeys(t)
	vs := []AnchorVersion{
		{Name: "daily-1", Msg: 10, Seq: 5, Created: 1761650000, Size: 4096, SHA256: strings.Repeat("0f", 32)},
		{Name: "daily-2", Msg: 12, Seq: 9, Created: 1761736400, Size: 8192},
	}
	body, _, err := k.SealAnchor(Anchor{SnapMsg: 3, Versions: vs})
	if err != nil {
		t.Fatal(err)
	}
	a, err := k.OpenAnchor(body)
	if err != nil || len(a.Versions) != 2 || a.Versions[0] != vs[0] || a.Versions[1] != vs[1] {
		t.Fatalf("round trip %+v %v", a.Versions, err)
	}
	var old AnchorVersion
	if err := json.Unmarshal([]byte(`{"name":"x","msg":1,"seq":2,"created":3}`), &old); err != nil || old.Size != 0 || old.SHA256 != "" {
		t.Fatalf("an entry without size/hash: %+v %v", old, err)
	}
}
