package wire

import (
	"bytes"
	"crypto/rand"
	"errors"
	"strings"
	"testing"
	"time"

	"telfs/internal/compress"
	"telfs/internal/crypto"
)

const testFS = "8f14e45f-ceea-467a-9d2b-1c3e2f9a0b11"

func testKeys(t *testing.T) (*Keys, []byte) {
	t.Helper()
	dek := make([]byte, 32)
	rand.Read(dek)
	k, err := DeriveKeys(dek, testFS, &Counters{})
	if err != nil {
		t.Fatal(err)
	}
	return k, dek
}

func TestChunkRoundTrip(t *testing.T) {
	k, _ := testKeys(t)
	z, _ := compress.NewZstd()
	for _, comp := range []compress.Codec{compress.None{}, z} {
		cc, err := NewChunkCodec(k, comp)
		if err != nil {
			t.Fatal(err)
		}
		for _, pt := range [][]byte{
			[]byte("small"),
			bytes.Repeat([]byte("compressible "), 10000),
			randBytes(4 << 20), // a full chunk
			{},
		} {
			wire, hash, err := cc.Encode(pt)
			if err != nil {
				t.Fatal(err)
			}
			got, err := cc.Decode(hash, wire)
			if err != nil || !bytes.Equal(got, pt) {
				t.Fatalf("%s len=%d: round trip %v", comp.Name(), len(pt), err)
			}
			if len(pt) == 4<<20 && len(wire) != 4<<20+chunkHdrLen+crypto.BoxOverhead {
				t.Fatalf("a full random chunk must pay no padding: wire=%d", len(wire))
			}
			// Filed under another hash → refused (AAD).
			other := k.Hash([]byte("other content"))
			if _, err := cc.Decode(other, wire); err == nil {
				t.Fatal("a wire must not open under another hash")
			}
		}
	}
}

func TestChunkHashRecheck(t *testing.T) {
	// A chunk sealed (by the key holder) under a hash that doesn't match
	// its content must fail the recheck, not return wrong data.
	k, _ := testKeys(t)
	cc, _ := NewChunkCodec(k, nil)
	lie := k.Hash([]byte("what it claims to be"))
	wire, _, err := cc.EncodeWithHash([]byte("what it really is"), lie)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cc.Decode(lie, wire); !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("recheck: %v", err)
	}
}

func TestHashIsKeyed(t *testing.T) {
	a, _ := testKeys(t)
	b, _ := testKeys(t)
	pt := []byte("same content")
	if bytes.Equal(a.Hash(pt), b.Hash(pt)) {
		t.Fatal("two filesystems must hash the same content differently")
	}
	if !bytes.Equal(a.Hash(pt), a.Hash(pt)) {
		t.Fatal("the hash must be deterministic within a filesystem")
	}
}

func TestCaptionFixedLengthAndTruncation(t *testing.T) {
	k, _ := testKeys(t)
	long := "/home/arash/" + strings.Repeat("ψ-very-long-folder-name/", 60) + "final-file.pdf"
	for _, c := range []Caption{
		{K: KindSnapshot, Seq: 7, TS: time.Now().Unix()},
		{K: KindChunk, Ino: 12345, Idx: 3, H: k.Hash([]byte("x")), Len: 4 << 20, CS: 4 << 20, Name: "/docs/a.txt"},
		{K: KindChunk, Ino: 1, H: k.Hash([]byte("y")), Name: long},
	} {
		s, err := k.SealCaption(c)
		if err != nil {
			t.Fatal(err)
		}
		if len(s) != CaptionLen || !strings.HasPrefix(s, CaptionTag) {
			t.Fatalf("caption length %d", len(s))
		}
		got, err := k.OpenCaption(s)
		if err != nil {
			t.Fatal(err)
		}
		if got.K != c.K || got.Ino != c.Ino || got.FS != testFS {
			t.Fatalf("caption round trip: %+v", got)
		}
		if c.Name == long {
			if !strings.HasPrefix(got.Name, "…") || !strings.HasSuffix(got.Name, "final-file.pdf") {
				t.Fatalf("long name must be cut from the left: %q", got.Name)
			}
		} else if got.Name != c.Name {
			t.Fatalf("name %q, want %q", got.Name, c.Name)
		}
	}
	other, _ := testKeys(t)
	s, _ := k.SealCaption(Caption{K: KindJournal})
	if _, err := other.OpenCaption(s); !errors.Is(err, ErrNotOurs) {
		t.Fatalf("another filesystem's caption: %v", err)
	}
	if _, err := k.OpenCaption("hello"); !errors.Is(err, ErrNotOurs) {
		t.Fatal("plain text is not ours")
	}
}

func TestPackIndexRoundTripAndPadding(t *testing.T) {
	k, _ := testKeys(t)
	var entries []PackEntry
	var extents int64
	for i := 0; i < 500; i++ {
		entries = append(entries, PackEntry{Ino: int64(100 + i), Off: extents, WLen: 1000, Len: 900,
			H: k.Hash([]byte{byte(i)}), Name: "/src/project/file" + strings.Repeat("x", i%7) + ".go"})
		extents += 1000
	}
	idx, err := k.SealPackIndex(extents, entries)
	if err != nil {
		t.Fatal(err)
	}
	total := int(extents) + len(idx)
	if total != crypto.PadmeLen(total) {
		t.Fatalf("whole pack %d is not Padmé-sized", total)
	}
	got, err := k.OpenPackIndex(idx)
	if err != nil || len(got) != 500 || got[499].Ino != 599 || got[3].Name != entries[3].Name {
		t.Fatalf("index round trip: %d entries, %v", len(got), err)
	}
}

func TestMetaBodies(t *testing.T) {
	k, _ := testKeys(t)
	data := []byte(`{"ops":[{"seq":1,"op":{"op":"create","name":"secret-name.txt"}}]}`)
	w, err := k.SealMeta("journal", data)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(w, []byte("secret-name")) {
		t.Fatal("journal body leaks a name")
	}
	got, err := k.OpenMeta("journal", w)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("journal: %v", err)
	}
	if _, err := k.OpenMeta("snap", w); err == nil {
		t.Fatal("a journal body must not open as a snapshot")
	}
}

func TestKeyHeaderAndExport(t *testing.T) {
	dek := randBytes(32)
	params := crypto.ArgonParams{Time: 1, Memory: 8 * 1024, Threads: 1}
	h, err := NewKeyHeader([]byte("correct horse"), dek, testFS, params)
	if err != nil {
		t.Fatal(err)
	}
	msg := h.Message()
	if !IsKeyMessage(msg) {
		t.Fatal("prefix")
	}
	h2, js, err := ParseKeyMessage(msg)
	if err != nil || !bytes.Equal(js, h.JSON()) {
		t.Fatalf("parse: %v", err)
	}
	got, fs, err := h2.Unlock([]byte("correct horse"))
	if err != nil || !bytes.Equal(got, dek) || fs != testFS {
		t.Fatalf("unlock: %v fs=%q", err, fs)
	}
	if _, _, err := h2.Unlock([]byte("wrong")); !errors.Is(err, crypto.ErrWrongSecret) {
		t.Fatalf("wrong secret: %v", err)
	}

	text := Export(h, ExportInfo{FSUUID: testFS, ChannelID: 42, ChannelTitle: "TelFS: home", Profile: "home", Created: time.Unix(1761400000, 0)})
	h3, fsLine, err := ParseExport("some notes before\n" + text + "after")
	if err != nil || !bytes.Equal(h3.JSON(), h.JSON()) || fsLine != testFS {
		t.Fatalf("export round trip: %v", err)
	}
	// A mistyped copy fails on the check line.
	lines := strings.Split(text, "\n")
	for i, l := range lines {
		if !strings.Contains(l, ":") && len(l) == 64 {
			b := []byte(l)
			if b[10] == 'A' {
				b[10] = 'B'
			} else {
				b[10] = 'A'
			}
			lines[i] = string(b)
			break
		}
	}
	if _, _, err := ParseExport(strings.Join(lines, "\n")); err == nil || !strings.Contains(err.Error(), "check line") {
		t.Fatalf("mistyped export: %v", err)
	}
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}
