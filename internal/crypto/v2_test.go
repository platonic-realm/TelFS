package crypto

import (
	"bytes"
	"errors"
	"testing"
)

func TestDeriveSubkeyLabelsDistinct(t *testing.T) {
	root := bytes.Repeat([]byte{7}, 32)
	seen := map[string]string{}
	for _, l := range []string{LabelChunk, LabelHash, LabelMeta, LabelCaption, LabelSession} {
		k, err := DeriveSubkey(root, l)
		if err != nil || len(k) != 32 {
			t.Fatalf("%s: %v len=%d", l, err, len(k))
		}
		if prev, dup := seen[string(k)]; dup {
			t.Fatalf("labels %q and %q derive the same key", prev, l)
		}
		seen[string(k)] = l
	}
	if _, err := DeriveSubkey([]byte("short"), LabelChunk); err == nil {
		t.Fatal("a short root must be refused")
	}
}

func TestBoxRoundTripAndAAD(t *testing.T) {
	b, _ := NewBox(bytes.Repeat([]byte{1}, 32))
	w1, _ := b.Seal([]byte("hello"), []byte("aad-1"))
	w2, _ := b.Seal([]byte("hello"), []byte("aad-1"))
	if bytes.Equal(w1, w2) {
		t.Fatal("random nonces must make two seals differ")
	}
	if pt, err := b.Open(w1, []byte("aad-1")); err != nil || string(pt) != "hello" {
		t.Fatalf("open: %q %v", pt, err)
	}
	if _, err := b.Open(w1, []byte("aad-2")); !errors.Is(err, ErrAuth) {
		t.Fatalf("wrong aad must fail: %v", err)
	}
	w1[len(w1)-1] ^= 1
	if _, err := b.Open(w1, []byte("aad-1")); !errors.Is(err, ErrAuth) {
		t.Fatalf("tamper must fail: %v", err)
	}
	if len(w2) != len("hello")+BoxOverhead {
		t.Fatalf("overhead: %d", len(w2)-len("hello"))
	}
}

func TestUseCounterLimits(t *testing.T) {
	b, _ := NewBox(bytes.Repeat([]byte{2}, 32))
	var warned uint64
	c := &UseCounter{OnWarn: func(n uint64) { warned = n }}
	b.Counter = c
	c.Seed(NonceWarnAt - 1)
	if _, err := b.Seal([]byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	if warned != NonceWarnAt {
		t.Fatalf("warning at %d, want %d", warned, NonceWarnAt)
	}
	c.Seed(NonceLimit - 1)
	if _, err := b.Seal([]byte("x"), nil); err != nil {
		t.Fatalf("the last allowed use must work: %v", err)
	}
	if _, err := b.Seal([]byte("x"), nil); !errors.Is(err, ErrNonceLimit) {
		t.Fatalf("over the limit: %v", err)
	}
	if c.Load() != NonceLimit {
		t.Fatalf("a refused seal must not count: %d", c.Load())
	}
}

func TestPadme(t *testing.T) {
	cases := map[int]int{
		0: 0, 1: 1, 2: 2, 3: 3, 9: 10, 100: 104,
		4 << 20:       4 << 20, // full chunk: no padding
		16 << 20:      16 << 20,
		4<<20 + 1:     4<<20 + 128<<10,
		1000000:       1015808, // multiple of 32 KiB
		256<<10 + 123: 256<<10 + 8<<10,
	}
	for in, want := range cases {
		if got := PadmeLen(in); got != want {
			t.Errorf("PadmeLen(%d) = %d, want %d", in, got, want)
		}
	}
	// Overhead ≤ 3.1 % from 64 KiB up; padded ≥ input; monotone.
	prev := 0
	for L := 1; L < 64<<20; L = L*3/2 + 7 {
		p := PadmeLen(L)
		if p < L || p < prev {
			t.Fatalf("PadmeLen(%d)=%d not ≥ input / monotone", L, p)
		}
		if L >= 64<<10 && float64(p-L)/float64(L) > 0.0313 {
			t.Fatalf("PadmeLen(%d)=%d overhead %.2f%%", L, p, 100*float64(p-L)/float64(L))
		}
		prev = p
	}
}

func TestWrapDEKv2(t *testing.T) {
	kek := bytes.Repeat([]byte{3}, 32)
	dek, _ := NewDEK()
	w, err := WrapDEKv2(kek, dek)
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnwrapDEKv2(kek, w)
	if err != nil || !bytes.Equal(got, dek) {
		t.Fatalf("unwrap: %v", err)
	}
	if _, err := UnwrapDEKv2(bytes.Repeat([]byte{4}, 32), w); !errors.Is(err, ErrWrongSecret) {
		t.Fatalf("wrong kek: %v", err)
	}
	// A format-1 wrap must not unwrap as format 2.
	w1, _ := WrapDEK(kek, dek)
	if _, err := UnwrapDEKv2(kek, w1); err == nil {
		t.Fatal("a format-1 wrapped DEK must not open as format 2")
	}
}
