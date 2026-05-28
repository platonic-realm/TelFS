package crypto

import (
	"bytes"
	"testing"
)

// Use cheap params for tests — the default 64 MiB Argon2 round would
// burn seconds.
var testArgon = ArgonParams{Time: 1, Memory: 8 * 1024, Threads: 1}

func TestDeriveKeyDeterministic(t *testing.T) {
	salt := []byte("0123456789abcdef")
	a := DeriveKey([]byte("hunter2"), salt, testArgon)
	b := DeriveKey([]byte("hunter2"), salt, testArgon)
	if !bytes.Equal(a, b) {
		t.Fatalf("same input gave different keys")
	}
	if len(a) != KeyLen {
		t.Fatalf("key len = %d, want %d", len(a), KeyLen)
	}
}

func TestDeriveKeyDependsOnPassphrase(t *testing.T) {
	salt := []byte("0123456789abcdef")
	a := DeriveKey([]byte("right"), salt, testArgon)
	b := DeriveKey([]byte("wrong"), salt, testArgon)
	if bytes.Equal(a, b) {
		t.Fatalf("different passphrases produced same key")
	}
}

func TestDeriveKeyDependsOnSalt(t *testing.T) {
	a := DeriveKey([]byte("hunter2"), []byte("0123456789abcdef"), testArgon)
	b := DeriveKey([]byte("hunter2"), []byte("ABCDEFGHIJKLMNOP"), testArgon)
	if bytes.Equal(a, b) {
		t.Fatalf("different salts produced same key")
	}
}

func TestNewSaltIsRandom(t *testing.T) {
	a, err := NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := NewSalt()
	if bytes.Equal(a, b) {
		t.Fatalf("two NewSalt calls returned the same value")
	}
	if len(a) != SaltLen {
		t.Fatalf("salt len = %d, want %d", len(a), SaltLen)
	}
}

func TestArgonParamsRoundTrip(t *testing.T) {
	want := ArgonParams{Time: 5, Memory: 128 * 1024, Threads: 8}
	b, err := MarshalArgonParams(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalArgonParams(b)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}
