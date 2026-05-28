package crypto

import (
	"bytes"
	"crypto/rand"
	"errors"
	"testing"
)

func newCipher(t *testing.T) *AESGCM {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	c, err := NewAESGCM(key)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestAESGCMRoundTrip(t *testing.T) {
	c := newCipher(t)
	pt := []byte("hello chunk")
	ct, err := c.Seal(42, 7, pt)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(ct, pt) {
		t.Fatalf("ciphertext equals plaintext — encryption did nothing")
	}
	got, err := c.Open(42, 7, ct)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !bytes.Equal(got, pt) {
		t.Fatalf("round-trip mismatch: got %q want %q", got, pt)
	}
}

// TestAESGCMNonceIsRandom: two seals of the same (ino, idx, plaintext)
// must produce different ciphertexts because the nonce is random.
func TestAESGCMNonceIsRandom(t *testing.T) {
	c := newCipher(t)
	pt := []byte("repeat me")
	a, _ := c.Seal(1, 1, pt)
	b, _ := c.Seal(1, 1, pt)
	if bytes.Equal(a, b) {
		t.Fatalf("two seals produced identical ciphertext — nonce reuse risk")
	}
}

// TestAESGCMTamperFails: flipping a byte in ciphertext must fail
// authentication on Open.
func TestAESGCMTamperFails(t *testing.T) {
	c := newCipher(t)
	pt := []byte("don't tamper with me")
	ct, _ := c.Seal(5, 0, pt)
	ct[len(ct)-1] ^= 0xff // flip a tag byte
	if _, err := c.Open(5, 0, ct); !errors.Is(err, ErrAuth) {
		t.Fatalf("tampered ciphertext: err = %v, want ErrAuth", err)
	}
}

// TestAESGCMSlotMismatchFails: ciphertext sealed for (ino=5, idx=0)
// must NOT open as (ino=5, idx=1) — AAD binds ciphertext to its slot.
func TestAESGCMSlotMismatchFails(t *testing.T) {
	c := newCipher(t)
	ct, _ := c.Seal(5, 0, []byte("slot 0 content"))
	if _, err := c.Open(5, 1, ct); !errors.Is(err, ErrAuth) {
		t.Fatalf("slot mismatch: err = %v, want ErrAuth", err)
	}
	if _, err := c.Open(6, 0, ct); !errors.Is(err, ErrAuth) {
		t.Fatalf("ino mismatch: err = %v, want ErrAuth", err)
	}
}

// TestAESGCMWrongKeyFails: ciphertext sealed under one key must not
// decrypt under another.
func TestAESGCMWrongKeyFails(t *testing.T) {
	c1 := newCipher(t)
	c2 := newCipher(t)
	ct, _ := c1.Seal(0, 0, []byte("secret"))
	if _, err := c2.Open(0, 0, ct); !errors.Is(err, ErrAuth) {
		t.Fatalf("wrong key: err = %v, want ErrAuth", err)
	}
}

func TestAESGCMRejectsShortKey(t *testing.T) {
	if _, err := NewAESGCM(make([]byte, 16)); err == nil {
		t.Fatalf("NewAESGCM should reject a 16-byte key")
	}
}

func TestAESGCMRejectsTruncatedCiphertext(t *testing.T) {
	c := newCipher(t)
	ct, _ := c.Seal(0, 0, []byte("hi"))
	if _, err := c.Open(0, 0, ct[:5]); err == nil {
		t.Fatalf("Open should reject truncated ciphertext")
	}
}
