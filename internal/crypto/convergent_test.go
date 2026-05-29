package crypto

import (
	"bytes"
	"testing"
)

// TestConvergentDeterminism is the load-bearing invariant for v3:
// sealing the SAME plaintext under the SAME DEK MUST produce
// byte-identical output. That's what makes dedup work — the writer
// hashes plaintext, finds an indexed message with that hash, and
// reuses it without re-uploading. If two seals produced different
// bytes, the channel would hold a stale ciphertext that the dedup
// path then tries to decrypt with the new (different) nonce → tag
// failure on every shared-chunk read.
func TestConvergentDeterminism(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	c, err := NewAESGCMConvergent(key)
	if err != nil {
		t.Fatal(err)
	}
	pt := []byte("the exact same chunk bytes, twice")
	a, err := c.Seal(0, 0, pt)
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Seal(0, 0, pt)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("convergent Seal is not deterministic — dedup would break:\n  a=%x\n  b=%x", a, b)
	}
	// ino, idx must not affect output — those are ignored under v3.
	c2, err := c.Seal(999, 7, pt)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, c2) {
		t.Fatalf("convergent Seal must IGNORE (ino, idx); shared chunks across slots would otherwise re-upload")
	}
}

// TestConvergentDistinctness: different plaintexts MUST produce
// different ciphertexts (with very high probability). A failure here
// would mean nonce derivation collapsed distinct content onto one
// nonce — catastrophic for AES-GCM (key+nonce reuse on distinct
// plaintext lets an attacker recover the GMAC key).
func TestConvergentDistinctness(t *testing.T) {
	key := make([]byte, 32)
	c, err := NewAESGCMConvergent(key)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := c.Seal(0, 0, []byte("alpha"))
	b, _ := c.Seal(0, 0, []byte("beta"))
	if bytes.Equal(a, b) {
		t.Fatalf("distinct plaintexts produced identical ciphertext — nonce derivation is broken")
	}
	// Nonces (the first 12 bytes) must also be distinct.
	if bytes.Equal(a[:12], b[:12]) {
		t.Fatalf("distinct plaintexts produced identical nonces — would break AES-GCM")
	}
}

// TestConvergentRoundTrip covers the basic Seal→Open path including
// the critical "Open must use nil AAD and ignore (ino, idx)" rule.
// If Open were copied from AESGCM (which binds AAD to (ino, idx)),
// every dedup'd read would tag-fail because the same blob is
// referenced from multiple slots.
func TestConvergentRoundTrip(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i * 7)
	}
	c, err := NewAESGCMConvergent(key)
	if err != nil {
		t.Fatal(err)
	}
	pt := []byte("the chunk plaintext that must round-trip")
	ct, err := c.Seal(5, 2, pt)
	if err != nil {
		t.Fatal(err)
	}
	// Open it back under DIFFERENT (ino, idx) — this is the
	// dedup-across-slots case and must work.
	got, err := c.Open(99, 17, ct)
	if err != nil {
		t.Fatalf("Open across different slot failed (would break dedup'd reads): %v", err)
	}
	if !bytes.Equal(got, pt) {
		t.Fatalf("round-trip mismatch")
	}
}

// TestConvergentSurvivesCipherRecreation: a Seal under one cipher
// instance must decrypt under a freshly-constructed cipher with the
// same key. Catches DEK-copy bugs and any hidden state in the AESGCM
// AEAD that might be process-local.
func TestConvergentSurvivesCipherRecreation(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(0x42 ^ i)
	}
	sealer, _ := NewAESGCMConvergent(key)
	ct, err := sealer.Seal(0, 0, []byte("cross-process payload"))
	if err != nil {
		t.Fatal(err)
	}
	opener, _ := NewAESGCMConvergent(key)
	got, err := opener.Open(0, 0, ct)
	if err != nil {
		t.Fatalf("fresh cipher instance failed to open: %v", err)
	}
	if string(got) != "cross-process payload" {
		t.Fatalf("payload mismatch across cipher instances")
	}
}

// TestConvergentTamperDetection: flipping any bit in the ciphertext
// must produce ErrAuth from Open.
func TestConvergentTamperDetection(t *testing.T) {
	key := make([]byte, 32)
	c, _ := NewAESGCMConvergent(key)
	ct, _ := c.Seal(0, 0, []byte("hi"))
	for i := 0; i < len(ct); i++ {
		bad := append([]byte{}, ct...)
		bad[i] ^= 0x01
		if _, err := c.Open(0, 0, bad); err == nil {
			t.Fatalf("tampered ciphertext at byte %d decrypted successfully — GCM tag did not catch it", i)
		}
	}
}

// TestConvergentWrongKeyFails: a different DEK must NOT decrypt
// (the GCM tag will fail).
func TestConvergentWrongKeyFails(t *testing.T) {
	k1 := make([]byte, 32)
	k2 := make([]byte, 32)
	k2[0] = 1
	c1, _ := NewAESGCMConvergent(k1)
	c2, _ := NewAESGCMConvergent(k2)
	ct, _ := c1.Seal(0, 0, []byte("secret"))
	if _, err := c2.Open(0, 0, ct); err == nil {
		t.Fatal("wrong-key Open should fail")
	}
}

// TestDeterministicMarker: the dedup-safe marker must be present on
// NoopCipher and AESGCMConvergent and ABSENT on AESGCM. The writer
// uses a marker-interface assertion (not a switch on concrete types)
// to decide dedup eligibility; if anyone adds Deterministic() to
// AESGCM in the future, this test catches it before it ships.
func TestDeterministicMarker(t *testing.T) {
	type marker interface{ Deterministic() bool }
	if _, ok := any(NoopCipher{}).(marker); !ok {
		t.Fatal("NoopCipher must implement Deterministic()")
	}
	c, _ := NewAESGCMConvergent(make([]byte, 32))
	if _, ok := any(c).(marker); !ok {
		t.Fatal("AESGCMConvergent must implement Deterministic()")
	}
	a, _ := NewAESGCM(make([]byte, 32))
	if _, ok := any(a).(marker); ok {
		t.Fatal("AESGCM must NOT implement Deterministic() — random-nonce ciphers are not dedup-safe")
	}
}
