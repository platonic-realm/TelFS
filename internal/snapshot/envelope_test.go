package snapshot

import (
	"bytes"
	"testing"

	"telfs/internal/crypto"
)

// TestEnvelopeRoundTrip is the regression that catches a broken
// encrypted-snapshot recovery path before the user loses data.
// Simulates the full lifecycle: derive a key from a known passphrase,
// Wrap a gzipped plaintext, Unwrap on a "fresh" cipher derived from
// the same passphrase via the envelope's embedded salt, verify
// canary, decrypt body, compare to original.
func TestEnvelopeRoundTrip(t *testing.T) {
	// Cheap KDF params for the test — real defaults take ~500ms.
	params := crypto.ArgonParams{Time: 1, Memory: 8 * 1024, Threads: 1}
	argonJSON, err := crypto.MarshalArgonParams(params)
	if err != nil {
		t.Fatal(err)
	}

	salt, err := crypto.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	passphrase := []byte("hunter2")
	key := crypto.DeriveKey(passphrase, salt, params)
	cipher, err := crypto.NewAESGCM(key)
	if err != nil {
		t.Fatal(err)
	}
	canary, err := crypto.SealCanary(cipher)
	if err != nil {
		t.Fatal(err)
	}

	// Plaintext is what `snapshot.Take` would return — a gzipped
	// SQLite blob. Content doesn't matter for the envelope test;
	// any bytes will do.
	plaintext := []byte("this is the gzipped DB pretend-plaintext, ~50B")
	wrapped, err := Wrap(cipher, salt, argonJSON, canary, plaintext)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}

	// Sanity: wrapping must NOT look like the plaintext.
	if bytes.Equal(wrapped, plaintext) {
		t.Fatalf("Wrap produced identity output — encryption did nothing")
	}
	if !IsWrapped(wrapped) {
		t.Fatalf("Wrap output isn't recognized by IsWrapped")
	}

	// Recovery side: parse the envelope cold, derive a fresh key from
	// the user's passphrase, decrypt.
	gotSalt, gotArgonJSON, gotCanary, err := EnvelopeKDFParams(wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotSalt, salt) {
		t.Fatalf("envelope salt mismatch")
	}
	if !bytes.Equal(gotCanary, canary) {
		t.Fatalf("envelope canary mismatch")
	}
	recoveredParams, err := crypto.UnmarshalArgonParams(gotArgonJSON)
	if err != nil {
		t.Fatal(err)
	}
	if recoveredParams != params {
		t.Fatalf("argon params mismatch: %+v vs %+v", recoveredParams, params)
	}

	// Derive from passphrase + envelope's salt; should produce the
	// same key.
	recoveredKey := crypto.DeriveKey(passphrase, gotSalt, recoveredParams)
	if !bytes.Equal(recoveredKey, key) {
		t.Fatalf("recovered key does not match (envelope KDF state corrupted?)")
	}
	recoveredCipher, err := crypto.NewAESGCM(recoveredKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := crypto.VerifyCanary(recoveredCipher, gotCanary); err != nil {
		t.Fatalf("VerifyCanary on recovered cipher: %v", err)
	}
	body, err := UnwrapBody(wrapped)
	if err != nil {
		t.Fatal(err)
	}
	got, err := recoveredCipher.Open(0, -1, body)
	if err != nil {
		t.Fatalf("Open envelope body: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("round-trip mismatch: got %q want %q", got, plaintext)
	}
}

// TestEnvelopeWrongPassphraseFailsAtCanary mirrors the live mount-time
// behavior: a wrong passphrase derives a wrong key, and we expect
// canary verification to fail BEFORE we attempt to decrypt the body.
func TestEnvelopeWrongPassphraseFailsAtCanary(t *testing.T) {
	params := crypto.ArgonParams{Time: 1, Memory: 8 * 1024, Threads: 1}
	argonJSON, _ := crypto.MarshalArgonParams(params)
	salt, _ := crypto.NewSalt()
	cipher, _ := crypto.NewAESGCM(crypto.DeriveKey([]byte("right"), salt, params))
	canary, _ := crypto.SealCanary(cipher)
	wrapped, err := Wrap(cipher, salt, argonJSON, canary, []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	_, _, gotCanary, _ := EnvelopeKDFParams(wrapped)
	wrongCipher, _ := crypto.NewAESGCM(crypto.DeriveKey([]byte("wrong"), salt, params))
	if err := crypto.VerifyCanary(wrongCipher, gotCanary); err == nil {
		t.Fatalf("canary should NOT verify under wrong-passphrase key")
	}
}

// TestIsWrappedRejectsPlaintextGzip — a plaintext gzipped DB starts
// with 0x1F 0x8B; IsWrapped must say "not a wrapped envelope" for
// those, so the dual-path recovery code routes correctly.
func TestIsWrappedRejectsPlaintextGzip(t *testing.T) {
	gz := []byte{0x1F, 0x8B, 0x08, 0x00, 0, 0, 0, 0}
	if IsWrapped(gz) {
		t.Fatalf("IsWrapped(<gzip magic>) returned true — would misroute legacy snapshots")
	}
}
