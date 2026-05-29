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
	wrapped, err := Wrap(cipher, WrapOpts{
		Mode:   crypto.ModeAESGCMv1,
		Salt:   salt,
		Argon:  argonJSON,
		Canary: canary,
	}, plaintext)
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
	wrapped, err := Wrap(cipher, WrapOpts{
		Mode:   crypto.ModeAESGCMv1,
		Salt:   salt,
		Argon:  argonJSON,
		Canary: canary,
	}, []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	_, _, gotCanary, _ := EnvelopeKDFParams(wrapped)
	wrongCipher, _ := crypto.NewAESGCM(crypto.DeriveKey([]byte("wrong"), salt, params))
	if err := crypto.VerifyCanary(wrongCipher, gotCanary); err == nil {
		t.Fatalf("canary should NOT verify under wrong-passphrase key")
	}
}

// TestEnvelopeV2RoundTrip mirrors the v2 cold-recovery path:
// passphrase → KEK → unwrap(envelope.WrappedDEK) → DEK → canary +
// body decrypt. The DEK never leaves memory; the channel sees only
// nonce(12)||GCM(KEK, DEK).
func TestEnvelopeV2RoundTrip(t *testing.T) {
	params := crypto.ArgonParams{Time: 1, Memory: 8 * 1024, Threads: 1}
	argonJSON, _ := crypto.MarshalArgonParams(params)
	salt, _ := crypto.NewSalt()
	passphrase := []byte("hunter2")
	kek := crypto.DeriveKey(passphrase, salt, params)

	dek, err := crypto.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	wrappedDEK, err := crypto.WrapDEK(kek, dek)
	if err != nil {
		t.Fatal(err)
	}
	bodyCipher, _ := crypto.NewAESGCM(dek)
	canary, _ := crypto.SealCanary(bodyCipher)

	plaintext := []byte("the v2 snapshot body — same shape as v1, different key derivation")
	wrapped, err := Wrap(bodyCipher, WrapOpts{
		Mode:       crypto.ModeAESGCMv2,
		Salt:       salt,
		Argon:      argonJSON,
		Canary:     canary,
		WrappedDEK: wrappedDEK,
	}, plaintext)
	if err != nil {
		t.Fatalf("Wrap v2: %v", err)
	}

	// Recovery side: pretend the local DB is gone. All we have is the
	// passphrase + the envelope blob.
	hdr, body, err := UnwrapHeaderAndBody(wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Mode != crypto.ModeAESGCMv2 {
		t.Fatalf("envelope mode: got %q want %q", hdr.Mode, crypto.ModeAESGCMv2)
	}
	if len(hdr.WrappedDEK) == 0 {
		t.Fatalf("v2 envelope must carry wrapped DEK")
	}
	recoveredKEK := crypto.DeriveKey(passphrase, hdr.Salt, params)
	recoveredDEK, err := crypto.UnwrapDEK(recoveredKEK, hdr.WrappedDEK)
	if err != nil {
		t.Fatalf("UnwrapDEK with correct passphrase: %v", err)
	}
	if !bytes.Equal(recoveredDEK, dek) {
		t.Fatalf("recovered DEK does not match original")
	}
	recoveredCipher, _ := crypto.NewAESGCM(recoveredDEK)
	if err := crypto.VerifyCanary(recoveredCipher, hdr.Canary); err != nil {
		t.Fatalf("VerifyCanary on v2 recovery: %v", err)
	}
	got, err := recoveredCipher.Open(0, -1, body)
	if err != nil {
		t.Fatalf("Open v2 body: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("v2 round-trip mismatch")
	}

	// Negative path: wrong passphrase produces a KEK that can't unwrap
	// the DEK — fails at UnwrapDEK, not at the canary.
	wrongKEK := crypto.DeriveKey([]byte("wrong"), hdr.Salt, params)
	if _, err := crypto.UnwrapDEK(wrongKEK, hdr.WrappedDEK); err == nil {
		t.Fatalf("UnwrapDEK with wrong passphrase should fail")
	}
}

// TestEnvelopeV2RotationSimulated mirrors what `telfs encrypt rotate`
// does: same DEK, new salt, new KEK, new wrapped DEK. The DEK is
// unchanged so existing chunks remain readable; the rewrap is O(1)
// regardless of FS size.
func TestEnvelopeV2RotationSimulated(t *testing.T) {
	params := crypto.ArgonParams{Time: 1, Memory: 8 * 1024, Threads: 1}
	oldSalt, _ := crypto.NewSalt()
	oldKEK := crypto.DeriveKey([]byte("old-pass"), oldSalt, params)
	dek, _ := crypto.NewDEK()
	oldWrapped, _ := crypto.WrapDEK(oldKEK, dek)

	// Rotation: derive a new KEK from a new passphrase + new salt,
	// unwrap with the OLD KEK, rewrap with the NEW KEK.
	newSalt, _ := crypto.NewSalt()
	if bytes.Equal(newSalt, oldSalt) {
		t.Fatalf("NewSalt collision — extremely unlikely; check entropy source")
	}
	newKEK := crypto.DeriveKey([]byte("new-pass"), newSalt, params)
	recoveredDEK, err := crypto.UnwrapDEK(oldKEK, oldWrapped)
	if err != nil {
		t.Fatalf("unwrap with old kek: %v", err)
	}
	if !bytes.Equal(recoveredDEK, dek) {
		t.Fatalf("recovered DEK mismatch — rotation would lose data")
	}
	newWrapped, err := crypto.WrapDEK(newKEK, recoveredDEK)
	if err != nil {
		t.Fatalf("rewrap: %v", err)
	}

	// After rotation: old passphrase fails to unwrap.
	if _, err := crypto.UnwrapDEK(oldKEK, newWrapped); err == nil {
		t.Fatalf("old KEK should NOT unwrap the new wrapped DEK")
	}
	// New passphrase unwraps the same DEK as before.
	got, err := crypto.UnwrapDEK(newKEK, newWrapped)
	if err != nil {
		t.Fatalf("new KEK should unwrap the rewrapped DEK: %v", err)
	}
	if !bytes.Equal(got, dek) {
		t.Fatalf("rotation lost DEK identity")
	}
}

// TestEnvelopeV3RoundTrip mirrors v3 cold-recovery: passphrase → KEK
// → unwrap(envelope.WrappedDEK) → DEK → AESGCMConvergent → canary +
// body decrypt. Identical to v2 in structure; the only difference is
// the cipher type once the DEK is in hand. Verifies that the snapshot
// envelope path is correctly routed for v3, since a regression here
// would make every v3 FS unrecoverable after cache wipe.
func TestEnvelopeV3RoundTrip(t *testing.T) {
	params := crypto.ArgonParams{Time: 1, Memory: 8 * 1024, Threads: 1}
	argonJSON, _ := crypto.MarshalArgonParams(params)
	salt, _ := crypto.NewSalt()
	passphrase := []byte("convergent-pass")
	kek := crypto.DeriveKey(passphrase, salt, params)

	dek, _ := crypto.NewDEK()
	wrappedDEK, _ := crypto.WrapDEK(kek, dek)
	bodyCipher, err := crypto.NewAESGCMConvergent(dek)
	if err != nil {
		t.Fatal(err)
	}
	canary, _ := crypto.SealCanary(bodyCipher)

	plaintext := []byte("a v3 snapshot body — same envelope format, different cipher")
	wrapped, err := Wrap(bodyCipher, WrapOpts{
		Mode:       crypto.ModeAESGCMv3,
		Salt:       salt,
		Argon:      argonJSON,
		Canary:     canary,
		WrappedDEK: wrappedDEK,
	}, plaintext)
	if err != nil {
		t.Fatalf("Wrap v3: %v", err)
	}

	// Recovery side: rebuild from envelope + passphrase only.
	hdr, body, err := UnwrapHeaderAndBody(wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Mode != crypto.ModeAESGCMv3 {
		t.Fatalf("envelope mode: got %q want %q", hdr.Mode, crypto.ModeAESGCMv3)
	}
	recoveredKEK := crypto.DeriveKey(passphrase, hdr.Salt, params)
	recoveredDEK, err := crypto.UnwrapDEK(recoveredKEK, hdr.WrappedDEK)
	if err != nil {
		t.Fatalf("UnwrapDEK v3: %v", err)
	}
	recoveredCipher, _ := crypto.NewAESGCMConvergent(recoveredDEK)
	if err := crypto.VerifyCanary(recoveredCipher, hdr.Canary); err != nil {
		t.Fatalf("VerifyCanary v3: %v", err)
	}
	got, err := recoveredCipher.Open(0, -1, body)
	if err != nil {
		t.Fatalf("Open v3 body: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("v3 envelope round-trip mismatch")
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
