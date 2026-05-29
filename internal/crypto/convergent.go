package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
)

// hmacNonceLabel is the personalization string baked into the HMAC
// that derives a chunk's nonce from its plaintext. Domain-separates
// the nonce derivation from any other use of the DEK as an HMAC key
// (e.g., a future content-hash scheme), so cross-purpose collisions
// can't happen.
const hmacNonceLabel = "telfs-v3-nonce\x00"

// AESGCMConvergent is the deterministic-encryption Cipher used by
// aes-gcm-v3 filesystems. Two seals of the same plaintext under the
// same DEK produce byte-identical output — that's what makes
// content-addressed dedup work on encrypted FSes.
//
// Construction:
//
//	nonce = HMAC-SHA256(DEK, hmacNonceLabel || plaintext)[:12]
//	ciphertext = AES-256-GCM.Seal(key=DEK, nonce=nonce, plaintext, aad=nil)
//	wire = nonce(12) || ciphertext_with_tag
//
// AAD is nil — binding to (ino, idx) would defeat dedup since
// identical plaintext at different slots would produce different
// ciphertext. The GCM tag still authenticates the ciphertext under
// the DEK, so tampering is still detected.
//
// Threat model implications (vs the random-nonce AESGCM):
//
//   - Confirmation-of-file does NOT apply. The nonce derivation is
//     keyed on the per-FS DEK, so a channel observer without the DEK
//     cannot compute the ciphertext for a candidate plaintext and
//     test possession. The DEK secrecy is the same as v2.
//
//   - The residual leak is equality detection. Identical chunk
//     plaintext yields identical chunk bytes on the channel; an
//     observer learns the count of *distinct* chunks rather than
//     just the total chunk count. The plaintext-FS dedup path leaks
//     this too (anyone can read the chunks anyway), so v3's leak is
//     strictly less than plaintext-FS leakage and more than v1/v2.
//
//   - Nonce collision risk: GCM is catastrophic on (key, nonce) reuse
//     across DISTINCT plaintexts. Here, equal plaintext is the only
//     way to collide nonces by construction, so reuse is always
//     across IDENTICAL plaintexts — safe. The remaining risk is an
//     HMAC-truncated-to-96-bit collision on different plaintexts:
//     birthday bound ~2^48 chunks, i.e. ~2^-33 even at 4 billion
//     chunks per FS. Negligible at any practical FS scale.
//
//   - Invariant: nonce derivation MUST cover the full plaintext, not
//     a prefix or a sample. Truncating the HMAC input would collapse
//     distinct plaintexts onto the same nonce and break GCM. The
//     constant + full-plaintext HMAC input above enforces this.
//
// The standards-track equivalent is AES-GCM-SIV (RFC 8452) — nonce-
// misuse resistant by construction. This implementation is a
// hand-rolled thinner variant; AES-GCM-SIV would be marginally safer
// against any future construction error but is not in the Go standard
// library. For a single-user FS the choice is acceptable; the
// invariant comment above is what keeps it correct.
type AESGCMConvergent struct {
	key  []byte // DEK; retained because we need it for HMAC, not just GCM
	aead cipher.AEAD
}

// NewAESGCMConvergent constructs the convergent cipher from a 32-byte
// DEK. The DEK is RETAINED in the struct (we need it as the HMAC key
// for nonce derivation), so callers must treat the cipher as
// key-bearing — don't pass it to untrusted code, zero on shutdown
// where it matters.
func NewAESGCMConvergent(key []byte) (*AESGCMConvergent, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("crypto: AESGCMConvergent needs a 32-byte DEK, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("crypto: aes.NewCipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: cipher.NewGCM: %w", err)
	}
	// Defensive copy so caller's slice can be zeroed without breaking us.
	dek := make([]byte, len(key))
	copy(dek, key)
	return &AESGCMConvergent{key: dek, aead: aead}, nil
}

// Seal encrypts plaintext deterministically. ino and idx are ignored
// — they're part of the Cipher interface for v1/v2 AAD binding but
// have no role here. Same plaintext + same DEK → byte-identical
// output.
func (c *AESGCMConvergent) Seal(_ int64, _ int32, plaintext []byte) ([]byte, error) {
	nonce := c.deriveNonce(plaintext)
	out := make([]byte, nonceSize, nonceSize+len(plaintext)+c.aead.Overhead())
	copy(out, nonce)
	out = c.aead.Seal(out, nonce, plaintext, nil)
	return out, nil
}

// Open decrypts a v3 ciphertext. Symmetric to Seal: nil AAD, ino and
// idx ignored. The GCM tag authenticates the ciphertext under the
// DEK.
//
// CRITICAL: this MUST NOT pass ino/idx as AAD. A dedup'd chunk is
// referenced from multiple (ino, idx) slots, and binding AAD to the
// slot would tag-fail every shared-chunk read. Tested by the v3
// dedup-survives-unlink regression in the encrypt_test suite.
func (c *AESGCMConvergent) Open(_ int64, _ int32, ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < nonceSize+c.aead.Overhead() {
		return nil, fmt.Errorf("crypto: convergent ciphertext too short (%d bytes)", len(ciphertext))
	}
	nonce := ciphertext[:nonceSize]
	body := ciphertext[nonceSize:]
	pt, err := c.aead.Open(nil, nonce, body, nil)
	if err != nil {
		return nil, ErrAuth
	}
	return pt, nil
}

// deriveNonce computes the deterministic nonce for `plaintext` under
// this cipher's DEK. Result is HMAC-SHA256(DEK, label || plaintext)
// truncated to 12 bytes. The label prevents cross-purpose collisions
// if the DEK is ever used as an HMAC key elsewhere.
func (c *AESGCMConvergent) deriveNonce(plaintext []byte) []byte {
	mac := hmac.New(sha256.New, c.key)
	mac.Write([]byte(hmacNonceLabel))
	mac.Write(plaintext)
	return mac.Sum(nil)[:nonceSize]
}

// Deterministic marks this cipher as suitable for content-addressed
// dedup. The writer queries this via the DedupSafe interface (see
// internal/chunk/writer.go) instead of type-asserting against concrete
// types — keeps the dedup decision a property of the cipher rather
// than a switch buried in the writer.
func (c *AESGCMConvergent) Deterministic() bool { return true }

// NoopCipher and AESGCMConvergent both qualify as dedup-safe; only
// the random-nonce AESGCM does not.
func (NoopCipher) Deterministic() bool { return true }

// (Intentionally NOT implemented on *AESGCM — the absence of this
// method is what the writer detects via the DedupSafe type assertion.
// Adding `Deterministic() bool { return false }` would also work but
// the missing-method approach makes accidental flips at the
// type-system level instead of leaving the safety in a runtime bool.)
