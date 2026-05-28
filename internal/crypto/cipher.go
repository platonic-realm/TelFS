package crypto

// Cipher transforms chunk payloads on the way to/from the channel.
// The chunk pipeline always calls through this interface; NoopCipher is
// the default (plaintext), AESGCM provides authenticated encryption.
//
// (ino, idx) are bound into the AAD so a ciphertext that authenticates
// successfully for slot (5, 0) cannot be silently substituted into slot
// (6, 0) — even though they share the same key. This defends against an
// attacker with channel-write access who tries to splice ciphertexts
// across files. (It does NOT defend against substituting an older
// ciphertext for the SAME slot — replay attacks on identical (ino, idx).
// The threat model for M7 is "channel reader", not "channel writer";
// the single-mounter assumption means we ARE the only legitimate
// channel poster.)
type Cipher interface {
	// Seal encrypts plaintext for chunk (ino, idx). Implementations
	// produce self-describing output (nonce-prefixed for AESGCM).
	Seal(ino int64, idx int32, plaintext []byte) ([]byte, error)

	// Open decrypts a ciphertext previously produced by Seal for the same
	// (ino, idx). Returns ErrAuth on tag mismatch / tamper detection.
	Open(ino int64, idx int32, ciphertext []byte) ([]byte, error)
}

// NoopCipher is the identity transform — used when encryption isn't
// enabled for the filesystem.
type NoopCipher struct{}

func (NoopCipher) Seal(_ int64, _ int32, pt []byte) ([]byte, error) { return pt, nil }
func (NoopCipher) Open(_ int64, _ int32, ct []byte) ([]byte, error) { return ct, nil }
