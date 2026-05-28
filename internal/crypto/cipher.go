package crypto

// Cipher transforms chunk payloads on the way to/from the channel.
// v1 ships a no-op implementation (NoopCipher); v2 will provide
// AES-256-GCM. The chunk pipeline only ever depends on this interface.
type Cipher interface {
	// Seal encrypts plaintext for chunk (ino, idx). The returned byte slice
	// is suitable for upload as-is. Implementations must be deterministic
	// w.r.t. (ino, idx) only insofar as they emit a self-describing
	// ciphertext (e.g. nonce-prefixed); they are not required to produce
	// identical output for identical inputs.
	Seal(ino uint64, idx uint32, plaintext []byte) ([]byte, error)

	// Open decrypts a ciphertext previously produced by Seal for the same
	// (ino, idx). Returns the plaintext.
	Open(ino uint64, idx uint32, ciphertext []byte) ([]byte, error)
}

// NoopCipher is the v1 identity transform. It performs no encryption; the
// chunk pipeline is wired through it so v2 can swap in AES-256-GCM without
// changes upstream.
type NoopCipher struct{}

func (NoopCipher) Seal(_ uint64, _ uint32, pt []byte) ([]byte, error) { return pt, nil }
func (NoopCipher) Open(_ uint64, _ uint32, ct []byte) ([]byte, error) { return ct, nil }
