package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
)

// ErrAuth is returned by AESGCM.Open on tag mismatch (tampering,
// truncation, or wrong key).
var ErrAuth = errors.New("crypto: authentication failed")

// nonceSize is the GCM standard nonce size in bytes.
const nonceSize = 12

// aadSize is the size of our AAD (Additional Authenticated Data):
// 8 bytes big-endian ino, 4 bytes big-endian idx.
const aadSize = 12

// AESGCM is an authenticated-encryption Cipher using AES-256-GCM.
// Ciphertext format on the wire: nonce(12) || GCM-output(plaintext+tag).
// AAD is the 12-byte (ino || idx) tuple so a successfully-authenticated
// ciphertext is bound to the slot it was sealed for.
type AESGCM struct {
	aead cipher.AEAD
}

// NewAESGCM constructs an AESGCM cipher from a 32-byte key.
func NewAESGCM(key []byte) (*AESGCM, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("crypto: AES-256 needs a 32-byte key, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("crypto: aes.NewCipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: cipher.NewGCM: %w", err)
	}
	return &AESGCM{aead: aead}, nil
}

// Seal encrypts plaintext for chunk (ino, idx). Output is nonce-prefixed.
func (c *AESGCM) Seal(ino int64, idx int32, plaintext []byte) ([]byte, error) {
	nonce := make([]byte, nonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("crypto: nonce: %w", err)
	}
	aad := makeAAD(ino, idx)
	// Pre-allocate to avoid an internal append; nonce + ciphertext + tag.
	out := make([]byte, nonceSize, nonceSize+len(plaintext)+c.aead.Overhead())
	copy(out, nonce)
	out = c.aead.Seal(out, nonce, plaintext, aad)
	return out, nil
}

// Open decrypts ciphertext for chunk (ino, idx). Returns ErrAuth if the
// GCM tag doesn't authenticate.
func (c *AESGCM) Open(ino int64, idx int32, ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < nonceSize+c.aead.Overhead() {
		return nil, fmt.Errorf("crypto: ciphertext too short (%d bytes)", len(ciphertext))
	}
	nonce := ciphertext[:nonceSize]
	body := ciphertext[nonceSize:]
	aad := makeAAD(ino, idx)
	pt, err := c.aead.Open(nil, nonce, body, aad)
	if err != nil {
		return nil, ErrAuth
	}
	return pt, nil
}

// makeAAD encodes (ino, idx) into the AAD bytes.
func makeAAD(ino int64, idx int32) []byte {
	var b [aadSize]byte
	binary.BigEndian.PutUint64(b[0:8], uint64(ino))
	binary.BigEndian.PutUint32(b[8:12], uint32(idx))
	return b[:]
}
