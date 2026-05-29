package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
)

// DEKLen is the data-encryption-key size in bytes (AES-256).
const DEKLen = 32

// dekWrapAADConstant is the AAD attached to the wrapped-DEK ciphertext.
// Binds the wrap to "this is a TelFS DEK", so a wrapped key can't be
// repurposed across blob types accidentally.
const dekWrapAADConstant = "telfs-dek-wrap-v2"

// NewDEK returns 32 fresh random bytes suitable for use as a data
// encryption key in ModeAESGCMv2 filesystems.
func NewDEK() ([]byte, error) {
	dek := make([]byte, DEKLen)
	if _, err := rand.Read(dek); err != nil {
		return nil, fmt.Errorf("crypto: dek randomness: %w", err)
	}
	return dek, nil
}

// WrapDEK encrypts dek under kek using AES-256-GCM. Output format:
// nonce(12) || GCM(KEK, dek, AAD=dekWrapAADConstant). The AAD ties
// the ciphertext to its purpose so it can't be silently misused.
//
// kek must be 32 bytes; dek must be DEKLen bytes.
func WrapDEK(kek, dek []byte) ([]byte, error) {
	if len(kek) != 32 {
		return nil, fmt.Errorf("crypto: WrapDEK kek must be 32 bytes, got %d", len(kek))
	}
	if len(dek) != DEKLen {
		return nil, fmt.Errorf("crypto: WrapDEK dek must be %d bytes, got %d", DEKLen, len(dek))
	}
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, fmt.Errorf("crypto: aes.NewCipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: cipher.NewGCM: %w", err)
	}
	nonce := make([]byte, nonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("crypto: dek wrap nonce: %w", err)
	}
	out := make([]byte, nonceSize, nonceSize+len(dek)+aead.Overhead())
	copy(out, nonce)
	out = aead.Seal(out, nonce, dek, []byte(dekWrapAADConstant))
	return out, nil
}

// UnwrapDEK reverses WrapDEK. Returns the original DEK or an error if
// the kek is wrong or the wrapped blob is tampered. The error doesn't
// distinguish "wrong key" from "tampered blob" — GCM doesn't either.
func UnwrapDEK(kek, wrapped []byte) ([]byte, error) {
	if len(kek) != 32 {
		return nil, fmt.Errorf("crypto: UnwrapDEK kek must be 32 bytes, got %d", len(kek))
	}
	if len(wrapped) < nonceSize+DEKLen {
		return nil, errors.New("crypto: wrapped DEK too short")
	}
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, fmt.Errorf("crypto: aes.NewCipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: cipher.NewGCM: %w", err)
	}
	nonce := wrapped[:nonceSize]
	body := wrapped[nonceSize:]
	dek, err := aead.Open(nil, nonce, body, []byte(dekWrapAADConstant))
	if err != nil {
		return nil, fmt.Errorf("crypto: UnwrapDEK: %w (wrong passphrase or tampered wrapped-dek)", err)
	}
	if len(dek) != DEKLen {
		return nil, fmt.Errorf("crypto: UnwrapDEK produced unexpected length %d", len(dek))
	}
	return dek, nil
}
