package crypto

import (
	"fmt"
)

// meta_kv key names used by the encryption layer. The crypto package
// stays meta-free, but exporting these constants lets callers
// (cmd/telfs and internal/fs) wire them into meta.Store.GetKV /
// meta.Store.PutKV without re-typing strings.
const (
	KVMode       = "crypto_mode"          // identifies the cipher; "" = plaintext
	KVSalt       = "crypto_salt"          // bytes used by Argon2id
	KVArgon      = "crypto_argon_params"  // JSON-encoded ArgonParams
	KVCanary     = "crypto_canary"        // ciphertext of canaryPlaintext under the FS's key
	KVWrappedDEK = "crypto_wrapped_dek"   // v2: AES-GCM(KEK, nonce, DEK, AAD=salt)
)

// Cipher modes. ModeAESGCMv1 was the only mode shipped through v0.13;
// the passphrase-derived KEK directly encrypted chunks and snapshots.
// That meant rotating the passphrase required re-encrypting every
// chunk — impractical at scale.
//
// ModeAESGCMv2 adds a level of indirection: the user's passphrase
// derives a KEK; the KEK wraps a per-FS random DEK; chunks and
// snapshots use the DEK. Rotation only rewraps the DEK with the new
// passphrase-derived KEK — O(1) work regardless of FS size.
//
// Existing v1 filesystems keep working unchanged; the loader picks
// the cipher based on which mode is recorded in meta_kv. Rotation is
// only available on v2 filesystems.
const (
	ModeAESGCMv1 = "aes-gcm-v1"
	ModeAESGCMv2 = "aes-gcm-v2"
)

// canaryPlaintext is the well-known string we encrypt under the FS key
// to detect wrong-passphrase mounts early — before any user data flows
// through Open and surfaces confusing EIOs.
const canaryPlaintext = "telfs-canary-v1"

// SealCanary encrypts canaryPlaintext under c with AAD (ino=0, idx=0)
// — slot zero is reserved for the canary since it can never collide
// with a real chunk (chunk_map.idx is per-inode; ino=0 is reserved
// because it can't be allocated to a real inode).
func SealCanary(c Cipher) ([]byte, error) {
	return c.Seal(0, 0, []byte(canaryPlaintext))
}

// VerifyCanary returns nil if the encoded canary decrypts under c to
// the expected plaintext. Used by mount to fail fast on
// wrong-passphrase before any user-visible IO happens.
func VerifyCanary(c Cipher, encoded []byte) error {
	pt, err := c.Open(0, 0, encoded)
	if err != nil {
		return fmt.Errorf("crypto: canary did not decrypt — wrong passphrase? (%w)", err)
	}
	if string(pt) != canaryPlaintext {
		return fmt.Errorf("crypto: canary plaintext mismatch (corrupted state)")
	}
	return nil
}
