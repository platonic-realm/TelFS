package crypto

import (
	"crypto/rand"
	"encoding/json"
	"fmt"

	"golang.org/x/crypto/argon2"
)

// KDFName labels the key-derivation algorithm used by a given salt.
// Only "argon2id" is supported in M7; future versions can add new
// labels without breaking existing data.
const KDFName = "argon2id"

// KeyLen is the derived-key length in bytes (32 for AES-256).
const KeyLen = 32

// SaltLen is the salt length in bytes — 16 follows the Argon2 RFC's
// recommendation.
const SaltLen = 16

// ArgonParams holds the tunable Argon2id parameters. Stored in
// meta_kv['crypto_argon_params'] so a future upgrade to stronger
// parameters doesn't lock out existing filesystems.
//
// Defaults: time=3, memory=64 MiB, threads=4. OWASP's 2024 baseline.
// Peak RAM at derivation = memory * threads = 256 MiB; users on
// memory-constrained machines may want to dial this down at
// `encrypt init` time.
type ArgonParams struct {
	Time    uint32 `json:"time"`
	Memory  uint32 `json:"memory"` // in KiB
	Threads uint8  `json:"threads"`
}

// DefaultArgonParams is the parameter set used by `telfs encrypt init`
// unless overridden.
func DefaultArgonParams() ArgonParams {
	return ArgonParams{
		Time:    3,
		Memory:  64 * 1024, // 64 MiB
		Threads: 4,
	}
}

// MarshalArgonParams returns the JSON encoding used in meta_kv.
func MarshalArgonParams(p ArgonParams) ([]byte, error) { return json.Marshal(p) }

// UnmarshalArgonParams parses the JSON encoding from meta_kv.
func UnmarshalArgonParams(b []byte) (ArgonParams, error) {
	var p ArgonParams
	if err := json.Unmarshal(b, &p); err != nil {
		return ArgonParams{}, fmt.Errorf("crypto: unmarshal argon params: %w", err)
	}
	return p, nil
}

// NewSalt returns SaltLen random bytes.
func NewSalt() ([]byte, error) {
	salt := make([]byte, SaltLen)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("crypto: new salt: %w", err)
	}
	return salt, nil
}

// DeriveKey runs Argon2id over the passphrase + salt with the given
// parameters and returns a 32-byte AES key.
func DeriveKey(passphrase []byte, salt []byte, p ArgonParams) []byte {
	return argon2.IDKey(passphrase, salt, p.Time, p.Memory, p.Threads, KeyLen)
}
