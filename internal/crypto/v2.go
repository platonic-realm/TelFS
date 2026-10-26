package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/bits"
	"sync/atomic"
)

// Format-2 key labels (spec P1 §1). Every format-2 key derives from
// one root by HKDF-SHA256 with a label per purpose, so no key ever
// serves two purposes.
const (
	LabelChunk   = "telfs2 chunk"   // chunk and pack-extent bodies
	LabelHash    = "telfs2 hash"    // keyed content hash (HMAC-SHA256)
	LabelMeta    = "telfs2 meta"    // snapshot / journal bodies, pack indexes
	LabelCaption = "telfs2 caption" // captions + the key message check
	LabelSession = "telfs2 session" // session.enc (root = Argon2id(secret, session salt))
)

// DeriveSubkey returns HKDF-SHA256(root, info=label), 32 bytes.
func DeriveSubkey(root []byte, label string) ([]byte, error) {
	if len(root) < 32 {
		return nil, fmt.Errorf("crypto: subkey root must be ≥ 32 bytes, got %d", len(root))
	}
	return hkdf.Key(sha256.New, root, nil, label, 32)
}

// RFC 9106's first recommended Argon2id setting — the format-2
// default (0.30 s on S9, measured 2026-10-25). Memory is the TOTAL in
// KiB (x/crypto/argon2 splits it over the lanes).
func DefaultArgonParamsV2() ArgonParams {
	return ArgonParams{Time: 1, Memory: 2 << 20, Threads: 4}
}

// Nonce limit for AES-GCM with random 96-bit nonces: at most 2^32
// encryptions per key (NIST SP 800-38D §8.3). TelFS warns at 2^31.
const (
	NonceWarnAt uint64 = 1 << 31
	NonceLimit  uint64 = 1 << 32
)

// ErrNonceLimit is returned once a key has been used NonceLimit times.
var ErrNonceLimit = errors.New("crypto: this key has reached the 2^32-encryption limit of AES-GCM with random nonces — the filesystem is read-only from here; move the data to a new filesystem")

// UseCounter counts encryptions under one key. Safe for concurrent use.
type UseCounter struct {
	n      atomic.Uint64
	warned atomic.Bool
	// OnWarn, if set, is called once when the count crosses NonceWarnAt.
	OnWarn func(n uint64)
}

// Load returns the current count.
func (c *UseCounter) Load() uint64 { return c.n.Load() }

// Seed sets the count (from persisted state).
func (c *UseCounter) Seed(v uint64) { c.n.Store(v) }

// add reserves one use. Over the limit it refuses without counting.
func (c *UseCounter) add() error {
	for {
		cur := c.n.Load()
		if cur >= NonceLimit {
			return ErrNonceLimit
		}
		if c.n.CompareAndSwap(cur, cur+1) {
			if cur+1 >= NonceWarnAt && c.warned.CompareAndSwap(false, true) && c.OnWarn != nil {
				c.OnWarn(cur + 1)
			}
			return nil
		}
	}
}

// Box is AES-256-GCM with a fresh random 12-byte nonce per seal. Wire:
// nonce ‖ ciphertext ‖ tag. A nil Counter means "not counted" (tests,
// one-off use).
type Box struct {
	aead    cipher.AEAD
	Counter *UseCounter
}

// NewBox builds a Box from a 32-byte key.
func NewBox(key []byte) (*Box, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("crypto: box needs a 32-byte key, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Box{aead: aead}, nil
}

// Overhead is the bytes a seal adds (nonce + tag).
const BoxOverhead = nonceSize + 16

// Seal encrypts plaintext with aad bound in.
func (b *Box) Seal(plaintext, aad []byte) ([]byte, error) {
	if b.Counter != nil {
		if err := b.Counter.add(); err != nil {
			return nil, err
		}
	}
	out := make([]byte, nonceSize, nonceSize+len(plaintext)+b.aead.Overhead())
	if _, err := rand.Read(out); err != nil {
		return nil, fmt.Errorf("crypto: nonce: %w", err)
	}
	return b.aead.Seal(out, out[:nonceSize], plaintext, aad), nil
}

// Open reverses Seal; ErrAuth on a wrong key, wrong aad or tampering.
func (b *Box) Open(wire, aad []byte) ([]byte, error) {
	if len(wire) < BoxOverhead {
		return nil, fmt.Errorf("crypto: sealed data too short (%d bytes)", len(wire))
	}
	pt, err := b.aead.Open(nil, wire[:nonceSize], wire[nonceSize:], aad)
	if err != nil {
		return nil, ErrAuth
	}
	return pt, nil
}

// PadmeLen returns the Padmé-padded length for L bytes (Nikitin et al.,
// "Reducing Metadata Leakage from Encrypted Files and Communication
// with PURBs", 2019): only the top ~log2(log2 L) bits of the length
// stay free, so sizes are hidden to within ≤ 12 % (≤ 3.1 % from 64 KiB
// up). Powers of two are their own padded length.
func PadmeLen(L int) int {
	if L < 2 {
		return L
	}
	e := bits.Len(uint(L)) - 1 // floor(log2 L)
	s := bits.Len(uint(e))     // floor(log2 e) + 1
	lastBits := e - s
	if lastBits <= 0 {
		return L
	}
	mask := (1 << lastBits) - 1
	return (L + mask) &^ mask
}

// WrapDEKv2 / UnwrapDEKv2: format 2 wraps the DEK under its own AAD so
// a format-1 wrapped key can never be fed to format 2 or vice versa.
const dekWrapAADv2 = "telfs2 dek wrap"

func WrapDEKv2(kek, dek []byte) ([]byte, error) {
	b, err := NewBox(kek)
	if err != nil {
		return nil, err
	}
	if len(dek) != DEKLen {
		return nil, fmt.Errorf("crypto: dek must be %d bytes", DEKLen)
	}
	return b.Seal(dek, []byte(dekWrapAADv2))
}

func UnwrapDEKv2(kek, wrapped []byte) ([]byte, error) {
	b, err := NewBox(kek)
	if err != nil {
		return nil, err
	}
	dek, err := b.Open(wrapped, []byte(dekWrapAADv2))
	if err != nil {
		return nil, ErrWrongSecret
	}
	if len(dek) != DEKLen {
		return nil, fmt.Errorf("crypto: unwrapped DEK has length %d", len(dek))
	}
	return dek, nil
}

// ErrWrongSecret: the secret does not open the key (GCM cannot tell a
// wrong secret from a damaged header; both read as this).
var ErrWrongSecret = errors.New("the secret does not open this key (wrong secret, or the key header is damaged)")
