// Package wire is TelFS channel format 2 (spec P1 §4, decisions D2, D3,
// D6): what every message posted for a format-2 filesystem looks like.
//
//   - Every document is named DocName; every caption is exactly
//     CaptionLen characters: "tfs2" + base64 of an AES-GCM seal under
//     the caption key. The only clear content TelFS ever posts is the
//     key header (KeyMessage).
//   - Every body's plaintext is Padmé-padded before encryption; the
//     real length travels inside the ciphertext.
//   - Chunk (and pack-extent) bodies are sealed under the chunk key
//     with AAD = the chunk's keyed content hash; the reader re-checks
//     HMAC(plaintext) == hash.
package wire

import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"

	"telfs/internal/crypto"
)

// DocName is the filename attribute of every format-2 document.
const DocName = "blob"

// HashLen is the keyed content hash length (HMAC-SHA256).
const HashLen = 32

// Counters are the per-key encryption counts (nonce limit, P1 §1).
type Counters struct {
	Chunk, Meta, Caption crypto.UseCounter
}

// Keys are a format-2 filesystem's working keys, derived from its DEK.
type Keys struct {
	FSUUID  string
	Chunk   *crypto.Box
	Meta    *crypto.Box
	Caption *crypto.Box
	hashKey []byte
}

// DeriveKeys derives the working keys from the DEK. counters may be nil
// (not counted — tests and read-only tools).
func DeriveKeys(dek []byte, fsUUID string, counters *Counters) (*Keys, error) {
	if fsUUID == "" {
		return nil, errors.New("wire: fs_uuid is required")
	}
	k := &Keys{FSUUID: fsUUID}
	mk := func(label string, ctr *crypto.UseCounter) (*crypto.Box, error) {
		sub, err := crypto.DeriveSubkey(dek, label)
		if err != nil {
			return nil, err
		}
		defer zero(sub)
		b, err := crypto.NewBox(sub)
		if err != nil {
			return nil, err
		}
		b.Counter = ctr
		return b, nil
	}
	var cc, mc, capc *crypto.UseCounter
	if counters != nil {
		cc, mc, capc = &counters.Chunk, &counters.Meta, &counters.Caption
	}
	var err error
	if k.Chunk, err = mk(crypto.LabelChunk, cc); err != nil {
		return nil, err
	}
	if k.Meta, err = mk(crypto.LabelMeta, mc); err != nil {
		return nil, err
	}
	if k.Caption, err = mk(crypto.LabelCaption, capc); err != nil {
		return nil, err
	}
	if k.hashKey, err = crypto.DeriveSubkey(dek, crypto.LabelHash); err != nil {
		return nil, err
	}
	return k, nil
}

// Hash returns the keyed content hash of a chunk's plaintext. Equal
// content has an equal hash only under the same filesystem key, so the
// hash says nothing to anyone without it.
func (k *Keys) Hash(plaintext []byte) []byte {
	m := hmac.New(sha256.New, k.hashKey)
	m.Write(plaintext)
	return m.Sum(nil)
}

// HashMatches is the constant-time recheck the reader runs after
// decrypting a chunk.
func (k *Keys) HashMatches(plaintext, hash []byte) bool {
	return hmac.Equal(k.Hash(plaintext), hash)
}

// ErrHashMismatch: a chunk decrypted but its content does not match the
// hash it is filed under.
var ErrHashMismatch = errors.New("chunk content does not match its hash")

func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

func checkHash(h []byte) error {
	if len(h) != HashLen {
		return fmt.Errorf("wire: hash must be %d bytes, got %d", HashLen, len(h))
	}
	return nil
}
