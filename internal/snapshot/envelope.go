package snapshot

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"telfs/internal/crypto"
)

// envelopeMagic identifies an encrypted snapshot blob. Chosen so it
// cannot be confused with a plaintext gzipped SQLite file — gzip's
// own magic is 0x1F 0x8B, never these bytes.
const envelopeMagic = "TFSE"

// envelopeVersion is the wire-format version. Bump for breaking
// changes; older versions can still be parsed via a switch.
const envelopeVersion byte = 1

// envelopeHeader is the per-snapshot bundle of public KDF state we
// embed in the encrypted body. Recovery (which has no local DB yet)
// reads this, prompts for the passphrase, and derives the key.
type envelopeHeader struct {
	// Mode mirrors meta_kv['crypto_mode'] ("aes-gcm-v1" for now).
	Mode string `json:"mode"`
	// Salt + Argon + Canary mirror the meta_kv values. We carry them
	// here so cold-mount recovery is self-contained: it doesn't need
	// any local state to find the right key.
	Salt   []byte          `json:"salt"`
	Argon  json.RawMessage `json:"argon"`
	Canary []byte          `json:"canary"`
}

// Wrap envelops gzipped plaintext bytes with an AES-GCM ciphertext
// plus the public KDF state needed to recover the key from a
// passphrase later. Format:
//
//	[ "TFSE" 4B ][ version 1B ][ hdr_len 2B BE ][ hdr_json ][ ciphertext ]
//
// AAD for the inner Seal is (ino=0, idx=-1) — slot reserved for
// per-FS-scoped data (canary uses (0,0); the snapshot uses (0,-1)).
func Wrap(c crypto.Cipher, salt, argonJSON, canary, plaintext []byte) ([]byte, error) {
	hdr := envelopeHeader{
		Mode:   crypto.ModeAESGCMv1,
		Salt:   salt,
		Argon:  argonJSON,
		Canary: canary,
	}
	hdrBytes, err := json.Marshal(hdr)
	if err != nil {
		return nil, fmt.Errorf("encode envelope header: %w", err)
	}
	if len(hdrBytes) > 0xFFFF {
		return nil, fmt.Errorf("envelope header too large (%d > 65535)", len(hdrBytes))
	}
	body, err := c.Seal(0, -1, plaintext)
	if err != nil {
		return nil, fmt.Errorf("seal snapshot body: %w", err)
	}

	var buf bytes.Buffer
	buf.WriteString(envelopeMagic)
	buf.WriteByte(envelopeVersion)
	_ = binary.Write(&buf, binary.BigEndian, uint16(len(hdrBytes)))
	buf.Write(hdrBytes)
	buf.Write(body)
	return buf.Bytes(), nil
}

// IsWrapped reports whether b looks like an encrypted snapshot
// envelope. Used by the restore path to sniff the format before
// deciding whether to ask the user for a passphrase.
func IsWrapped(b []byte) bool {
	return len(b) >= 4 && string(b[:4]) == envelopeMagic
}

// Unwrap parses an envelope and returns the public KDF state plus
// the encrypted body. The caller is expected to derive a key from
// the user's passphrase + Salt + Argon, build a Cipher, verify the
// Canary, and then call cipher.Open(0, -1, body) to get the
// plaintext (which is still gzipped — feed it to Restore).
func Unwrap(b []byte) (hdr envelopeHeader, body []byte, err error) {
	if !IsWrapped(b) {
		return hdr, nil, errors.New("snapshot: not a wrapped envelope")
	}
	if len(b) < 7 {
		return hdr, nil, errors.New("snapshot: envelope too short")
	}
	if b[4] != envelopeVersion {
		return hdr, nil, fmt.Errorf("snapshot: unsupported envelope version %d", b[4])
	}
	hdrLen := int(binary.BigEndian.Uint16(b[5:7]))
	if 7+hdrLen > len(b) {
		return hdr, nil, errors.New("snapshot: envelope truncated")
	}
	if err := json.Unmarshal(b[7:7+hdrLen], &hdr); err != nil {
		return envelopeHeader{}, nil, fmt.Errorf("snapshot: bad header: %w", err)
	}
	body = b[7+hdrLen:]
	return hdr, body, nil
}

// EnvelopeMode reports the cipher mode declared in a wrapped envelope,
// or "" if b is not wrapped.
func EnvelopeMode(b []byte) string {
	if !IsWrapped(b) {
		return ""
	}
	hdr, _, err := Unwrap(b)
	if err != nil {
		return ""
	}
	return hdr.Mode
}

// EnvelopeKDFParams returns the public KDF state from a wrapped
// envelope so a caller can derive the key from a user-supplied
// passphrase.
func EnvelopeKDFParams(b []byte) (salt []byte, argonJSON []byte, canary []byte, err error) {
	hdr, _, err := Unwrap(b)
	if err != nil {
		return nil, nil, nil, err
	}
	return hdr.Salt, hdr.Argon, hdr.Canary, nil
}

// UnwrapBody returns just the ciphertext body. Convenience for the
// recovery path once a Cipher has been built from the header.
func UnwrapBody(b []byte) ([]byte, error) {
	_, body, err := Unwrap(b)
	return body, err
}
