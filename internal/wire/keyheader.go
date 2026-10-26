package wire

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"telfs/internal/crypto"
)

// KeyMessagePrefix starts the key message: a TEXT message (not a
// document), pinned in the channel. It is the only clear content TelFS
// posts: the KDF parameters, the salt and the wrapped DEK — useless
// without the secret (spec P1 §4.2, N1).
const KeyMessagePrefix = "tfs2k "

const keyCheckPlain = "telfs2 key check "

// KeyHeader is the content of the key message.
type KeyHeader struct {
	V    int    `json:"v"`
	KDF  string `json:"kdf"`
	T    uint32 `json:"t"`
	M    uint32 `json:"m"`
	P    uint8  `json:"p"`
	Salt []byte `json:"salt"`
	WDEK []byte `json:"wdek"`
	// Check binds the header to its filesystem: the caption key's seal
	// of "telfs2 key check <fs_uuid>". Opening it proves the DEK and
	// tells recovery the fs_uuid before any snapshot is read.
	Check []byte `json:"check"`
}

// Params returns the header's Argon2id parameters.
func (h KeyHeader) Params() crypto.ArgonParams {
	return crypto.ArgonParams{Time: h.T, Memory: h.M, Threads: h.P}
}

// NewKeyHeader wraps dek under a KEK derived from secret and builds the
// header. Costs one Argon2id derivation.
func NewKeyHeader(secret, dek []byte, fsUUID string, params crypto.ArgonParams) (KeyHeader, error) {
	salt, err := crypto.NewSalt()
	if err != nil {
		return KeyHeader{}, err
	}
	kek := crypto.DeriveKey(secret, salt, params)
	defer zero(kek)
	wdek, err := crypto.WrapDEKv2(kek, dek)
	if err != nil {
		return KeyHeader{}, err
	}
	keys, err := DeriveKeys(dek, fsUUID, nil)
	if err != nil {
		return KeyHeader{}, err
	}
	check, err := keys.Caption.Seal([]byte(keyCheckPlain+fsUUID), []byte("tfs2k"))
	if err != nil {
		return KeyHeader{}, err
	}
	return KeyHeader{V: 2, KDF: crypto.KDFName, T: params.Time, M: params.Memory, P: params.Threads,
		Salt: salt, WDEK: wdek, Check: check}, nil
}

// Unlock derives the KEK from secret, unwraps the DEK and opens the
// check. crypto.ErrWrongSecret on a wrong secret.
func (h KeyHeader) Unlock(secret []byte) (dek []byte, fsUUID string, err error) {
	if h.V != 2 || h.KDF != crypto.KDFName {
		return nil, "", fmt.Errorf("key header: unsupported version %d / kdf %q", h.V, h.KDF)
	}
	kek := crypto.DeriveKey(secret, h.Salt, h.Params())
	defer zero(kek)
	dek, err = crypto.UnwrapDEKv2(kek, h.WDEK)
	if err != nil {
		return nil, "", err
	}
	sub, err := crypto.DeriveSubkey(dek, crypto.LabelCaption)
	if err != nil {
		return nil, "", err
	}
	defer zero(sub)
	box, err := crypto.NewBox(sub)
	if err != nil {
		return nil, "", err
	}
	pt, err := box.Open(h.Check, []byte("tfs2k"))
	if err != nil || !strings.HasPrefix(string(pt), keyCheckPlain) {
		zero(dek)
		return nil, "", errors.New("key header: the check does not open under the unwrapped key (damaged header)")
	}
	return dek, strings.TrimPrefix(string(pt), keyCheckPlain), nil
}

// JSON returns the header's canonical JSON (what the message and the
// export carry, and what status/fsck compare byte for byte).
func (h KeyHeader) JSON() []byte {
	b, _ := json.Marshal(h)
	return b
}

// Message renders the key message text.
func (h KeyHeader) Message() string {
	return KeyMessagePrefix + base64.StdEncoding.EncodeToString(h.JSON())
}

// IsKeyMessage is the cheap prefilter for a key message.
func IsKeyMessage(text string) bool { return strings.HasPrefix(text, KeyMessagePrefix) }

// ParseKeyMessage parses a key message text.
func ParseKeyMessage(text string) (KeyHeader, []byte, error) {
	var h KeyHeader
	if !IsKeyMessage(text) {
		return h, nil, errors.New("not a key message")
	}
	js, err := base64.StdEncoding.DecodeString(strings.TrimSpace(strings.TrimPrefix(text, KeyMessagePrefix)))
	if err != nil {
		return h, nil, fmt.Errorf("key message: %w", err)
	}
	if err := json.Unmarshal(js, &h); err != nil {
		return h, nil, fmt.Errorf("key message: %w", err)
	}
	return h, js, nil
}

// ExportInfo is the human context printed around an exported header.
type ExportInfo struct {
	FSUUID       string
	ChannelID    int64
	ChannelTitle string
	Profile      string
	Created      time.Time
	SecretSet    time.Time
}

const (
	exportBegin = "-----BEGIN TELFS KEY HEADER-----"
	exportEnd   = "-----END TELFS KEY HEADER-----"
)

// Export renders the text block for the filesystem's KeePassXC entry and
// the break-glass kit (Arash's addition 1): the exact key message JSON,
// base64 in 64 columns, with a check line that catches a mistyped copy.
func Export(h KeyHeader, info ExportInfo) string {
	js := h.JSON()
	sum := sha256.Sum256(js)
	var b strings.Builder
	b.WriteString(exportBegin + "\n")
	fmt.Fprintf(&b, "fs:       %s\n", info.FSUUID)
	fmt.Fprintf(&b, "channel:  %d %q\n", info.ChannelID, info.ChannelTitle)
	fmt.Fprintf(&b, "profile:  %s\n", info.Profile)
	created := "unknown"
	if !info.Created.IsZero() {
		created = info.Created.UTC().Format(time.RFC3339)
	}
	set := created
	if !info.SecretSet.IsZero() {
		set = info.SecretSet.UTC().Format(time.RFC3339)
	}
	fmt.Fprintf(&b, "created:  %s   (secret last changed: %s)\n", created, set)
	enc := base64.StdEncoding.EncodeToString(js)
	for len(enc) > 64 {
		b.WriteString(enc[:64] + "\n")
		enc = enc[64:]
	}
	if enc != "" {
		b.WriteString(enc + "\n")
	}
	fmt.Fprintf(&b, "check:    %s\n", hex.EncodeToString(sum[:8]))
	b.WriteString(exportEnd + "\n")
	return b.String()
}

// ParseExport reads an exported block (surrounding text is ignored).
func ParseExport(text string) (KeyHeader, string, error) {
	var h KeyHeader
	start := strings.Index(text, exportBegin)
	end := strings.Index(text, exportEnd)
	if start < 0 || end < start {
		return h, "", errors.New("key header export: BEGIN/END lines not found")
	}
	var b64 strings.Builder
	var check, fsLine string
	for _, line := range strings.Split(text[start+len(exportBegin):end], "\n") {
		line = strings.TrimSpace(line)
		// Base64 has no ':' — every line with one is a label line.
		switch {
		case line == "":
		case strings.HasPrefix(line, "check:"):
			check = strings.TrimSpace(strings.TrimPrefix(line, "check:"))
		case strings.HasPrefix(line, "fs:"):
			fsLine = strings.TrimSpace(strings.TrimPrefix(line, "fs:"))
		case strings.Contains(line, ":"):
			// channel / profile / created
		default:
			b64.WriteString(line)
		}
	}
	js, err := base64.StdEncoding.DecodeString(b64.String())
	if err != nil {
		return h, "", fmt.Errorf("key header export: the base64 lines don't decode (mistyped?): %w", err)
	}
	sum := sha256.Sum256(js)
	if check == "" || !bytes.Equal([]byte(check), []byte(hex.EncodeToString(sum[:8]))) {
		return h, "", errors.New("key header export: the check line doesn't match — the copy is mistyped or damaged")
	}
	if err := json.Unmarshal(js, &h); err != nil {
		return h, "", fmt.Errorf("key header export: %w", err)
	}
	return h, fsLine, nil
}
