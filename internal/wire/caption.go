package wire

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// Caption geometry: CaptionTag + base64(nonce ‖ sealed(plain, 736) ‖ tag)
// = 4 + base64(12 + 736 + 16 = 764 bytes) = 4 + 1020 = 1024 characters,
// Telegram's caption limit for accounts without Premium. Every caption
// has exactly this length, so captions say nothing about their kind.
const (
	CaptionTag      = "tfs2"
	CaptionLen      = 1024
	captionPlainLen = 736
)

// Caption kinds.
const (
	KindChunk    = "c"
	KindPack     = "p"
	KindSnapshot = "s"
	KindJournal  = "j"
	KindAnchor   = "a" // the pinned anchor naming the newest snapshot (anchor.go)
)

// Caption is the encrypted description every format-2 document carries.
// Which fields are set depends on K.
type Caption struct {
	K  string `json:"k"`
	FS string `json:"fs"`

	// Chunk (K = "c"): identity, real length, codec, the inode's chunk
	// size, and — for a rebuild without any snapshot (feature 10) — the
	// file's size, mode, mtime, link count and path when uploaded.
	Ino   int64  `json:"ino,omitempty"`
	Idx   int32  `json:"idx,omitempty"`
	H     []byte `json:"h,omitempty"`
	Len   int64  `json:"len,omitempty"`
	Codec string `json:"codec,omitempty"`
	CS    int64  `json:"cs,omitempty"`
	FSz   int64  `json:"fsz,omitempty"`
	Mode  uint32 `json:"mode,omitempty"`
	Mtime int64  `json:"mtime,omitempty"`
	NL    int64  `json:"nl,omitempty"`
	Name  string `json:"name,omitempty"`

	// Pack (K = "p"): entry count and where the encrypted index sits.
	N  int   `json:"n,omitempty"`
	IO int64 `json:"io,omitempty"`
	IL int64 `json:"il,omitempty"`

	// Snapshot (K = "s"), journal batch (K = "j") and anchor (K = "a":
	// Seq = its snapshot's seq).
	Seq   int64 `json:"seq,omitempty"`
	TS    int64 `json:"ts,omitempty"`
	DBSz  int64 `json:"dbsz,omitempty"`
	First int64 `json:"first,omitempty"`
	Last  int64 `json:"last,omitempty"`
}

// ErrNotOurs: a caption is not a format-2 caption of THIS filesystem
// (no tag, bad encoding, or it doesn't open under our caption key).
var ErrNotOurs = errors.New("not a caption of this filesystem")

// SealCaption encodes c into a CaptionLen-character caption. The name
// hint is truncated from the left ("…tail") until the caption fits.
func (k *Keys) SealCaption(c Caption) (string, error) {
	if c.FS == "" {
		c.FS = k.FSUUID
	}
	js, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	for len(js) > captionPlainLen {
		if c.Name == "" {
			return "", fmt.Errorf("wire: caption is %d bytes without a name (limit %d)", len(js), captionPlainLen)
		}
		c.Name = truncateLeft(c.Name, len(js)-captionPlainLen)
		if js, err = json.Marshal(c); err != nil {
			return "", err
		}
	}
	plain := make([]byte, captionPlainLen)
	copy(plain, js)
	sealed, err := k.Caption.Seal(plain, []byte(CaptionTag))
	if err != nil {
		return "", err
	}
	out := CaptionTag + base64.StdEncoding.EncodeToString(sealed)
	if len(out) != CaptionLen {
		return "", fmt.Errorf("wire: caption length %d, want %d", len(out), CaptionLen)
	}
	return out, nil
}

// OpenCaption decodes a caption, or returns ErrNotOurs.
func (k *Keys) OpenCaption(s string) (Caption, error) {
	var c Caption
	if !strings.HasPrefix(s, CaptionTag) || len(s) != CaptionLen {
		return c, ErrNotOurs
	}
	sealed, err := base64.StdEncoding.DecodeString(s[len(CaptionTag):])
	if err != nil {
		return c, ErrNotOurs
	}
	plain, err := k.Caption.Open(sealed, []byte(CaptionTag))
	if err != nil {
		return c, ErrNotOurs
	}
	plain = bytes.TrimRight(plain, "\x00")
	if err := json.Unmarshal(plain, &c); err != nil {
		return c, fmt.Errorf("wire: caption JSON: %w", err)
	}
	if c.FS != k.FSUUID {
		// Same key, different fs_uuid can only mean tampering or a bug.
		return c, fmt.Errorf("wire: caption names fs %q, this filesystem is %q", c.FS, k.FSUUID)
	}
	return c, nil
}

// LooksLikeCaption is a cheap prefilter before trying to decrypt.
func LooksLikeCaption(s string) bool {
	return len(s) == CaptionLen && strings.HasPrefix(s, CaptionTag)
}

// truncateLeft drops at least `over` bytes from the front of name (plus
// room for the "…" marker), cutting on a rune boundary.
func truncateLeft(name string, over int) string {
	const mark = "…"
	cut := over + len(mark)
	if cut >= len(name) {
		return mark
	}
	for cut < len(name) && !utf8.RuneStart(name[cut]) {
		cut++
	}
	return mark + name[cut:]
}
