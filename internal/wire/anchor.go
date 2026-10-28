package wire

import (
	"encoding/json"
	"fmt"
	"time"
)

// The anchor (spec v1 §2, "Snapshots"): a small pinned document that
// names the filesystem's newest snapshot, so a recovery finds it with one
// request (the pinned list) instead of scanning the channel's history.
// Its caption is an ordinary sealed caption of kind KindAnchor; its body
// is SealMeta("anchor", JSON). A new anchor is posted after every
// snapshot (and, later, every version change); the previous one is
// unpinned and deleted.

// AnchorVersion is a version listed in the anchor (filled by the
// versions feature; the anchor carries the list from the start).
type AnchorVersion struct {
	Name    string `json:"name"`
	Msg     int    `json:"msg"`
	Seq     int64  `json:"seq"`
	Created int64  `json:"created"`
}

// Anchor is the anchor body's plaintext.
type Anchor struct {
	V  int    `json:"v"`
	FS string `json:"fs"`
	// SnapMsg, SnapSeq, SnapTS: the newest snapshot's message id, the
	// journal seq it covers and when it was taken (unix seconds).
	SnapMsg int   `json:"snap_msg"`
	SnapSeq int64 `json:"snap_seq"`
	SnapTS  int64 `json:"snap_ts"`
	// JnlAfter bounds the journal walk: every journal batch holding an
	// op newer than the snapshot has a message id above it. It is the
	// channel's newest message id read just before the snapshot was
	// taken — NOT the snapshot's own id: batches posted while the
	// snapshot uploaded carry ops it lacks and have smaller ids.
	JnlAfter int             `json:"jnl_after"`
	Versions []AnchorVersion `json:"versions"`
}

// AnchorVersionV1 is the anchor body version this build writes.
const AnchorVersionV1 = 1

// anchorMetaKind is the SealMeta kind (AAD) of anchor bodies.
const anchorMetaKind = "anchor"

// SealAnchor returns the anchor's sealed body and caption.
func (k *Keys) SealAnchor(a Anchor) (body []byte, caption string, err error) {
	if a.V == 0 {
		a.V = AnchorVersionV1
	}
	if a.FS == "" {
		a.FS = k.FSUUID
	}
	if a.Versions == nil {
		a.Versions = []AnchorVersion{}
	}
	js, err := json.Marshal(a)
	if err != nil {
		return nil, "", err
	}
	if body, err = k.SealMeta(anchorMetaKind, js); err != nil {
		return nil, "", err
	}
	caption, err = k.SealCaption(Caption{K: KindAnchor, Seq: a.SnapSeq, TS: time.Now().Unix()})
	if err != nil {
		return nil, "", err
	}
	return body, caption, nil
}

// OpenAnchor decrypts an anchor body and checks that it belongs to this
// filesystem and that this build reads its version.
func (k *Keys) OpenAnchor(body []byte) (Anchor, error) {
	var a Anchor
	js, err := k.OpenMeta(anchorMetaKind, body)
	if err != nil {
		return a, err
	}
	if err := json.Unmarshal(js, &a); err != nil {
		return a, fmt.Errorf("anchor: %w", err)
	}
	if a.V != AnchorVersionV1 {
		return a, fmt.Errorf("anchor version %d is newer than this telfs — upgrade", a.V)
	}
	if a.FS != k.FSUUID {
		return a, fmt.Errorf("anchor names filesystem %q, this filesystem is %q", a.FS, k.FSUUID)
	}
	if a.SnapMsg <= 0 {
		return a, fmt.Errorf("anchor names no snapshot")
	}
	return a, nil
}
