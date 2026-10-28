package wire

import (
	"encoding/hex"
	"fmt"
)

// A version (spec v1 §2, "Versions"): a whole-DB snapshot kept by name
// until it is deleted. Its body is SealMeta("version", gzipped SQLite DB)
// — a version never opens as a snapshot, nor the other way round — and
// its caption an ordinary sealed caption of kind KindVersion. Nothing
// ever edits the message: a version is immutable by construction.

// versionMetaKind is the SealMeta kind (AAD) of version bodies.
const versionMetaKind = "version"

// SealVersion returns a version's sealed body (gz = the gzipped DB) and
// caption. v names it: Name, Seq, Created, Size (the DB's size) and
// SHA256 (hex, the DB's hash); Msg is not known yet.
func (k *Keys) SealVersion(v AnchorVersion, gz []byte) (body []byte, caption string, err error) {
	if body, err = k.SealMeta(versionMetaKind, gz); err != nil {
		return nil, "", err
	}
	var h []byte
	if v.SHA256 != "" {
		if h, err = hex.DecodeString(v.SHA256); err != nil {
			return nil, "", fmt.Errorf("version %q: sha256: %w", v.Name, err)
		}
	}
	c := Caption{K: KindVersion, Name: v.Name, Seq: v.Seq, TS: v.Created, DBSz: v.Size, H: h}
	if caption, err = k.SealCaption(c); err != nil {
		return nil, "", err
	}
	if back, err := k.OpenCaption(caption); err != nil || back.Name != v.Name {
		// SealCaption shortens an over-long name hint; a version's name
		// must survive whole.
		return nil, "", fmt.Errorf("version name %q does not fit a caption", v.Name)
	}
	return body, caption, nil
}

// OpenVersion decrypts a version body to the gzipped DB.
func (k *Keys) OpenVersion(body []byte) ([]byte, error) {
	return k.OpenMeta(versionMetaKind, body)
}

// VersionFromCaption is the registry entry a version caption describes
// (msgID = the message carrying it).
func VersionFromCaption(c Caption, msgID int) AnchorVersion {
	v := AnchorVersion{Name: c.Name, Msg: msgID, Seq: c.Seq, Created: c.TS, Size: c.DBSz}
	if len(c.H) > 0 {
		v.SHA256 = hex.EncodeToString(c.H)
	}
	return v
}
