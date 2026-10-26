package wire

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"telfs/internal/compress"
	"telfs/internal/crypto"
)

// A format-2 pack document is
//
//	extent₁ ‖ … ‖ extentₙ ‖ index
//
// where each extent is a chunk wire exactly as for a chunk message (so a
// ranged download reads one extent) and index is sealed under the meta
// key. Index plaintext: uint32 length ‖ zstd(JSON entries) ‖ zero
// padding, sized so the WHOLE document is Padmé-sized. The pack's
// caption gives the index's offset (IO) and length (IL), so a rebuild
// downloads only that range.

// PackEntry describes one extent (the same facts as a chunk caption).
type PackEntry struct {
	Ino   int64  `json:"ino"`
	Idx   int32  `json:"idx,omitempty"`
	Off   int64  `json:"off"`
	WLen  int32  `json:"wlen"`
	Len   int64  `json:"len"`
	H     []byte `json:"h"`
	Codec string `json:"codec,omitempty"`
	CS    int64  `json:"cs,omitempty"`
	FSz   int64  `json:"fsz,omitempty"`
	Mode  uint32 `json:"mode,omitempty"`
	Mtime int64  `json:"mtime,omitempty"`
	NL    int64  `json:"nl,omitempty"`
	Name  string `json:"name,omitempty"`
}

func (k *Keys) packIndexAAD() []byte { return []byte("telfs2 packindex " + k.FSUUID) }

// SealPackIndex builds the index for extents totalling extentsLen
// bytes. The returned bytes go right after the extents.
func (k *Keys) SealPackIndex(extentsLen int64, entries []PackEntry) ([]byte, error) {
	js, err := json.Marshal(entries)
	if err != nil {
		return nil, err
	}
	z, err := compress.NewZstd()
	if err != nil {
		return nil, err
	}
	comp, err := z.Compress(js)
	if err != nil {
		return nil, err
	}
	inner := 4 + len(comp)
	total := crypto.PadmeLen(int(extentsLen) + crypto.BoxOverhead + inner)
	plain := make([]byte, total-int(extentsLen)-crypto.BoxOverhead)
	binary.BigEndian.PutUint32(plain, uint32(len(comp)))
	copy(plain[4:], comp)
	return k.Meta.Seal(plain, k.packIndexAAD())
}

// OpenPackIndex reverses SealPackIndex.
func (k *Keys) OpenPackIndex(index []byte) ([]PackEntry, error) {
	plain, err := k.Meta.Open(index, k.packIndexAAD())
	if err != nil {
		return nil, fmt.Errorf("pack index: %w", err)
	}
	if len(plain) < 4 {
		return nil, errors.New("pack index: too short")
	}
	n := int(binary.BigEndian.Uint32(plain))
	if 4+n > len(plain) {
		return nil, errors.New("pack index: length past the end")
	}
	z, err := compress.NewZstd()
	if err != nil {
		return nil, err
	}
	js, err := z.Decompress(plain[4 : 4+n])
	if err != nil {
		return nil, fmt.Errorf("pack index: %w", err)
	}
	var out []PackEntry
	if err := json.Unmarshal(js, &out); err != nil {
		return nil, fmt.Errorf("pack index: %w", err)
	}
	return out, nil
}

// Snapshot and journal bodies: uint64 length ‖ data ‖ zero padding to
// 8 + PadmeLen(length), sealed under the meta key with a per-kind AAD.

func (k *Keys) metaAAD(kind string) []byte { return []byte("telfs2 " + kind + " " + k.FSUUID) }

// SealMeta seals a snapshot ("snap") or journal ("journal") body.
func (k *Keys) SealMeta(kind string, data []byte) ([]byte, error) {
	plain := make([]byte, 8+crypto.PadmeLen(len(data)))
	binary.BigEndian.PutUint64(plain, uint64(len(data)))
	copy(plain[8:], data)
	return k.Meta.Seal(plain, k.metaAAD(kind))
}

// OpenMeta reverses SealMeta.
func (k *Keys) OpenMeta(kind string, wire []byte) ([]byte, error) {
	plain, err := k.Meta.Open(wire, k.metaAAD(kind))
	if err != nil {
		return nil, fmt.Errorf("%s body: %w", kind, err)
	}
	if len(plain) < 8 {
		return nil, fmt.Errorf("%s body: too short", kind)
	}
	n := binary.BigEndian.Uint64(plain)
	if 8+n > uint64(len(plain)) {
		return nil, fmt.Errorf("%s body: length past the end", kind)
	}
	return plain[8 : 8+n], nil
}

// Journal batch plaintext: 'z' + zstd(JSON) (journal JSON compresses
// about tenfold — rsync adds setattr ops for every file). Batches written
// before compression was added start with '{' (plain JSON); both decode.

// PackJournal compresses a journal batch's JSON for SealMeta.
func PackJournal(js []byte) ([]byte, error) {
	z, err := compress.NewZstd()
	if err != nil {
		return nil, err
	}
	comp, err := z.Compress(js)
	if err != nil {
		return nil, err
	}
	return append([]byte{'z'}, comp...), nil
}

// UnpackJournal reverses PackJournal (and passes plain JSON through).
func UnpackJournal(plain []byte) ([]byte, error) {
	if len(plain) == 0 || plain[0] == '{' {
		return plain, nil
	}
	if plain[0] != 'z' {
		return nil, errors.New("journal body: unknown encoding")
	}
	z, err := compress.NewZstd()
	if err != nil {
		return nil, err
	}
	return z.Decompress(plain[1:])
}
