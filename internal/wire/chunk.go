package wire

import (
	"encoding/binary"
	"errors"
	"fmt"

	"telfs/internal/compress"
	"telfs/internal/crypto"
)

// Chunk body plaintext (before sealing):
//
//	'c' ‖ version ‖ codec ‖ 0 ‖ uint32 plaintext length ‖ uint32 payload length ‖ payload ‖ zero padding
//
// The PAYLOAD (not the header) is padded to PadmeLen(payload length),
// so a full uncompressed chunk — a power of two — pays no padding and
// every full chunk has the same wire size. codec says what THIS chunk
// used: a chunk zstd doesn't shrink is stored raw.
const (
	chunkMagic   = 'c'
	chunkVersion = 1
	chunkHdrLen  = 12

	codecNone byte = 0
	codecZstd byte = 1
)

// ChunkCodec turns chunk plaintext into format-2 wire bytes and back.
type ChunkCodec struct {
	Keys *Keys
	// Compressor is the filesystem's codec (compress.None or zstd).
	Compressor compress.Codec
	zstd       compress.Codec
}

// NewChunkCodec builds the codec. comp may be nil (no compression).
func NewChunkCodec(keys *Keys, comp compress.Codec) (*ChunkCodec, error) {
	if comp == nil {
		comp = compress.None{}
	}
	c := &ChunkCodec{Keys: keys, Compressor: comp}
	// Decoding must handle zstd chunks whatever the FS's write codec is.
	z, err := compress.NewZstd()
	if err != nil {
		return nil, err
	}
	c.zstd = z
	return c, nil
}

// Encode seals plaintext; returns the wire bytes and the keyed hash
// (the AAD, and the chunk's identity for dedup).
func (c *ChunkCodec) Encode(plaintext []byte) (wire, hash []byte, err error) {
	hash = c.Keys.Hash(plaintext)
	wire, _, err = c.EncodeWithHash(plaintext, hash)
	return wire, hash, err
}

// EncodeWithHash seals plaintext under a hash the caller already
// computed (the writer hashes first to look up dedup). codec is what
// this chunk actually used ("none" or "zstd"), for its caption.
func (c *ChunkCodec) EncodeWithHash(plaintext, hash []byte) (wire []byte, codec string, err error) {
	if err := checkHash(hash); err != nil {
		return nil, "", err
	}
	payload, cb := plaintext, codecNone
	if c.Compressor.Name() == compress.NameZstd && len(plaintext) > 0 {
		comp, cerr := c.Compressor.Compress(plaintext)
		if cerr != nil {
			return nil, "", fmt.Errorf("compress chunk: %w", cerr)
		}
		if len(comp) < len(plaintext) {
			payload, cb = comp, codecZstd
		}
	}
	padded := crypto.PadmeLen(len(payload))
	body := make([]byte, chunkHdrLen+padded)
	body[0], body[1], body[2] = chunkMagic, chunkVersion, cb
	binary.BigEndian.PutUint32(body[4:], uint32(len(plaintext)))
	binary.BigEndian.PutUint32(body[8:], uint32(len(payload)))
	copy(body[chunkHdrLen:], payload)
	wire, err = c.Keys.Chunk.Seal(body, hash)
	return wire, codecName(cb), err
}

// Decode opens wire bytes filed under hash, decompresses, and checks
// the content against the hash.
func (c *ChunkCodec) Decode(hash, wire []byte) ([]byte, error) {
	if err := checkHash(hash); err != nil {
		return nil, err
	}
	body, err := c.Keys.Chunk.Open(wire, hash)
	if err != nil {
		return nil, fmt.Errorf("decrypt chunk: %w", err)
	}
	if len(body) < chunkHdrLen || body[0] != chunkMagic || body[1] != chunkVersion {
		return nil, errors.New("decrypt chunk: unknown body format")
	}
	plainLen := int(binary.BigEndian.Uint32(body[4:]))
	payLen := int(binary.BigEndian.Uint32(body[8:]))
	if chunkHdrLen+payLen > len(body) {
		return nil, errors.New("decrypt chunk: payload length past the body")
	}
	payload := body[chunkHdrLen : chunkHdrLen+payLen]
	var plain []byte
	switch body[2] {
	case codecNone:
		plain = payload
	case codecZstd:
		if plain, err = c.zstd.Decompress(payload); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("decrypt chunk: unknown codec %d", body[2])
	}
	if len(plain) != plainLen {
		return nil, fmt.Errorf("decrypt chunk: length %d, header says %d", len(plain), plainLen)
	}
	if !c.Keys.HashMatches(plain, hash) {
		return nil, ErrHashMismatch
	}
	return plain, nil
}

// CodecName reports the codec a wire's chunk used, for captions — the
// writer knows it from Encode's choice; this keeps the caption honest
// without reopening the wire.
func codecName(b byte) string {
	if b == codecZstd {
		return compress.NameZstd
	}
	return compress.NameNone
}
