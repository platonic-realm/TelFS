// Package compress is the chunk-payload compression layer.
//
// It sits between the chunk Writer's caller (plaintext bytes) and the
// Cipher (which sees compressed bytes). On the read side the Cache
// runs the layers in reverse: download → decrypt → decompress →
// in-memory plaintext.
//
// Codec choice is per-filesystem and IMMUTABLE — committed at
// `telfs init` time and stored in meta_kv. Switching codecs on an
// existing filesystem would silently corrupt every chunk whose row
// was written under the old codec. None and Zstd are the only
// supported codecs in v1.
//
// Composition with encryption and dedup, in order:
//
//   plaintext ──compress──▶ compressed ──encrypt──▶ ciphertext ──▶ TG
//
// The content-addressed dedup hash (chunk_blob.hash) is computed
// over the PLAINTEXT — that way identical plaintext dedups across
// the wire regardless of codec, and switching codecs (in a fresh
// filesystem) doesn't fragment the dedup index.
package compress

import (
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// Codec is the interface every compression backend implements. Both
// methods MUST be deterministic for a given codec configuration —
// otherwise convergent-encryption dedup over compressed payloads
// would break.
type Codec interface {
	// Name returns the kv-stored identifier ("none", "zstd").
	Name() string
	// Compress returns a compressed copy of src.
	Compress(src []byte) ([]byte, error)
	// Decompress returns a plaintext copy of src.
	Decompress(src []byte) ([]byte, error)
}

// Codec name constants are also the values stored in meta_kv under
// `compression_codec`. None is the default for filesystems that
// never opted in.
const (
	NameNone = "none"
	NameZstd = "zstd"
)

// New returns the Codec for the given name. Errors on unknown names
// so the daemon refuses to mount a filesystem whose codec it can't
// decode — silent fallback would shred reads.
func New(name string) (Codec, error) {
	switch name {
	case "", NameNone:
		return None{}, nil
	case NameZstd:
		return NewZstd()
	default:
		return nil, fmt.Errorf("compress: unknown codec %q (binary may be older than the filesystem)", name)
	}
}

// None is the no-op codec: bytes pass through untouched. Used for
// filesystems initialized without compression and as the default
// when a freshly opened DB has no codec kv yet.
type None struct{}

func (None) Name() string                       { return NameNone }
func (None) Compress(src []byte) ([]byte, error) { return src, nil }
func (None) Decompress(src []byte) ([]byte, error) {
	return src, nil
}

// Zstd is the standards-aligned compression codec. Uses
// klauspost/compress/zstd which is pure Go and ships zero-allocation
// reusable encoder/decoder objects — important because chunk uploads
// fire concurrently from many goroutines and we don't want a
// per-call allocation spike.
type Zstd struct {
	enc *zstd.Encoder
	dec *zstd.Decoder
}

// NewZstd constructs a Zstd codec. enc and dec are reusable across
// goroutines; klauspost/compress's EncodeAll/DecodeAll are safe for
// concurrent use.
func NewZstd() (*Zstd, error) {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		return nil, fmt.Errorf("compress: init zstd encoder: %w", err)
	}
	dec, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
	)
	if err != nil {
		enc.Close()
		return nil, fmt.Errorf("compress: init zstd decoder: %w", err)
	}
	return &Zstd{enc: enc, dec: dec}, nil
}

func (z *Zstd) Name() string { return NameZstd }

// Compress returns a zstd frame containing src. EncodeAll(nil, src)
// is the documented one-shot API; we don't reuse dst buffers across
// goroutines because each call allocates its own output anyway.
func (z *Zstd) Compress(src []byte) ([]byte, error) {
	return z.enc.EncodeAll(src, nil), nil
}

// Decompress reverses Compress.
func (z *Zstd) Decompress(src []byte) ([]byte, error) {
	out, err := z.dec.DecodeAll(src, nil)
	if err != nil {
		return nil, fmt.Errorf("compress: zstd decode: %w", err)
	}
	return out, nil
}
