package chunk

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"telfs/internal/compress"
	"telfs/internal/crypto"
	"telfs/internal/meta"
	"telfs/internal/wire"
)

// WireCodec is how the chunk pipeline turns chunk plaintext into
// channel bytes and back, and how it names and captions the documents
// it posts. Format 1 (v0.22 and older filesystems) binds a chunk to its
// (ino, idx) slot and posts named, captionless documents; format 2 binds
// it to its keyed content hash and posts `blob` documents with encrypted
// captions (spec P1 §4).
type WireCodec interface {
	// Format is 1 or 2.
	Format() int
	// Hash returns the chunk's dedup identity, or nil when this codec
	// does not deduplicate. On format 2 it is also the AAD and must
	// be stored in chunk_map.
	Hash(plaintext []byte) []byte
	// Encode seals plaintext for slot (ino, idx) under hash (Hash's
	// result, may be nil on format 1). codec is what the chunk used.
	Encode(ino int64, idx int32, plaintext, hash []byte) (wire []byte, codec string, err error)
	// Decode reverses Encode for a chunk_map row.
	Decode(ino int64, idx int32, hash, wire []byte) ([]byte, error)
	// ChunkDoc names and captions a direct chunk upload.
	ChunkDoc(ctx context.Context, d ChunkDocInfo) (name, caption string, err error)
	// PackDoc returns the bytes appended after a pack's extents (the
	// encrypted index; nil on format 1) and the pack's name and caption.
	PackDoc(ctx context.Context, items []PackItem, extentsLen int64) (tail []byte, name, caption string, err error)
}

// ChunkDocInfo describes one direct chunk upload.
type ChunkDocInfo struct {
	Ino   int64
	Idx   int32
	Hash  []byte
	Len   int64
	Codec string
	// FileSize is the writer's in-memory file size at upload time (the
	// DB's size lags until close commits it); 0 = take it from the DB.
	FileSize int64
}

// PackItem describes one extent of a pack being assembled.
type PackItem struct {
	Ino      int64
	Idx      int32
	Off      int64
	WLen     int32
	Len      int64
	Hash     []byte
	Codec    string
	FileSize int64 // as ChunkDocInfo.FileSize
}

// ---- format 1 ----

// Format1Codec is the v0.22 pipeline: compress, then the FS cipher with
// (ino, idx) AAD. Dedup only when the cipher is deterministic (plaintext
// or aes-gcm-v3), keyed by sha256 of the plaintext.
type Format1Codec struct {
	Cipher crypto.Cipher
	Codec  compress.Codec
}

// NewFormat1Codec applies the nil defaults (NoopCipher, compress.None).
func NewFormat1Codec(c crypto.Cipher, cc compress.Codec) *Format1Codec {
	if c == nil {
		c = crypto.NoopCipher{}
	}
	if cc == nil {
		cc = compress.None{}
	}
	return &Format1Codec{Cipher: c, Codec: cc}
}

func (f *Format1Codec) Format() int { return 1 }

func (f *Format1Codec) Hash(plaintext []byte) []byte {
	if d, ok := f.Cipher.(dedupSafe); ok && d.Deterministic() {
		s := sha256.Sum256(plaintext)
		return s[:]
	}
	return nil
}

func (f *Format1Codec) Encode(ino int64, idx int32, plaintext, _ []byte) ([]byte, string, error) {
	compressed, err := f.Codec.Compress(plaintext)
	if err != nil {
		return nil, "", fmt.Errorf("compress chunk %d: %w", idx, err)
	}
	w, err := f.Cipher.Seal(ino, idx, compressed)
	if err != nil {
		return nil, "", fmt.Errorf("encrypt chunk %d: %w", idx, err)
	}
	return w, f.Codec.Name(), nil
}

func (f *Format1Codec) Decode(ino int64, idx int32, _ []byte, w []byte) ([]byte, error) {
	compressed, err := f.Cipher.Open(ino, idx, w)
	if err != nil {
		return nil, fmt.Errorf("decrypt chunk %d/%d: %w", ino, idx, err)
	}
	data, err := f.Codec.Decompress(compressed)
	if err != nil {
		return nil, fmt.Errorf("decompress chunk %d/%d: %w", ino, idx, err)
	}
	return data, nil
}

func (f *Format1Codec) ChunkDoc(_ context.Context, d ChunkDocInfo) (string, string, error) {
	return fmt.Sprintf("ino%d-idx%d", d.Ino, d.Idx), "", nil
}

func (f *Format1Codec) PackDoc(context.Context, []PackItem, int64) ([]byte, string, string, error) {
	return nil, fmt.Sprintf("pack-%d", time.Now().UnixNano()), "", nil
}

// ---- format 2 ----

// Format2Codec is channel format 2: wire.ChunkCodec bodies, `blob`
// documents, encrypted captions with name hints (feature 10), and an
// encrypted contents index at the end of every pack.
type Format2Codec struct {
	Keys  *wire.Keys
	Chunk *wire.ChunkCodec
	Meta  *meta.Store
}

// NewFormat2Codec builds the codec from the FS keys and its codec.
func NewFormat2Codec(keys *wire.Keys, comp compress.Codec, m *meta.Store) (*Format2Codec, error) {
	cc, err := wire.NewChunkCodec(keys, comp)
	if err != nil {
		return nil, err
	}
	return &Format2Codec{Keys: keys, Chunk: cc, Meta: m}, nil
}

func (f *Format2Codec) Format() int { return 2 }

func (f *Format2Codec) Hash(plaintext []byte) []byte { return f.Keys.Hash(plaintext) }

func (f *Format2Codec) Encode(_ int64, _ int32, plaintext, hash []byte) ([]byte, string, error) {
	if hash == nil {
		hash = f.Keys.Hash(plaintext)
	}
	return f.Chunk.EncodeWithHash(plaintext, hash)
}

func (f *Format2Codec) Decode(ino int64, idx int32, hash, w []byte) ([]byte, error) {
	if len(hash) == 0 {
		return nil, fmt.Errorf("chunk %d/%d: no content hash recorded (format 2 needs it to decrypt)", ino, idx)
	}
	pt, err := f.Chunk.Decode(hash, w)
	if err != nil {
		return nil, fmt.Errorf("chunk %d/%d: %w", ino, idx, err)
	}
	return pt, nil
}

// inodeFacts are the rebuild facts a caption carries about the file.
type inodeFacts struct {
	size, cs, mtime, nlink int64
	mode                   uint32
	name                   string
}

func (f *Format2Codec) facts(ctx context.Context, ino int64, fileSize int64, dirs map[int64]string) inodeFacts {
	var out inodeFacts
	if f.Meta != nil {
		if in, err := f.Meta.GetInode(ctx, ino); err == nil {
			out.size, out.cs, out.mtime, out.nlink, out.mode = in.Size, in.ChunkSize, in.Mtime, int64(in.Nlink), in.Mode
		}
		if p, ok := f.Meta.PathOfCached(ctx, ino, dirs); ok {
			out.name = p
		}
	}
	// The writer's in-memory size is newer than the DB's until close.
	if fileSize > out.size {
		out.size = fileSize
	}
	return out
}

func (f *Format2Codec) ChunkDoc(ctx context.Context, d ChunkDocInfo) (string, string, error) {
	fa := f.facts(ctx, d.Ino, d.FileSize, nil)
	c := wire.Caption{K: wire.KindChunk, Ino: d.Ino, Idx: d.Idx, H: d.Hash, Len: d.Len, Codec: d.Codec,
		CS: fa.cs, FSz: fa.size, Mode: fa.mode, Mtime: fa.mtime, NL: fa.nlink, Name: fa.name}
	capt, err := f.Keys.SealCaption(c)
	return wire.DocName, capt, err
}

func (f *Format2Codec) PackDoc(ctx context.Context, items []PackItem, extentsLen int64) ([]byte, string, string, error) {
	dirs := make(map[int64]string)
	entries := make([]wire.PackEntry, len(items))
	for i, it := range items {
		fa := f.facts(ctx, it.Ino, it.FileSize, dirs)
		entries[i] = wire.PackEntry{Ino: it.Ino, Idx: it.Idx, Off: it.Off, WLen: it.WLen, Len: it.Len, H: it.Hash,
			Codec: it.Codec, CS: fa.cs, FSz: fa.size, Mode: fa.mode, Mtime: fa.mtime, NL: fa.nlink, Name: fa.name}
	}
	tail, err := f.Keys.SealPackIndex(extentsLen, entries)
	if err != nil {
		return nil, "", "", err
	}
	capt, err := f.Keys.SealCaption(wire.Caption{K: wire.KindPack, N: len(items), IO: extentsLen, IL: int64(len(tail))})
	if err != nil {
		return nil, "", "", err
	}
	return tail, wire.DocName, capt, nil
}
