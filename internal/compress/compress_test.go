package compress

import (
	"bytes"
	"crypto/rand"
	"testing"
)

// TestRoundTrip: every codec must round-trip a representative
// plaintext byte-perfect.
func TestRoundTrip(t *testing.T) {
	codecs := []string{NameNone, NameZstd}
	inputs := map[string][]byte{
		"empty":      nil,
		"short text": []byte("hello world"),
		"repeating":  bytes.Repeat([]byte{'A'}, 4096),
	}
	for _, name := range codecs {
		t.Run(name, func(t *testing.T) {
			c, err := New(name)
			if err != nil {
				t.Fatal(err)
			}
			for in, src := range inputs {
				t.Run(in, func(t *testing.T) {
					compressed, err := c.Compress(src)
					if err != nil {
						t.Fatal(err)
					}
					out, err := c.Decompress(compressed)
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(out, src) {
						t.Fatalf("round trip mismatch: got %v, want %v", out, src)
					}
				})
			}
		})
	}
}

// TestZstdCompressesRepetitive: a repetitive payload should be
// noticeably smaller after zstd. Otherwise we'd be paying CPU for
// no win.
func TestZstdCompressesRepetitive(t *testing.T) {
	c, err := New(NameZstd)
	if err != nil {
		t.Fatal(err)
	}
	src := bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog "), 256)
	compressed, _ := c.Compress(src)
	if len(compressed) >= len(src)/4 {
		t.Fatalf("zstd compressed %d bytes to %d — expected <%d", len(src), len(compressed), len(src)/4)
	}
}

// TestZstdRandomDoesNotInflate: zstd on random bytes shouldn't blow
// up wire size by more than a constant epsilon (the frame header).
// Documents the worst-case overhead so callers know it's bounded.
func TestZstdRandomDoesNotInflate(t *testing.T) {
	c, _ := New(NameZstd)
	src := make([]byte, 32*1024)
	_, _ = rand.Read(src)
	compressed, _ := c.Compress(src)
	if len(compressed) > len(src)+128 {
		t.Fatalf("zstd inflated random bytes %d → %d (overhead > 128 B)", len(src), len(compressed))
	}
}

// TestZstdDeterministic: same input, same codec, same output.
// Important because convergent-encryption dedup over compressed
// bytes would break if Compress varied across calls.
func TestZstdDeterministic(t *testing.T) {
	c, _ := New(NameZstd)
	src := []byte("hello hello hello hello hello hello hello hello hello hello")
	a, _ := c.Compress(src)
	b, _ := c.Compress(src)
	if !bytes.Equal(a, b) {
		t.Fatal("zstd output differs across identical inputs — convergent dedup over compressed bytes will break")
	}
}

// TestUnknownCodec: New errors on names it doesn't recognize, with
// a hint about binary version.
func TestUnknownCodec(t *testing.T) {
	if _, err := New("brotli"); err == nil {
		t.Fatal("New(\"brotli\") should have errored")
	}
}
