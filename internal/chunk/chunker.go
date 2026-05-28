package chunk

import (
	"io"
)

// Chunker splits a stream into fixed-size pieces of chunkSize bytes.
// Each call to Next reads and returns the next chunk; the last chunk
// may be shorter. Returns io.EOF after the last chunk has been
// delivered.
//
// Used by the seed-file CLI and (in M4) by the write path when files
// grow past a chunk boundary.
type Chunker struct {
	r         io.Reader
	chunkSize int
	buf       []byte
	done      bool
}

// NewChunker constructs a Chunker that reads from r with the given
// chunk size. chunkSize defaults to ChunkSize if zero or negative.
func NewChunker(r io.Reader, chunkSize int) *Chunker {
	if chunkSize <= 0 {
		chunkSize = int(ChunkSize)
	}
	return &Chunker{r: r, chunkSize: chunkSize, buf: make([]byte, chunkSize)}
}

// Next returns the next chunk's bytes (a slice that's valid until the
// next call to Next; copy if you need to keep it). Returns io.EOF when
// the stream is exhausted.
func (c *Chunker) Next() ([]byte, error) {
	if c.done {
		return nil, io.EOF
	}
	n, err := io.ReadFull(c.r, c.buf)
	switch {
	case err == nil:
		return c.buf[:n], nil
	case err == io.EOF:
		c.done = true
		return nil, io.EOF
	case err == io.ErrUnexpectedEOF:
		// Partial last chunk.
		c.done = true
		if n == 0 {
			return nil, io.EOF
		}
		return c.buf[:n], nil
	default:
		return nil, err
	}
}
