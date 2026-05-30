package chunk

import (
	"bytes"
	"context"
	"fmt"

	"telfs/internal/tg"
)

// TGFetcher implements the Fetcher interface against a live tg.Session.
// Each Fetch is one MTProto channels.getMessages + upload.getFile streaming
// pair (with transparent FILE_REFERENCE_EXPIRED retry inside the session).
type TGFetcher struct {
	Session *tg.Session
}

// Fetch downloads the document attached to message tgMessageID and
// returns its bytes.
func (f *TGFetcher) Fetch(ctx context.Context, key Key, tgMessageID int64) ([]byte, error) {
	var buf bytes.Buffer
	n, err := f.Session.DownloadDocument(ctx, int(tgMessageID), &buf)
	if err != nil {
		return nil, fmt.Errorf("download msg %d: %w", tgMessageID, err)
	}
	if int64(buf.Len()) != n {
		return nil, fmt.Errorf("download size mismatch: reported %d, buffered %d", n, buf.Len())
	}
	return buf.Bytes(), nil
}

// FetchRange downloads bytes [offset, offset+length) of the message.
// Used by Cache when a packed extent's slice is wanted, so a large
// shared blob doesn't have to be downloaded in full for each chunk
// row that references it. Round-trip count is ceil(length / 1 MiB).
func (f *TGFetcher) FetchRange(ctx context.Context, key Key, tgMessageID int64, offset, length int64) ([]byte, error) {
	var buf bytes.Buffer
	n, err := f.Session.DownloadDocumentRange(ctx, int(tgMessageID), &buf, offset, length)
	if err != nil {
		return nil, fmt.Errorf("download msg %d [%d:%d]: %w", tgMessageID, offset, offset+length, err)
	}
	if int64(buf.Len()) != n {
		return nil, fmt.Errorf("ranged download size mismatch: reported %d, buffered %d", n, buf.Len())
	}
	return buf.Bytes(), nil
}
