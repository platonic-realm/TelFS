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
