package chunk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"telfs/internal/tg"
)

// ErrReadStalled: a download made no progress for the read's offline
// timeout ([read] offline_timeout) — Telegram is unreachable. The FUSE
// read answers EIO. It deliberately wraps no context error: the FUSE layer
// turns a cancellation into EINTR, which the kernel restarts — the read
// would never end.
var ErrReadStalled = errors.New("telegram unreachable: the download made no progress")

// docDownloader is what TGFetcher needs from a session (*tg.Session; fakes
// in tests).
type docDownloader interface {
	DownloadDocument(ctx context.Context, msgID int, w io.Writer) (int64, error)
	DownloadDocumentRange(ctx context.Context, msgID int, w io.Writer, offset, length int64) (int64, error)
}

// TGFetcher implements the Fetcher interface against a live tg.Session.
// Each Fetch is one MTProto channels.getMessages + upload.getFile streaming
// pair (with transparent FILE_REFERENCE_EXPIRED retry inside the session).
type TGFetcher struct {
	Session *tg.Session
	// OfflineTimeout fails a fetch that made no progress (no connection,
	// no bytes) for this long with ErrReadStalled; 0 = wait for ever. A
	// slow download that still moves is never cut off.
	OfflineTimeout time.Duration

	dl docDownloader // tests; nil = Session
}

func (f *TGFetcher) downloader() docDownloader {
	if f.dl != nil {
		return f.dl
	}
	return f.Session
}

// Fetch downloads the document attached to message tgMessageID and
// returns its bytes.
func (f *TGFetcher) Fetch(ctx context.Context, key Key, tgMessageID int64) ([]byte, error) {
	var buf bytes.Buffer
	n, err := f.watched(ctx, &buf, func(ctx context.Context, w io.Writer) (int64, error) {
		return f.downloader().DownloadDocument(ctx, int(tgMessageID), w)
	})
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
	n, err := f.watched(ctx, &buf, func(ctx context.Context, w io.Writer) (int64, error) {
		return f.downloader().DownloadDocumentRange(ctx, int(tgMessageID), w, offset, length)
	})
	if err != nil {
		return nil, fmt.Errorf("download msg %d [%d:%d]: %w", tgMessageID, offset, offset+length, err)
	}
	if int64(buf.Len()) != n {
		return nil, fmt.Errorf("ranged download size mismatch: reported %d, buffered %d", n, buf.Len())
	}
	return buf.Bytes(), nil
}

// errStalled is the watchdog's cancel cause.
var errStalled = errors.New("no progress")

// watched runs download under a progress watchdog: every write of
// downloaded bytes resets it; if it fires (OfflineTimeout without
// progress), the download is cancelled and ErrReadStalled returned.
func (f *TGFetcher) watched(ctx context.Context, w io.Writer, download func(context.Context, io.Writer) (int64, error)) (int64, error) {
	d := f.OfflineTimeout
	if d <= 0 {
		return download(ctx, w)
	}
	wctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	t := time.AfterFunc(d, func() { cancel(errStalled) })
	defer t.Stop()
	n, err := download(wctx, &progressWriter{w: w, kick: func() { t.Reset(d) }})
	if err != nil && ctx.Err() == nil && errors.Is(context.Cause(wctx), errStalled) {
		return n, fmt.Errorf("%w for %s", ErrReadStalled, d)
	}
	return n, err
}

// progressWriter counts every write as progress.
type progressWriter struct {
	mu   sync.Mutex
	w    io.Writer
	kick func()
}

func (p *progressWriter) Write(b []byte) (int, error) {
	p.kick()
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.w.Write(b)
}
