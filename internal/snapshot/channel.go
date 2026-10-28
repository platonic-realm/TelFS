package snapshot

import (
	"context"
	"io"

	"telfs/internal/tg"
)

// Channel is the part of a Telegram session the format-2 code uses.
// *tg.Session implements it; tests use a fake.
type Channel interface {
	UploadDocument(ctx context.Context, r io.Reader, filename, caption string) (int, error)
	DownloadDocument(ctx context.Context, msgID int, w io.Writer) (int64, error)
	PostMessage(ctx context.Context, text string) (int, error)
	DeleteChannelMessages(ctx context.Context, ids ...int) error
	PinMessage(ctx context.Context, msgID int) error
	PinnedMessages(ctx context.Context) ([]tg.ChannelMessage, error)
	LatestMessageID(ctx context.Context) (int, error)
	WalkChannelMessages(ctx context.Context, pageLimit int, fn func(tg.ChannelMessage) error) error
	WalkChannelMessagesExhaust(ctx context.Context, pageLimit int, fn func(tg.ChannelMessage) error) (bool, error)
	WalkChannelMessagesAfter(ctx context.Context, minID int, fn func(tg.ChannelMessage) error) error
}

var _ Channel = (*tg.Session)(nil)
