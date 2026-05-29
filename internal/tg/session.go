package tg

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"telfs/internal/config"
)

// Session is a live MTProto context handed to a callback by
// Client.RunSession. All Telegram operations within a session share the
// same underlying connection — no per-call handshake — which is what the
// FUSE daemon needs.
//
// Sessions are NOT safe to use after the RunSession callback returns; the
// underlying telegram.Client is torn down at that point and any further
// RPCs fail with "client already closed". Lifecycle owners (e.g. the
// mount daemon) must ensure their consumers — in-flight FUSE Read
// goroutines, in particular — have drained before returning from the
// callback.
type Session struct {
	api *tg.Client
	cfg *config.Config
}

// API returns the live gotd *tg.Client.
func (s *Session) API() *tg.Client { return s.api }

// Config returns the configuration the session was opened with.
func (s *Session) Config() *config.Config { return s.cfg }

// channelPeer returns MTProto peer references for the configured backing
// channel.
func (s *Session) channelPeer() (*tg.InputPeerChannel, *tg.InputChannel, error) {
	if err := s.cfg.RequireChannel(); err != nil {
		return nil, nil, err
	}
	return &tg.InputPeerChannel{
			ChannelID:  s.cfg.Channel.ID,
			AccessHash: s.cfg.Channel.AccessHash,
		}, &tg.InputChannel{
			ChannelID:  s.cfg.Channel.ID,
			AccessHash: s.cfg.Channel.AccessHash,
		}, nil
}

// ListChannels enumerates the user's channels from their dialogs. Includes
// both channels they own and channels they're a member of.
func (s *Session) ListChannels(ctx context.Context) ([]ChannelInfo, error) {
	res, err := s.api.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
		OffsetPeer: &tg.InputPeerEmpty{},
		Limit:      200,
	})
	if err != nil {
		return nil, fmt.Errorf("get dialogs: %w", err)
	}
	var chats []tg.ChatClass
	switch r := res.(type) {
	case *tg.MessagesDialogs:
		chats = r.Chats
	case *tg.MessagesDialogsSlice:
		chats = r.Chats
	default:
		return nil, fmt.Errorf("unexpected dialogs response: %T", res)
	}
	var out []ChannelInfo
	for _, chat := range chats {
		ch, ok := chat.(*tg.Channel)
		if !ok {
			continue
		}
		if ch.Left {
			continue
		}
		out = append(out, ChannelInfo{
			ID:         ch.ID,
			AccessHash: ch.AccessHash,
			Title:      ch.Title,
			Username:   ch.Username,
			IsCreator:  ch.Creator,
			CanPost:    ch.Creator || ch.AdminRights.PostMessages,
		})
	}
	return out, nil
}

// ResolveChannel looks up the given channel id in the user's dialogs and
// returns its full ChannelInfo (including AccessHash). The id is normalized
// via NormalizeChannelID first, so both raw and -100... forms are accepted.
func (s *Session) ResolveChannel(ctx context.Context, id int64) (ChannelInfo, error) {
	id, err := NormalizeChannelID(id)
	if err != nil {
		return ChannelInfo{}, err
	}
	chans, err := s.ListChannels(ctx)
	if err != nil {
		return ChannelInfo{}, err
	}
	for _, ch := range chans {
		if ch.ID == id {
			return ch, nil
		}
	}
	return ChannelInfo{}, fmt.Errorf("channel %d not found in your dialogs (run 'telfs channel list' to see ids)", id)
}

// SetChannel resolves the channel id, validates it can be posted to, and
// persists it to the config. Mutates s.cfg in-place.
//
// For user accounts (dialogs accessible) the normal path is to look the
// channel up by id and harvest its access_hash from the dialog list.
// For bot accounts, dialogs return nothing — pass the access_hash
// explicitly (non-zero) and we'll trust it, skipping the scan.
func (s *Session) SetChannel(ctx context.Context, id int64, accessHash int64) (ChannelInfo, error) {
	if accessHash != 0 {
		// Trust-the-caller path. Used by `channel set --access-hash` and
		// is the only way bots can bind a channel.
		nid, err := NormalizeChannelID(id)
		if err != nil {
			return ChannelInfo{}, err
		}
		info := ChannelInfo{
			ID:         nid,
			AccessHash: accessHash,
			Title:      fmt.Sprintf("channel-%d", nid),
			CanPost:    true,
		}
		s.cfg.Channel = config.ChannelConfig{
			ID:         info.ID,
			AccessHash: info.AccessHash,
			Title:      info.Title,
			Username:   info.Username,
		}
		if err := s.cfg.Save(); err != nil {
			return ChannelInfo{}, fmt.Errorf("save config: %w", err)
		}
		return info, nil
	}

	info, err := s.ResolveChannel(ctx, id)
	if err != nil {
		return ChannelInfo{}, err
	}
	if !info.CanPost {
		return ChannelInfo{}, fmt.Errorf("you don't have post permission on channel %q (%d)", info.Title, info.ID)
	}
	s.cfg.Channel = config.ChannelConfig{
		ID:         info.ID,
		AccessHash: info.AccessHash,
		Title:      info.Title,
		Username:   info.Username,
	}
	if err := s.cfg.Save(); err != nil {
		return ChannelInfo{}, fmt.Errorf("save config: %w", err)
	}
	return info, nil
}

// PostMessage sends a plain text message to the configured channel and
// returns the new message id.
func (s *Session) PostMessage(ctx context.Context, text string) (int, error) {
	peer, _, err := s.channelPeer()
	if err != nil {
		return 0, err
	}
	rid, err := randomID()
	if err != nil {
		return 0, err
	}
	upd, err := s.api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
		Peer:     peer,
		Message:  text,
		RandomID: rid,
	})
	if err != nil {
		return 0, fmt.Errorf("send message: %w", err)
	}
	return extractNewMessageID(upd, rid)
}

// GetMessageText fetches a message by id and returns its text body.
func (s *Session) GetMessageText(ctx context.Context, msgID int) (string, error) {
	msg, err := s.getMessage(ctx, msgID)
	if err != nil {
		return "", err
	}
	return msg.Message, nil
}

// uploadMaxAttempts caps how many times UploadDocument will retry a
// transient failure. Each attempt is a fresh uploader.NewUploader +
// SaveFilePart chain, so retrying isn't free — but it's much cheaper
// than propagating EIO to the FUSE write path and forcing the user
// to retry the entire cp.
const uploadMaxAttempts = 4

// UploadDocument uploads the contents of r as a document attached to a
// message in the configured channel. The document is sent with the given
// filename (used for the DocumentAttributeFilename) and optional caption.
// Returns the new message id.
//
// The reader is buffered up-front so the upload can be retried on
// transient gotd errors ("engine forcibly closed", connection reset,
// i/o timeout) without the caller having to re-stage the source. We
// don't retry caller-canceled contexts (Close racing a Flush) — that's
// honest cancellation. FLOOD_WAIT is handled before reaching us by
// the gotd middleware.
func (s *Session) UploadDocument(ctx context.Context, r io.Reader, filename, caption string) (int, error) {
	peer, _, err := s.channelPeer()
	if err != nil {
		return 0, err
	}
	// Buffer once — the reader is typically a chunk's wire bytes (≤ 4 MiB
	// at default chunk size + GCM overhead), so the memory cost is bounded.
	body, err := io.ReadAll(r)
	if err != nil {
		return 0, fmt.Errorf("read upload bytes: %w", err)
	}

	var lastErr error
	for attempt := 1; attempt <= uploadMaxAttempts; attempt++ {
		if attempt > 1 {
			// 1s, 2s, 4s exponential backoff between retries.
			delay := time.Duration(1<<(attempt-2)) * time.Second
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(delay):
			}
		}
		msgID, err := s.tryUploadOnce(ctx, peer, bytes.NewReader(body), filename, caption)
		if err == nil {
			return msgID, nil
		}
		lastErr = err
		if !isRetriableUploadErr(ctx, err) {
			return 0, err
		}
	}
	return 0, fmt.Errorf("upload failed after %d attempts: %w", uploadMaxAttempts, lastErr)
}

func (s *Session) tryUploadOnce(ctx context.Context, peer *tg.InputPeerChannel, r io.Reader, filename, caption string) (int, error) {
	u := uploader.NewUploader(s.api)
	uf, err := u.FromReader(ctx, filename, r)
	if err != nil {
		return 0, fmt.Errorf("upload bytes: %w", err)
	}
	media := &tg.InputMediaUploadedDocument{
		File:     uf,
		MimeType: "application/octet-stream",
		Attributes: []tg.DocumentAttributeClass{
			&tg.DocumentAttributeFilename{FileName: filename},
		},
	}
	rid, err := randomID()
	if err != nil {
		return 0, err
	}
	upd, err := s.api.MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
		Peer:     peer,
		Media:    media,
		RandomID: rid,
		Message:  caption,
	})
	if err != nil {
		return 0, fmt.Errorf("send media: %w", err)
	}
	return extractNewMessageID(upd, rid)
}

// isRetriableUploadErr decides whether an UploadDocument failure looks
// transient enough to be worth retrying. We never retry once the
// caller's ctx is done (honest shutdown). Otherwise we look for the
// few gotd error stringings we've actually observed in the wild —
// keeping the allow-list narrow so a real misconfiguration (bad
// channel id, expired session) doesn't get masked by a retry loop.
func isRetriableUploadErr(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	s := err.Error()
	for _, marker := range []string{
		"engine forcibly closed",
		"connection reset",
		"i/o timeout",
		"broken pipe",
		"unexpected EOF",
	} {
		if strings.Contains(s, marker) {
			return true
		}
	}
	return false
}

// DownloadDocument fetches the document attached to msgID in the
// configured channel and streams its bytes to w. Returns the byte count.
//
// FILE_REFERENCE_EXPIRED is transparently handled: gotd returns this
// error when the file_reference token cached in the InputDocumentFileLocation
// is too old (typically hours). On that error we re-fetch the message to
// pick up a fresh reference and retry once.
func (s *Session) DownloadDocument(ctx context.Context, msgID int, w io.Writer) (int64, error) {
	doc, err := s.fetchDocument(ctx, msgID)
	if err != nil {
		return 0, err
	}
	n, err := s.streamDocument(ctx, doc, w)
	if err == nil {
		return n, nil
	}
	if !tgerr.Is(err, "FILE_REFERENCE_EXPIRED") {
		return n, err
	}
	// Refresh the file_reference and retry once.
	doc, err = s.fetchDocument(ctx, msgID)
	if err != nil {
		return 0, fmt.Errorf("refresh file_reference: %w", err)
	}
	return s.streamDocument(ctx, doc, w)
}

// downloadThreads is the parallelism inside ONE document download.
// gotd's Builder.Parallel splits the file into 512 KiB parts (the
// downloader's default part size) and fetches `threads` of them at
// once via concurrent UploadGetFile RPCs. For a 4 MiB chunk that's
// 8 parts; 4 threads gets us 2 round-trip passes instead of 8 serial
// ones, which is the difference between "slow chunk reads" and
// "reads at par with writes".
const downloadThreads = 4

func (s *Session) streamDocument(ctx context.Context, doc *tg.Document, w io.Writer) (int64, error) {
	loc := &tg.InputDocumentFileLocation{
		ID:            doc.ID,
		AccessHash:    doc.AccessHash,
		FileReference: doc.FileReference,
	}
	// We need an io.WriterAt for Builder.Parallel. We don't always know
	// the document size up front (.Size on tg.Document is set but the
	// API doesn't guarantee it matches the bytes actually streamed),
	// so we use a growable bufferWriterAt that allocates on first
	// non-contiguous write. After download completes, we copy the
	// resulting bytes into the caller's io.Writer.
	buf := &bufferWriterAt{}
	d := downloader.NewDownloader()
	if _, err := d.Download(s.api, loc).WithThreads(downloadThreads).Parallel(ctx, buf); err != nil {
		return 0, err
	}
	n, err := w.Write(buf.bytes())
	return int64(n), err
}

// bufferWriterAt is a minimal io.WriterAt that grows its backing slice
// to fit out-of-order writes. gotd's Parallel downloader writes the
// file in fixed-size parts to known offsets; the size field tracks the
// high-water mark so bytes() returns exactly the downloaded length.
type bufferWriterAt struct {
	mu   sync.Mutex
	data []byte
	size int64
}

func (b *bufferWriterAt) WriteAt(p []byte, off int64) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	end := off + int64(len(p))
	if end > int64(len(b.data)) {
		// Grow with some headroom to avoid a tiny re-alloc on every part.
		newCap := int64(len(b.data)) * 2
		if newCap < end {
			newCap = end
		}
		grown := make([]byte, end, newCap)
		copy(grown, b.data)
		b.data = grown
	}
	copy(b.data[off:], p)
	if end > b.size {
		b.size = end
	}
	return len(p), nil
}

func (b *bufferWriterAt) bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.data[:b.size]
}

// getMessage fetches a single channel message by id, returning it as the
// concrete *tg.Message (rejecting MessageEmpty / MessageService).
func (s *Session) getMessage(ctx context.Context, msgID int) (*tg.Message, error) {
	_, inCh, err := s.channelPeer()
	if err != nil {
		return nil, err
	}
	res, err := s.api.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
		Channel: inCh,
		ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: msgID}},
	})
	if err != nil {
		return nil, fmt.Errorf("get messages: %w", err)
	}
	var msgs []tg.MessageClass
	switch r := res.(type) {
	case *tg.MessagesChannelMessages:
		msgs = r.Messages
	case *tg.MessagesMessages:
		msgs = r.Messages
	case *tg.MessagesMessagesSlice:
		msgs = r.Messages
	default:
		return nil, fmt.Errorf("unexpected messages response: %T", res)
	}
	for _, m := range msgs {
		switch mm := m.(type) {
		case *tg.Message:
			if mm.ID == msgID {
				return mm, nil
			}
		case *tg.MessageEmpty:
			return nil, fmt.Errorf("message %d is empty (deleted?)", msgID)
		}
	}
	return nil, fmt.Errorf("message %d not found in channel", msgID)
}

// fetchDocument retrieves the document attached to a channel message.
func (s *Session) fetchDocument(ctx context.Context, msgID int) (*tg.Document, error) {
	msg, err := s.getMessage(ctx, msgID)
	if err != nil {
		return nil, err
	}
	media, ok := msg.Media.(*tg.MessageMediaDocument)
	if !ok {
		return nil, fmt.Errorf("message %d has no document (media=%T)", msgID, msg.Media)
	}
	doc, ok := media.Document.(*tg.Document)
	if !ok {
		return nil, fmt.Errorf("message %d document is %T (expected *tg.Document)", msgID, media.Document)
	}
	return doc, nil
}

// extractNewMessageID walks the UpdatesClass returned by SendMessage /
// SendMedia and finds the id of the message corresponding to the
// caller's RandomID `rid`.
//
// MTProto's SendMessage / SendMedia responses can contain MULTIPLE
// updates — including UpdateNewChannelMessage entries for messages
// that other concurrent calls created. (We have at least three
// background producers on one Session: the chunk write path, the
// snapshot manager every 5 min, and the trash GC. Snapshot-while-cp
// is the easy reproducer.) Picking the first UpdateNewChannelMessage
// in the response — what we used to do — silently returns the wrong
// message id whenever there's contention, which records the wrong
// (ino, idx, tg_message_id) tuple in chunk_map and causes the
// "preHash == readHash, file md5 differs" corruption pattern that
// dogged v0.5..v0.7 on the live mount.
//
// The canonical Telegram primitive for "match my SendMedia call to
// the server-assigned message id" is UpdateMessageID, which carries
// our original RandomID alongside the new id. We use it
// preferentially. If the server didn't include UpdateMessageID (some
// API methods that return UpdateShortSentMessage don't), we fall back
// to the first new-message update — that case is the no-contention
// path and works correctly.
func extractNewMessageID(upd tg.UpdatesClass, rid int64) (int, error) {
	switch u := upd.(type) {
	case *tg.UpdateShortSentMessage:
		return u.ID, nil
	case *tg.Updates:
		return findIDByRandomID(u.Updates, rid)
	case *tg.UpdatesCombined:
		return findIDByRandomID(u.Updates, rid)
	}
	return 0, fmt.Errorf("no new-message update in response (%T)", upd)
}

// findIDByRandomID first looks for an UpdateMessageID whose RandomID
// matches `rid` — that's the unambiguous answer. Without that, falls
// back to the first new-message update.
func findIDByRandomID(updates []tg.UpdateClass, rid int64) (int, error) {
	for _, up := range updates {
		if mid, ok := up.(*tg.UpdateMessageID); ok && mid.RandomID == rid {
			return mid.ID, nil
		}
	}
	// No UpdateMessageID in the response — typical for "stand-alone"
	// sends with no other concurrent traffic. Use the first new-message
	// update as before; safe because there's at most one such update in
	// this case.
	for _, up := range updates {
		if id, ok := messageIDFromUpdate(up); ok {
			return id, nil
		}
	}
	return 0, fmt.Errorf("no UpdateMessageID for RandomID %d and no fallback new-message update", rid)
}

func messageIDFromUpdate(u tg.UpdateClass) (int, bool) {
	switch up := u.(type) {
	case *tg.UpdateNewChannelMessage:
		if m, ok := up.Message.(*tg.Message); ok {
			return m.ID, true
		}
	case *tg.UpdateNewMessage:
		if m, ok := up.Message.(*tg.Message); ok {
			return m.ID, true
		}
	}
	return 0, false
}

// randomID returns the 64-bit random message id required by MTProto for
// idempotent retries.
func randomID() (int64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(b[:])), nil
}

// Static check that ErrNotAuthorized round-trips errors.Is — Run wraps it
// via fmt.Errorf but inner ops here may return it bare.
var _ = errors.Is
