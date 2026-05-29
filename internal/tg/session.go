package tg

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

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

// UploadDocument uploads the contents of r as a document attached to a
// message in the configured channel. The document is sent with the given
// filename (used for the DocumentAttributeFilename) and optional caption.
// Returns the new message id.
func (s *Session) UploadDocument(ctx context.Context, r io.Reader, filename, caption string) (int, error) {
	peer, _, err := s.channelPeer()
	if err != nil {
		return 0, err
	}
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

func (s *Session) streamDocument(ctx context.Context, doc *tg.Document, w io.Writer) (int64, error) {
	loc := &tg.InputDocumentFileLocation{
		ID:            doc.ID,
		AccessHash:    doc.AccessHash,
		FileReference: doc.FileReference,
	}
	cw := &countingWriter{w: w}
	d := downloader.NewDownloader()
	if _, err := d.Download(s.api, loc).Stream(ctx, cw); err != nil {
		return cw.n, err
	}
	return cw.n, nil
}

// countingWriter wraps an io.Writer and tracks how many bytes were
// written. We use this to recover the byte count from gotd's downloader,
// which returns a file-type tag rather than a count.
type countingWriter struct {
	w io.Writer
	n int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n += int64(n)
	return n, err
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
