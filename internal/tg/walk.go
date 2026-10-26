package tg

import (
	"context"
	"fmt"
	"strings"

	"github.com/gotd/td/tg"
)

// MessageKind classifies a channel message for GC purposes.
type MessageKind int

const (
	// KindUnknown is anything we don't recognize — could be a text
	// message, an unrelated document, or a future TelFS message kind.
	// GC leaves these alone.
	KindUnknown MessageKind = iota
	// KindChunk is a TelFS chunk: document with an empty caption.
	KindChunk
	// KindSnapshot is a TelFS snapshot: document with a JSON caption
	// that parses with k:"snap".
	KindSnapshot
	// KindJournal is a journal-op batch (or legacy single op):
	// document with a JSON caption that parses with k:"j". gc prunes
	// these once every retained snapshot already covers their seqs.
	KindJournal
)

// ChannelMessage is the GC-friendly view of one channel message.
type ChannelMessage struct {
	ID         int
	Date       int // unix seconds the message was posted (0 if unknown)
	Kind       MessageKind
	HasDoc     bool // true if MessageMediaDocument was attached
	SnapCap    SnapshotCaption
	JnlCap     JournalCaption // populated when Kind == KindJournal
	RawCaption string
	DocName    string // the document's filename attribute (HasDoc only)
	DocSize    int64  // the document's size in bytes (HasDoc only)
}

// WalkChannelMessages calls fn for every message in the configured
// channel, newest-first, up to a soft cap of pageLimit*100 messages.
// fn may return ErrStopWalk to halt iteration early.
func (s *Session) WalkChannelMessages(ctx context.Context, pageLimit int, fn func(ChannelMessage) error) error {
	_, err := s.WalkChannelMessagesExhaust(ctx, pageLimit, fn)
	return err
}

// WalkChannelMessagesExhaust is WalkChannelMessages that also reports
// whether the walk reached the START of the channel (exhausted = true)
// rather than stopping at the page cap or on ErrStopWalk — what a scan
// that must not be silently truncated needs to know.
func (s *Session) WalkChannelMessagesExhaust(ctx context.Context, pageLimit int, fn func(ChannelMessage) error) (exhausted bool, err error) {
	if pageLimit <= 0 {
		pageLimit = 50
	}
	peer, _, err := s.channelPeer()
	if err != nil {
		return false, err
	}
	api, err := s.currentAPI()
	if err != nil {
		return false, err
	}
	offsetID := 0
	for page := 0; page < pageLimit; page++ {
		res, err := api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:     peer,
			OffsetID: offsetID,
			Limit:    100,
		})
		if err != nil {
			return false, fmt.Errorf("get history page %d: %w", page, err)
		}
		msgs := extractMessages(res)
		if len(msgs) == 0 {
			return true, nil
		}
		for _, m := range msgs {
			msg, ok := m.(*tg.Message)
			if !ok {
				continue
			}
			cm := classifyMessage(msg)
			if err := fn(cm); err != nil {
				if err == ErrStopWalk {
					return false, nil
				}
				return false, err
			}
		}
		next, ok := lastMessageID(msgs)
		if !ok {
			// Page of nothing but unidentifiable entries — can't
			// advance the cursor, so stop rather than loop forever.
			return false, nil
		}
		offsetID = next
	}
	return false, nil
}

// lastMessageID returns the ID of the last message in a page regardless
// of its concrete class (Message, MessageService, MessageEmpty — all
// carry an ID). Earlier code bailed out of pagination entirely when the
// page's last element wasn't a *tg.Message, which silently truncated
// every scan at the first pin / title-change / service message — the
// recovery scanner would then conclude "no snapshot" on a channel that
// had one a page further back.
func lastMessageID(msgs []tg.MessageClass) (int, bool) {
	for i := len(msgs) - 1; i >= 0; i-- {
		if withID, ok := msgs[i].(interface{ GetID() int }); ok {
			if id := withID.GetID(); id > 0 {
				return id, true
			}
		}
	}
	return 0, false
}

// ErrStopWalk signals WalkChannelMessages to halt iteration.
var ErrStopWalk = stopWalk{}

type stopWalk struct{}

func (stopWalk) Error() string { return "stop walk" }

// classifyMessage decides what flavor of TelFS object a given channel
// message represents. Conservative: anything we can't positively
// identify is KindUnknown (left alone by GC).
func classifyMessage(msg *tg.Message) ChannelMessage {
	cm := ChannelMessage{ID: msg.ID, Date: msg.Date, RawCaption: msg.Message}
	media, ok := msg.Media.(*tg.MessageMediaDocument)
	if !ok {
		// Plain text or other media — not a TelFS object.
		return cm
	}
	cm.HasDoc = true
	if doc, ok := media.Document.(*tg.Document); ok {
		cm.DocSize = doc.Size
		for _, a := range doc.Attributes {
			if fn, ok := a.(*tg.DocumentAttributeFilename); ok {
				cm.DocName = fn.FileName
			}
		}
	}
	if strings.TrimSpace(msg.Message) == "" {
		cm.Kind = KindChunk
		return cm
	}
	if cap, ok := parseSnapshotCaption(msg.Message); ok {
		cm.Kind = KindSnapshot
		cm.SnapCap = cap
		return cm
	}
	if cap, ok := parseJournalCaption(msg.Message); ok {
		cm.Kind = KindJournal
		cm.JnlCap = cap
		return cm
	}
	return cm
}
