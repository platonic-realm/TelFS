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
)

// ChannelMessage is the GC-friendly view of one channel message.
type ChannelMessage struct {
	ID         int
	Kind       MessageKind
	HasDoc     bool // true if MessageMediaDocument was attached
	SnapCap    SnapshotCaption
	RawCaption string
}

// WalkChannelMessages calls fn for every message in the configured
// channel, newest-first, up to a soft cap of pageLimit*100 messages.
// fn may return ErrStopWalk to halt iteration early.
func (s *Session) WalkChannelMessages(ctx context.Context, pageLimit int, fn func(ChannelMessage) error) error {
	if pageLimit <= 0 {
		pageLimit = 50
	}
	peer, _, err := s.channelPeer()
	if err != nil {
		return err
	}
	offsetID := 0
	for page := 0; page < pageLimit; page++ {
		res, err := s.api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:     peer,
			OffsetID: offsetID,
			Limit:    100,
		})
		if err != nil {
			return fmt.Errorf("get history page %d: %w", page, err)
		}
		msgs := extractMessages(res)
		if len(msgs) == 0 {
			return nil
		}
		for _, m := range msgs {
			msg, ok := m.(*tg.Message)
			if !ok {
				continue
			}
			cm := classifyMessage(msg)
			if err := fn(cm); err != nil {
				if err == ErrStopWalk {
					return nil
				}
				return err
			}
		}
		last := msgs[len(msgs)-1]
		lastMsg, ok := last.(*tg.Message)
		if !ok {
			return nil
		}
		offsetID = lastMsg.ID
	}
	return nil
}

// ErrStopWalk signals WalkChannelMessages to halt iteration.
var ErrStopWalk = stopWalk{}

type stopWalk struct{}

func (stopWalk) Error() string { return "stop walk" }

// classifyMessage decides what flavor of TelFS object a given channel
// message represents. Conservative: anything we can't positively
// identify is KindUnknown (left alone by GC).
func classifyMessage(msg *tg.Message) ChannelMessage {
	cm := ChannelMessage{ID: msg.ID, RawCaption: msg.Message}
	if _, ok := msg.Media.(*tg.MessageMediaDocument); !ok {
		// Plain text or other media — not a TelFS object.
		return cm
	}
	cm.HasDoc = true
	if strings.TrimSpace(msg.Message) == "" {
		cm.Kind = KindChunk
		return cm
	}
	if cap, ok := parseSnapshotCaption(msg.Message); ok {
		cm.Kind = KindSnapshot
		cm.SnapCap = cap
		return cm
	}
	return cm
}
