package tg

import (
	"context"
	"fmt"

	"github.com/gotd/td/tg"
)

// maxPinnedPages bounds PinnedMessages (100 a page). A TelFS channel
// carries two pins (key message + anchor); anything near this is not a
// TelFS channel.
const maxPinnedPages = 10

// PinnedMessages lists every pinned message of the channel, newest
// first (messages.search with the pinned filter). channels.getFullChannel
// names only the NEWEST pin, and a format-2 channel has two: the key
// message and the snapshot anchor (spec v1 §2).
func (s *Session) PinnedMessages(ctx context.Context) ([]ChannelMessage, error) {
	peer, _, err := s.channelPeer()
	if err != nil {
		return nil, err
	}
	api, err := s.currentAPI()
	if err != nil {
		return nil, err
	}
	var out []ChannelMessage
	offsetID := 0
	for page := 0; page < maxPinnedPages; page++ {
		res, err := api.MessagesSearch(ctx, &tg.MessagesSearchRequest{
			Peer:     peer,
			Filter:   &tg.InputMessagesFilterPinned{},
			OffsetID: offsetID,
			Limit:    100,
		})
		if err != nil {
			return nil, fmt.Errorf("list pinned messages: %w", err)
		}
		msgs := extractMessages(res)
		for _, m := range msgs {
			if msg, ok := m.(*tg.Message); ok {
				out = append(out, classifyMessage(msg))
			}
		}
		next, ok := lastMessageID(msgs)
		if len(msgs) < 100 || !ok {
			return out, nil
		}
		offsetID = next
	}
	return out, nil
}

// LatestMessageID returns the id of the channel's newest message (0 for
// an empty channel). A format-2 snapshot records it before it is taken:
// every journal batch holding ops the snapshot lacks is posted later and
// so has a larger id (spec v1 §2, the anchor's jnl_after).
func (s *Session) LatestMessageID(ctx context.Context) (int, error) {
	peer, _, err := s.channelPeer()
	if err != nil {
		return 0, err
	}
	api, err := s.currentAPI()
	if err != nil {
		return 0, err
	}
	res, err := api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{Peer: peer, Limit: 1})
	if err != nil {
		return 0, fmt.Errorf("get newest message: %w", err)
	}
	newest := 0
	for _, m := range extractMessages(res) {
		if withID, ok := m.(interface{ GetID() int }); ok && withID.GetID() > newest {
			newest = withID.GetID()
		}
	}
	return newest, nil
}

// WalkChannelMessagesAfter calls fn for every message with an id larger
// than minID, newest first, until the walk reaches minID (min_id of
// messages.getHistory). It reads only what came after minID — never the
// whole history. fn may return ErrStopWalk to halt early.
func (s *Session) WalkChannelMessagesAfter(ctx context.Context, minID int, fn func(ChannelMessage) error) error {
	peer, _, err := s.channelPeer()
	if err != nil {
		return err
	}
	api, err := s.currentAPI()
	if err != nil {
		return err
	}
	offsetID := 0
	for page := 0; ; page++ {
		res, err := api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:     peer,
			OffsetID: offsetID,
			MinID:    minID,
			Limit:    100,
		})
		if err != nil {
			return fmt.Errorf("get history after %d, page %d: %w", minID, page, err)
		}
		msgs := extractMessages(res)
		if len(msgs) == 0 {
			return nil
		}
		for _, m := range msgs {
			msg, ok := m.(*tg.Message)
			if !ok || msg.ID <= minID {
				continue
			}
			if err := fn(classifyMessage(msg)); err != nil {
				if err == ErrStopWalk {
					return nil
				}
				return err
			}
		}
		next, ok := lastMessageID(msgs)
		if !ok || next <= minID+1 || (offsetID > 0 && next >= offsetID) {
			return nil
		}
		offsetID = next
	}
}
