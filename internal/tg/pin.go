package tg

import (
	"context"
	"errors"
	"fmt"

	"github.com/gotd/td/tg"
)

// PinnedMessageID returns the id of the channel's pinned message (0 when
// nothing is pinned). Format 2 keeps its key message pinned so a cold
// recovery finds it with one request (spec P1 §4.2).
func (s *Session) PinnedMessageID(ctx context.Context) (int, error) {
	_, inCh, err := s.channelPeer()
	if err != nil {
		return 0, err
	}
	api, err := s.currentAPI()
	if err != nil {
		return 0, err
	}
	res, err := api.ChannelsGetFullChannel(ctx, inCh)
	if err != nil {
		return 0, fmt.Errorf("get full channel: %w", err)
	}
	full, ok := res.FullChat.(*tg.ChannelFull)
	if !ok {
		return 0, errors.New("get full channel: not a channel")
	}
	id, _ := full.GetPinnedMsgID()
	return id, nil
}

// PinMessage pins a message silently (no notification to members).
func (s *Session) PinMessage(ctx context.Context, msgID int) error {
	peer, _, err := s.channelPeer()
	if err != nil {
		return err
	}
	api, err := s.currentAPI()
	if err != nil {
		return err
	}
	if _, err := api.MessagesUpdatePinnedMessage(ctx, &tg.MessagesUpdatePinnedMessageRequest{
		Silent: true,
		Peer:   peer,
		ID:     msgID,
	}); err != nil {
		return fmt.Errorf("pin message %d: %w", msgID, err)
	}
	return nil
}

// Unpin removes a message's pin (tests and repair tooling).
func (s *Session) Unpin(ctx context.Context, msgID int) error {
	peer, _, err := s.channelPeer()
	if err != nil {
		return err
	}
	api, err := s.currentAPI()
	if err != nil {
		return err
	}
	_, err = api.MessagesUpdatePinnedMessage(ctx, &tg.MessagesUpdatePinnedMessageRequest{Unpin: true, Peer: peer, ID: msgID})
	return err
}
