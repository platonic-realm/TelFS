package tg

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"

	"github.com/gotd/td/tg"
)

// PostMessage sends a plain text message to the configured channel and
// returns the new message id.
func (c *Client) PostMessage(ctx context.Context, text string) (int, error) {
	var msgID int
	err := c.Run(ctx, func(ctx context.Context, api *tg.Client) error {
		peer, _, err := c.configuredPeer()
		if err != nil {
			return err
		}
		rid, err := randomID()
		if err != nil {
			return err
		}
		updates, err := api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peer,
			Message:  text,
			RandomID: rid,
		})
		if err != nil {
			return fmt.Errorf("send message: %w", err)
		}
		msgID, err = extractNewMessageID(updates)
		if err != nil {
			return fmt.Errorf("extract message id: %w", err)
		}
		return nil
	})
	return msgID, err
}

// GetMessageText fetches a previously-sent message by id from the configured
// channel and returns its text body.
func (c *Client) GetMessageText(ctx context.Context, msgID int) (string, error) {
	var text string
	err := c.Run(ctx, func(ctx context.Context, api *tg.Client) error {
		_, inCh, err := c.configuredPeer()
		if err != nil {
			return err
		}
		res, err := api.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
			Channel: inCh,
			ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: msgID}},
		})
		if err != nil {
			return fmt.Errorf("get messages: %w", err)
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
			return fmt.Errorf("unexpected messages response: %T", res)
		}
		for _, m := range msgs {
			switch mm := m.(type) {
			case *tg.Message:
				if mm.ID == msgID {
					text = mm.Message
					return nil
				}
			case *tg.MessageEmpty:
				return fmt.Errorf("message %d is empty (deleted?)", msgID)
			}
		}
		return fmt.Errorf("message %d not found in channel", msgID)
	})
	return text, err
}

// extractNewMessageID walks the UpdatesClass returned by SendMessage and
// finds the id of the message that was just created.
func extractNewMessageID(upd tg.UpdatesClass) (int, error) {
	switch u := upd.(type) {
	case *tg.UpdateShortSentMessage:
		return u.ID, nil
	case *tg.Updates:
		for _, up := range u.Updates {
			if id, ok := messageIDFromUpdate(up); ok {
				return id, nil
			}
		}
	case *tg.UpdatesCombined:
		for _, up := range u.Updates {
			if id, ok := messageIDFromUpdate(up); ok {
				return id, nil
			}
		}
	}
	return 0, fmt.Errorf("no new-message update in response (%T)", upd)
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

// randomID generates the 64-bit random message id required by MTProto's
// MessagesSendMessage to deduplicate retries.
func randomID() (int64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(b[:])), nil
}
