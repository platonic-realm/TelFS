package tg

import (
	"context"
	"fmt"

	"github.com/gotd/td/tg"
)

// MaxMessagesPerGet is how many ids one channels.getMessages call takes
// (`telfs fsck --quick` asks in batches of this size).
const MaxMessagesPerGet = 100

// MessageDoc is what the channel says about one message id: whether the
// message carries a document, and the document's size.
type MessageDoc struct {
	HasDoc bool
	Size   int64 // bytes (HasDoc only)
}

// MessageDocs asks the channel about up to MaxMessagesPerGet message ids
// in ONE channels.getMessages call, downloading nothing. The map holds
// an entry for every id that is a message in the channel (with HasDoc
// false when it carries no document); an id that was deleted or never
// existed has no entry.
func (s *Session) MessageDocs(ctx context.Context, ids []int) (map[int]MessageDoc, error) {
	if len(ids) == 0 {
		return map[int]MessageDoc{}, nil
	}
	if len(ids) > MaxMessagesPerGet {
		return nil, fmt.Errorf("message docs: %d ids in one call (at most %d)", len(ids), MaxMessagesPerGet)
	}
	_, inCh, err := s.channelPeer()
	if err != nil {
		return nil, err
	}
	api, err := s.currentAPI()
	if err != nil {
		return nil, err
	}
	in := make([]tg.InputMessageClass, len(ids))
	for i, id := range ids {
		in[i] = &tg.InputMessageID{ID: id}
	}
	res, err := api.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{Channel: inCh, ID: in})
	if err != nil {
		return nil, fmt.Errorf("get messages (%d ids from %d): %w", len(ids), ids[0], err)
	}
	return messageDocsOf(ids, extractMessages(res)), nil
}

// messageDocsOf reads a channels.getMessages answer: only the ids asked
// for count; MessageEmpty (deleted) is no entry; a service message or one
// whose media is not a document (or an empty document) has HasDoc false.
func messageDocsOf(ids []int, msgs []tg.MessageClass) map[int]MessageDoc {
	asked := make(map[int]bool, len(ids))
	for _, id := range ids {
		asked[id] = true
	}
	out := make(map[int]MessageDoc, len(ids))
	for _, m := range msgs {
		switch mm := m.(type) {
		case *tg.Message:
			if !asked[mm.ID] {
				continue
			}
			var d MessageDoc
			if media, ok := mm.Media.(*tg.MessageMediaDocument); ok {
				if doc, ok := media.Document.(*tg.Document); ok {
					d = MessageDoc{HasDoc: true, Size: doc.Size}
				}
			}
			out[mm.ID] = d
		case *tg.MessageService:
			if asked[mm.ID] {
				out[mm.ID] = MessageDoc{}
			}
		}
	}
	return out
}
