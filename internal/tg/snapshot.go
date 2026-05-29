package tg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/gotd/td/tg"
)

// MessageKindSnapshot is the discriminator we put in snapshot message
// captions. Future kinds ("c" for chunk, "m" for meta-op) will share
// the same envelope.
const MessageKindSnapshot = "snap"

// SnapshotCaption is the JSON we encode into a snapshot message's
// caption. Parsed back by FindLatestSnapshot during cold mount.
type SnapshotCaption struct {
	Kind   string `json:"k"`       // always "snap"
	Seq    int64  `json:"seq"`     // highest journal seq included
	TSUnix int64  `json:"ts"`      // wall-clock at upload
	FSUUID string `json:"fs_uuid"` // identifies the originating TelFS instance
}

// UploadSnapshot posts gzipped snapshot bytes as a channel document with
// the SnapshotCaption above as the caption. Returns the new message id.
func (s *Session) UploadSnapshot(ctx context.Context, gzipped []byte, seq, ts int64, fsUUID string) (int, error) {
	cap := SnapshotCaption{Kind: MessageKindSnapshot, Seq: seq, TSUnix: ts, FSUUID: fsUUID}
	payload, err := json.Marshal(cap)
	if err != nil {
		return 0, fmt.Errorf("encode caption: %w", err)
	}
	name := fmt.Sprintf("telfs-snap-%s-%d.sqlite.gz", fsUUID, ts)
	return s.UploadDocument(ctx, bytes.NewReader(gzipped), name, string(payload))
}

// LatestSnapshot describes the most-recent snapshot found in the
// channel (matching the given fs_uuid, if non-empty).
type LatestSnapshot struct {
	MessageID int
	Caption   SnapshotCaption
}

// FindLatestSnapshot scans the channel newest-first and returns the
// first message whose caption parses as a snapshot. If fsUUID is
// non-empty, snapshots with a different fs_uuid are skipped (defensive
// guard against channels containing messages from a previous TelFS
// instance).
//
// Returns (nil, nil) if no snapshot is found within `pageLimit` pages
// of 100 messages each.
func (s *Session) FindLatestSnapshot(ctx context.Context, fsUUID string, pageLimit int) (*LatestSnapshot, error) {
	if pageLimit <= 0 {
		pageLimit = 50 // 5000 messages of history before we give up
	}
	peer, _, err := s.channelPeer()
	if err != nil {
		return nil, err
	}
	offsetID := 0
	for page := 0; page < pageLimit; page++ {
		res, err := s.api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:     peer,
			OffsetID: offsetID,
			Limit:    100,
		})
		if err != nil {
			return nil, fmt.Errorf("get history: %w", err)
		}
		msgs := extractMessages(res)
		if len(msgs) == 0 {
			return nil, nil // exhausted
		}
		for _, m := range msgs {
			msg, ok := m.(*tg.Message)
			if !ok {
				continue
			}
			if cap, ok := parseSnapshotCaption(msg.Message); ok {
				if fsUUID != "" && cap.FSUUID != fsUUID {
					continue
				}
				return &LatestSnapshot{MessageID: msg.ID, Caption: cap}, nil
			}
		}
		// Paginate.
		last := msgs[len(msgs)-1]
		lastMsg, ok := last.(*tg.Message)
		if !ok {
			return nil, nil
		}
		offsetID = lastMsg.ID
	}
	return nil, nil
}

// ListSnapshots scans the channel newest-first and returns every
// snapshot message whose caption parses as a SnapshotCaption. If
// fsUUID is non-empty, snapshots from other TelFS instances are
// filtered out — the same defensive guard FindLatestSnapshot uses.
//
// Returns at most `max` entries. With max <= 0 the cap defaults to
// 200 (covering ~16h of 5-min snapshots) so a misbehaving caller
// can't stream the entire channel history into RAM.
func (s *Session) ListSnapshots(ctx context.Context, fsUUID string, max int) ([]LatestSnapshot, error) {
	if max <= 0 {
		max = 200
	}
	peer, _, err := s.channelPeer()
	if err != nil {
		return nil, err
	}
	out := make([]LatestSnapshot, 0, 16)
	offsetID := 0
	const pageLimit = 50 // 5000 messages of history before we give up
	for page := 0; page < pageLimit && len(out) < max; page++ {
		res, err := s.api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:     peer,
			OffsetID: offsetID,
			Limit:    100,
		})
		if err != nil {
			return nil, fmt.Errorf("get history: %w", err)
		}
		msgs := extractMessages(res)
		if len(msgs) == 0 {
			break
		}
		for _, m := range msgs {
			msg, ok := m.(*tg.Message)
			if !ok {
				continue
			}
			if cap, ok := parseSnapshotCaption(msg.Message); ok {
				if fsUUID != "" && cap.FSUUID != fsUUID {
					continue
				}
				out = append(out, LatestSnapshot{MessageID: msg.ID, Caption: cap})
				if len(out) >= max {
					return out, nil
				}
			}
		}
		last := msgs[len(msgs)-1]
		lastMsg, ok := last.(*tg.Message)
		if !ok {
			break
		}
		offsetID = lastMsg.ID
	}
	return out, nil
}

// DeleteChannelMessages removes the given message ids from the
// configured channel. Used by snapshot cleanup. Errors from individual
// deletes are joined with %w.
func (s *Session) DeleteChannelMessages(ctx context.Context, ids ...int) error {
	if len(ids) == 0 {
		return nil
	}
	_, inCh, err := s.channelPeer()
	if err != nil {
		return err
	}
	if _, err := s.api.ChannelsDeleteMessages(ctx, &tg.ChannelsDeleteMessagesRequest{
		Channel: inCh,
		ID:      ids,
	}); err != nil {
		return fmt.Errorf("delete messages %v: %w", ids, err)
	}
	return nil
}

// DownloadSnapshot is a convenience wrapper that fetches a snapshot
// message's document into a buffer.
func (s *Session) DownloadSnapshot(ctx context.Context, msgID int) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := s.DownloadDocument(ctx, msgID, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DownloadSnapshotTo streams a snapshot message's document to w.
func (s *Session) DownloadSnapshotTo(ctx context.Context, msgID int, w io.Writer) (int64, error) {
	return s.DownloadDocument(ctx, msgID, w)
}

// extractMessages flattens MessagesGetHistory's polymorphic response.
func extractMessages(res tg.MessagesMessagesClass) []tg.MessageClass {
	switch r := res.(type) {
	case *tg.MessagesMessages:
		return r.Messages
	case *tg.MessagesMessagesSlice:
		return r.Messages
	case *tg.MessagesChannelMessages:
		return r.Messages
	}
	return nil
}

// parseSnapshotCaption tries to decode a snapshot caption. Returns
// (cap, false) if the text isn't JSON-shaped or doesn't carry our
// snapshot discriminator.
func parseSnapshotCaption(text string) (SnapshotCaption, bool) {
	text = strings.TrimSpace(text)
	if len(text) == 0 || text[0] != '{' {
		return SnapshotCaption{}, false
	}
	var c SnapshotCaption
	if err := json.Unmarshal([]byte(text), &c); err != nil {
		return SnapshotCaption{}, false
	}
	if c.Kind != MessageKindSnapshot {
		return SnapshotCaption{}, false
	}
	return c, true
}
