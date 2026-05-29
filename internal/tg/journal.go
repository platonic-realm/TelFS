package tg

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/gotd/td/tg"
)

// MessageKindJournal is the discriminator we put in journal-op message
// captions. Different from MessageKindSnapshot ("snap") so the same
// channel can carry both without ambiguity.
const MessageKindJournal = "j"

// JournalCaption is the JSON envelope around a meta journal op.
// Seq is the local journal's autoincrement primary key — recovery
// uses it to skip ops that are already part of the restored snapshot.
type JournalCaption struct {
	Kind    string          `json:"k"`       // always "j"
	Seq     int64           `json:"seq"`     // local journal.seq
	TSUnix  int64           `json:"ts"`      // wall-clock at post
	FSUUID  string          `json:"fs_uuid"` // identifies the originating TelFS instance
	Payload json.RawMessage `json:"op"`      // the meta.JournalOp shape
}

// JournalMessage describes a single journal-op message found on the
// channel. The Payload is opaque from this package's perspective —
// it's a meta.JournalOp; the caller deserializes it.
type JournalMessage struct {
	MessageID int
	Caption   JournalCaption
}

// UploadJournalOp posts a journal-op message to the configured channel.
// The op body is sent as a 0-byte document with the JSON caption
// carrying the actual payload — using a document message (not just
// MessagesSendMessage) keeps journal ops and snapshots on the same
// "documents that scroll past in the channel" surface for tooling
// like ListSnapshots / ListJournalOps.
//
// Returns the new channel message id, which the caller persists via
// MarkJournalPosted so we don't re-post after restart.
func (s *Session) UploadJournalOp(ctx context.Context, opJSON []byte, seq, ts int64, fsUUID string) (int, error) {
	cap := JournalCaption{
		Kind: MessageKindJournal, Seq: seq, TSUnix: ts, FSUUID: fsUUID,
		Payload: json.RawMessage(opJSON),
	}
	payload, err := json.Marshal(cap)
	if err != nil {
		return 0, fmt.Errorf("encode journal caption: %w", err)
	}
	// Compact document name — journal ops are tiny and we don't need
	// a per-op filename to mean anything beyond grouping in tooling.
	name := fmt.Sprintf("telfs-jop-%s-%d.json", fsUUID, seq)
	return s.UploadDocument(ctx, strings.NewReader(string(opJSON)), name, string(payload))
}

// ListJournalOps scans the channel newest-first and returns every
// journal-op message with seq > sinceSeq (matching fsUUID if
// non-empty). Newest-first is convenient for ListSnapshots
// symmetry; the caller sorts ascending before replaying.
//
// max caps the number of entries returned so a misbehaving caller
// can't stream the entire channel into memory; 0 means use 1000
// (covering ~4h at 1op/15s — generous for a typical workload).
func (s *Session) ListJournalOps(ctx context.Context, fsUUID string, sinceSeq int64, max int) ([]JournalMessage, error) {
	if max <= 0 {
		max = 1000
	}
	peer, _, err := s.channelPeer()
	if err != nil {
		return nil, err
	}
	out := make([]JournalMessage, 0, 64)
	offsetID := 0
	const pageLimit = 100 // up to 10000 messages of scrollback
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
		stop := false
		for _, m := range msgs {
			msg, ok := m.(*tg.Message)
			if !ok {
				continue
			}
			cap, ok := parseJournalCaption(msg.Message)
			if !ok {
				continue
			}
			if fsUUID != "" && cap.FSUUID != fsUUID {
				continue
			}
			if cap.Seq <= sinceSeq {
				// Older than what we already have — stop scanning;
				// channel order matches seq order well enough that
				// further pages would only contain even older entries.
				stop = true
				break
			}
			out = append(out, JournalMessage{MessageID: msg.ID, Caption: cap})
			if len(out) >= max {
				return out, nil
			}
		}
		if stop {
			break
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

// parseJournalCaption tries to decode a journal-op caption. Returns
// (cap, false) when the text isn't JSON-shaped or doesn't carry the
// journal kind discriminator.
func parseJournalCaption(text string) (JournalCaption, bool) {
	text = strings.TrimSpace(text)
	if len(text) == 0 || text[0] != '{' {
		return JournalCaption{}, false
	}
	var c JournalCaption
	if err := json.Unmarshal([]byte(text), &c); err != nil {
		return JournalCaption{}, false
	}
	if c.Kind != MessageKindJournal {
		return JournalCaption{}, false
	}
	return c, true
}
