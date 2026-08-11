package tg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/gotd/td/tg"
)

// MessageKindJournal is the discriminator we put in journal-op message
// captions. Different from MessageKindSnapshot ("snap") so the same
// channel can carry both without ambiguity.
const MessageKindJournal = "j"

// JournalCaption is the JSON envelope around a meta journal op (or
// batch of ops). Recovery reads this without downloading the body,
// so it can cheaply decide whether the batch overlaps with seqs it
// still needs.
//
// Two shapes share this struct (the JSON tags use omitempty so a
// reader sees the right "is this a batch?" signal):
//
//	batch (post-2026 default):
//	  {k: "j", fs_uuid, ts, first_seq, last_seq, count}    ← in caption
//	  {ops: [{seq, op}, ...]}                               ← in body
//
//	single-op (legacy — kept for read compat with old channels):
//	  {k: "j", fs_uuid, ts, seq, op: <op-json>}             ← all in caption
//
// The discriminator is `count > 0` (batch) vs `len(payload) > 0`
// (single). Either one fully identifies the shape.
type JournalCaption struct {
	Kind     string          `json:"k"`
	TSUnix   int64           `json:"ts"`
	FSUUID   string          `json:"fs_uuid"`
	Seq      int64           `json:"seq,omitempty"`       // single-op only
	Payload  json.RawMessage `json:"op,omitempty"`        // single-op only
	FirstSeq int64           `json:"first_seq,omitempty"` // batch only
	LastSeq  int64           `json:"last_seq,omitempty"`  // batch only
	Count    int             `json:"count,omitempty"`     // batch only
}

// IsBatch reports whether this caption describes a multi-op batch
// (body must be fetched to recover the ops) vs a single-op caption
// with the op inline.
func (c JournalCaption) IsBatch() bool { return c.Count > 0 }

// JournalBatchBody is the document body schema for a batched
// journal-op message. Concatenation of (seq, op) pairs ordered by
// seq ascending.
type JournalBatchBody struct {
	Ops []JournalBatchEntry `json:"ops"`
}

// JournalBatchEntry is one (seq, op) pair inside a batch body.
type JournalBatchEntry struct {
	Seq int64           `json:"seq"`
	Op  json.RawMessage `json:"op"`
}

// JournalMessage describes a single journal-op message found on the
// channel. The Payload is opaque from this package's perspective —
// it's a meta.JournalOp; the caller deserializes it.
type JournalMessage struct {
	MessageID int
	Caption   JournalCaption
}

// UploadJournalOpBatch posts MANY journal entries as a SINGLE TG
// channel message. Caption carries a small marker (kind, fs_uuid,
// seq range, count) so recovery can decide whether to download the
// body. Body is a JSON `{ops:[{seq,op}, ...]}` array so recovery
// reconstructs every op the batch covers in one MTProto round trip
// per batch message instead of one per op.
//
// This is the load-bearing fix for the "many small messages"
// observation: 10k FS mutations no longer become 10k TG messages.
// At ~200 bytes per op + JSON overhead, ~1000 ops fit comfortably
// in a 256 KB body that uploads in 1–2 SaveFilePart calls.
//
// Caller passes the seq + opJSON pairs newest-last (ascending by
// seq); the schema preserves that ordering so recovery replays in
// the right order.
//
// Returns the new channel message id; caller persists posted_at
// for every seq in the batch via Meta.MarkJournalPosted.
func (s *Session) UploadJournalOpBatch(ctx context.Context, ops []JournalBatchEntry, ts int64, fsUUID string) (int, error) {
	if len(ops) == 0 {
		return 0, errors.New("UploadJournalOpBatch: empty batch")
	}
	body, err := json.Marshal(JournalBatchBody{Ops: ops})
	if err != nil {
		return 0, fmt.Errorf("encode journal batch body: %w", err)
	}
	cap := JournalCaption{
		Kind:     MessageKindJournal,
		TSUnix:   ts,
		FSUUID:   fsUUID,
		FirstSeq: ops[0].Seq,
		LastSeq:  ops[len(ops)-1].Seq,
		Count:    len(ops),
	}
	capJSON, err := json.Marshal(cap)
	if err != nil {
		return 0, fmt.Errorf("encode journal batch caption: %w", err)
	}
	name := fmt.Sprintf("telfs-jbatch-%s-%d-%d.json", fsUUID, cap.FirstSeq, cap.LastSeq)
	return s.UploadDocument(ctx, strings.NewReader(string(body)), name, string(capJSON))
}

// DownloadJournalBatchBody fetches the body of a journal-batch
// message and parses it. Used by recovery when a batch's seq range
// overlaps with seqs the caller hasn't replayed yet.
func (s *Session) DownloadJournalBatchBody(ctx context.Context, msgID int) ([]JournalBatchEntry, error) {
	var buf bytes.Buffer
	if _, err := s.DownloadDocument(ctx, msgID, &buf); err != nil {
		return nil, fmt.Errorf("download journal batch body msg=%d: %w", msgID, err)
	}
	var body JournalBatchBody
	if err := json.Unmarshal(buf.Bytes(), &body); err != nil {
		return nil, fmt.Errorf("parse journal batch body msg=%d: %w", msgID, err)
	}
	return body.Ops, nil
}

// ListJournalOps scans the channel newest-first and returns every
// journal op (single or expanded from a batch) with seq > sinceSeq
// (matching fsUUID if non-empty). Newest-first is convenient for
// ListSnapshots symmetry; the caller sorts ascending before
// replaying.
//
// For batch messages, the caption alone is enough to decide
// whether the batch overlaps with sinceSeq (LastSeq > sinceSeq);
// if it does, the body is downloaded and parsed for the ops.
// Single-op messages (legacy format) are decoded inline.
//
// max caps the number of entries returned so a misbehaving caller
// can't stream the entire channel into memory; 0 means use 100000.
//
// Completeness is load-bearing: replay applies these ops on top of a
// restored snapshot, and a missing PREFIX (the oldest ops) silently
// corrupts the result — every op after a dropped create applies to
// the wrong inode. So this function scans until it positively reaches
// the sinceSeq boundary (or true channel exhaustion) and returns an
// ERROR if any cap (max entries, page hard-cap) stops it early —
// never a silently-truncated suffix.
func (s *Session) ListJournalOps(ctx context.Context, fsUUID string, sinceSeq int64, max int) ([]JournalMessage, error) {
	if max <= 0 {
		max = 100000
	}
	peer, _, err := s.channelPeer()
	if err != nil {
		return nil, err
	}
	api, err := s.currentAPI()
	if err != nil {
		return nil, err
	}
	out := make([]JournalMessage, 0, 64)
	offsetID := 0
	boundaryReached := false
	// Hard page cap is a runaway guard, not a tuning knob: 2000 pages
	// = 200k messages of scrollback. Journal batches pack up to 1000
	// ops per message, so any realistic snapshot-to-crash window fits
	// well inside this; hitting the cap means something is deeply
	// wrong and we must not pretend the replay set is complete.
	const pageHardCap = 2000
	for page := 0; page < pageHardCap; page++ {
		res, err := api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:     peer,
			OffsetID: offsetID,
			Limit:    100,
		})
		if err != nil {
			return nil, fmt.Errorf("get history: %w", err)
		}
		msgs := extractMessages(res)
		if len(msgs) == 0 {
			// True channel exhaustion — we've read all the way back to
			// the beginning, so the set is complete even though we
			// never saw a seq <= sinceSeq (e.g. sinceSeq == 0 on a
			// young channel).
			boundaryReached = true
			break
		}
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
			if cap.IsBatch() {
				if cap.LastSeq <= sinceSeq {
					// Whole batch is older than what we need — the
					// contiguous range down to sinceSeq is covered.
					boundaryReached = true
					break
				}
				// Download body, expand into per-seq messages.
				entries, err := s.DownloadJournalBatchBody(ctx, msg.ID)
				if err != nil {
					return nil, err
				}
				batchCoversBoundary := cap.FirstSeq <= sinceSeq+1
				for _, e := range entries {
					if e.Seq <= sinceSeq {
						continue
					}
					if len(out) >= max {
						return nil, fmt.Errorf("journal scan hit the %d-op cap before reaching seq %d — refusing to return a truncated replay set", max, sinceSeq)
					}
					out = append(out, JournalMessage{
						MessageID: msg.ID,
						Caption: JournalCaption{
							Kind:    MessageKindJournal,
							TSUnix:  cap.TSUnix,
							FSUUID:  cap.FSUUID,
							Seq:     e.Seq,
							Payload: e.Op,
						},
					})
				}
				if batchCoversBoundary {
					boundaryReached = true
					break
				}
				continue
			}
			// Legacy single-op caption.
			if cap.Seq <= sinceSeq {
				boundaryReached = true
				break
			}
			if len(out) >= max {
				return nil, fmt.Errorf("journal scan hit the %d-op cap before reaching seq %d — refusing to return a truncated replay set", max, sinceSeq)
			}
			out = append(out, JournalMessage{MessageID: msg.ID, Caption: cap})
		}
		if boundaryReached {
			break
		}
		next, ok := lastMessageID(msgs)
		if !ok {
			boundaryReached = true // page of unidentifiable entries — treat as exhausted
			break
		}
		offsetID = next
	}
	if !boundaryReached {
		return nil, fmt.Errorf("journal scan exhausted its page budget before reaching seq %d — the channel has more history than the scanner covered; NOT returning a truncated replay set", sinceSeq)
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
