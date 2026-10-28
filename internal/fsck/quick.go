// Package fsck holds `telfs fsck --quick` (spec v1 row 7): every channel
// message that chunk_map references must still exist and carry a
// document, and every packed extent must fit inside its document
// (pack_offset + pack_size <= document size) — checked from the
// messages' metadata alone, in batches of 100 ids (one
// channels.getMessages each), without downloading any content. At
// 0.5 M files that is ~1 k requests; the full `telfs fsck` downloads and
// decrypts every chunk.
package fsck

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"telfs/internal/meta"
	"telfs/internal/tg"
)

// Refs is the metadata side: the messages chunk_map references
// (*meta.Store).
type Refs interface {
	// MessageRefs pages the referenced messages in id order.
	MessageRefs(ctx context.Context, after int64, limit int) ([]meta.MessageRef, error)
	// ExtentsPast counts a message's packed extents ending past size.
	ExtentsPast(ctx context.Context, msgID, size int64) (rows int64, ino int64, err error)
	// PathOfCached names an inode (for the report's example path).
	PathOfCached(ctx context.Context, ino int64, dirs map[int64]string) (string, bool)
}

// Messages is the channel side (*tg.Session): what one batch of message
// ids holds — an entry per id that is a message in the channel.
type Messages interface {
	MessageDocs(ctx context.Context, ids []int) (map[int]tg.MessageDoc, error)
}

// ErrChannel: the channel could not be asked (after the retries) —
// Telegram's or the network's failure, not a finding.
var ErrChannel = errors.New("the channel could not be read")

// Why a referenced message counts as missing.
const (
	ReasonDeleted = "deleted"            // no such message (deleted, or never posted)
	ReasonNoDoc   = "no document"        // the message carries no document
	ReasonBadID   = "invalid message id" // chunk_map holds an id no message can have
)

// Missing is a referenced message that is gone or has no document.
type Missing struct {
	MsgID int64 `json:"msg_id"`
	// Rows is how many chunk_map rows reference it.
	Rows int64 `json:"rows"`
	// ExamplePath is one file that uses it ("" when that inode has no
	// name, e.g. unlinked while open); ExampleIno is its inode.
	ExamplePath string `json:"example_path"`
	ExampleIno  int64  `json:"example_ino"`
	Reason      string `json:"reason"`
}

// TooSmall is a message whose document is shorter than the packed
// extents chunk_map places in it.
type TooSmall struct {
	MsgID int64 `json:"msg_id"`
	// Rows is how many of its extents end past the document's end.
	Rows        int64  `json:"rows"`
	ExamplePath string `json:"example_path"`
	ExampleIno  int64  `json:"example_ino"`
	// DocSize is the document's size; Need the end of the furthest
	// extent.
	DocSize int64 `json:"doc_size"`
	Need    int64 `json:"need"`
}

// Report is the outcome of a quick check (`telfs fsck --quick --json`).
type Report struct {
	MessagesChecked int64      `json:"messages_checked"`
	RowsChecked     int64      `json:"rows_checked"`
	Missing         []Missing  `json:"missing"`
	TooSmall        []TooSmall `json:"too_small"`
	TookS           float64    `json:"took_s"`
}

// OK reports whether nothing was found.
func (r *Report) OK() bool { return len(r.Missing) == 0 && len(r.TooSmall) == 0 }

// RowsAffected is how many chunk_map rows the findings touch.
func (r *Report) RowsAffected() int64 {
	var n int64
	for _, m := range r.Missing {
		n += m.Rows
	}
	for _, t := range r.TooSmall {
		n += t.Rows
	}
	return n
}

// DefaultAttempts is how often a failing batch is asked (Options.Attempts).
const DefaultAttempts = 6

// Options tune Quick.
type Options struct {
	// Batch is the ids per channel request (default and maximum:
	// tg.MaxMessagesPerGet).
	Batch int
	// Retriable says whether a failed request is worth repeating (nil:
	// never). A batch is asked at most Attempts times (default
	// DefaultAttempts), the waits doubling from 1 s to at most 30 s.
	Retriable func(error) bool
	Attempts  int
	// Sleep waits between attempts (tests); nil = real time.
	Sleep func(context.Context, time.Duration) error
	// Progress, when set, is called after every batch.
	Progress func(messages, rows int64)
}

// Quick checks every message chunk_map references (see the package
// comment). It reads the references a page at a time — a short query
// each, so it can run beside a live mount — and asks the channel about
// each page in one request. An error is returned only when the check
// could not finish (ErrChannel: Telegram; else the DB or ctx); findings
// are in the report.
func Quick(ctx context.Context, refs Refs, msgs Messages, opt Options) (*Report, error) {
	start := time.Now()
	batch := opt.Batch
	if batch <= 0 || batch > tg.MaxMessagesPerGet {
		batch = tg.MaxMessagesPerGet
	}
	rep := &Report{Missing: []Missing{}, TooSmall: []TooSmall{}}
	dirs := map[int64]string{}
	example := func(ino int64) string {
		if p, ok := refs.PathOfCached(ctx, ino, dirs); ok {
			return p
		}
		return ""
	}
	after := int64(math.MinInt64)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, err := refs.MessageRefs(ctx, after, batch)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			break
		}
		after = page[len(page)-1].MsgID
		ids := make([]int, 0, len(page))
		for _, r := range page {
			if validID(r.MsgID) {
				ids = append(ids, int(r.MsgID))
			}
		}
		docs, err := ask(ctx, msgs, ids, opt)
		if err != nil {
			return nil, err
		}
		for _, r := range page {
			rep.MessagesChecked++
			rep.RowsChecked += r.Rows
			var d tg.MessageDoc
			reason := ""
			if !validID(r.MsgID) {
				reason = ReasonBadID
			} else if got, ok := docs[int(r.MsgID)]; !ok {
				reason = ReasonDeleted
			} else if !got.HasDoc {
				reason = ReasonNoDoc
			} else {
				d = got
			}
			if reason != "" {
				rep.Missing = append(rep.Missing, Missing{MsgID: r.MsgID, Rows: r.Rows, ExampleIno: r.Ino, ExamplePath: example(r.Ino), Reason: reason})
				continue
			}
			if r.End > d.Size {
				n, ino, err := refs.ExtentsPast(ctx, r.MsgID, d.Size)
				if err != nil {
					return nil, err
				}
				rep.TooSmall = append(rep.TooSmall, TooSmall{MsgID: r.MsgID, Rows: n, ExampleIno: ino, ExamplePath: example(ino), DocSize: d.Size, Need: r.End})
			}
		}
		if opt.Progress != nil {
			opt.Progress(rep.MessagesChecked, rep.RowsChecked)
		}
		if len(page) < batch {
			break
		}
	}
	rep.TookS = math.Round(time.Since(start).Seconds()*1000) / 1000
	return rep, nil
}

// validID: a channel message id is a positive 32-bit number.
func validID(id int64) bool { return id > 0 && id <= math.MaxInt32 }

// ask asks the channel about one batch, repeating a retriable failure.
func ask(ctx context.Context, msgs Messages, ids []int, opt Options) (map[int]tg.MessageDoc, error) {
	if len(ids) == 0 {
		return map[int]tg.MessageDoc{}, nil
	}
	attempts := opt.Attempts
	if attempts <= 0 {
		attempts = DefaultAttempts
	}
	sleep := opt.Sleep
	if sleep == nil {
		sleep = sleepCtx
	}
	wait := time.Second
	for attempt := 1; ; attempt++ {
		docs, err := msgs.MessageDocs(ctx, ids)
		if err == nil {
			return docs, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if attempt >= attempts || opt.Retriable == nil || !opt.Retriable(err) {
			return nil, fmt.Errorf("%w: messages %d to %d: %w", ErrChannel, ids[0], ids[len(ids)-1], err)
		}
		if err := sleep(ctx, wait); err != nil {
			return nil, err
		}
		wait = min(2*wait, 30*time.Second)
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
