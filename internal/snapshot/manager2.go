package snapshot

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"telfs/internal/meta"
	"telfs/internal/wire"
)

// The format-2 side of Manager: the policy loop, the snapshot + anchor,
// and retention that never deletes the anchored snapshot.

func (m *Manager) ch() Channel {
	if m.Channel != nil {
		return m.Channel
	}
	return m.Session
}

func (m *Manager) logger() *log.Logger {
	if m.Logger != nil {
		return m.Logger
	}
	return log.Default()
}

// Retry spacing after a failed format-2 step.
const (
	retryMin = time.Minute
	retryMax = 30 * time.Minute
)

func nextRetry(b time.Duration) time.Duration {
	if b <= 0 {
		return retryMin
	}
	if b *= 2; b > retryMax {
		b = retryMax
	}
	return b
}

// run2 evaluates the policy at start (no snapshot when nothing changed
// since the last) and every Tick until ctx is canceled.
func (m *Manager) run2(ctx context.Context) error {
	tick := m.Tick
	if tick <= 0 {
		tick = DefaultPolicyTick
	}
	logger := m.logger()
	var retryAt time.Time
	var backoff time.Duration
	attempt := func() {
		now := time.Now()
		if now.Before(retryAt) {
			return
		}
		act, err := m.step2(ctx, now)
		if err == nil {
			backoff = 0
			return
		}
		if ctx.Err() != nil {
			return
		}
		backoff = nextRetry(backoff)
		retryAt = now.Add(backoff)
		logger.Printf("[snapshot] %s failed (next try in %s): %v%s", act, backoff, err, channelLostHint(err))
	}
	attempt()
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			attempt()
		}
	}
}

// step2 is one evaluation of the policy at time now, and what it did:
// ActNone, ActAnchor (posted), ActSnapshot/ActSnapshotQuiet (taken) or
// ActWait (a due snapshot waits for a quiet moment).
func (m *Manager) step2(ctx context.Context, now time.Time) (Action, error) {
	st, err := LoadPolicyState(ctx, m.Meta)
	if err != nil {
		return ActNone, fmt.Errorf("read snapshot state: %w", err)
	}
	p := m.Policy.withDefaults()
	act, why := p.Decide(st, now)
	logger := m.logger()
	switch act {
	case ActNone:
		m.dueSince = time.Time{}
		return ActNone, nil
	case ActAnchor:
		m.dueSince = time.Time{}
		if !st.HaveJnlAfter {
			// Nothing was journaled since that snapshot (Decide checked),
			// so every later op is posted after it: its own id bounds
			// the journal.
			if err := putKVInt(ctx, m.Meta, KVSnapJnlAfter, int64(st.SnapMsg)); err != nil {
				return act, err
			}
		}
		id, err := PostAnchor(ctx, m.ch(), m.Meta, m.Keys, logger.Printf)
		if err != nil {
			return act, err
		}
		logger.Printf("[snapshot] anchor %d names snapshot %d (%s)", id, st.SnapMsg, why)
		return act, nil
	case ActSnapshotQuiet:
		if m.dueSince.IsZero() {
			m.dueSince = now
		}
		quiet := m.QuiesceCheck == nil || m.QuiesceCheck(ctx)
		if !quiet {
			if now.Sub(m.dueSince) < p.MaxQuietWait {
				return ActWait, nil
			}
			why += fmt.Sprintf("; no quiet moment within %s", p.MaxQuietWait)
		}
	}
	logger.Printf("[snapshot] taking a snapshot: %s", why)
	id, aerr, err := m.once2(ctx)
	if err != nil {
		return act, err
	}
	if aerr != nil {
		logger.Printf("[snapshot] snapshot %d is posted, its anchor is not (retried): %v", id, aerr)
	}
	m.dueSince = time.Time{}
	logger.Printf("[snapshot] snapshot %d posted", id)
	return act, nil
}

// once2 takes a format-2 snapshot, records it, prunes the local journal
// it covers, posts the anchor naming it and applies retention. A failed
// anchor is returned apart (anchorErr), not as the error: the snapshot
// stands, the policy loop posts the anchor later, and until then the
// previous anchor (whose snapshot retention keeps) still recovers
// everything.
func (m *Manager) once2(ctx context.Context) (id int, anchorErr, err error) {
	ch := m.ch()
	// The journal boundary, read BEFORE the snapshot is taken: a batch
	// holding an op the snapshot lacks is posted after this point, so
	// its id is larger — including batches posted while the snapshot
	// uploads, whose ids are smaller than the snapshot's own.
	after, err := ch.LatestMessageID(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("newest message id: %w", err)
	}
	// The mark that a snapshot is owed, as it is before the snapshot is
	// taken: only that mark is cleared by recording it (MarkDue makes
	// every mark unique, so a change marked while this snapshot was
	// being taken keeps its mark and gets the next one).
	due, derr := m.Meta.GetKV(ctx, KVSnapDue)
	if derr != nil && !errors.Is(derr, meta.ErrNotFound) {
		return 0, nil, derr
	}
	snap, err := Take(ctx, m.Meta)
	if err != nil {
		return 0, nil, fmt.Errorf("take: %w", err)
	}
	ts := time.Now().Unix()
	body, err := m.Keys.SealMeta("snap", snap.Bytes)
	if err != nil {
		return 0, nil, err
	}
	capt, err := m.Keys.SealCaption(wire.Caption{K: wire.KindSnapshot, Seq: snap.JournalSeq,
		TS: ts, DBSz: int64(len(snap.Bytes))})
	if err != nil {
		return 0, nil, err
	}
	id, err = ch.UploadDocument(ctx, bytes.NewReader(body), wire.DocName, capt)
	if err != nil {
		return 0, nil, fmt.Errorf("upload: %w", err)
	}
	// One transaction: a reader (an anchor being posted for a version)
	// or a crash never sees this snapshot's id with the previous one's
	// seq or journal boundary.
	if err := m.Meta.UpdateKV(context.WithoutCancel(ctx), func(k *meta.KVTx) error {
		for _, kv := range []struct {
			key string
			v   int64
		}{{KVCurrentMsgID, int64(id)}, {KVCurrentJournalSeq, snap.JournalSeq}, {KVLastSnapUnix, ts}, {KVSnapJnlAfter, int64(after)}} {
			if err := putInt(k, kv.key, kv.v); err != nil {
				return err
			}
		}
		if derr == nil {
			now, err := k.Get(KVSnapDue)
			if err == nil && bytes.Equal(now, due) {
				return k.Delete(KVSnapDue)
			}
		}
		return nil
	}); err != nil {
		return id, nil, fmt.Errorf("record snapshot %d: %w", id, err)
	}
	m.pruneLocalJournal(ctx, snap.JournalSeq)
	logger := m.logger()
	if _, err := PostAnchor(ctx, ch, m.Meta, m.Keys, logger.Printf); err != nil {
		anchorErr = err
	}
	if err := m.pruneRetention2(ctx, id); err != nil {
		logger.Printf("[snapshot] retention prune: %v", err)
	}
	m.setBaseline(m.Meta.WriteCounter())
	return id, anchorErr, nil
}

// SnapshotAndAnchor takes a format-2 snapshot and posts the anchor naming
// it, and fails unless both are on the channel: for a command that makes
// the local DB a new state the channel's anchor must not keep naming the
// old one (`telfs snapshot restore`).
func (m *Manager) SnapshotAndAnchor(ctx context.Context) (int, error) {
	if m.Keys == nil {
		return 0, errors.New("snapshot and anchor: format 2 only")
	}
	id, aerr, err := m.once2(ctx)
	if err != nil {
		return 0, err
	}
	if aerr != nil {
		return id, fmt.Errorf("snapshot %d is posted, the anchor naming it is not: %w", id, aerr)
	}
	return id, nil
}

// MarkDue records that a snapshot is owed: the local DB holds something
// the channel's snapshot chain does not and no journal op carries (a
// setting kept only in meta_kv). The daemon takes the snapshot at its
// next policy check, the next mount at once; until it lands the
// filesystem is not synced. Format 1 (its 5-minute cadence snapshots
// whatever changed) is left alone. Each mark is unique (the time is
// part of it), so a snapshot taken while a mark is being made never
// clears the newer mark.
func MarkDue(ctx context.Context, m *meta.Store, reason string) error {
	if f, err := m.Format(ctx); err != nil || f != 2 {
		return err
	}
	return m.PutKV(ctx, KVSnapDue, []byte(fmt.Sprintf("%s (%s)", reason, time.Now().UTC().Format(time.RFC3339Nano))))
}

// Owed says why the channel does not describe the local DB of a format-2
// filesystem until a snapshot is taken ("" when it does): snap_due is set
// (`snapshot restore` whose own snapshot failed, a rebuild from captions,
// a setting change).
func Owed(ctx context.Context, m *meta.Store) string {
	v, err := m.GetKV(ctx, KVSnapDue)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		return ""
	case err != nil:
		return "the snapshot state can't be read: " + err.Error()
	case len(v) == 0:
		return "a snapshot is due"
	}
	return "a snapshot is due: " + string(v)
}

// pruneRetention2 keeps the newest Retention snapshots from the local
// list and deletes the rest — never the snapshot the posted anchor names
// (a failed anchor post leaves the previous anchor current).
func (m *Manager) pruneRetention2(ctx context.Context, justUploaded int) error {
	ids := loadSnapIDs(ctx, m.Meta)
	found := false
	for _, x := range ids {
		if x == justUploaded {
			found = true
		}
	}
	if !found {
		ids = append(ids, justUploaded)
	}
	anchored := int(kvInt(ctx, m.Meta, KVAnchorSnapID))
	keep := m.retention()
	var del []int
	if len(ids) > keep {
		var kept []int
		for _, x := range ids[:len(ids)-keep] {
			if x == anchored {
				kept = append(kept, x)
			} else {
				del = append(del, x)
			}
		}
		ids = append(kept, ids[len(ids)-keep:]...)
	}
	if err := storeSnapIDs(ctx, m.Meta, ids); err != nil {
		return err
	}
	if len(del) == 0 {
		return nil
	}
	return m.ch().DeleteChannelMessages(ctx, del...)
}
