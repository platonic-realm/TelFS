package snapshot

import (
	"context"
	"errors"
	"fmt"
	"time"

	"telfs/internal/meta"
)

// Format-2 snapshot policy (spec v1 §2, row 4). Format 2 journals every
// change, chunk placements included, so the newest snapshot plus the
// journal after it IS the filesystem: a whole-DB snapshot is needed only
// to keep recovery's replay short. One is taken when the journal has
// grown by EveryOps since the last, or Every has passed and the journal
// moved — at a quiet moment (upload queue drained) when one comes within
// MaxQuietWait, else anyway. Nothing at mount when nothing changed, none
// when the queue drains, none at unmount: the posted journal suffices.
// Format 1 keeps its 5-minute cadence (Manager.Interval).

// Policy defaults (config.toml [snapshot] every_ops / every).
const (
	DefaultEveryOps     = 500_000
	DefaultEvery        = 24 * time.Hour
	DefaultMaxQuietWait = time.Hour
	// DefaultPolicyTick is how often the format-2 loop looks.
	DefaultPolicyTick = time.Minute
)

// Policy is when a format-2 filesystem takes a snapshot.
type Policy struct {
	EveryOps     int64         // journal ops since the last snapshot (0 = DefaultEveryOps)
	Every        time.Duration // time since the last snapshot, if the journal moved (0 = DefaultEvery)
	MaxQuietWait time.Duration // longest wait for a quiet moment past a threshold (0 = DefaultMaxQuietWait)
}

func (p Policy) withDefaults() Policy {
	if p.EveryOps <= 0 {
		p.EveryOps = DefaultEveryOps
	}
	if p.Every <= 0 {
		p.Every = DefaultEvery
	}
	if p.MaxQuietWait <= 0 {
		p.MaxQuietWait = DefaultMaxQuietWait
	}
	return p
}

// PolicyState is what the policy looks at, read from the local DB.
type PolicyState struct {
	HaveSnap     bool      // a snapshot of this filesystem is recorded (snap_journal_seq)
	SnapMsg      int       // its message id
	SnapSeq      int64     // the journal seq it covers
	CurrentSeq   int64     // the journal seq now
	LastSnap     time.Time // when it was taken (zero: unknown)
	Due          string    // a snapshot is owed now (snap_due), the reason
	HaveJnlAfter bool      // its journal boundary is recorded (not for snapshots from before v1)
	AnchorSnap   int       // the snapshot the posted anchor names (0: no anchor)
	AnchorDue    bool      // the posted anchor's version list is out of date (anchor_due)
}

// Action is what the policy asks for.
type Action int

const (
	ActNone          Action = iota
	ActAnchor               // post an anchor for the recorded snapshot; no new snapshot
	ActSnapshot             // take a snapshot now
	ActSnapshotQuiet        // take a snapshot at a quiet moment (at most MaxQuietWait from now)
	ActWait                 // (Manager step only) the snapshot waits for a quiet moment
)

func (a Action) String() string {
	switch a {
	case ActNone:
		return "none"
	case ActAnchor:
		return "anchor"
	case ActSnapshot:
		return "snapshot"
	case ActSnapshotQuiet:
		return "snapshot-when-quiet"
	case ActWait:
		return "wait"
	}
	return fmt.Sprintf("action(%d)", int(a))
}

// Decide returns what the policy asks for in state st at time now, and
// why.
func (p Policy) Decide(st PolicyState, now time.Time) (Action, string) {
	p = p.withDefaults()
	if !st.HaveSnap {
		return ActSnapshot, "no snapshot recorded yet"
	}
	if st.Due != "" {
		return ActSnapshot, st.Due
	}
	ops := st.CurrentSeq - st.SnapSeq
	if ops < 0 {
		return ActSnapshot, fmt.Sprintf("the journal (seq %d) is behind the recorded snapshot (seq %d)", st.CurrentSeq, st.SnapSeq)
	}
	if !st.HaveJnlAfter {
		if ops == 0 {
			// Nothing journaled since: every later op is posted after
			// now, so the snapshot's own id bounds its journal.
			return ActAnchor, "first anchor for a snapshot from before v1"
		}
		return ActSnapshot, "the newest snapshot predates anchors"
	}
	if ops >= p.EveryOps {
		return ActSnapshotQuiet, fmt.Sprintf("%d journal ops since the last snapshot (threshold %d)", ops, p.EveryOps)
	}
	if ops > 0 && (st.LastSnap.IsZero() || now.Sub(st.LastSnap) >= p.Every) {
		return ActSnapshotQuiet, fmt.Sprintf("%d journal ops and %s since the last snapshot (threshold %s)", ops, age(st.LastSnap, now), p.Every)
	}
	if st.AnchorSnap != st.SnapMsg {
		return ActAnchor, "the anchor does not name the newest snapshot"
	}
	if st.AnchorDue {
		return ActAnchor, "the anchor's version list is out of date"
	}
	return ActNone, ""
}

func age(t, now time.Time) string {
	if t.IsZero() {
		return "unknown time"
	}
	return now.Sub(t).Round(time.Minute).String()
}

// LoadPolicyState reads the policy's inputs from the local DB.
func LoadPolicyState(ctx context.Context, m *meta.Store) (PolicyState, error) {
	var st PolicyState
	seq, have, err := loadCurrentJournalSeq(ctx, m)
	if err != nil {
		return st, err
	}
	st.HaveSnap, st.SnapSeq = have, seq
	if st.CurrentSeq, err = m.LastJournalSeq(ctx); err != nil {
		return st, err
	}
	st.SnapMsg = int(kvInt(ctx, m, KVCurrentMsgID))
	if st.SnapMsg <= 0 {
		st.HaveSnap = false
	}
	if ts := kvInt(ctx, m, KVLastSnapUnix); ts > 0 {
		st.LastSnap = time.Unix(ts, 0)
	}
	if v, err := m.GetKV(ctx, KVSnapDue); err == nil {
		st.Due = string(v)
		if st.Due == "" {
			st.Due = "a snapshot is due"
		}
	} else if !errors.Is(err, meta.ErrNotFound) {
		return st, err
	}
	_, err = m.GetKV(ctx, KVSnapJnlAfter)
	st.HaveJnlAfter = err == nil
	st.AnchorSnap = int(kvInt(ctx, m, KVAnchorSnapID))
	if _, err := m.GetKV(ctx, KVAnchorDue); err == nil {
		st.AnchorDue = true
	} else if !errors.Is(err, meta.ErrNotFound) {
		return st, err
	}
	return st, nil
}
