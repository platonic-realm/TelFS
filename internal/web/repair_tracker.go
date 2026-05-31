package web

import (
	"sync"
	"time"

	"telfs/internal/meta"
)

// RepairTracker holds the live state of an async RepairPartialFiles
// run kicked off from the /state page. Single run at a time —
// in-flight or recently-finished. Lock-protected so the request
// handler can read while the goroutine writes.
type RepairTracker struct {
	mu  sync.Mutex
	run *repairRun
}

type repairRunState int

const (
	repairIdle repairRunState = iota
	repairRunning
	repairDone
	repairError
)

// repairRun is the snapshot the /state page reads. Fields are
// shallow-copied under the lock; the caller never sees the
// in-progress slice mutating from under it.
type repairRun struct {
	State      repairRunState
	Started    time.Time
	Finished   time.Time // zero while State == repairRunning
	Total      int       // total candidate inodes (known after the scan completes)
	Done       int       // 1-based index of last completed inode
	Reports    []meta.RepairReport
	LastReport *meta.RepairReport // most recent non-nil report — for the "currently fixing" line
	Err        string             // populated when State == repairError
	BytesSaved int64              // running sum of (OldSize - NewSize) across reports
}

// Snapshot returns a read-only copy of the current run, or nil if
// no run has ever happened. Caller can render the result without
// holding the tracker's lock.
func (t *RepairTracker) Snapshot() *repairRunSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.run == nil {
		return nil
	}
	r := t.run
	reports := make([]meta.RepairReport, len(r.Reports))
	copy(reports, r.Reports)
	var last *meta.RepairReport
	if r.LastReport != nil {
		lr := *r.LastReport
		last = &lr
	}
	return &repairRunSnapshot{
		State:      r.State,
		Started:    r.Started,
		Finished:   r.Finished,
		Total:      r.Total,
		Done:       r.Done,
		Reports:    reports,
		LastReport: last,
		Err:        r.Err,
		BytesSaved: r.BytesSaved,
	}
}

// repairRunSnapshot is the value the template renders. Identical
// shape to repairRun but owned by the caller; no aliasing.
type repairRunSnapshot struct {
	State      repairRunState
	Started    time.Time
	Finished   time.Time
	Total      int
	Done       int
	Reports    []meta.RepairReport
	LastReport *meta.RepairReport
	Err        string
	BytesSaved int64
}

func (s *repairRunSnapshot) IsRunning() bool { return s != nil && s.State == repairRunning }
func (s *repairRunSnapshot) IsDone() bool    { return s != nil && s.State == repairDone }
func (s *repairRunSnapshot) IsError() bool   { return s != nil && s.State == repairError }

// Pct returns 0–100 progress; safe to call even before Total is known.
func (s *repairRunSnapshot) Pct() int {
	if s == nil || s.Total == 0 {
		return 0
	}
	p := (s.Done * 100) / s.Total
	if p > 100 {
		p = 100
	}
	return p
}

// Duration returns wall-clock time elapsed for this run.
func (s *repairRunSnapshot) Duration() time.Duration {
	if s == nil || s.Started.IsZero() {
		return 0
	}
	end := s.Finished
	if end.IsZero() {
		end = time.Now()
	}
	return end.Sub(s.Started).Round(time.Second)
}

// Begin starts tracking a new run. Refuses if one is already
// in-flight — the caller should surface that to the user. Returns
// a private handle the caller passes back to the per-inode and
// finish hooks.
func (t *RepairTracker) Begin() (started bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.run != nil && t.run.State == repairRunning {
		return false
	}
	t.run = &repairRun{
		State:   repairRunning,
		Started: time.Now(),
	}
	return true
}

// OnProgress is the per-inode callback the meta layer invokes after
// processing each candidate. Updates the live snapshot.
func (t *RepairTracker) OnProgress(report *meta.RepairReport, idx, total int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.run == nil {
		return
	}
	t.run.Total = total
	t.run.Done = idx
	if report != nil {
		t.run.Reports = append(t.run.Reports, *report)
		r := *report
		t.run.LastReport = &r
		t.run.BytesSaved += report.OldSize - report.NewSize
	}
}

// Finish marks the run completed (or errored). Safe to call without
// a preceding Begin (no-op).
func (t *RepairTracker) Finish(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.run == nil {
		return
	}
	t.run.Finished = time.Now()
	if err != nil {
		t.run.State = repairError
		t.run.Err = err.Error()
		return
	}
	t.run.State = repairDone
}
