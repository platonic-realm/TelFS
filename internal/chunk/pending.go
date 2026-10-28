package chunk

import (
	"context"
	"sync"
)

// PendingInodes is the per-inode read-after-write barrier the upload
// pool maintains so that a reader who opens a file just after its
// writer closed sees the data, not stale or empty meta. It is the
// safety mechanism that makes async close (Flush returning before
// uploads complete) correct against subsequent reads.
//
// Lifecycle: every chunk dispatch calls Begin(ino) before the upload
// goroutine starts; the goroutine defers End(ino) so even a panicking
// upload still decrements. Wait blocks until the current batch
// drains. New uploads arriving after Wait returned are the caller's
// concern — POSIX already leaves cross-fd ordering undefined without
// explicit synchronization, and TelFS won't try to fix that here.
type PendingInodes struct {
	mu     sync.Mutex
	counts map[int64]int           // ino -> in-flight upload count
	drains map[int64]chan struct{} // ino -> closed when count returns to 0
}

func NewPendingInodes() *PendingInodes {
	return &PendingInodes{
		counts: make(map[int64]int),
		drains: make(map[int64]chan struct{}),
	}
}

// Begin records a new in-flight upload for ino. Pairs with End.
func (p *PendingInodes) Begin(ino int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.counts[ino]++
	if _, ok := p.drains[ino]; !ok {
		p.drains[ino] = make(chan struct{})
	}
}

// End decrements ino's pending count and, if it reaches zero, signals
// every Wait caller currently parked on this ino. A subsequent Begin
// for the same ino starts a fresh batch with a fresh drain channel.
func (p *PendingInodes) End(ino int64) {
	p.mu.Lock()
	p.counts[ino]--
	if p.counts[ino] > 0 {
		p.mu.Unlock()
		return
	}
	ch := p.drains[ino]
	delete(p.counts, ino)
	delete(p.drains, ino)
	p.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

// Total returns the sum of in-flight upload counts across all inos.
// Used by the metrics endpoint as a gauge.
func (p *PendingInodes) Total() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	var total int64
	for _, c := range p.counts {
		total += int64(c)
	}
	return total
}

// Inos returns the inodes with uploads in flight (any order).
func (p *PendingInodes) Inos() []int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]int64, 0, len(p.counts))
	for ino, c := range p.counts {
		if c > 0 {
			out = append(out, ino)
		}
	}
	return out
}

// Wait blocks until the current batch of in-flight uploads for ino
// drains. Returns immediately if nothing is in flight. Returns
// ctx.Err() if ctx fires first.
//
// "Current batch" semantics: Wait snapshots the drain channel under
// the lock and waits for THAT channel to close. If a new upload
// arrives after Wait already snapshot the channel, it gets a fresh
// channel and Wait does not extend itself to cover it.
func (p *PendingInodes) Wait(ctx context.Context, ino int64) error {
	p.mu.Lock()
	ch, ok := p.drains[ino]
	p.mu.Unlock()
	if !ok {
		return nil
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
