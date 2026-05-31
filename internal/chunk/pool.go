package chunk

import (
	"context"
	"sync"
)

// UploadPool is the backend-scoped dispatch layer all Writers funnel
// chunk uploads through. It owns three things that, prior to async
// close, lived per-Writer:
//
//   - a SINGLE concurrency semaphore capping total in-flight uploads
//     across the entire mount (was per-writer; per-writer + detached
//     handles would let one cp -r spawn unbounded TG round trips and
//     FLOOD_WAIT itself out of the channel),
//   - a long-lived ctx that survives any single file handle's Release
//     (so detached uploads don't get canceled when the kernel closes
//     the fd), canceled only at Backend.Shutdown,
//   - a global WaitGroup over every dispatched goroutine so unmount
//     can drain them before the gotd session tears down.
//
// It also owns the per-inode read-after-write barrier (Pending) so
// that a Reader opening the file just after the writer closed sees
// the data — without this barrier, async close would silently break
// read-after-write consistency.
type UploadPool struct {
	uploadCtx    context.Context
	uploadCancel context.CancelFunc
	sem          chan struct{}
	wg           sync.WaitGroup
	Pending      *PendingInodes
	// PendingDir is the on-disk staging directory for wires awaiting
	// upload. Writers PersistPending here BEFORE Submitting to the
	// packer; packer flush DeletePending here AFTER the chunk_map
	// row lands. Empty string disables persistence (used by tests
	// that mock the uploader and don't care about restart recovery).
	PendingDir string
}

// NewUploadPool constructs a pool with the given concurrency cap.
// concurrency <= 0 falls back to DefaultUploadConcurrency.
func NewUploadPool(concurrency int) *UploadPool {
	if concurrency <= 0 {
		concurrency = DefaultUploadConcurrency
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &UploadPool{
		uploadCtx:    ctx,
		uploadCancel: cancel,
		sem:          make(chan struct{}, concurrency),
		Pending:      NewPendingInodes(),
	}
}

// UploadCtx is the long-lived context for in-flight uploads. Survives
// any single Writer's Detach; canceled by Shutdown.
func (p *UploadPool) UploadCtx() context.Context {
	return p.uploadCtx
}

// InFlight returns the number of concurrency slots currently held
// by active uploads. The sem channel is "send-to-acquire"; len(sem)
// reflects how many tokens have been pushed (= in-flight count).
// Cheap to call from a status endpoint.
func (p *UploadPool) InFlight() int { return len(p.sem) }

// Cap returns the upload-concurrency cap this pool was constructed
// with. Used by the web UI to render "N of M slots in use" without
// having to know the value out of band.
func (p *UploadPool) Cap() int { return cap(p.sem) }

// Reservation tracks a pool entry's three resources separately so a
// pack-route uploader can release just the SEM slot early (freeing a
// new dispatch to start) while keeping the Pending counter + wg held
// until the actual chunk_map row has been written by the packer's
// flush. Without this split, the pool's sem caps the number of small
// files the packer can aggregate per batch at DefaultUploadConcurrency
// — defeating most of the point of having a packer.
type Reservation struct {
	pool       *UploadPool
	ino        int64
	semHeld    bool
	pendingHeld bool
}

// ReleaseSem returns the upload concurrency slot to the pool while
// keeping Pending + wg held. Used by the pack route in uploadOne
// after Submit returns: the wire is now the packer's responsibility,
// but readers + Drain must still see the upload as pending until the
// packer's PutChunk lands.
func (r *Reservation) ReleaseSem() {
	if !r.semHeld {
		return
	}
	r.semHeld = false
	<-r.pool.sem
}

// Release returns every resource. Idempotent — safe to defer in
// uploadOne whether or not ReleaseSem already fired.
func (r *Reservation) Release() {
	r.ReleaseSem()
	if !r.pendingHeld {
		return
	}
	r.pendingHeld = false
	r.pool.Pending.End(r.ino)
	r.pool.wg.Done()
}

// Reserve registers a pending upload for ino and acquires a global
// concurrency slot, BLOCKING if the pool is saturated. Returns a
// *Reservation the upload goroutine MUST Release exactly once on
// completion (success or failure).
//
// Cancellation: the wait can be unblocked by either the caller's ctx
// or the pool's own ctx (shutdown). Returns the canceling ctx's err
// in either case; in that case any resources already-acquired are
// returned so Begin/End stay balanced.
func (p *UploadPool) Reserve(ctx context.Context, ino int64) (*Reservation, error) {
	p.wg.Add(1)
	p.Pending.Begin(ino)
	select {
	case p.sem <- struct{}{}:
	case <-ctx.Done():
		p.Pending.End(ino)
		p.wg.Done()
		return nil, ctx.Err()
	case <-p.uploadCtx.Done():
		p.Pending.End(ino)
		p.wg.Done()
		return nil, p.uploadCtx.Err()
	}
	return &Reservation{
		pool:        p,
		ino:         ino,
		semHeld:     true,
		pendingHeld: true,
	}, nil
}

// Drain blocks until every dispatched upload completes or ctx fires.
// Returns true if the pool drained cleanly, false on ctx timeout.
// Caller is expected to follow up with Shutdown() in either case.
func (p *UploadPool) Drain(ctx context.Context) bool {
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

// Shutdown cancels the upload ctx so any in-flight gotd round-trips
// abort. Subsequent Reserve calls fail immediately. Idempotent.
func (p *UploadPool) Shutdown() {
	p.uploadCancel()
}
