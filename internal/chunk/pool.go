package chunk

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

// DefaultStagedCapBytes bounds how much dispatched-but-not-yet-
// uploaded chunk data (plaintext-accounted) may be held by upload
// goroutines at once. Within the cap, dispatch is ASYNC: close(2) of
// a freshly-written file returns at local-disk speed while the wires
// (already staged to pending/ — the durability story) drain to the
// channel in the background. Past the cap, dispatch blocks, so a
// sustained writer is still throttled to upload bandwidth instead of
// ballooning RAM/disk without bound. 256 MiB absorbs a typical burst
// (a compile's artifacts, a phone-camera video) without letting a
// 100 GB dd think it "finished" in a minute.
const DefaultStagedCapBytes int64 = 256 << 20

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

	// PendingIndex is the in-process map (ino, idx) → on-disk path
	// that accelerates Reader.tryPendingFallback by skipping the
	// O(N) filepath.Glob over PendingDir. Maintained by the writer
	// (on PersistPending success) and the packer (on DeletePending
	// success), and consulted by the reader. Nil disables the
	// acceleration; the disk scan still works.
	PendingIndex *PendingIndex

	// staged bounds the bytes of dirty-chunk data handed to upload
	// goroutines that haven't completed yet (see DefaultStagedCapBytes).
	// Acquired in the writer's dispatch path (the only place that
	// blocks the FUSE writeback flusher now), released when the
	// upload goroutine finishes — success or failure.
	staged    *semaphore.Weighted
	stagedCap int64

	// Disk, when set (production: PendingDir is set), switches the writer
	// to the disk-backed staging queue (spec v1, row 9): a chunk's wire is
	// written to PendingDir and its memory released before it waits for
	// anything; direct uploads then run from the queue below (the workers
	// replace the sem), packed ones from the packer, both reading the wire
	// back from disk. Nil keeps the in-memory pipeline (tests, mocks).
	Disk *DiskGate

	// Registrations by number (Register → Release), for `telfs sync`.
	regMu   sync.Mutex
	regNext uint64
	regOpen map[uint64]time.Time

	qmu       sync.Mutex
	queue     []*stagedJob
	qsignal   chan struct{}
	qstart    sync.Once
	busy      atomic.Int32
	queued    atomic.Int64
	retryBase time.Duration
}

// stagedJob is one staged direct upload. run returns true when the job is
// finished (uploaded, or dropped because its inode is gone) and false to
// be retried later from the staged wire.
type stagedJob struct {
	run     func(ctx context.Context) bool
	attempt int
}

// DefaultStagedRetryBase is the first retry delay of a failed staged upload;
// it doubles up to stagedRetryMax. The wire is on disk, so the job keeps
// retrying for as long as the daemon runs (a network outage resumes by
// itself); at shutdown it stays staged and the next mount re-submits it.
const (
	DefaultStagedRetryBase = 5 * time.Second
	stagedRetryMax         = 5 * time.Minute
)

// EnqueueStaged queues a staged direct upload (queue mode only).
func (p *UploadPool) EnqueueStaged(run func(ctx context.Context) bool) {
	p.qstart.Do(p.startWorkers)
	p.push(&stagedJob{run: run})
}

func (p *UploadPool) push(j *stagedJob) {
	p.qmu.Lock()
	p.queue = append(p.queue, j)
	p.qmu.Unlock()
	p.queued.Add(1)
	select {
	case p.qsignal <- struct{}{}:
	default:
	}
}

func (p *UploadPool) pop() *stagedJob {
	p.qmu.Lock()
	defer p.qmu.Unlock()
	if len(p.queue) == 0 {
		return nil
	}
	j := p.queue[0]
	p.queue[0] = nil
	p.queue = p.queue[1:]
	p.queued.Add(-1)
	return j
}

// startWorkers runs one worker per concurrency slot.
func (p *UploadPool) startWorkers() {
	for i := 0; i < cap(p.sem); i++ {
		go p.worker()
	}
}

func (p *UploadPool) worker() {
	for {
		j := p.pop()
		if j == nil {
			select {
			case <-p.qsignal:
				continue
			case <-p.uploadCtx.Done():
				return
			}
		}
		if p.uploadCtx.Err() != nil {
			return // shutting down: the wire stays staged for the next mount
		}
		p.busy.Add(1)
		done := runStagedJob(p.uploadCtx, j)
		p.busy.Add(-1)
		if done {
			// More work may be waiting behind this one.
			select {
			case p.qsignal <- struct{}{}:
			default:
			}
			continue
		}
		j.attempt++
		base := p.retryBase
		if base <= 0 {
			base = DefaultStagedRetryBase
		}
		d := base << min(j.attempt-1, 16)
		if d > stagedRetryMax || d <= 0 {
			d = stagedRetryMax
		}
		time.AfterFunc(d, func() {
			if p.uploadCtx.Err() == nil {
				p.push(j)
			}
		})
	}
}

// runStagedJob runs a job, turning a panic into "finished" (logged by the
// job's own guard) so one bad job can never kill a worker.
func runStagedJob(ctx context.Context, j *stagedJob) (done bool) {
	defer func() {
		if r := recover(); r != nil {
			done = true
		}
	}()
	return j.run(ctx)
}

// StagedQueue reports the staged direct uploads waiting and running.
func (p *UploadPool) StagedQueue() (waiting int64, running int32) {
	return p.queued.Load(), p.busy.Load()
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
		staged:       semaphore.NewWeighted(DefaultStagedCapBytes),
		stagedCap:    DefaultStagedCapBytes,
		qsignal:      make(chan struct{}, 1),
	}
}

// AcquireStaged blocks until n bytes of staging allowance are free (or
// ctx / the pool ctx fires). Returns the possibly-CLAMPED weight the
// caller must later pass to ReleaseStaged: a single chunk larger than
// the whole cap is admitted at cap weight instead of deadlocking.
func (p *UploadPool) AcquireStaged(ctx context.Context, n int64) (int64, error) {
	if n > p.stagedCap {
		n = p.stagedCap
	}
	// A child ctx canceled on pool shutdown, so a dispatch waiting on
	// the gate can't outlive the pool.
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(p.uploadCtx, cancel)
	defer stop()
	if err := p.staged.Acquire(waitCtx, n); err != nil {
		if p.uploadCtx.Err() != nil {
			return 0, p.uploadCtx.Err()
		}
		return 0, err
	}
	return n, nil
}

// ReleaseStaged returns allowance taken by AcquireStaged. Pass the
// clamped weight AcquireStaged returned, not the original size.
func (p *UploadPool) ReleaseStaged(n int64) {
	if n > 0 {
		p.staged.Release(n)
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
func (p *UploadPool) InFlight() int {
	if p.Disk != nil {
		return int(p.busy.Load())
	}
	return len(p.sem)
}

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
	pool        *UploadPool
	ino         int64
	seq         uint64
	semHeld     bool
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
	r.pool.regMu.Lock()
	delete(r.pool.regOpen, r.seq)
	r.pool.regMu.Unlock()
	r.pool.Pending.End(r.ino)
	r.pool.wg.Done()
}

// Register records a pending upload for ino WITHOUT acquiring a
// concurrency slot: the R-A-W barrier (Pending) and the drain
// WaitGroup are armed synchronously, so a reader or unmount that
// races the dispatch always sees the work. Never blocks. The upload
// goroutine acquires its slot later via Reservation.AcquireSem and
// MUST Release exactly once on completion (success or failure).
func (p *UploadPool) Register(ino int64) *Reservation {
	p.wg.Add(1)
	p.Pending.Begin(ino)
	p.regMu.Lock()
	p.regNext++
	seq := p.regNext
	if p.regOpen == nil {
		p.regOpen = map[uint64]time.Time{}
	}
	p.regOpen[seq] = time.Now()
	p.regMu.Unlock()
	return &Reservation{pool: p, ino: ino, seq: seq, pendingHeld: true}
}

// LastRegistered is the number of the newest registration (0 = none yet).
// `telfs sync` takes it as its barrier: every upload registered up to it
// was dispatched before the sync was asked for (spec v1, §3).
func (p *UploadPool) LastRegistered() uint64 {
	p.regMu.Lock()
	defer p.regMu.Unlock()
	return p.regNext
}

// OpenUpTo counts the registrations up to seq that have not been released
// (their upload has not landed).
func (p *UploadPool) OpenUpTo(seq uint64) int {
	p.regMu.Lock()
	defer p.regMu.Unlock()
	n := 0
	for s := range p.regOpen {
		if s <= seq {
			n++
		}
	}
	return n
}

// OpenStats reports the open registrations and when the oldest was made
// (zero time when none).
func (p *UploadPool) OpenStats() (open int, oldest time.Time) {
	p.regMu.Lock()
	defer p.regMu.Unlock()
	for _, t := range p.regOpen {
		if oldest.IsZero() || t.Before(oldest) {
			oldest = t
		}
	}
	return len(p.regOpen), oldest
}

// AcquireSem blocks until an upload concurrency slot frees (or a ctx
// fires). Idempotent-safe with ReleaseSem/Release via semHeld.
//
// Cancellation: the wait can be unblocked by either the caller's ctx
// or the pool's own ctx (shutdown). Returns the canceling ctx's err;
// the registration itself stays held either way (the caller's
// Release handles it).
func (r *Reservation) AcquireSem(ctx context.Context) error {
	if r.semHeld {
		return nil
	}
	select {
	case r.pool.sem <- struct{}{}:
		r.semHeld = true
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-r.pool.uploadCtx.Done():
		return r.pool.uploadCtx.Err()
	}
}

// Reserve registers a pending upload for ino and acquires a global
// concurrency slot, BLOCKING if the pool is saturated. Kept for
// callers that want the two steps fused (tests, tools); the FUSE
// writer dispatches via Register + a deferred AcquireSem instead so
// the writeback flusher never waits on the network.
func (p *UploadPool) Reserve(ctx context.Context, ino int64) (*Reservation, error) {
	res := p.Register(ino)
	if err := res.AcquireSem(ctx); err != nil {
		res.Release()
		return nil, err
	}
	return res, nil
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
