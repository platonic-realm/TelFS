package chunk

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"telfs/internal/meta"
	"telfs/internal/metrics"
)

// DefaultPackThreshold is the chunk-payload size at-or-below which
// chunks are aggregated by the Packer instead of each getting their
// own TG message. Set low: the packer's value is amortizing the
// per-message round-trip for truly small chunks (tiny files); for
// full 4 MiB chunks the per-message overhead is already negligible
// vs the payload, and routing them through the serial packer
// flusher would needlessly bottleneck big-file uploads on a single
// goroutine instead of the pool's 4-way parallelism.
//
// Bug history: was briefly raised to 8 MiB to "reduce message count
// for big files" — combined with the (then-unbounded) flush batch
// this funneled multi-GB worth of wires into one giant blob that
// timed out. Big files belong in the parallel pool; the packer is
// for tiny ones only.
const DefaultPackThreshold int32 = 256 << 10

// DefaultPackTarget is the soft cap on the bytes a single packed
// blob upload will carry. 32 MiB is the sweet spot: large enough to
// collapse hundreds of sub-256-KB files into a handful of TG
// messages and reduce per-file round-trip cost meaningfully, small
// enough that one flush completes in seconds on a typical residential
// uplink. With RangedFetcher in the read path, a large blob doesn't
// hurt random-access reads — each chunk's extent is fetched
// independently via upload.getFile(offset, limit).
const DefaultPackTarget int64 = 32 << 20

// DefaultPackWindow is how long the packer will wait for more
// wires before flushing what it has. Async close makes this
// invisible to close() (returns long before the window matters);
// fsync waits on the packer's per-chunk result, so its latency
// IS bounded by this. 1 s is generous enough for staggered
// many-tiny-file bursts (git clone, tar -x, slow upload of many
// small attachments) to coalesce into one TG message instead of
// firing per-200ms-tick blobs that each carry only a few files.
const DefaultPackWindow = 1 * time.Second

// Packer aggregates small chunk wires into shared "packed blobs" —
// one TG message carrying many chunks back-to-back — and writes
// chunk_map rows with (pack_offset, pack_size) so Reader can slice
// them out on download. One Packer is shared by every Writer in a
// mount (lives on Backend).
//
// Lifecycle:
//   - NewPacker spawns a goroutine that loops on the flush window.
//   - Writer.uploadOne routes "small" chunks here instead of pool.
//     Submit appends to the buffer and returns a chan; the writer
//     blocks on the chan for the result so its uploadWg / Pending
//     bookkeeping matches the unpacked path.
//   - When buffer >= packTarget OR window elapses, flusher uploads
//     the assembled blob, writes one chunk_map row per pending
//     chunk, signals all waiters.
//   - Close stops the flusher, drains the buffer.
//
// Concurrency: each flush is ONE upload, no parallelism within the
// packer. Multiple flushes can be in flight (one per flush-window
// tick) but in practice the simple serial flusher is enough for
// the target workload (many small files). The cost: at most one
// extra in-flight upload above the pool's sem cap.
type Packer struct {
	meta     *meta.Store
	uploader Uploader

	threshold int32
	target    int64
	window    time.Duration

	mu       sync.Mutex
	pending  []packReq
	bufBytes int64
	wake     chan struct{}

	// stop signals the loop to take a final drain pass and exit.
	// It is intentionally SEPARATE from the upload context: a final
	// flush must complete on a live ctx even though Close has been
	// called, otherwise every buffered chunk evaporates and we
	// silently lose data the writer's Sync already returned-success
	// for. PutChunk uses context.Background() for the same reason.
	stop chan struct{}
	done chan struct{}
}

type packReq struct {
	ino           int64
	idx           int32
	plaintextSize int32
	wire          []byte
	// pendingPath, when non-empty, points at this wire's on-disk
	// staging file. Flush deletes it AFTER PutChunk lands so a
	// daemon kill mid-upload doesn't drop the chunk — the next
	// startup scans the pending dir and re-submits any leftover
	// wires to the packer.
	pendingPath string
	result      chan packResult
}

type packResult struct {
	msgID     int64
	packOff   int64
	err       error
}

// NewPacker constructs a Packer with the given dependencies and
// thresholds; zero values default to DefaultPackThreshold, target,
// window.
func NewPacker(m *meta.Store, u Uploader, threshold int32, target int64, window time.Duration) *Packer {
	if threshold <= 0 {
		threshold = DefaultPackThreshold
	}
	if target <= 0 {
		target = DefaultPackTarget
	}
	if window <= 0 {
		window = DefaultPackWindow
	}
	p := &Packer{
		meta:      m,
		uploader:  u,
		threshold: threshold,
		target:    target,
		window:    window,
		wake:      make(chan struct{}, 1),
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	go p.loop()
	return p
}

// Threshold returns the chunk-size cutoff below which Submit is the
// right call. Callers (Writer.uploadOne) check this before deciding
// the dispatch path.
func (p *Packer) Threshold() int32 { return p.threshold }

// QueueDepth returns the number of buffered wires + their total
// bytes. Snapshot under the packer's mutex, so the value is a
// point-in-time count rather than an instantaneous summary as the
// flusher drains. Surfaced through the daemon's /telegram JSON for
// the web UI.
func (p *Packer) QueueDepth() (chunks int, bytes int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pending), p.bufBytes
}

// Target returns the per-flush byte cap (the threshold above which
// the buffer wakes the flusher). Exposed for the queue panel so the
// user can compare packer_pending_bytes against the trigger.
func (p *Packer) Target() int64 { return p.target }

// Submit enqueues a wire for the next packed-blob upload and
// returns a chan that will deliver the (msgID, packOffset) once
// the upload lands. The caller MUST receive on the chan — otherwise
// the flusher's distribution goroutine leaks.
//
// pendingPath, when non-empty, is the on-disk staging file for this
// wire. Flush deletes it after PutChunk lands.
//
// Submit only blocks long enough to append; the flush window is in
// a separate goroutine. If the new wire would push the buffer past
// the target, Submit wakes the flusher immediately (a "trigger
// flush" condition complementing the window-elapsed condition).
func (p *Packer) Submit(ino int64, idx int32, plaintextSize int32, wire []byte, pendingPath string) <-chan packResult {
	ch := make(chan packResult, 1)
	req := packReq{
		ino:           ino,
		idx:           idx,
		plaintextSize: plaintextSize,
		wire:          wire,
		pendingPath:   pendingPath,
		result:        ch,
	}
	p.mu.Lock()
	p.pending = append(p.pending, req)
	p.bufBytes += int64(len(wire))
	overTarget := p.bufBytes >= p.target
	p.mu.Unlock()
	if overTarget {
		select {
		case p.wake <- struct{}{}:
		default:
		}
	}
	return ch
}

// Close stops the flusher, runs a final drain on a live ctx so the
// last batch lands its TG message + chunk_map rows, and unblocks
// any callers waiting on their result chan. Idempotent.
func (p *Packer) Close() {
	select {
	case <-p.stop:
		// already closed
	default:
		close(p.stop)
	}
	<-p.done
}

// loop is the flusher goroutine. Wakes on the window timer or on
// a Submit that pushed the buffer over the target.
func (p *Packer) loop() {
	defer close(p.done)
	timer := time.NewTimer(p.window)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			p.flush()
			timer.Reset(p.window)
		case <-p.wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			p.flush()
			timer.Reset(p.window)
		case <-p.stop:
			// Final drain — the buffered batch must land before
			// Close returns. Uploads use Background ctx (see flush)
			// so this isn't racing a cancelled context.
			p.flush()
			return
		}
	}
}

// flush takes UP TO p.target bytes of pending wires and uploads them
// as one blob. Wires beyond the target stay in p.pending for the
// next flush; if any remain, we re-signal wake so the loop runs
// again promptly instead of waiting for the next window tick.
//
// Always takes at least one wire so an oversized single wire still
// makes progress (would otherwise stall forever if its size alone
// exceeded target).
//
// Bug history: an earlier version drained the entire pending slice
// in one shot. Fast burst submits + recovery (recoverPending feeding
// in 173 wires in a tight loop) coalesced into one 689 MiB blob
// that couldn't finish inside the 5-min upload timeout — and on
// each retry the same 689 MiB blob got rebuilt. The bound fixes
// both the steady-state and the recovery path.
func (p *Packer) flush() {
	p.mu.Lock()
	if len(p.pending) == 0 {
		p.mu.Unlock()
		return
	}
	var batch []packReq
	var batchBytes int64
	taken := 0
	for i, r := range p.pending {
		w := int64(len(r.wire))
		// Take at least one — even if it alone exceeds target —
		// so we always make progress.
		if i > 0 && batchBytes+w > p.target {
			break
		}
		batch = append(batch, r)
		batchBytes += w
		taken = i + 1
	}
	// Slide the tail forward instead of allocating; len(p.pending)
	// here is bounded by the dispatch rate, not unbounded growth.
	p.pending = p.pending[taken:]
	p.bufBytes -= batchBytes
	morePending := len(p.pending) > 0
	p.mu.Unlock()
	if morePending {
		// Wake the loop again so the residual drains promptly
		// instead of waiting another p.window tick.
		select {
		case p.wake <- struct{}{}:
		default:
		}
	}

	// Assemble blob. Per-chunk pack_offset is the running prefix sum.
	var buf bytes.Buffer
	offsets := make([]int64, len(batch))
	for i, r := range batch {
		offsets[i] = int64(buf.Len())
		buf.Write(r.wire)
	}

	name := fmt.Sprintf("pack-%d", time.Now().UnixNano())
	// Uploads use a long-deadline context so a stuck MTProto
	// connection — where TCP send buffer is queued but Telegram
	// isn't acking, gotd has no app-level timeout — doesn't wedge
	// the packer indefinitely.
	//
	// Pick the deadline carefully. A 32 MiB packed blob is ~30
	// UploadSaveBigFilePart RPCs plus a MessagesSendMedia finalize.
	// Each RPC is gated by the rate limiter; with a conservative cap
	// of 30/min one batch takes a full minute at the absolute floor.
	// Add FLOOD_WAIT internal retries (gotd's middleware can sleep up
	// to 60 min PER attempt), uploadWithRetry's own backoff, and one
	// reconnect cycle (the stall watchdog at the session layer kicks
	// at 90 s), and a single batch's wall-clock floor under hostile
	// network + rate-limit conditions is in the 15-20 min range.
	//
	// The PREVIOUS value (5 min) was too aggressive. Under throttled
	// upload, packed batches timed out mid-flight; their constituent
	// writers had already been Closed by FUSE Release (since Flush
	// returns async), so the result error fell through to recordLost
	// — chunk_map rows were never written, while inode.size had
	// already been persisted. Tens of thousands of files ended up
	// with size > 0 and zero chunks, reading back as sparse zeros.
	// Wires stayed in pending/ for recovery, but until the next
	// daemon restart drained them the user saw corrupt files.
	//
	// 60 min lets every realistic combination of rate limit + flood
	// wait + retry land before we give up. The stall watchdog is the
	// right level for "the connection is wedged" detection; this
	// timeout is just the upper bound that prevents one truly stuck
	// upload from blocking the packer's flusher forever.
	uploadCtx, cancelUpload := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancelUpload()
	//
	// Snapshot the assembled body via bytes.NewReader so the size
	// log line below stays meaningful AFTER UploadDocument's
	// internal io.ReadAll consumes the reader. Using &buf directly
	// would still work (UploadDocument buffers internally for
	// retries), but the post-call log would show "0 bytes" because
	// the bytes.Buffer was drained.
	blobBytes := buf.Bytes()
	blobSize := len(blobBytes)
	msgID, err := uploadWithRetry(uploadCtx, p.uploader, blobBytes, name, "")
	if err != nil {
		metrics.UploadErrors.Inc()
		fmt.Fprintf(os.Stderr, "[packer] upload (%d chunks, %d bytes): %v\n", len(batch), blobSize, err)
		for _, r := range batch {
			r.result <- packResult{err: fmt.Errorf("packer upload: %w", err)}
			close(r.result)
		}
		return
	}
	metrics.ChunksUploaded.Add(int64(len(batch)))
	metrics.BytesUploaded.Add(int64(blobSize))
	// Write chunk_map rows in a single transaction so a partial
	// failure doesn't leave half the batch's writers waiting for a
	// row that never lands. PutChunk uses Background ctx so a
	// Close() racing with this flush doesn't strand a successful
	// upload.
	for i, r := range batch {
		c := meta.Chunk{
			Ino:         r.ino,
			Idx:         r.idx,
			TGMessageID: int64(msgID),
			Size:        r.plaintextSize,
			PackOffset:  offsets[i],
			PackSize:    int32(len(r.wire)),
		}
		if err := p.meta.PutChunk(context.Background(), c); err != nil {
			r.result <- packResult{err: fmt.Errorf("packer put_chunk: %w", err)}
			close(r.result)
			continue
		}
		// chunk_map row landed — safe to drop the on-disk staging
		// copy. Leaving it would re-upload on next startup (which
		// would orphan the just-landed message on the channel).
		if r.pendingPath != "" {
			_ = DeletePending(r.pendingPath)
		}
		r.result <- packResult{msgID: int64(msgID), packOff: offsets[i]}
		close(r.result)
	}
}
