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

// DefaultPackThreshold is the chunk-payload size below which the
// backend's Packer aggregates instead of uploading directly.
// 256 KiB is one-sixteenth of the standard 4 MiB chunk; below this
// the per-message RTT dominates the actual data cost (a 1 KiB file
// pays the same 300-700 ms RTT as a 4 MiB file when uploaded alone).
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
// wires before flushing what it has. Async close makes 200 ms
// invisible to the close() caller (close returned long before);
// fsync waits on the packer's per-chunk result, so its latency
// IS bounded by this. Pick small enough to not surprise fsync
// users, large enough for many cp processes to converge.
const DefaultPackWindow = 200 * time.Millisecond

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
	result        chan packResult
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

// Submit enqueues a wire for the next packed-blob upload and
// returns a chan that will deliver the (msgID, packOffset) once
// the upload lands. The caller MUST receive on the chan — otherwise
// the flusher's distribution goroutine leaks.
//
// Submit only blocks long enough to append; the flush window is in
// a separate goroutine. If the new wire would push the buffer past
// the target, Submit wakes the flusher immediately (a "trigger
// flush" condition complementing the window-elapsed condition).
func (p *Packer) Submit(ino int64, idx int32, plaintextSize int32, wire []byte) <-chan packResult {
	ch := make(chan packResult, 1)
	req := packReq{
		ino:           ino,
		idx:           idx,
		plaintextSize: plaintextSize,
		wire:          wire,
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

// flush takes the current pending batch and uploads it as one blob.
// Distributes per-chunk results so waiters unblock. No-op if the
// buffer is empty.
func (p *Packer) flush() {
	p.mu.Lock()
	if len(p.pending) == 0 {
		p.mu.Unlock()
		return
	}
	batch := p.pending
	p.pending = nil
	p.bufBytes = 0
	p.mu.Unlock()

	// Assemble blob. Per-chunk pack_offset is the running prefix sum.
	var buf bytes.Buffer
	offsets := make([]int64, len(batch))
	for i, r := range batch {
		offsets[i] = int64(buf.Len())
		buf.Write(r.wire)
	}

	name := fmt.Sprintf("pack-%d", time.Now().UnixNano())
	// Uploads use context.Background() — the buffered batch must
	// survive Close (final drain) to avoid the torn-snapshot
	// scenario where inode.size persisted but chunk_map didn't.
	// The 30s shutdown watchdog in cmdMount is the backstop.
	//
	// Snapshot the assembled body via bytes.NewReader so the size
	// log line below stays meaningful AFTER UploadDocument's
	// internal io.ReadAll consumes the reader. Using &buf directly
	// would still work (UploadDocument buffers internally for
	// retries), but the post-call log would show "0 bytes" because
	// the bytes.Buffer was drained.
	blobBytes := buf.Bytes()
	blobSize := len(blobBytes)
	msgID, err := p.uploader.UploadDocument(context.Background(), bytes.NewReader(blobBytes), name, "")
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
		r.result <- packResult{msgID: int64(msgID), packOff: offsets[i]}
		close(r.result)
	}
}
