package chunk

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"
)

// uploadWithRetry wraps Uploader.UploadDocument with bounded retries
// on transient errors. The hot path through writer.uploadOne and
// packer.flush used to issue a single UploadDocument call and surface
// any error to the caller — which for a write tar saw as EIO on the
// next write(), aborting the whole extract even though the chunk was
// safely restored to dirty for a later Flush that tar never called.
//
// What counts as transient: anything that isn't ctx cancellation. The
// list of reasons UploadDocument can fail under heavy load (FLOOD_WAIT
// past the middleware's cap, transient RPC timeouts, brief network
// blips, single MTProto packet loss) is large and not always
// programmatically distinguishable. Treating "everything except ctx
// canceled" as transient is the safe default: at worst we retry an
// upload that was about to succeed on the second try anyway. ctx
// cancellation IS distinguished because it means the daemon is
// shutting down and further retries are pointless.
//
// Each retried attempt creates a NEW Telegram message — duplicates
// are not a correctness issue for TelFS (only chunk_map rows are the
// source of truth; orphans become `telfs gc` fodder).
//
// Backoff: 500ms, 1s, 2s, 4s, 8s (capped). 5 attempts × growing
// backoff is up to ~15s of waiting on top of the per-attempt upload
// time. Practical worst case for a real 1 MiB chunk is on the order
// of a minute before final give-up; that's the point where surfacing
// the error to userspace is the right call.
// uploadRetryFirst is the first backoff (a variable so tests can shorten it).
var uploadRetryFirst = 500 * time.Millisecond

func uploadWithRetry(ctx context.Context, u Uploader, body []byte, name, caption string) (int, error) {
	const maxAttempts = 5
	backoff := uploadRetryFirst
	const backoffCap = 8 * time.Second

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		msgID, err := u.UploadDocument(ctx, bytes.NewReader(body), name, caption)
		if err == nil {
			if attempt > 1 {
				fmt.Fprintf(os.Stderr, "[chunk] upload %q succeeded on attempt %d/%d\n",
					name, attempt, maxAttempts)
			}
			return msgID, nil
		}
		lastErr = err
		// Distinguish daemon shutdown (don't retry) from in-flight error
		// (retry). ctx.Err() != nil means the parent ctx was canceled —
		// upload would never have completed.
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if attempt < maxAttempts {
			fmt.Fprintf(os.Stderr, "[chunk] upload %q attempt %d/%d failed (%v), retrying after %s\n",
				name, attempt, maxAttempts, err, backoff)
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > backoffCap {
				backoff = backoffCap
			}
		}
	}
	return 0, fmt.Errorf("upload %q failed after %d attempts: %w", name, maxAttempts, lastErr)
}
