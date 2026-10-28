package tg

import (
	"context"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"golang.org/x/time/rate"
)

// uploadPartMax is the largest upload part Telegram accepts (and the one
// TelFS sends): 512 KiB. It is the byte limiter's burst, so one part
// never has to be split and a short pause costs at most one part's worth
// of catching up.
const uploadPartMax = 512 << 10

// ByteLimiter caps the bytes per second of upload parts
// (upload.saveFilePart / upload.saveBigFilePart) — a token bucket in
// bytes, not in RPCs ([upload] max_rate, spec v1 row 2). Every connection
// shares it: the main connection's chain and the upload pool's chain hold
// the same limiter, so the cap is for the whole process. Everything that
// is not an upload part passes untouched. The RPC-per-minute RateLimiter
// (adaptive, FLOOD_WAIT-driven) stays as it is.
//
// A nil *ByteLimiter, or one made with 0, does not limit.
type ByteLimiter struct {
	bps int64
	lim *rate.Limiter
}

// NewByteLimiter returns a limiter of bytesPerSec bytes per second; 0 (or
// less) = no cap.
func NewByteLimiter(bytesPerSec int64) *ByteLimiter {
	if bytesPerSec <= 0 {
		return &ByteLimiter{}
	}
	return &ByteLimiter{bps: bytesPerSec, lim: rate.NewLimiter(rate.Limit(bytesPerSec), uploadPartMax)}
}

// Rate is the cap in bytes per second; 0 = no cap.
func (b *ByteLimiter) Rate() int64 {
	if b == nil {
		return 0
	}
	return b.bps
}

// WaitN blocks until n bytes may be sent, or ctx ends. Safe for
// concurrent callers: each one reserves its share of the bucket in turn,
// so together they never exceed the rate (beyond the one-part burst).
func (b *ByteLimiter) WaitN(ctx context.Context, n int) error {
	if b == nil || b.lim == nil {
		return nil
	}
	for n > 0 {
		step := n
		if step > uploadPartMax {
			step = uploadPartMax // rate.Limiter refuses more than its burst at once
		}
		if err := b.lim.WaitN(ctx, step); err != nil {
			return err
		}
		n -= step
	}
	return nil
}

// Handle implements telegram.Middleware: upload parts wait for their bytes,
// everything else passes.
func (b *ByteLimiter) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		if n := uploadPartBytes(input); n > 0 {
			if err := b.WaitN(ctx, n); err != nil {
				return err
			}
		}
		return next.Invoke(ctx, input, output)
	}
}

// uploadPartBytes is the payload of an upload-part RPC, 0 for any other.
func uploadPartBytes(input bin.Encoder) int {
	switch r := input.(type) {
	case *tg.UploadSaveBigFilePartRequest:
		return len(r.Bytes)
	case *tg.UploadSaveFilePartRequest:
		return len(r.Bytes)
	}
	return 0
}
