package tg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestIsRetriableUploadErr(t *testing.T) {
	bg := context.Background()
	canceled, cancel := context.WithCancel(bg)
	cancel()

	cases := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{"caller-canceled-context", canceled, errors.New("engine forcibly closed: context canceled"), false},
		{"engine-closed-fresh-ctx", bg, errors.New("send media: upload bytes: send upload part 4 RPC: engine forcibly closed: context canceled"), true},
		{"connection-reset", bg, errors.New("read tcp ...: connection reset by peer"), true},
		{"io-timeout", bg, errors.New("dial tcp 149.154.167.50:443: i/o timeout"), true},
		{"broken-pipe", bg, errors.New("write tcp ...: broken pipe"), true},
		{"unexpected-eof", bg, errors.New("read tcp ...: unexpected EOF"), true},
		{"flood-wait-not-retried-here", bg, errors.New("FLOOD_WAIT_30 (handled upstream)"), false},
		{"plain-eof-not-retried", bg, errors.New("EOF"), false},
		{"bad-channel", bg, errors.New("CHANNEL_INVALID"), false},
		{"nil-error-not-meaningful", bg, errors.New(""), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := isRetriableUploadErr(c.ctx, c.err)
			if got != c.want {
				t.Errorf("got %v, want %v for err=%q", got, c.want, c.err.Error())
			}
		})
	}
}

// TestRetryDelaysDontExplode checks that the exponential backoff is
// bounded by uploadMaxAttempts. With max=4 the total wait is ~7s
// (1+2+4); this fixture pins it so future tuning is intentional.
func TestRetryDelaysAreBounded(t *testing.T) {
	if uploadMaxAttempts < 2 || uploadMaxAttempts > 8 {
		t.Errorf("uploadMaxAttempts=%d outside the sane [2,8] range; retune intentionally", uploadMaxAttempts)
	}
	// Compute total worst-case wait time: 1, 2, 4, ... seconds.
	var totalSec int
	for attempt := 2; attempt <= uploadMaxAttempts; attempt++ {
		totalSec += 1 << (attempt - 2)
	}
	if totalSec > 30 {
		t.Errorf("retry total wait %ds exceeds 30s budget — would mask honest network outages", totalSec)
	}
}

// helpfulErrText exists only to keep the error builders out of the
// table above readable. (Sample real-world gotd error spelled exactly
// as it landed in our daemon log.)
func helpfulErrText() string {
	return strings.Join([]string{
		"upload chunk 13",
		"upload bytes",
		"upload part",
		"send upload part 8 RPC",
		"rpcDoRequest",
		"retryUntilAck",
		"engine forcibly closed",
		"context canceled",
	}, ": ")
}

var _ = fmt.Sprintf // keep fmt in the file for future ad-hoc test additions
var _ = helpfulErrText
