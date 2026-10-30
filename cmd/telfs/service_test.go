package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gotd/td/tgerr"

	"telfs/internal/meta"
	"telfs/internal/tg"
)

// Only the network or Telegram being unreachable or slow is waited out at
// boot; refusals end the service with their exit code.
func TestRetriable(t *testing.T) {
	yes := []error{
		context.DeadlineExceeded,
		fmt.Errorf("verify: %w", tg.ErrNotConnected),
		&net.OpError{Op: "dial", Err: syscall.ECONNREFUSED},
		fmt.Errorf("x: %w", syscall.ENETUNREACH),
		&tgerr.Error{Code: 500, Type: "INTERNAL"},
	}
	no := []error{
		nil,
		context.Canceled,
		&tgerr.Error{Code: 400, Type: "CHANNEL_INVALID"},
		&tgerr.Error{Code: 401, Type: "AUTH_KEY_UNREGISTERED"},
		errors.New("wrong secret"),
		fmt.Errorf("x: %w", errIntegrity),
	}
	for _, e := range yes {
		if !retriable(e) {
			t.Errorf("%v: want retriable", e)
		}
	}
	for _, e := range no {
		if retriable(e) {
			t.Errorf("%v: want final", e)
		}
	}
}

func TestRetryAtBoot(t *testing.T) {
	ctx := context.Background()
	calls := 0
	flaky := func(context.Context) error {
		if calls++; calls < 2 {
			return context.DeadlineExceeded
		}
		return nil
	}
	// Outside service mode: one attempt, the error as it is.
	if err := newService(false).retryAtBoot(ctx, "x", flaky); !errors.Is(err, context.DeadlineExceeded) || calls != 1 {
		t.Fatalf("plain mount: err %v after %d call(s)", err, calls)
	}
	// A refusal ends at once even in service mode.
	refused := &tgerr.Error{Code: 400, Type: "CHANNEL_PRIVATE"}
	if err := newService(true).retryAtBoot(ctx, "x", func(context.Context) error { return refused }); err != refused {
		t.Fatalf("refusal: %v", err)
	}
	// Cancellation stops the waiting.
	cctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := newService(true).retryAtBoot(cctx, "x", func(context.Context) error { return context.DeadlineExceeded }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled wait: %v", err)
	}
}

// READY=1 once mounted, STATUS while waiting, STOPPING=1 on stop — and
// nothing at all without --service.
func TestServiceNotifications(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notify")
	ln, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	t.Setenv("NOTIFY_SOCKET", path)
	read := func() string {
		ln.SetReadDeadline(time.Now().Add(time.Second))
		buf := make([]byte, 256)
		n, _, err := ln.ReadFromUnix(buf)
		if err != nil {
			return ""
		}
		return string(buf[:n])
	}
	newService(false).markReady("/mnt") // not a service: silent
	if got := read(); got != "" {
		t.Fatalf("plain mount notified %q", got)
	}
	s := newService(true)
	s.status("connecting to Telegram")
	if got := read(); got != "STATUS=connecting to Telegram" {
		t.Fatalf("status %q", got)
	}
	s.markReady("/mnt/backup")
	if got := read(); !strings.HasPrefix(got, "READY=1\n") {
		t.Fatalf("ready %q", got)
	}
	s.stopping()
	if got := read(); !strings.HasPrefix(got, "STOPPING=1") {
		t.Fatalf("stopping %q", got)
	}
}

func TestOwnerChecks(t *testing.T) {
	if !ownerChecks(true, false) || ownerChecks(true, true) || ownerChecks(false, false) {
		t.Fatal("default_permissions: on with --allow-other unless opted out, off without --allow-other")
	}
}

type fakeVerify struct{ errs []error }

func (f *fakeVerify) VerifyAccess(context.Context) error {
	if len(f.errs) == 0 {
		return nil
	}
	err := f.errs[0]
	f.errs = f.errs[1:]
	return err
}

// A mount that came up without waiting for Telegram (v1.1 I2) checks the
// channel in the background: it waits out an unreachable network, marks the
// DB checked once through, and returns a definite refusal.
func TestBackgroundPreflight(t *testing.T) {
	old := bgPreflightRetry
	bgPreflightRetry = time.Millisecond
	defer func() { bgPreflightRetry = old }()
	ctx := context.Background()
	m, err := meta.Open(filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if channelCheckedBefore(ctx, m) {
		t.Fatal("a new DB is not checked")
	}
	connected := make(chan error, 1)
	connected <- nil
	if err := backgroundPreflight(ctx, connected, &fakeVerify{errs: []error{tg.ErrNotConnected, context.DeadlineExceeded}}, m); err != nil {
		t.Fatalf("retriable errors are waited out: %v", err)
	}
	if !channelCheckedBefore(ctx, m) {
		t.Fatal("a passed check marks the DB")
	}
	connected <- nil
	refused := &tgerr.Error{Code: 400, Message: "CHANNEL_INVALID", Type: "CHANNEL_INVALID"}
	if err := backgroundPreflight(ctx, connected, &fakeVerify{errs: []error{refused}}, m); !errors.Is(err, refused) {
		t.Fatalf("a refusal is returned: %v", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := backgroundPreflight(cctx, make(chan error), &fakeVerify{}, m); err != nil {
		t.Fatalf("a stopping daemon is no refusal: %v", err)
	}
}

// The link comes back = the first success after it was down: down after
// an error, or after linkDownAfter of silence with work pending.
func TestLinkStateObserve(t *testing.T) {
	t0 := time.Unix(1_800_000_000, 0)
	var l linkState
	if l.observe(t0, time.Time{}, true, t0) {
		t.Fatal("the first success is no recovery")
	}
	if l.observe(t0, t0.Add(time.Second), true, t0.Add(2*time.Second)) || !l.down {
		t.Fatal("an error after the last success: down")
	}
	if !l.observe(t0.Add(5*time.Second), t0.Add(time.Second), true, t0.Add(5*time.Second)) || l.down {
		t.Fatal("a success after an error: back")
	}
	// Hanging calls record no error: silence with work pending.
	ok := t0.Add(5 * time.Second)
	if l.observe(ok, time.Time{}, true, ok.Add(10*time.Second)) || l.down {
		t.Fatal("10 s of silence is not down yet")
	}
	if l.observe(ok, time.Time{}, true, ok.Add(linkDownAfter)) || !l.down {
		t.Fatal("silence with work pending for linkDownAfter: down")
	}
	if !l.observe(ok.Add(time.Minute), time.Time{}, true, ok.Add(time.Minute)) {
		t.Fatal("the next success: back")
	}
	// Idle silence is not an outage.
	var idle linkState
	idle.observe(t0, time.Time{}, false, t0)
	if idle.observe(t0, time.Time{}, false, t0.Add(time.Hour)) || idle.down {
		t.Fatal("an idle link is not down")
	}
}
