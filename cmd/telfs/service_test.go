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
