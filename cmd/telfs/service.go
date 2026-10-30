package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/gotd/td/tgerr"

	"telfs/internal/sdnotify"
	"telfs/internal/tg"
)

// noPrompt is set by `telfs mount --service`: a service never prompts, so a
// prompt-source profile fails at once (exit 2) instead of hanging.
var noPrompt bool

// service is `telfs mount --service` (spec v1 §3): systemd Type=notify,
// waiting out an unreachable network or Telegram at boot, never prompting.
type service struct {
	on    bool
	ready chan struct{}
	once  sync.Once
}

func newService(on bool) *service { return &service{on: on, ready: make(chan struct{})} }

// status tells systemd what the service is doing (systemctl status shows it).
func (s *service) status(msg string) {
	if s.on {
		_ = sdnotify.Notify("STATUS=" + msg)
	}
}

// markReady: the filesystem is mounted and its control socket serves.
func (s *service) markReady(mountpoint string) {
	if !s.on {
		return
	}
	s.once.Do(func() {
		close(s.ready)
		_ = sdnotify.Notify("READY=1\nSTATUS=mounted at " + mountpoint)
	})
}

func (s *service) stopping() {
	if s.on {
		_ = sdnotify.Notify("STOPPING=1\nSTATUS=stopping: draining uploads, posting the journal")
	}
}

// keepStarting extends systemd's start timeout every 30 s until READY=1,
// so a boot without network waits for it instead of being killed after
// TimeoutStartSec.
func (s *service) keepStarting(ctx context.Context) {
	if !s.on {
		return
	}
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		_ = sdnotify.Notify("EXTEND_TIMEOUT_USEC=90000000")
		select {
		case <-s.ready:
			return
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// retriable reports a failure worth waiting out at boot — the network or
// Telegram unreachable or slow — and never a refusal (wrong secret, channel
// gone, session revoked, integrity).
func retriable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if rpc, ok := tgerr.As(err); ok {
		return rpc.Code >= 500 || rpc.Code == -503
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, tg.ErrNotConnected) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	for _, e := range []syscall.Errno{syscall.ECONNREFUSED, syscall.ECONNRESET, syscall.ENETUNREACH,
		syscall.EHOSTUNREACH, syscall.ETIMEDOUT, syscall.ENETDOWN, syscall.EPIPE} {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}

// retryAtBoot runs step until it succeeds, fails for good, or ctx ends.
// Outside service mode it runs step once. While waiting it says why (stderr
// + STATUS) and backs off 5 s → 5 min.
func (s *service) retryAtBoot(ctx context.Context, what string, step func(context.Context) error) error {
	delay := 5 * time.Second
	for {
		err := step(ctx)
		if err == nil || !s.on || !retriable(err) || ctx.Err() != nil {
			return err
		}
		msg := fmt.Sprintf("waiting for Telegram (%s): %v — retrying in %s", what, err, delay)
		fmt.Fprintln(os.Stderr, msg)
		s.status(msg)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		if delay *= 2; delay > 5*time.Minute {
			delay = 5 * time.Minute
		}
	}
}

// ownerChecks says whether a mount passes default_permissions (v1.1 B2):
// with --allow-other the kernel checks every access against the stored
// owners and modes unless --no-default-permissions says otherwise. Without
// --allow-other only the mounting user reaches the mount; no check needed.
func ownerChecks(allowOther, optOut bool) bool { return allowOther && !optOut }
