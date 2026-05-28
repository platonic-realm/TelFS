package fs

import (
	"fmt"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// serverHandle wraps a *fuse.Server with TelFS's preferred teardown
// semantics: unmount severs the kernel link, Wait drains in-flight
// requests. Callers should ALWAYS call both in that order on the same
// handle before tearing down the chunk.Reader / tg.Session below it.
type serverHandle struct {
	srv        *fuse.Server
	mountpoint string
}

// Mountpoint returns the directory the server is bound to.
func (h *serverHandle) Mountpoint() string { return h.mountpoint }

// Unmount asks the kernel to release the mount. Returns nil if the
// mount was already gone.
func (h *serverHandle) Unmount() error {
	if h == nil || h.srv == nil {
		return nil
	}
	if err := h.srv.Unmount(); err != nil {
		return fmt.Errorf("fs: unmount %s: %w", h.mountpoint, err)
	}
	return nil
}

// Wait blocks until the FUSE server loop exits (which it does after
// Unmount). Wait returns no error — fuse.Server.Wait has no return.
func (h *serverHandle) Wait() {
	if h == nil || h.srv == nil {
		return
	}
	h.srv.Wait()
}
