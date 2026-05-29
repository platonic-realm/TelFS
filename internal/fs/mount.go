package fs

import (
	"fmt"

	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// MountOptions configures a TelFS mount.
type MountOptions struct {
	MountPoint string
	AllowOther bool
	Debug      bool
}

// Mount creates and starts a FUSE server backed by b at opts.MountPoint.
// It returns the server so the caller can drive teardown:
//
//	server := Mount(...)
//	<-ctx.Done()
//	server.Unmount() // disconnect the kernel-side mount
//	server.Wait()    // wait for all in-flight requests to drain
//
// That order is critical for the daemon lifecycle: unmount severs the
// kernel-to-userspace channel, then Wait blocks until all goroutines
// servicing pending requests have returned. Only after Wait is it safe
// to tear down the *Session that owns the underlying gotd client.
func Mount(opts MountOptions, b *Backend) (*serverHandle, error) {
	root := NewRoot(b)
	fuseOpts := &gofuse.Options{}
	fuseOpts.MountOptions.Debug = opts.Debug
	fuseOpts.MountOptions.AllowOther = opts.AllowOther
	fuseOpts.MountOptions.Name = "telfs"
	fuseOpts.MountOptions.FsName = "telfs"
	// CAP_WRITEBACK_CACHE lets the kernel coalesce small writes into the
	// MaxWrite-sized requests we already handle efficiently. For workloads
	// that stream many small write()s into a single open file (logs,
	// SQLite WALs, dd bs=4k), this collapses thousands of FUSE round trips
	// into a handful. AND-masked against kernel support in opcode.go's
	// doInit, so falls through cleanly on kernels that don't support it.
	// Our Setattr already accepts mtime updates, which is the one mode
	// change writeback_cache introduces on the upcall side.
	fuseOpts.MountOptions.ExtraCapabilities = fuse.CAP_WRITEBACK_CACHE

	srv, err := gofuse.Mount(opts.MountPoint, root, fuseOpts)
	if err != nil {
		return nil, fmt.Errorf("fs: mount %s: %w", opts.MountPoint, err)
	}
	return &serverHandle{srv: srv, mountpoint: opts.MountPoint}, nil
}
