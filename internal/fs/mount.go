package fs

import (
	"fmt"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"telfs/internal/chunk"
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
	// MaxWrite caps the size of a single WRITE (and READ — go-fuse sets
	// max_read equal to MaxWrite) request. The go-fuse default is
	// 128 KiB; the kernel supports 1 MiB. With writeback_cache on,
	// dirty pages flush in MaxWrite-sized upcalls, so this is an 8×
	// cut in write round trips. It also raises the ceiling on
	// MaxReadAhead below, which the kernel clamps to MaxWrite.
	fuseOpts.MountOptions.MaxWrite = fuse.MAX_KERNEL_WRITE
	// MaxReadAhead controls how much the kernel reads ahead on top of
	// the actual read() syscall. The default (128 KiB on Linux) is
	// tuned for local-filesystem workloads where readahead is cheap;
	// for TelFS each kernel-side read becomes a FUSE round trip into
	// Node.Read → Reader.ReadAt, where we coalesce against chunk
	// boundaries. We ask for one full chunk; the effective value is
	// min(MaxReadAhead, MaxWrite) = 1 MiB today, which still turns
	// the 128 KiB-per-call default into 1 MiB reads (4 round trips
	// per 4 MiB chunk instead of 32) and keeps the chunk pipeline's
	// prefetcher working AHEAD of the next chunk instead of mid-chunk.
	// Keep requesting ChunkSize so readahead tracks any future
	// MaxWrite/kernel-limit increase automatically.
	fuseOpts.MountOptions.MaxReadAhead = int(chunk.ChunkSize)
	// Kernel-side metadata caching. go-fuse leaves these nil, which
	// means ZERO — without them every path-component resolution is a
	// LOOKUP upcall, every stat() a GETATTR, and failed lookups are
	// never remembered, so metadata-heavy workloads (compiles: make
	// stat-ing every prerequisite, cc probing each include dir per
	// #include — mostly ENOENT) hammer the daemon + SQLite with
	// millions of avoidable round trips. libfuse/NFS default to 1 s
	// under multi-writer assumptions TelFS doesn't have: the local DB
	// is authoritative and mutated only through this mount, so the
	// kernel's cached view is self-coherent. The one out-of-band
	// mutator, the trash TTL GC, only touches aged /.trash entries —
	// staleness there is bounded by this timeout and harmless.
	cacheTimeout := 5 * time.Second
	fuseOpts.EntryTimeout = &cacheTimeout
	fuseOpts.AttrTimeout = &cacheTimeout
	fuseOpts.NegativeTimeout = &cacheTimeout
	// MaxBackground caps how many concurrent FUSE requests the kernel
	// dispatches before queuing further ones. The default (12) is
	// tight enough that a multi-threaded reader can stall behind
	// background prefetches; raising it lets the prefetcher and
	// foreground reads coexist freely.
	fuseOpts.MountOptions.MaxBackground = 64

	srv, err := gofuse.Mount(opts.MountPoint, root, fuseOpts)
	if err != nil {
		return nil, fmt.Errorf("fs: mount %s: %w", opts.MountPoint, err)
	}
	return &serverHandle{srv: srv, mountpoint: opts.MountPoint}, nil
}
