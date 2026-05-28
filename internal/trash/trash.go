// Package trash implements TelFS's `.trash/` safety-net.
//
// When trash is enabled (meta_kv["trash_enabled"]="1"), every
// kernel-issued unlink/rmdir from FUSE is rerouted to a top-level
// `.trash/` directory instead of really deleting. A TTL GC then
// actually unlinks anything that's been parked longer than
// meta_kv["trash_ttl_secs"] seconds (default 7 days). The dot prefix
// hides the directory from default `ls`, while standard tools (`mv`)
// can still restore items by moving them back out.
package trash

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"telfs/internal/meta"
)

// DirName is the name of the top-level safety-net directory. Hidden
// by convention (dot prefix), so default `ls` doesn't show it but
// `ls -a` and the web file browser do.
const DirName = ".trash"

// DefaultGCInterval is how often the background loop scans .trash/
// for expired entries. Independent of TTL — a small interval just
// makes deletion fairly prompt after expiry.
const DefaultGCInterval = 5 * time.Minute

// EnsureRootDir returns the inode number of /.trash, creating it at
// root if missing. The directory is owned by the mounting user (taken
// from the OS) with mode 0700 — only the FS owner can see its
// contents.
//
// Safe to call repeatedly; concurrent first-time creates will race on
// the underlying CreateChild and the loser will fall through to the
// lookup.
func EnsureRootDir(ctx context.Context, mstore *meta.Store) (int64, error) {
	if in, err := mstore.Lookup(ctx, meta.RootIno, DirName); err == nil {
		if in.Kind != meta.KindDir {
			return 0, fmt.Errorf("/.trash exists but is not a directory (kind=%s)", in.Kind)
		}
		return in.Ino, nil
	} else if !errors.Is(err, meta.ErrNotFound) {
		return 0, err
	}
	now := time.Now().Unix()
	dir := meta.Inode{
		Kind:  meta.KindDir,
		Mode:  0o40700, // dir | rwx for owner only — trash is private by default
		UID:   uint32(os.Getuid()),
		GID:   uint32(os.Getgid()),
		Nlink: 1,
		Mtime: now,
		Ctime: now,
	}
	ino, err := mstore.CreateChild(ctx, meta.RootIno, DirName, dir)
	if err != nil {
		// A concurrent caller may have won the race; fall back to lookup.
		if errors.Is(err, meta.ErrExists) {
			in, err2 := mstore.Lookup(ctx, meta.RootIno, DirName)
			if err2 == nil {
				return in.Ino, nil
			}
		}
		return 0, fmt.Errorf("create /.trash: %w", err)
	}
	return ino, nil
}

// uniqueName returns a unique name for the given original by
// prefixing with the current unix-nanosecond timestamp. Two unlinks
// at the same nanosecond are vanishingly unlikely on real workloads;
// the trailing original name preserves recognizability.
//
// Format: <unix-nano>-<original>
func uniqueName(now time.Time, original string) string {
	return fmt.Sprintf("%d-%s", now.UnixNano(), original)
}

// MoveToTrash relocates the dirent (parentIno, name) under /.trash
// with a uniquified name. The inode itself is not touched — only the
// dirent is moved — so file content and chunks are untouched and
// restorable via plain `mv`.
//
// Caller is responsible for not invoking this when parentIno == trashIno
// (the intercept layer in internal/fs handles that check).
func MoveToTrash(ctx context.Context, mstore *meta.Store, trashIno, parentIno int64, name string) error {
	return mstore.Rename(ctx, parentIno, name, trashIno, uniqueName(time.Now(), name))
}

// GCOnce walks /.trash and unlinks entries whose mtime is more than
// `ttl` in the past. Returns the count of entries removed.
//
// Behaviour for entry kinds:
//   - regular files: Meta.Unlink decrements nlink; chunks delete
//     inline when nlink hits 0 (the usual path).
//   - symlinks: same — Meta.Unlink handles them.
//   - directories: only unlinked if they're empty. Non-empty trashed
//     dirs are left in place; the warning is logged. The intercept
//     only ever moves *empty* directories into trash (rmdir requires
//     empty), so non-empty entries are user-driven (someone `mv`'d a
//     real subtree into .trash) — we don't recursively delete those.
func GCOnce(ctx context.Context, mstore *meta.Store, trashIno int64, ttl time.Duration) (int, error) {
	entries, err := mstore.Readdir(ctx, trashIno)
	if err != nil {
		return 0, fmt.Errorf("readdir /.trash: %w", err)
	}
	cutoff := time.Now().Add(-ttl).Unix()
	removed := 0
	for _, ent := range entries {
		in, err := mstore.GetInode(ctx, ent.ChildIno)
		if err != nil {
			continue
		}
		if in.Mtime > cutoff {
			continue
		}
		if in.Kind == meta.KindDir {
			children, err := mstore.Readdir(ctx, in.Ino)
			if err != nil || len(children) > 0 {
				continue // non-empty user-trashed dir; leave it
			}
		}
		if err := mstore.Unlink(ctx, trashIno, ent.Name); err == nil {
			removed++
		}
	}
	return removed, nil
}

// Run loops GCOnce until ctx is cancelled. Intended to be started as a
// goroutine from cmdMount when trash is enabled.
//
// The mstore handle stays valid for the lifetime of the mount; ctx
// cancellation is the normal shutdown signal.
func Run(ctx context.Context, mstore *meta.Store, trashIno int64, ttlFn func() time.Duration, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultGCInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	// Run once immediately so a long-stale trash starts pruning at
	// mount rather than waiting `interval`.
	if _, err := GCOnce(ctx, mstore, trashIno, ttlFn()); err != nil {
		// Best-effort — surface but don't crash.
		fmt.Fprintf(os.Stderr, "trash gc: %v\n", err)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := GCOnce(ctx, mstore, trashIno, ttlFn()); err != nil {
				fmt.Fprintf(os.Stderr, "trash gc: %v\n", err)
			}
		}
	}
}
