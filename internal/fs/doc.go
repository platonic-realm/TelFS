// Package fs implements the go-fuse Node tree that exposes a TelFS instance
// as a POSIX mount.
//
// It translates VFS calls into operations on the metadata store
// (internal/meta) and the chunk pipeline (internal/chunk). It owns no
// authoritative state of its own.
package fs
