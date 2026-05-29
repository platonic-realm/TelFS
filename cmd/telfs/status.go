package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"telfs/internal/config"
	"telfs/internal/crypto"
	"telfs/internal/meta"
	"telfs/internal/snapshot"
)

// cmdStatus prints a one-screen summary of the active profile,
// channel binding, encryption state, chunk size, on-disk footprint,
// last snapshot, and any TelFS FUSE mounts visible in /proc/mounts.
// Intended as the "what state am I in" command — purely local; never
// hits the network.
func cmdStatus(ctx context.Context, _ []string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	active := config.ActiveProfile()
	fmt.Println("== Profile ==")
	if active == "" {
		fmt.Println("  active: (none — using legacy path)")
	} else {
		fmt.Println("  active:", active)
	}
	fmt.Println("  data:  ", cfg.DataDir)

	fmt.Println("\n== Files ==")
	printFileStat(cfg.ConfigPath(), "config.toml")
	printFileStat(cfg.SessionPath(), "session.json")
	printFileStat(cfg.DBPath(), "db.sqlite")
	if size, count, err := dirStats(cfg.CachePath()); err == nil {
		fmt.Printf("  %-14s  %d file(s), %s\n", "cache/", count, humanBytes(size))
	} else if errors.Is(err, os.ErrNotExist) {
		fmt.Printf("  %-14s  (not yet populated)\n", "cache/")
	}

	fmt.Println("\n== Channel ==")
	if cfg.Channel.ID == 0 {
		fmt.Println("  (not set; run `telfs channel set <id>`)")
	} else {
		fmt.Printf("  title:   %s\n", cfg.Channel.Title)
		fmt.Printf("  id:      %d\n", cfg.Channel.ID)
		if cfg.Channel.Username != "" {
			fmt.Printf("  handle:  @%s\n", cfg.Channel.Username)
		}
	}
	fmt.Printf("  API ID:  %d\n", cfg.APIID)
	if cfg.DC != 0 {
		fmt.Printf("  DC:      %d\n", cfg.DC)
	}

	// Anything below this point requires opening the DB. If meta.Open
	// fails (no DB yet, etc.) we report what we have and bail
	// gracefully.
	metaStore, err := meta.Open(cfg.DBPath())
	if err != nil {
		fmt.Printf("\n(could not open metadata DB: %v)\n", err)
		printMountTable()
		return nil
	}
	defer metaStore.Close()

	fmt.Println("\n== Filesystem ==")
	if uuid, err := metaStore.FSUUID(ctx); err == nil {
		fmt.Printf("  fs_uuid:    %s\n", uuid)
	}
	if size, err := metaStore.ChunkSize(ctx); err == nil {
		fmt.Printf("  chunk size: %s (%d bytes)\n", humanBytes(size), size)
	}

	fmt.Println("\n== Encryption ==")
	mode, err := metaStore.GetKV(ctx, crypto.KVMode)
	encrypted := err == nil
	if errors.Is(err, meta.ErrNotFound) {
		fmt.Println("  disabled — chunk bytes are uploaded in the clear")
	} else if err != nil {
		fmt.Printf("  (error reading state: %v)\n", err)
	} else {
		fmt.Printf("  enabled (mode=%s)\n", string(mode))
		if argon, err := metaStore.GetKV(ctx, crypto.KVArgon); err == nil {
			fmt.Printf("  KDF params: %s\n", string(argon))
		}
	}

	fmt.Println("\n== Dedup ==")
	if encrypted {
		fmt.Println("  disabled — encrypted FSes upload every chunk (AAD is bound per-slot)")
	} else if blobs, err := metaStore.CountChunkBlobs(ctx); err == nil {
		fmt.Printf("  active — %d distinct content blob(s) indexed\n", blobs)
	} else {
		fmt.Printf("  (error reading chunk_blob: %v)\n", err)
	}

	fmt.Println("\n== Last Snapshot ==")
	if id, err := metaStore.GetKV(ctx, snapshot.KVCurrentMsgID); err == nil {
		fmt.Printf("  channel msg_id: %s\n", strings.TrimSpace(string(id)))
	} else {
		fmt.Println("  (none recorded yet)")
	}
	if seq, err := metaStore.LastJournalSeq(ctx); err == nil && seq > 0 {
		fmt.Printf("  journal high-water seq: %d\n", seq)
	}

	printMountTable()
	return nil
}

// printFileStat prints "name  size  mtime" for a single file, or a
// note if it's absent. Used in the Files section.
func printFileStat(path, label string) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		fmt.Printf("  %-14s  (missing)\n", label)
		return
	}
	if err != nil {
		fmt.Printf("  %-14s  (error: %v)\n", label, err)
		return
	}
	fmt.Printf("  %-14s  %s  mtime=%s\n", label, humanBytes(info.Size()), info.ModTime().Format("2006-01-02 15:04:05"))
}

// dirStats returns (totalBytes, fileCount) for everything under dir.
func dirStats(dir string) (int64, int, error) {
	var total int64
	var n int
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		n++
		return nil
	})
	return total, n, err
}

// printMountTable scans /proc/mounts for any fuse.telfs mounts and
// lists them. Doesn't try to correlate to specific profiles — that
// would require parsing /proc/<pid>/cmdline which is best-effort.
func printMountTable() {
	fmt.Println("\n== Active Mounts (FUSE) ==")
	f, err := os.Open("/proc/mounts")
	if err != nil {
		fmt.Println("  (/proc/mounts unavailable)")
		return
	}
	defer f.Close()
	scan := bufio.NewScanner(f)
	any := false
	for scan.Scan() {
		fields := strings.Fields(scan.Text())
		if len(fields) < 3 {
			continue
		}
		if fields[2] != "fuse.telfs" {
			continue
		}
		// fields[0]=source ("telfs"), fields[1]=mountpoint, [2]=fstype, [3]=opts
		fmt.Printf("  %s\n", fields[1])
		any = true
	}
	if !any {
		fmt.Println("  (no telfs mounts found)")
	}
}
