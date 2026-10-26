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
	"time"

	"telfs/internal/config"
	"telfs/internal/crypto"
	"telfs/internal/meta"
	"telfs/internal/secret"
	"telfs/internal/snapshot"
	"telfs/internal/tg"
	"telfs/internal/wire"
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
	fmt.Println("  auth:  ", authSummary(cfg))

	fmt.Println("\n== Files ==")
	printFileStat(cfg.ConfigPath(), "config.toml")
	printFileStat(cfg.SessionPath(), "session.enc")
	if cfg.HasLegacySession() {
		printFileStat(cfg.LegacySessionPath(), "session.json (PLAINTEXT — run `telfs session encrypt`)")
	}
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

	// Anything below this point requires the DB. Never CREATE one:
	// meta.Open on a missing file mints a fresh fs_uuid, and the next
	// mount would then skip recovery and serve an empty filesystem.
	if _, err := os.Stat(cfg.DBPath()); err != nil {
		fmt.Println("\n(no local metadata DB — mount to recover it from the channel, or `telfs init` for a new filesystem)")
		printMountTable()
		return nil
	}
	metaStore, err := meta.Open(cfg.DBPath())
	if err != nil {
		fmt.Printf("\n(could not open metadata DB: %v)\n", err)
		printMountTable()
		return nil
	}
	defer metaStore.Close()

	fmt.Println("\n== Filesystem ==")
	format, _ := metaStore.Format(ctx)
	if uuid, err := metaStore.FSUUID(ctx); err == nil {
		fmt.Printf("  fs_uuid:    %s\n", uuid)
	}
	fmt.Printf("  format:     %d\n", format)
	if size, err := metaStore.ChunkSize(ctx); err == nil {
		fmt.Printf("  chunk size: %s (%d bytes)\n", humanBytes(size), size)
	}
	if format == 2 {
		statusFormat2(ctx, cfg, metaStore)
		printMountTable()
		return nil
	}

	fmt.Println("\n== Encryption ==")
	mode, err := metaStore.GetKV(ctx, crypto.KVMode)
	encrypted := err == nil
	if errors.Is(err, meta.ErrNotFound) {
		fmt.Println("  disabled — chunk bytes are uploaded in the clear")
		fmt.Println("  (this profile's [secret] protects only the Telegram session, not your files)")
	} else if err != nil {
		fmt.Printf("  (error reading state: %v)\n", err)
	} else {
		fmt.Printf("  enabled (mode=%s)\n", string(mode))
		if argon, err := metaStore.GetKV(ctx, crypto.KVArgon); err == nil {
			fmt.Printf("  KDF params: %s\n", string(argon))
		}
	}

	fmt.Println("\n== Dedup ==")
	dedupEligible := !encrypted || string(mode) == crypto.ModeAESGCMv3
	switch {
	case !dedupEligible:
		fmt.Println("  disabled — this format-1 encrypted FS uploads every chunk (AAD is bound per-slot)")
		fmt.Println("  a new filesystem (`telfs init`, format 2) always deduplicates")
	default:
		if blobs, err := metaStore.CountChunkBlobs(ctx); err == nil {
			label := "plaintext content"
			if encrypted {
				label = "convergent-encrypted content"
			}
			fmt.Printf("  active (%s) — %d distinct blob(s) indexed\n", label, blobs)
		} else {
			fmt.Printf("  (error reading chunk_blob: %v)\n", err)
		}
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

// statusFormat2 prints the format-2 key state: the local header, the
// pinned-header check (live when the secret can be read without a
// prompt, else the daemon's last result), and the key-use counts.
func statusFormat2(ctx context.Context, cfg *config.Config, m *meta.Store) {
	fmt.Println("\n== Encryption (format 2) ==")
	if h, _, err := keyHeaderOf(ctx, m); err == nil {
		fmt.Printf("  key header: Argon2id %d pass, %d MiB\n", h.T, h.M/1024)
	} else {
		fmt.Printf("  key header: %v\n", err)
	}
	if v, err := m.GetKV(ctx, meta.KVKeyMsgID); err == nil {
		fmt.Printf("  key message: %s\n", v)
	}
	if v, err := m.GetKV(ctx, meta.KVSecretSet); err == nil {
		fmt.Printf("  secret set: %s\n", v)
	}
	fmt.Printf("  secret source: %s\n", cfg.Secret.EffectiveSource())
	live := false
	if sec, err := secret.Read(ctx, cfg.Secret, secret.Request{Override: secretFrom, NoPrompt: true, Stderr: os.Stderr}); err == nil {
		defer sec.Zero()
		if client, err := newClientWith(cfg, sec); err == nil {
			if keys, counters, err := unlock2(ctx, m, sec); err == nil {
				_ = keys
				sec.Zero()
				err = client.RunSession(ctx, func(ctx context.Context, sess *tg.Session) error {
					kc := checkPinnedHeader(ctx, sess, m)
					storeKeyCheck(ctx, m, kc)
					fmt.Printf("  pinned header: %s (checked now)\n", kc.Detail)
					return nil
				})
				if err == nil {
					live = true
				} else {
					fmt.Printf("  pinned header: not checked (%v)\n", err)
				}
				printUses(counters)
			} else {
				fmt.Printf("  pinned header: not checked (%v)\n", err)
			}
		} else {
			fmt.Printf("  pinned header: not checked (%v)\n", err)
		}
	} else {
		fmt.Println("  pinned header: not checked (the secret is typed — `telfs fsck` checks it)")
	}
	if !live {
		if kc, ok := loadKeyCheck(ctx, m); ok {
			mark := "OK"
			if !kc.OK {
				mark = "PROBLEM"
			}
			fmt.Printf("  last daemon check (%s): %s — %s\n", time.Unix(kc.TS, 0).Format(time.RFC3339), mark, kc.Detail)
		}
		c := &wire.Counters{}
		loadUses(ctx, m, c)
		printUses(c)
	}
	if blobs, err := m.CountChunkBlobs(ctx); err == nil {
		fmt.Printf("\n== Dedup ==\n  active (keyed hash) — %d distinct chunk(s) indexed\n", blobs)
	}
	fmt.Println("\n== Last Snapshot ==")
	if id, err := m.GetKV(ctx, snapshot.KVCurrentMsgID); err == nil {
		fmt.Printf("  channel msg_id: %s\n", strings.TrimSpace(string(id)))
	} else {
		fmt.Println("  (none recorded yet)")
	}
}

// authSummary renders the profile's identity line: who session.json
// authenticates as.
func authSummary(cfg *config.Config) string {
	if !cfg.HasSession() {
		return "(not logged in — run `telfs login`)"
	}
	if cfg.Identity != nil {
		return cfg.Identity.Label()
	}
	// session.json without [identity]: either a login that never
	// completed (gotd writes the file at handshake time) or a legacy
	// pre-identity profile — a login or mount settles which.
	return "(unverified — complete `telfs login` or mount once)"
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
