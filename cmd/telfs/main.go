// Command telfs mounts a Telegram channel as a FUSE filesystem.
//
// Subcommands:
//
//	telfs login                       interactive MTProto auth
//	telfs channel list                list channels you can post to
//	telfs channel set <id>            pick the backing channel
//	telfs channel info                show the configured channel
//	telfs channel ping                smoke-test post + read-back round trip
//	telfs mount <mountpoint>          mount the filesystem (read-only in M3)
//	telfs debug seed-file <name> <n>  create a test file of n bytes for read-path verification
//
// See docs/architecture.md for the design.
package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"telfs/internal/chunk"
	"telfs/internal/config"
	telfsfs "telfs/internal/fs"
	"telfs/internal/meta"
	"telfs/internal/snapshot"
	"telfs/internal/tg"
)

const usage = `telfs — FUSE filesystem backed by a Telegram channel

Usage:
  telfs login                       Authenticate against Telegram (MTProto).
  telfs channel list                List your channels (id, title, post permission).
  telfs channel set <id>            Configure the backing channel.
  telfs channel info                Show the currently configured channel.
  telfs channel ping                Round-trip a test message (smoke test).
  telfs mount <mountpoint>          Mount the filesystem (read-only in M3).
  telfs debug seed-file <name> <n>  Upload a deterministic n-byte test file.

Environment:
  TELFS_DIR        Data directory (default: ./.telfs)
  TELFS_API_ID     Telegram API ID (https://my.telegram.org/apps)
  TELFS_API_HASH   Telegram API hash
  TELFS_PHONE      Phone number for login (prompted if unset)
  TELFS_DC         Starting datacenter override (default: gotd's DC 2)
`

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "telfs:", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	switch os.Args[1] {
	case "-h", "--help", "help":
		fmt.Print(usage)
		return nil
	case "login":
		return cmdLogin(ctx)
	case "channel":
		return cmdChannel(ctx, os.Args[2:])
	case "mount":
		if len(os.Args) < 3 {
			return errors.New("mount: missing mountpoint")
		}
		return cmdMount(ctx, os.Args[2])
	case "debug":
		return cmdDebug(ctx, os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n%s", os.Args[1], usage)
		os.Exit(2)
		return nil
	}
}

func cmdLogin(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	client, err := tg.New(cfg)
	if err != nil {
		return err
	}
	return client.Login(ctx)
}

func cmdChannel(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("channel: missing subcommand (list, set, info, ping)")
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	switch args[0] {
	case "list":
		return cmdChannelList(ctx, cfg)
	case "set":
		if len(args) < 2 {
			return errors.New("channel set: missing channel id")
		}
		id, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return fmt.Errorf("channel set: invalid id %q: %w", args[1], err)
		}
		return cmdChannelSet(ctx, cfg, id)
	case "info":
		return cmdChannelInfo(cfg)
	case "ping":
		return cmdChannelPing(ctx, cfg)
	default:
		return fmt.Errorf("channel: unknown subcommand %q", args[0])
	}
}

func cmdChannelList(ctx context.Context, cfg *config.Config) error {
	client, err := tg.New(cfg)
	if err != nil {
		return err
	}
	channels, err := client.ListChannels(ctx)
	if err != nil {
		return err
	}
	if len(channels) == 0 {
		fmt.Println("No channels found. Create a private channel in Telegram, then re-run this command.")
		return nil
	}
	fmt.Printf("%-15s  %-4s  %-4s  %s\n", "ID", "POST", "OWN", "TITLE")
	for _, ch := range channels {
		post := "no"
		if ch.CanPost {
			post = "yes"
		}
		own := "no"
		if ch.IsCreator {
			own = "yes"
		}
		title := ch.Title
		if ch.Username != "" {
			title += "  (@" + ch.Username + ")"
		}
		fmt.Printf("%-15d  %-4s  %-4s  %s\n", ch.ID, post, own, title)
	}
	return nil
}

func cmdChannelSet(ctx context.Context, cfg *config.Config, id int64) error {
	client, err := tg.New(cfg)
	if err != nil {
		return err
	}
	info, err := client.SetChannel(ctx, id)
	if err != nil {
		return err
	}
	fmt.Printf("Channel set: %s (id=%d) — saved to %s\n", info.Title, info.ID, cfg.ConfigPath())
	return nil
}

func cmdChannelInfo(cfg *config.Config) error {
	if err := cfg.RequireChannel(); err != nil {
		return err
	}
	fmt.Printf("ID:       %d\n", cfg.Channel.ID)
	fmt.Printf("Title:    %s\n", cfg.Channel.Title)
	if cfg.Channel.Username != "" {
		fmt.Printf("Username: @%s\n", cfg.Channel.Username)
	}
	fmt.Printf("Config:   %s\n", cfg.ConfigPath())
	return nil
}

func cmdChannelPing(ctx context.Context, cfg *config.Config) error {
	client, err := tg.New(cfg)
	if err != nil {
		return err
	}
	if err := cfg.RequireChannel(); err != nil {
		return err
	}
	text := fmt.Sprintf("telfs ping @%d", time.Now().Unix())
	fmt.Printf("→ Posting: %q\n", text)
	id, err := client.PostMessage(ctx, text)
	if err != nil {
		return fmt.Errorf("post: %w", err)
	}
	fmt.Printf("← Posted as message id=%d\n", id)

	got, err := client.GetMessageText(ctx, id)
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	fmt.Printf("← Read back: %q\n", got)
	if got != text {
		return fmt.Errorf("round-trip mismatch: sent %q, got %q", text, got)
	}
	fmt.Println("OK — Telegram round trip works.")
	return nil
}

// cmdMount mounts the filesystem and blocks until ctx is canceled.
//
// Teardown order (advisor-mandated, tested via ^C):
//  1. signal arrives → ctx canceled
//  2. server.Unmount() — kernel-side mount link severed
//  3. server.Wait() — waits for all in-flight FUSE requests to drain
//  4. RunSession callback returns — gotd client tears down its session
//  5. main exits
//
// If we get this order wrong, an in-flight Read could call into a
// torn-down session and panic.
func cmdMount(signalCtx context.Context, mountpoint string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if err := cfg.RequireChannel(); err != nil {
		return err
	}

	// Cold-mount recovery: if the local DB is missing, try to pull the
	// latest snapshot from the backing channel before opening meta.
	// If there's no snapshot in the channel either, we'll proceed with
	// a fresh empty DB (first-ever mount).
	if _, err := os.Stat(cfg.DBPath()); errors.Is(err, os.ErrNotExist) {
		if err := tryRecover(signalCtx, cfg); err != nil {
			return fmt.Errorf("recovery: %w", err)
		}
	}

	metaStore, err := meta.Open(cfg.DBPath())
	if err != nil {
		return err
	}
	defer metaStore.Close()

	client, err := tg.New(cfg)
	if err != nil {
		return err
	}

	// The gotd session must stay alive past signalCtx cancellation so
	// the final snapshot can post. We give RunSession its own
	// Background-derived context and let our callback drive shutdown
	// in the correct order (final snapshot → unmount FUSE → return →
	// gotd shuts down).
	err = client.RunSession(context.Background(), func(sessCtx context.Context, sess *tg.Session) error {
		fetcher := &chunk.TGFetcher{Session: sess}
		cache, err := chunk.NewCache(cfg.CachePath(), chunk.DefaultCacheCapBytes, fetcher)
		if err != nil {
			return err
		}
		reader := chunk.NewReader(metaStore, cache, chunk.ChunkSize)

		server, err := telfsfs.Mount(telfsfs.MountOptions{MountPoint: mountpoint}, &telfsfs.Backend{
			Meta:      metaStore,
			Reader:    reader,
			Cache:     cache,
			Uploader:  sess,
			ChunkSize: chunk.ChunkSize,
		})
		if err != nil {
			return err
		}

		// Snapshot cadence: takes one snapshot immediately (so a
		// freshly-recovered DB is re-baselined) then every interval.
		// We pass sessCtx — the goroutine exits when the gotd session
		// tears down. The user-stop signal stops it via snapCtx below.
		snapMgr := &snapshot.Manager{Meta: metaStore, Session: sess}
		snapCtx, stopSnap := context.WithCancel(sessCtx)
		snapDone := make(chan struct{})
		go func() {
			_ = snapMgr.Run(snapCtx)
			close(snapDone)
		}()

		fmt.Printf("Mounted at %s. Press ^C to unmount.\n", mountpoint)
		<-signalCtx.Done()

		// User asked to stop. Run the final snapshot synchronously
		// BEFORE we ask gotd to shut down — uploads after callback
		// return fail with "engine closed".
		stopSnap()
		<-snapDone
		fmt.Println("Taking final snapshot…")
		finalCtx, finalCancel := context.WithTimeout(context.Background(), 60*time.Second)
		if err := snapMgr.Once(finalCtx); err != nil {
			fmt.Fprintln(os.Stderr, "[snapshot] final failed:", err)
		}
		finalCancel()

		fmt.Println("Unmounting…")
		if uerr := server.Unmount(); uerr != nil {
			fmt.Fprintln(os.Stderr, "unmount:", uerr)
		}
		server.Wait()
		return nil
	})
	// Treat clean cancellation as success — the user asked for unmount.
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// tryRecover opens a transient MTProto session, scans the channel for
// the most recent snapshot message, and restores it to cfg.DBPath().
// If no snapshot exists in the channel, returns nil (fresh DB will be
// created by the subsequent meta.Open).
func tryRecover(ctx context.Context, cfg *config.Config) error {
	client, err := tg.New(cfg)
	if err != nil {
		return err
	}
	return client.RunSession(ctx, func(ctx context.Context, sess *tg.Session) error {
		fmt.Println("Local DB missing — scanning channel for snapshot…")
		latest, err := sess.FindLatestSnapshot(ctx, "", 50)
		if err != nil {
			return err
		}
		if latest == nil {
			fmt.Println("No snapshot in channel; starting with an empty DB.")
			return nil
		}
		fmt.Printf("Found snapshot msg=%d (ts %s, fs_uuid=%s)\n",
			latest.MessageID,
			time.Unix(latest.Caption.TSUnix, 0).UTC().Format(time.RFC3339),
			latest.Caption.FSUUID)
		data, err := sess.DownloadSnapshot(ctx, latest.MessageID)
		if err != nil {
			return err
		}
		if err := snapshot.Restore(ctx, data, cfg.DBPath()); err != nil {
			return err
		}
		fmt.Printf("Restored %d gzipped bytes → %s\n", len(data), cfg.DBPath())
		return nil
	})
}

func cmdDebug(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("debug: missing subcommand (seed-file)")
	}
	switch args[0] {
	case "seed-file":
		if len(args) < 3 {
			return errors.New("debug seed-file: usage: seed-file <name> <length-bytes>")
		}
		n, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil || n <= 0 {
			return fmt.Errorf("debug seed-file: invalid length %q", args[2])
		}
		return cmdDebugSeedFile(ctx, args[1], n)
	default:
		return fmt.Errorf("debug: unknown subcommand %q", args[0])
	}
}

// cmdDebugSeedFile generates a deterministic-pattern file of n bytes,
// uploads it as one or more chunks to the backing channel, and records
// the inode + chunk_map entries in the local SQLite store. Fails loudly
// if `name` is already present in the root directory.
func cmdDebugSeedFile(ctx context.Context, name string, n int64) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if err := cfg.RequireChannel(); err != nil {
		return err
	}
	metaStore, err := meta.Open(cfg.DBPath())
	if err != nil {
		return err
	}
	defer metaStore.Close()

	if _, err := metaStore.Lookup(ctx, meta.RootIno, name); err == nil {
		return fmt.Errorf("seed-file: %q already exists (rename / unlink first)", name)
	} else if !errors.Is(err, meta.ErrNotFound) {
		return err
	}

	client, err := tg.New(cfg)
	if err != nil {
		return err
	}

	data := generatePattern(n)
	sum := md5.Sum(data)

	return client.RunSession(ctx, func(ctx context.Context, sess *tg.Session) error {
		ino, err := metaStore.CreateChild(ctx, meta.RootIno, name, meta.Inode{
			Kind: meta.KindFile,
			Mode: 0o100644,
			Size: n,
		})
		if err != nil {
			return fmt.Errorf("create inode: %w", err)
		}

		chunker := chunk.NewChunker(bytes.NewReader(data), int(chunk.ChunkSize))
		var idx int32
		var uploaded int64
		for {
			piece, err := chunker.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return fmt.Errorf("chunker: %w", err)
			}
			cname := fmt.Sprintf("%s.part%d", name, idx)
			fmt.Printf("Uploading chunk %d (%d bytes)...\n", idx, len(piece))
			msgID, err := sess.UploadDocument(ctx, bytes.NewReader(piece), cname, "")
			if err != nil {
				return fmt.Errorf("upload chunk %d: %w", idx, err)
			}
			if err := metaStore.PutChunk(ctx, meta.Chunk{
				Ino: ino, Idx: idx, TGMessageID: int64(msgID), Size: int32(len(piece)),
			}); err != nil {
				return fmt.Errorf("put chunk: %w", err)
			}
			uploaded += int64(len(piece))
			idx++
		}

		fmt.Printf("\nSeeded %s\n", name)
		fmt.Printf("  ino:        %d\n", ino)
		fmt.Printf("  size:       %d bytes\n", uploaded)
		fmt.Printf("  chunks:     %d\n", idx)
		fmt.Printf("  expect md5: %s\n", hex.EncodeToString(sum[:]))
		return nil
	})
}

// generatePattern produces n bytes of repeating alphabet, useful for
// reproducible test files. Byte i is alphabet[i % len(alphabet)].
func generatePattern(n int64) []byte {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := int64(0); i < n; i++ {
		b[i] = alphabet[i%int64(len(alphabet))]
	}
	return b
}
