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
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"telfs/internal/chunk"
	"telfs/internal/compress"
	"telfs/internal/config"
	"telfs/internal/crypto"
	telfsfs "telfs/internal/fs"
	"telfs/internal/meta"
	"telfs/internal/metrics"
	"telfs/internal/snapshot"
	"telfs/internal/tg"
	"telfs/internal/trash"
)

// Build-time metadata. Overridden via -ldflags at release:
//
//	-X main.version=v0.4 -X main.commit=<sha> -X main.buildDate=<iso8601>
var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

const usage = `telfs — FUSE filesystem backed by a Telegram channel

Usage:
  telfs login [--bot <token>]       Authenticate against Telegram (MTProto user
                                      auth by default; --bot uses a BotFather
                                      token via auth.ImportBotAuthorization).
  telfs channel list                List your channels (id, title, post permission).
  telfs channel set [--access-hash N] <id>
                                    Configure the backing channel. In bot mode
                                    --access-hash is required (bots can't
                                    auto-discover it from the dialog list).
  telfs channel info                Show the currently configured channel.
  telfs channel ping                Round-trip a test message (smoke test).
  telfs mount [flags] <mountpoint>  Mount the filesystem.
                                      Flags: --readonly --allow-other --debug --no-recover
  telfs init [--chunk-size N]       Initialize FS settings before first mount.
  telfs gc [--yes] [--pages N]      Reclaim orphan chunks + old snapshots.
  telfs encrypt init [--convergent] Enable AES-256-GCM for this filesystem.
                                      --convergent picks aes-gcm-v3 so encrypted
                                      chunks also dedup (trade-off: equality
                                      detection visible on the channel).
  telfs encrypt status              Show whether encryption is enabled.
  telfs encrypt rotate              Change passphrase without re-encrypting chunks (v2/v3 FSes).
  telfs profile {list,show,create,delete,use,export,import}
                                    Manage multiple profiles (accounts/channels).
  telfs status                      One-screen summary of the active profile.
  telfs doctor                      Lint local profile + DB state for known invariants.
  telfs web [--listen ADDR] [--token TOKEN]
                                    HTTP management UI (default 127.0.0.1:8080).
  telfs fsck [--fix] [--stop-after N]
                                    Verify every chunk_map row against the channel.
  telfs snapshot {list, restore [--yes] <msg-id>}
                                    Inspect / roll back to a historical snapshot.
  telfs trash {enable [--ttl D], disable, status, list, empty}
                                    Manage the rm safety-net at /.trash.
  telfs ratelimit {show, set <N>, clear}
                                    View or change the TG requests/min cap.
                                    0 = unlimited; running daemon hot-reloads
                                    within 5 s of a CLI change.
  telfs debug seed-file <name> <n>  Upload a deterministic n-byte test file.

Environment:
  TELFS_PROFILE    Active profile name (overrides ~/.config/telfs/active)
  TELFS_API_ID     Telegram API ID (https://my.telegram.org/apps)
  TELFS_API_HASH   Telegram API hash
  TELFS_PHONE      Phone number for login (prompted if unset)
  TELFS_DC         Starting datacenter override (default: gotd's DC 2)
  TELFS_PASSPHRASE FS encryption passphrase (skip the prompt)
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
	case "version", "-v", "--version":
		fmt.Printf("telfs %s (commit %s, built %s)\n", version, commit, buildDate)
		return nil
	case "login":
		return cmdLogin(ctx, os.Args[2:])
	case "channel":
		return cmdChannel(ctx, os.Args[2:])
	case "mount":
		return cmdMount(ctx, os.Args[2:])
	case "init":
		return cmdInit(ctx, os.Args[2:])
	case "encrypt":
		return cmdEncrypt(ctx, os.Args[2:])
	case "profile":
		return cmdProfile(ctx, os.Args[2:])
	case "status":
		return cmdStatus(ctx, os.Args[2:])
	case "web":
		return cmdWeb(ctx, os.Args[2:])
	case "fsck":
		return cmdFsck(ctx, os.Args[2:])
	case "gc":
		return cmdGC(ctx, os.Args[2:])
	case "snapshot":
		return cmdSnapshot(ctx, os.Args[2:])
	case "trash":
		return cmdTrash(ctx, os.Args[2:])
	case "debug":
		return cmdDebug(ctx, os.Args[2:])
	case "doctor":
		return cmdDoctor(ctx, os.Args[2:])
	case "ratelimit":
		return cmdRateLimit(ctx, os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n%s", os.Args[1], usage)
		os.Exit(2)
		return nil
	}
}

func cmdLogin(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("login", flag.ContinueOnError)
	botToken := fs.String("bot", "", "log in as a bot using the given @BotFather token (overrides any phone-auth state)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	// --bot promotes the profile to bot mode and persists the token.
	// Subsequent `telfs login` (without --bot) on this profile picks
	// the same mode automatically because cfg.AuthMode is now "bot".
	if *botToken != "" {
		cfg.AuthMode = config.AuthModeBot
		cfg.BotToken = strings.TrimSpace(*botToken)
		if err := cfg.Save(); err != nil {
			return err
		}
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
		return cmdChannelSet(ctx, cfg, args[1:])
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

func cmdChannelSet(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("channel set", flag.ContinueOnError)
	accessHash := fs.Int64("access-hash", 0, "channel access_hash (REQUIRED in bot mode — bots cannot enumerate dialogs to discover it)")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: telfs channel set [--access-hash N] <channel-id>")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return errors.New("channel set: expected exactly one <channel-id>")
	}
	id, err := strconv.ParseInt(fs.Arg(0), 10, 64)
	if err != nil {
		return fmt.Errorf("channel set: invalid id %q: %w", fs.Arg(0), err)
	}
	if cfg.EffectiveAuthMode() == config.AuthModeBot && *accessHash == 0 {
		return errors.New("channel set: --access-hash is required in bot mode (bots can't auto-discover it; copy it from a user-account profile or from another tool)")
	}
	client, err := tg.New(cfg)
	if err != nil {
		return err
	}
	info, err := client.SetChannel(ctx, id, *accessHash)
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

// cmdInit creates the local meta DB (if missing) and applies FS-wide
// settings — currently just the per-FS chunk size. Idempotent for the
// same value; refuses to change chunk_size when chunks already exist
// because (ino, idx) → offset arithmetic depends on it.
func cmdInit(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	chunkSize := fs.Int64("chunk-size", meta.DefaultChunkSize, "chunk size in bytes (power of 2, 64KiB..1.5GiB)")
	compression := fs.String("compression", "", "chunk-payload compression codec: none, zstd. Empty leaves existing setting unchanged.")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: telfs init [--chunk-size BYTES] [--compression CODEC]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := meta.ValidateChunkSize(*chunkSize); err != nil {
		return err
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	metaStore, err := meta.Open(cfg.DBPath())
	if err != nil {
		return err
	}
	defer metaStore.Close()

	refs, err := metaStore.AllChunkMessageIDs(ctx)
	if err != nil {
		return err
	}

	current, err := metaStore.ChunkSize(ctx)
	if err != nil {
		return err
	}
	if current != *chunkSize {
		if len(refs) > 0 {
			return fmt.Errorf("init: refuses to change chunk_size from %d to %d while %d chunk(s) exist; "+
				"start a fresh TelFS instance with a new channel to use a different chunk size",
				current, *chunkSize, len(refs))
		}
		if err := metaStore.SetChunkSize(ctx, *chunkSize); err != nil {
			return err
		}
		fmt.Printf("chunk_size set to %d bytes (%s)\n", *chunkSize, humanBytes(*chunkSize))
	} else {
		fmt.Printf("chunk_size already set to %d bytes (%s). No change.\n", current, humanBytes(current))
	}

	if *compression != "" {
		if _, err := compress.New(*compression); err != nil {
			return err
		}
		currentCodec, err := metaStore.CompressionCodec(ctx)
		if err != nil {
			return err
		}
		if currentCodec == *compression {
			fmt.Printf("compression already set to %q. No change.\n", currentCodec)
		} else {
			if len(refs) > 0 {
				return fmt.Errorf("init: refuses to change compression from %q to %q while %d chunk(s) exist; "+
					"start a fresh TelFS instance with a new channel to use a different codec",
					currentCodec, *compression, len(refs))
			}
			if err := metaStore.SetCompressionCodec(ctx, *compression); err != nil {
				return err
			}
			fmt.Printf("compression set to %q\n", *compression)
		}
	}
	return nil
}

// humanBytes renders a byte count as KiB / MiB / GiB. Only powers of
// 1024 — chunk sizes are always powers of two so no fractions.
func humanBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%d GiB", n>>30)
	case n >= 1<<20:
		return fmt.Sprintf("%d MiB", n>>20)
	case n >= 1<<10:
		return fmt.Sprintf("%d KiB", n>>10)
	}
	return fmt.Sprintf("%d B", n)
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
func cmdMount(signalCtx context.Context, args []string) error {
	fs := flag.NewFlagSet("mount", flag.ContinueOnError)
	readonly := fs.Bool("readonly", false, "mount read-only (rejects all write ops with EROFS)")
	allowOther := fs.Bool("allow-other", false, "allow other users to access the mount (FUSE -o allow_other; requires user_allow_other in /etc/fuse.conf)")
	debug := fs.Bool("debug", false, "log every FUSE op")
	noRecover := fs.Bool("no-recover", false, "skip cold-mount recovery from channel snapshot (starts with an empty DB if local DB is missing)")
	metricsAddr := fs.String("metrics-addr", "", "bind a Prometheus /metrics endpoint at this addr (default: off). Example: 127.0.0.1:9090")
	cacheCap := fs.String("cache-cap", "", "override the LRU disk-cache size cap (e.g. 8GiB, 512MiB, 2GB). Flag wins over config; empty falls through to config.toml's cache_cap; that falls through to 1 GiB.")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: telfs mount [--readonly] [--allow-other] [--debug] [--metrics-addr ADDR] [--cache-cap SIZE] <mountpoint>")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return errors.New("mount: expected exactly one mountpoint argument")
	}
	mountpoint := fs.Arg(0)
	checkFusermount()

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
		if *noRecover {
			fmt.Println("--no-recover: skipping channel snapshot recovery; starting with an empty DB.")
		} else if err := tryRecover(signalCtx, cfg); err != nil {
			return fmt.Errorf("recovery: %w", err)
		}
	}

	metaStore, err := meta.Open(cfg.DBPath())
	if err != nil {
		return err
	}
	defer metaStore.Close()

	// Startup repair: a previous daemon may have died mid-upload
	// (FLOOD_WAIT, kill, OOM, etc.) and left files whose chunk_map
	// has gaps. Truncate each such inode to its largest contiguous
	// prefix BEFORE the FUSE server starts so user-visible reads
	// don't EIO on the gap. Idempotent; cheap on a clean FS.
	if repaired, err := metaStore.RepairPartialFiles(signalCtx); err != nil {
		fmt.Fprintln(os.Stderr, "[repair] warning:", err)
	} else if len(repaired) > 0 {
		fmt.Printf("Repaired %d partial file(s) from a prior crash:\n", len(repaired))
		for _, r := range repaired {
			fmt.Printf("  ino %d: size %d → %d (had %d chunks, first gap at idx %d, kept %d)\n",
				r.Ino, r.OldSize, r.NewSize, r.OldChunks, r.FirstGapAt, r.NewChunks)
		}
	}

	client, err := tg.New(cfg)
	if err != nil {
		return err
	}

	// The gotd session must stay alive past signalCtx cancellation so
	// the final snapshot can post. We give RunSession its own
	// Background-derived context and let our callback drive shutdown
	// in the correct order (final snapshot → unmount FUSE → return →
	// gotd shuts down).
	// Resolve the cipher up front: plaintext (NoopCipher) unless this
	// FS was set up with `telfs encrypt init` (crypto_mode set in
	// meta_kv). For the encrypted case we prompt for the passphrase
	// here and refuse to mount if the canary doesn't decrypt.
	cipher, err := loadCipher(signalCtx, metaStore)
	if err != nil {
		return err
	}

	// Per-FS chunk size — was committed at first init (or defaulted to
	// chunk.ChunkSize on legacy filesystems that predate this kv).
	chunkSize, err := metaStore.ChunkSize(signalCtx)
	if err != nil {
		return err
	}

	// Per-FS compression codec — was committed at `telfs init`.
	// Default "none" for filesystems initialized before compression
	// support. compress.New errors on an unknown name, so a binary
	// older than the FS can't silently feed a no-op decompressor a
	// zstd frame.
	codecName, err := metaStore.CompressionCodec(signalCtx)
	if err != nil {
		return err
	}
	codec, err := compress.New(codecName)
	if err != nil {
		return err
	}

	// Trash safety-net. Bootstrap /.trash if enabled; the inode is
	// passed to the FUSE backend so Unlink/Rmdir reroute into it. The
	// GC goroutine is spawned inside RunSession below so its ctx ties
	// to the session lifetime.
	trashEnabled, err := metaStore.TrashEnabled(signalCtx)
	if err != nil {
		return err
	}
	var trashIno int64
	if trashEnabled && !*readonly {
		trashIno, err = trash.EnsureRootDir(signalCtx, metaStore)
		if err != nil {
			return fmt.Errorf("trash: %w", err)
		}
	}

	// Long-lived session manager: keeps a stable *tg.Session whose
	// underlying gotd client can be torn down + re-established
	// without disturbing the mount above. The stall watchdog (below)
	// signals Reconnect when no upload progress for 90 s; in the
	// wedged-TCP scenario this auto-recovers within seconds instead
	// of waiting for the kernel's TCP timeout (~15 min).
	mgr := client.NewSessionManager()
	sess := mgr.Session()
	mgrCtx, stopMgr := context.WithCancel(context.Background())
	firstConnect := make(chan error, 1)
	mgrDone := make(chan error, 1)
	go func() {
		err := mgr.Run(mgrCtx, func() error {
			select {
			case firstConnect <- nil:
			default:
			}
			return nil
		})
		// Surface a pre-first-connect exit so the wait below
		// unblocks; non-blocking so a successful first-connect
		// path doesn't double-send.
		select {
		case firstConnect <- err:
		default:
		}
		mgrDone <- err
	}()
	select {
	case err := <-firstConnect:
		if err != nil {
			stopMgr()
			<-mgrDone
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
	case <-signalCtx.Done():
		stopMgr()
		<-mgrDone
		return nil
	}

	err = func() error {
		fetcher := &chunk.TGFetcher{Session: sess}
		// Cache cap resolution: --cache-cap flag wins, then config's
		// cache_cap, then chunk.DefaultCacheCapBytes. A parse error on
		// the flag aborts (operator typo, fail loudly); a parse error
		// in config is silent (treat as "use default" — config-edit
		// errors shouldn't fail mount when it would otherwise come
		// up cleanly).
		capBytes := chunk.DefaultCacheCapBytes
		if *cacheCap != "" {
			n, perr := config.ParseHumanBytes(*cacheCap)
			if perr != nil {
				return fmt.Errorf("--cache-cap: %w", perr)
			}
			if n > 0 {
				capBytes = n
			}
		} else if n := cfg.CacheCapBytes(); n > 0 {
			capBytes = n
		}
		fmt.Fprintf(os.Stderr, "cache cap: %s (%d bytes)\n", humanBytes(capBytes), capBytes)
		cache, err := chunk.NewCache(cfg.CachePath(), capBytes, fetcher, cipher, codec)
		if err != nil {
			return err
		}
		// Backend-global upload pool: one concurrency cap and one
		// long-lived ctx shared across every file handle on the
		// mount. With async close, individual Writers no longer own
		// their dispatcher — the pool does, and survives Release of
		// any single handle. Drained at unmount before tearing down
		// the gotd session.
		pool := chunk.NewUploadPool(chunk.DefaultUploadConcurrency)
		// Pending staging dir lives under the cache root; the upload
		// pool carries the path so Writer.uploadOne can persist
		// wires before dispatch and Packer.flush can clean them up
		// after PutChunk lands. A SIGKILL between dispatch and
		// PutChunk leaves the wire on disk; cmdMount's pre-mount
		// recovery (below) re-submits it on next startup.
		pendingDir, err := chunk.EnsurePendingDir(cache.Dir())
		if err != nil {
			return err
		}
		chunk.CleanupPendingTmp(pendingDir)
		pool.PendingDir = pendingDir
		reader := chunk.NewReader(metaStore, cache, pool.Pending, chunkSize)
		// Wire the staging-dir fallback so reads of freshly-written
		// chunks succeed even when chunk_map hasn't seen its row yet
		// (the wire is on disk, encoded the same way the Writer's
		// dispatch encoded it — same cipher, same codec).
		reader.SetPendingFallback(pendingDir, cipher, codec)
		// Small-file packer: aggregates chunks smaller than
		// chunk.DefaultPackThreshold into shared TG blobs to amortize
		// the per-message RTT. Nil-safe — Writer falls back to
		// direct upload when no packer is wired.
		packer := chunk.NewPacker(metaStore, sess, 0, 0, 0)
		// Cancel pending wires on unlink. When a file is deleted while
		// some of its chunks are still buffered in the packer (tar
		// overwriting an extracted tree, rm -rf during an upload, etc.),
		// uploading those chunks would just produce orphan TG messages
		// that the packer's PutChunk then FK-rejects. DropInode pulls
		// them from the queue before the upload happens — saving the
		// bandwidth AND removing the log spam.
		metaStore.SetInodeDeletedCallback(func(ino int64) {
			if n := packer.DropInode(ino); n > 0 {
				fmt.Fprintf(os.Stderr, "[packer] dropped %d pending wire(s) for unlinked ino=%d\n", n, ino)
			}
		})

		backend := &telfsfs.Backend{
			Meta:      metaStore,
			Reader:    reader,
			Cache:     cache,
			Uploader:  sess,
			Pool:      pool,
			Packer:    packer,
			Cipher:    cipher,
			Codec:     codec,
			ChunkSize: chunkSize,
			ReadOnly:  *readonly,
			TrashIno:  trashIno,
		}

		// Metrics endpoint (opt-in). Gauges are registered AFTER the
		// pool & cache exist so they read the live values, not the
		// nil pre-construction state. Counters are registered in
		// metrics' package init and increment from chunk/tg call
		// sites already.
		var metricsSrv *metrics.Server
		if *metricsAddr != "" {
			metrics.Default.AddGauge("telfs_cache_size_bytes",
				"Bytes currently held in the LRU disk cache.",
				func() int64 { return cache.SizeBytes() })
			metrics.Default.AddGauge("telfs_pending_uploads",
				"In-flight chunk uploads (detached + still in flight).",
				func() int64 { return pool.Pending.Total() })
			metricsSrv = metrics.NewServer(*metricsAddr)
			// /telegram exposes the gotd request log + last-success
			// timestamps so the web UI (and curl) can see why an
			// upload isn't progressing — was Telegram acking? are
			// we stuck in FLOOD_WAIT? did we get FILE_PART_MISSING?
			// JSON only for now; rendering is the web's problem.
			metricsSrv.RegisterHandler("/telegram", telegramJSONHandler(sess.Tracker(), cfg, packer, pool, pendingDir, metaStore))
			// /cache reports the LRU disk cache's live size + cap +
			// hit/miss counts so the /state web page can render the
			// cache panel without having to know about the metrics
			// scrape format. Cheap: in-memory atomic loads.
			metricsSrv.RegisterHandler("/cache", cacheJSONHandler(cache))
			if err := metricsSrv.Start(); err != nil {
				return err
			}
			fmt.Printf("Metrics: http://%s/metrics\n", metricsSrv.Addr())
			fmt.Printf("Telegram log: http://%s/telegram\n", metricsSrv.Addr())
		}
		server, err := telfsfs.Mount(telfsfs.MountOptions{
			MountPoint: mountpoint,
			AllowOther: *allowOther,
			Debug:      *debug,
		}, backend)
		if err != nil {
			return err
		}

		// Snapshot cadence: takes one snapshot immediately (so a
		// freshly-recovered DB is re-baselined) then every interval.
		// We pass signalCtx — the goroutine exits when the gotd session
		// tears down. The user-stop signal stops it via snapCtx below.
		snapMgr := &snapshot.Manager{Meta: metaStore, Session: sess, Cipher: cipher}
		snapCtx, stopSnap := context.WithCancel(signalCtx)
		snapDone := make(chan struct{})
		go func() {
			_ = snapMgr.Run(snapCtx)
			close(snapDone)
		}()

		// Journal poster: drains the local journal table every
		// DefaultPosterInterval and uploads each pending op as a
		// journal-op message on the channel. With this running,
		// recovery can replay mutations made between snapshot
		// cadences — narrowing the crash data-loss window from
		// 5 min (snapshot interval) to ~5 s (poster interval).
		journalPoster := &snapshot.Poster{Meta: metaStore, Session: sess}
		journalCtx, stopJournal := context.WithCancel(signalCtx)
		journalDone := make(chan struct{})
		go func() {
			_ = journalPoster.Run(journalCtx)
			close(journalDone)
		}()

		// Rate-limit poller: reads meta_kv every few seconds and
		// applies the value to the gotd middleware so CLI/web edits
		// propagate without a remount. Starts by applying whatever
		// value is in meta_kv now — if zero/missing, the limiter
		// stays unlimited (historical default).
		applyRateLimit := func() {
			rl := client.RateLimiter()
			if n, err := metaStore.TGRateLimitPerMin(signalCtx); err == nil {
				if n != rl.UploadPerMinute() {
					rl.SetUploadPerMinute(n)
					if n == 0 {
						fmt.Fprintln(os.Stderr, "[ratelimit] upload cleared (unlimited)")
					} else {
						fmt.Fprintf(os.Stderr, "[ratelimit] upload set to %d requests/min\n", n)
					}
				}
			}
			if n, err := metaStore.TGDownloadRateLimitPerMin(signalCtx); err == nil {
				if n != rl.DownloadPerMinute() {
					rl.SetDownloadPerMinute(n)
					if n == 0 {
						fmt.Fprintln(os.Stderr, "[ratelimit] download cleared (unlimited)")
					} else {
						fmt.Fprintf(os.Stderr, "[ratelimit] download set to %d requests/min\n", n)
					}
				}
			}
		}
		applyRateLimit()
		rateLimitCtx, stopRateLimit := context.WithCancel(signalCtx)
		rateLimitDone := make(chan struct{})
		go func() {
			defer close(rateLimitDone)
			t := time.NewTicker(5 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-rateLimitCtx.Done():
					return
				case <-t.C:
					applyRateLimit()
				}
			}
		}()

		// WAL checkpointer: SQLite's auto-checkpoint heuristic (default
		// at every 1000 pages crossed) can lag badly under bursty
		// write workloads — we observed db.sqlite-wal at 116 MiB
		// during the kernel-untar incident, which is wasted disk and
		// slow next-mount recovery. A 30 s PASSIVE checkpoint never
		// blocks readers or writers; it just opportunistically drains
		// frames that aren't actively in use.
		go func() {
			t := time.NewTicker(30 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-signalCtx.Done():
					return
				case <-t.C:
					_, _, _, _ = metaStore.CheckpointWAL(signalCtx)
				}
			}
		}()

		// Trash GC: reads ttl on every tick so changes via
		// `telfs trash enable --ttl <D>` take effect without remount.
		if trashIno != 0 {
			go trash.Run(signalCtx, metaStore, trashIno,
				func() time.Duration {
					d, err := metaStore.TrashTTL(signalCtx)
					if err != nil {
						return time.Duration(meta.DefaultTrashTTLSecs) * time.Second
					}
					return d
				}, trash.DefaultGCInterval)
		}

		// Pending-uploads recovery: a previous daemon may have left
		// wires on disk between PersistPending and Packer.flush's
		// DeletePending (e.g., killed mid-upload). Read each one and
		// re-submit via the packer; on success the file is removed
		// the normal way. Done after the snapshot manager + journal
		// poster start so any in-flight startup work isn't blocked,
		// and runs in a goroutine so a large pending dir doesn't
		// delay the "Mounted at" line. Errors are logged but not
		// fatal — they don't break the mount, just leave staged
		// wires for the next attempt.
		go recoverPending(signalCtx, pendingDir, packer, metaStore)

		// Stall watchdog: if there's chunk work pending but no
		// upload completion has registered in metrics for a while,
		// fire the SessionManager's Reconnect signal so a wedged
		// MTProto connection auto-recovers without manual restart.
		// Also logs the event so it's visible in stderr + /telegram.
		go stallWatchdog(signalCtx, pool, mgr)

		fmt.Printf("Mounted at %s. Press ^C to unmount.\n", mountpoint)
		<-signalCtx.Done()

		// User asked to stop. Two failsafes:
		//  1. A second ^C (or SIGTERM) force-exits via lazy unmount.
		//     Standard Unix expectation — first signal is "stop
		//     nicely", second is "stop now".
		//  2. A 25-second hard watchdog: even without a second
		//     signal, if the orderly shutdown wedges (FLOOD_WAIT
		//     storm, gotd stuck), lazy-unmount + os.Exit so the
		//     user's terminal returns and the kernel mount isn't
		//     orphaned.
		armForceExit(mountpoint)

		done := make(chan struct{})
		go func() {
			defer close(done)
			// Stop the journal poster first so any in-flight ops
			// drain to the channel before snapshot. Its Run does a
			// final best-effort drain on ctx-cancel; we wait for it.
			stopJournal()
			<-journalDone
			stopSnap()
			<-snapDone
			stopRateLimit()
			<-rateLimitDone
			// Unmount + drain BEFORE the final snapshot. Two-step:
			//   1. Unmount severs the kernel-to-userspace channel so
			//      no new FUSE writes can fire; server.Wait drains
			//      any FUSE request goroutines still in flight.
			//   2. backend.Shutdown drains the upload pool — the
			//      uploadOne goroutines that outlived their handle
			//      via async close. Until this returns, freshly-
			//      closed files may have inode.size persisted but
			//      no chunk_map row.
			// THEN take the snapshot, so the channel state reflects
			// a fully-quiescent DB. Snapshotting before drain would
			// ship a torn DB (file present, chunks missing) — fine
			// for warm restart (local DB is authoritative) but
			// catastrophic for recover-from-channel.
			fmt.Println("Unmounting…")
			if uerr := server.Unmount(); uerr != nil {
				fmt.Fprintln(os.Stderr, "unmount:", uerr)
			}
			server.Wait()
			drainCtx, cancelDrain := context.WithTimeout(context.Background(), 12*time.Second)
			backend.Shutdown(drainCtx)
			cancelDrain()
			if metricsSrv != nil {
				mctx, mcancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = metricsSrv.Shutdown(mctx)
				mcancel()
			}
			// Final snapshot, post-drain. Uploads after RunSession
			// returns fail with "engine closed", so this MUST run
			// inside the callback. 10-second budget keeps the total
			// (12 drain + 10 snapshot + slack) under the 30s
			// watchdog below.
			fmt.Println("Taking final snapshot…")
			finalCtx, finalCancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := snapMgr.Once(finalCtx); err != nil {
				fmt.Fprintln(os.Stderr, "[snapshot] final failed:", err)
			}
			finalCancel()
		}()
		select {
		case <-done:
			// Orderly shutdown complete.
		case <-time.After(30 * time.Second):
			fmt.Fprintln(os.Stderr, "telfs: orderly shutdown timed out — forcing lazy unmount + exit")
			_ = exec.Command("fusermount", "-uz", mountpoint).Run()
			os.Exit(1)
		}
		return nil
	}()
	// Stop the session manager AFTER the shutdown sequence ran the
	// final snapshot. The manager's goroutine then unwinds; mgrDone
	// signals when the gotd client's run loop has fully torn down.
	stopMgr()
	<-mgrDone
	// Treat clean cancellation as success — the user asked for unmount.
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// stallWatchdog periodically samples chunks_uploaded + pending and
// triggers a session reconnect when there's pending work but no
// upload completion in the last stallThreshold. 90 seconds is the
// threshold — short enough that a real stall is surfaced before
// the user notices, long enough that a slow but progressing upload
// doesn't trigger false alarms.
//
// On stall: logs to stderr (visible in daemon log) + calls
// mgr.Reconnect (tears down the gotd session, re-establishes via
// the SessionManager's run loop). Both pool.Pending count and
// last_uploaded reset naturally as the new session lands the
// retried wires. Cooldown between reconnects prevents a thrash
// loop if the underlying network is genuinely down.
func stallWatchdog(ctx context.Context, pool *chunk.UploadPool, mgr *tg.SessionManager) {
	const sampleInterval = 30 * time.Second
	const stallThreshold = 90 * time.Second
	const reconnectCooldown = 2 * time.Minute
	var (
		lastUploaded   int64
		stalledSince   time.Time
		lastReconnect  time.Time
	)
	tick := time.NewTicker(sampleInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			pending := pool.Pending.Total()
			uploaded := metrics.ChunksUploaded.Load()
			if pending == 0 || uploaded != lastUploaded {
				// Idle (nothing pending) OR making progress.
				stalledSince = time.Time{}
				lastUploaded = uploaded
				continue
			}
			// pending > 0, uploaded didn't move.
			if stalledSince.IsZero() {
				stalledSince = time.Now()
				continue
			}
			elapsed := time.Since(stalledSince)
			if elapsed < stallThreshold {
				continue
			}
			if time.Since(lastReconnect) < reconnectCooldown {
				continue // already kicked the session recently
			}
			fmt.Fprintf(os.Stderr,
				"[watchdog] STALL: %d chunks pending, no upload progress for %v. "+
					"Forcing MTProto reconnect.\n",
				pending, elapsed.Round(time.Second))
			mgr.Reconnect()
			lastReconnect = time.Now()
			stalledSince = time.Time{} // give the new session a chance
		}
	}
}

// cacheJSONHandler emits the LRU disk cache's live state — size,
// cap, hit/miss counters — so the web UI can render its cache panel
// against authoritative in-process numbers. Walking the cache dir
// from the web process would double-count (the pending/ sub-dir is
// NOT counted against the cap, but `du` doesn't know that).
func cacheJSONHandler(cache *chunk.Cache) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		size := cache.SizeBytes()
		cap_ := cache.CapBytes()
		hits := metrics.CacheHits.Load()
		misses := metrics.CacheMisses.Load()
		used := float64(0)
		if cap_ > 0 {
			used = float64(size) / float64(cap_)
		}
		hitRatio := float64(0)
		if hits+misses > 0 {
			hitRatio = float64(hits) / float64(hits+misses)
		}
		fmt.Fprintf(w,
			"{\"size_bytes\":%d,\"cap_bytes\":%d,\"used_ratio\":%.4f,\"hits\":%d,\"misses\":%d,\"hit_ratio\":%.4f}\n",
			size, cap_, used, hits, misses, hitRatio)
	}
}

// telegramJSONHandler returns an http.HandlerFunc that renders the
// tracker's current state as JSON. Format is intentionally simple so
// the web UI can fetch + render with a few lines of JS, and curl
// pipes nicely through jq.
//
// The packer, pool, pendingDir, and meta store parameters are used to
// surface the upload queue depth: how many chunks are waiting in the
// packer's buffer, how many wires are staged on disk for crash-
// recovery, how many concurrency slots the pool is using, and how
// many journal entries are unposted. All of those are cheap to read
// (one mutex grab + one ReadDir + one SELECT COUNT) on a request that
// fires every 2 s.
func telegramJSONHandler(tr *tg.Tracker, cfg *config.Config, packer *chunk.Packer, pool *chunk.UploadPool, pendingDir string, mstore *meta.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		snap := tr.Snapshot()
		fmt.Fprintln(w, "{")
		// Channel binding so the web can label aggregates with
		// which channel the daemon is talking to. Today there's
		// one channel per daemon; if multi-channel ever ships
		// the JSON shape doesn't have to change.
		fmt.Fprintf(w, "  \"channel_id\": %d,\n", cfg.Channel.ID)
		fmt.Fprintf(w, "  \"channel_title\": %q,\n", cfg.Channel.Title)
		fmt.Fprintf(w, "  \"in_flight\": %d,\n", snap.InFlight)
		fmt.Fprintf(w, "  \"total_ok\": %d,\n", snap.TotalOK)
		fmt.Fprintf(w, "  \"total_err\": %d,\n", snap.TotalErr)
		fmt.Fprintf(w, "  \"total_bytes\": %d,\n", snap.TotalBytes)
		fmt.Fprintf(w, "  \"total_bytes_tx\": %d,\n", snap.TotalBytesTx)
		fmt.Fprintf(w, "  \"total_bytes_rx\": %d,\n", snap.TotalBytesRx)

		// Queue depths. Order:
		//   1. packer pending — chunks buffered in memory awaiting
		//      flush; small (default ~32 MiB at most).
		//   2. wires on disk — durable staging from PersistPending;
		//      reflects chunks dispatched but not yet PutChunk'd.
		//   3. pool in-flight — uploads holding a concurrency slot.
		//   4. journal unposted — meta ops not yet on the channel.
		// All cheap to read on a 2-second poll.
		packerChunks, packerBytes := 0, int64(0)
		if packer != nil {
			packerChunks, packerBytes = packer.QueueDepth()
		}
		var pendingFiles int
		var pendingBytes int64
		if pendingDir != "" {
			if ents, err := os.ReadDir(pendingDir); err == nil {
				for _, e := range ents {
					if e.IsDir() {
						continue
					}
					pendingFiles++
					if info, err := e.Info(); err == nil {
						pendingBytes += info.Size()
					}
				}
			}
		}
		var poolInFlight, poolCap int
		if pool != nil {
			poolInFlight, poolCap = pool.InFlight(), pool.Cap()
		}
		var journalUnposted int
		if mstore != nil {
			if v, err := mstore.Stats(r.Context()); err == nil {
				journalUnposted = int(v.JournalUnposted)
			}
		}
		fmt.Fprintln(w, "  \"queue\": {")
		fmt.Fprintf(w, "    \"packer_chunks\": %d,\n", packerChunks)
		fmt.Fprintf(w, "    \"packer_bytes\": %d,\n", packerBytes)
		fmt.Fprintf(w, "    \"packer_target_bytes\": %d,\n", func() int64 {
			if packer == nil {
				return 0
			}
			return packer.Target()
		}())
		fmt.Fprintf(w, "    \"pending_wires_count\": %d,\n", pendingFiles)
		fmt.Fprintf(w, "    \"pending_wires_bytes\": %d,\n", pendingBytes)
		fmt.Fprintf(w, "    \"pool_in_flight\": %d,\n", poolInFlight)
		fmt.Fprintf(w, "    \"pool_cap\": %d,\n", poolCap)
		fmt.Fprintf(w, "    \"journal_unposted\": %d\n", journalUnposted)
		fmt.Fprintln(w, "  },")
		if !snap.LastSuccess.IsZero() {
			fmt.Fprintf(w, "  \"last_success_unix\": %d,\n", snap.LastSuccess.Unix())
			fmt.Fprintf(w, "  \"last_success_age_seconds\": %d,\n", int64(time.Since(snap.LastSuccess).Seconds()))
		}
		if !snap.LastError.IsZero() {
			fmt.Fprintf(w, "  \"last_error_unix\": %d,\n", snap.LastError.Unix())
			fmt.Fprintf(w, "  \"last_error_age_seconds\": %d,\n", int64(time.Since(snap.LastError).Seconds()))
			fmt.Fprintf(w, "  \"last_error_message\": %q,\n", snap.LastErrorMsg)
		}
		// Per-method totals — what the user wants visible on the
		// web. Sorted by call count desc inside the tracker.
		fmt.Fprintln(w, "  \"per_method\": [")
		for i, mc := range snap.PerMethod {
			sep := ","
			if i == len(snap.PerMethod)-1 {
				sep = ""
			}
			fmt.Fprintf(w, "    {\"method\":%q,\"ok\":%d,\"err\":%d,\"flood_wait\":%d,\"bytes\":%d}%s\n",
				mc.Method, mc.OK, mc.Err, mc.FloodWait, mc.Bytes, sep)
		}
		fmt.Fprintln(w, "  ],")
		fmt.Fprintln(w, "  \"requests\": [")
		for i, r := range snap.Requests {
			sep := ","
			if i == len(snap.Requests)-1 {
				sep = ""
			}
			fmt.Fprintf(w, "    {\"id\":%d,\"method\":%q,\"started_unix\":%d,\"duration_ms\":%d,\"bytes\":%d,\"in_flight\":%t,\"err\":%q,\"flood_wait_seconds\":%d}%s\n",
				r.ID, r.Method, r.Started.Unix(),
				r.Duration.Milliseconds(), r.Bytes, r.InFlight,
				r.Err, int(r.FloodWait.Seconds()), sep)
		}
		fmt.Fprintln(w, "  ]")
		fmt.Fprintln(w, "}")
	}
}

// checkFusermount looks up fusermount3 (preferred — fuse3) and falls
// back to fusermount (fuse2). At least one is needed for the lazy-
// unmount fallback we rely on for graceful shutdown and for any
// recovery from a wedged kernel mount. If neither is present we print
// a clear remediation hint and continue — root-only setups can still
// mount + unmount via direct syscalls, so this is a warning rather
// than a hard failure.
func checkFusermount() {
	for _, name := range []string{"fusermount3", "fusermount"} {
		if p, err := exec.LookPath(name); err == nil {
			fmt.Fprintf(os.Stderr, "fuse helper: %s\n", p)
			return
		}
	}
	fmt.Fprintln(os.Stderr, "warning: fusermount / fusermount3 not found in PATH.")
	fmt.Fprintln(os.Stderr, "         Install your distro's FUSE userspace package:")
	fmt.Fprintln(os.Stderr, "           Debian/Ubuntu: apt install fuse3   (or 'fuse' on older systems)")
	fmt.Fprintln(os.Stderr, "           Arch:          pacman -S fuse3")
	fmt.Fprintln(os.Stderr, "           Alpine:        apk add fuse3       (or 'fuse')")
	fmt.Fprintln(os.Stderr, "         TelFS uses it for the lazy-unmount fallback when shutdown stalls.")
}

// recoverPending scans the pending-uploads directory and submits any
// staged wires back to the packer. Each entry corresponds to a chunk
// whose previous daemon process died between PersistPending and the
// packer's chunk_map write — without this, those bytes would be
// silently dropped (the kernel page cache is gone after unmount).
//
// Runs in a goroutine so it doesn't delay the "Mounted at" message.
// Errors from individual entries are logged but don't abort the
// recovery — leftover entries will be retried on the next startup.
func recoverPending(ctx context.Context, dir string, packer *chunk.Packer, mstore *meta.Store) {
	entries, err := chunk.ScanPending(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pending: scan:", err)
		return
	}
	if len(entries) == 0 {
		return
	}
	// First pass: drop entries whose inode no longer exists. Those
	// are orphan wires from a chunk whose file was unlinked between
	// PersistPending and Packer.flush. Re-submitting them would
	// just fail at PutChunk (foreign-key violation against the
	// missing inode), leaving the .bin file on disk forever.
	// `gc` doesn't reach pending/ either, so without this step
	// these files accumulate across crashes.
	live := entries[:0]
	orphans := 0
	for _, e := range entries {
		if ctx.Err() != nil {
			return
		}
		if _, err := mstore.GetInode(ctx, e.Ino); err != nil {
			if errors.Is(err, meta.ErrNotFound) {
				_ = chunk.DeletePending(e.Path)
				orphans++
				continue
			}
			fmt.Fprintf(os.Stderr, "pending: probe ino %d: %v\n", e.Ino, err)
			continue
		}
		live = append(live, e)
	}
	if orphans > 0 {
		fmt.Printf("Cleaned %d orphan pending wire(s) (inode gone)\n", orphans)
	}
	if len(live) == 0 {
		return
	}
	fmt.Printf("Recovering %d pending upload(s) from %s…\n", len(live), dir)
	for _, e := range live {
		if ctx.Err() != nil {
			return
		}
		wire, err := os.ReadFile(e.Path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "pending: read %s: %v\n", e.Path, err)
			continue
		}
		// Submit blocks only on enqueue. The packer's flusher
		// processes the batch normally; on success it deletes the
		// pending file. We discard the result chan — fire-and-
		// forget is appropriate since this code path is background
		// recovery and the packer already logs flush errors to
		// stderr; a failed retry leaves the pending file in place
		// for the NEXT startup scan to retry again.
		ch := packer.Submit(e.Ino, e.Idx, e.PlaintextSize, wire, e.Path)
		go func() { <-ch }()
	}
}

// armForceExit installs a signal handler that force-exits the process
// on a SECOND SIGINT/SIGTERM. The first signal is consumed by the
// outer signal.NotifyContext (which triggers orderly shutdown);
// a second signal during shutdown means the user has lost patience
// and wants out — we lazy-unmount and os.Exit so the kernel mount
// isn't left orphaned.
func armForceExit(mountpoint string) {
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch // first one is the orderly-shutdown trigger (already in flight)
		<-ch // second one means "I mean it"
		fmt.Fprintln(os.Stderr, "telfs: second signal — forcing lazy unmount + exit")
		_ = exec.Command("fusermount", "-uz", mountpoint).Run()
		os.Exit(130)
	}()
}

// tryRecover opens a transient MTProto session, scans the channel for
// the most recent snapshot message, and restores it to cfg.DBPath().
// If the snapshot is encrypted, it prompts for the passphrase (or
// reads TELFS_PASSPHRASE), derives the key from the salt embedded in
// the envelope header, decrypts, and restores.
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

		// Encrypted envelope? Detect via magic, derive a key from the
		// embedded KDF state + passphrase, decrypt to the plaintext
		// gzipped DB.
		plaintext, err := maybeUnwrapEncrypted(data)
		if err != nil {
			return fmt.Errorf("decrypt snapshot: %w", err)
		}
		if err := snapshot.Restore(ctx, plaintext, cfg.DBPath()); err != nil {
			return err
		}
		fmt.Printf("Restored %d gzipped bytes → %s\n", len(plaintext), cfg.DBPath())
		// Channel-side journal replay: any mutations made between the
		// snapshot we just restored and the crash live as journal-op
		// messages on the channel. Replay them in seq order so the
		// recovered DB matches the pre-crash state to within the
		// poster's interval (~5s) rather than the snapshot's (~5min).
		if err := replayJournalSince(ctx, sess, cfg, latest.Caption.Seq, latest.Caption.FSUUID); err != nil {
			fmt.Fprintf(os.Stderr, "[journal] replay failed: %v — proceeding with snapshot-only state\n", err)
		}
		return nil
	})
}

// replayJournalSince loads journal-op messages from the channel whose
// seq is strictly greater than sinceSeq (the snapshot's high-water
// mark) and applies them in ascending order to the just-restored
// local DB. Failures on individual ops are logged and skipped — the
// snapshot is still authoritative, the journal just refines it.
func replayJournalSince(ctx context.Context, sess *tg.Session, cfg *config.Config, sinceSeq int64, fsUUID string) error {
	msgs, err := sess.ListJournalOps(ctx, fsUUID, sinceSeq, 0)
	if err != nil {
		return fmt.Errorf("list journal ops: %w", err)
	}
	if len(msgs) == 0 {
		return nil
	}
	// Sort ascending by seq for in-order application.
	sort.Slice(msgs, func(i, j int) bool {
		return msgs[i].Caption.Seq < msgs[j].Caption.Seq
	})
	mstore, err := meta.Open(cfg.DBPath())
	if err != nil {
		return fmt.Errorf("open meta: %w", err)
	}
	defer mstore.Close()
	fmt.Printf("Replaying %d journal op(s) since snapshot seq=%d…\n", len(msgs), sinceSeq)
	applied := 0
	for _, m := range msgs {
		op, err := meta.DecodeOp(m.Caption.Payload)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[journal] decode op seq=%d: %v\n", m.Caption.Seq, err)
			continue
		}
		if err := mstore.ReplayOp(ctx, op); err != nil {
			fmt.Fprintf(os.Stderr, "[journal] apply op seq=%d (%s): %v\n", m.Caption.Seq, op.Op, err)
			continue
		}
		applied++
	}
	fmt.Printf("Replayed %d/%d journal ops.\n", applied, len(msgs))
	return nil
}

// maybeUnwrapEncrypted returns the plaintext gzipped-DB bytes given
// what was downloaded from the channel. If the blob is a TFSE
// envelope, prompts for the passphrase, derives the key via Argon2id
// from the envelope's salt, verifies the canary (fail-fast on wrong
// passphrase), and decrypts. If the blob isn't wrapped, returns it
// as-is.
//
// Handles both modes:
//   - v1 envelopes: passphrase → key → cipher → open(body).
//   - v2 envelopes: passphrase → KEK → unwrap envelope.WrappedDEK →
//     DEK → cipher → open(body). Cold recovery is fully self-
//     contained — the envelope carries the wrapped DEK alongside the
//     KDF state, so no local meta_kv is needed.
func maybeUnwrapEncrypted(data []byte) ([]byte, error) {
	if !snapshot.IsWrapped(data) {
		return data, nil
	}
	hdr, body, err := snapshot.UnwrapHeaderAndBody(data)
	if err != nil {
		return nil, err
	}
	params, err := crypto.UnmarshalArgonParams(hdr.Argon)
	if err != nil {
		return nil, err
	}
	pass, err := readPassphrase("Encrypted snapshot — passphrase: ")
	if err != nil {
		return nil, err
	}
	defer zero(pass)
	derived := crypto.DeriveKey(pass, hdr.Salt, params)
	defer zero(derived)

	var cipher crypto.Cipher
	switch hdr.Mode {
	case "", crypto.ModeAESGCMv1:
		// Empty mode = v1; envelopes written by pre-v0.14 clients didn't
		// always set it. Treat as v1.
		cipher, err = crypto.NewAESGCM(derived)
		if err != nil {
			return nil, err
		}
	case crypto.ModeAESGCMv2, crypto.ModeAESGCMv3:
		if len(hdr.WrappedDEK) == 0 {
			return nil, fmt.Errorf("snapshot: %s envelope missing wrapped_dek", hdr.Mode)
		}
		dek, derr := crypto.UnwrapDEK(derived, hdr.WrappedDEK)
		if derr != nil {
			return nil, derr
		}
		defer zero(dek)
		cipher, err = cipherForMode(hdr.Mode, dek)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("snapshot: unsupported envelope mode %q (this binary may be older than the FS — upgrade telfs)", hdr.Mode)
	}

	if err := crypto.VerifyCanary(cipher, hdr.Canary); err != nil {
		return nil, err
	}
	plaintext, err := cipher.Open(0, -1, body)
	if err != nil {
		return nil, fmt.Errorf("decrypt body: %w", err)
	}
	return plaintext, nil
}

// cmdGC walks the channel and removes messages that are no longer
// referenced by the local meta DB. Identifies two classes of garbage:
//
//   - Orphan chunks: document messages with empty captions whose msg_id
//     isn't in chunk_map. These come from M4 overwrites/unlinks (we
//     deliberately don't delete inline) and from any crash that uploaded
//     a chunk but didn't update chunk_map.
//   - Stale snapshots: snapshot-caption messages other than the one
//     recorded in meta_kv[snap_msg_id]. Each cadence cycle leaves at
//     most one prior snapshot uncleaned in the worst case; this catches
//     accumulated leftovers from failed delete-on-supersede calls.
//
// Default behavior is a dry-run report. Pass --yes to actually delete.
func cmdGC(ctx context.Context, args []string) error {
	gcfs := flag.NewFlagSet("gc", flag.ContinueOnError)
	doDelete := gcfs.Bool("yes", false, "actually delete the orphans (default is dry-run)")
	pages := gcfs.Int("pages", 50, "max history pages to scan (each page = 100 messages)")
	if err := gcfs.Parse(args); err != nil {
		return err
	}

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

	referenced, err := metaStore.AllChunkMessageIDs(ctx)
	if err != nil {
		return err
	}
	var currentSnap int
	if v, err := metaStore.GetKV(ctx, snapshot.KVCurrentMsgID); err == nil {
		if id, err := strconv.Atoi(string(v)); err == nil {
			currentSnap = id
		}
	}

	client, err := tg.New(cfg)
	if err != nil {
		return err
	}
	return client.RunSession(ctx, func(ctx context.Context, sess *tg.Session) error {
		var orphanChunks, staleSnaps []int
		var totalChunks, totalSnaps int
		fmt.Printf("Scanning channel (up to %d pages of 100 messages)…\n", *pages)
		if err := sess.WalkChannelMessages(ctx, *pages, func(cm tg.ChannelMessage) error {
			switch cm.Kind {
			case tg.KindChunk:
				totalChunks++
				if _, ref := referenced[int64(cm.ID)]; !ref {
					orphanChunks = append(orphanChunks, cm.ID)
				}
			case tg.KindSnapshot:
				totalSnaps++
				if cm.ID != currentSnap {
					staleSnaps = append(staleSnaps, cm.ID)
				}
			}
			return nil
		}); err != nil {
			return err
		}

		fmt.Printf("\nChunks  in channel: %d   referenced in chunk_map: %d   orphan: %d\n",
			totalChunks, len(referenced), len(orphanChunks))
		fmt.Printf("Snapshots in channel: %d   current msg_id: %d   stale: %d\n",
			totalSnaps, currentSnap, len(staleSnaps))
		if len(orphanChunks)+len(staleSnaps) == 0 {
			fmt.Println("\nNothing to do.")
			return nil
		}

		toDelete := append([]int{}, orphanChunks...)
		toDelete = append(toDelete, staleSnaps...)
		fmt.Printf("\nOrphan chunk msg_ids:   %v\n", orphanChunks)
		fmt.Printf("Stale snapshot msg_ids: %v\n", staleSnaps)

		if !*doDelete {
			fmt.Println("\nDry-run. Re-run with --yes to delete.")
			return nil
		}

		// channels.deleteMessages accepts a batch; chunk into 100s.
		const batch = 100
		for i := 0; i < len(toDelete); i += batch {
			j := i + batch
			if j > len(toDelete) {
				j = len(toDelete)
			}
			if err := sess.DeleteChannelMessages(ctx, toDelete[i:j]...); err != nil {
				return err
			}
			fmt.Printf("Deleted %d/%d…\n", j, len(toDelete))
		}
		fmt.Printf("\nDone — %d messages deleted.\n", len(toDelete))
		// Housekeeping: the dedup index (chunk_blob) may now point at
		// freshly-deleted messages. Prune those rows so the next write
		// doesn't pay an aliveness-check + tx-rollback per stale hit.
		// Safe to skip on failure — ReuseChunkByHash re-checks aliveness
		// in-line anyway.
		if pruned, err := metaStore.PruneStaleChunkBlobs(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: prune chunk_blob index: %v\n", err)
		} else if pruned > 0 {
			fmt.Printf("Pruned %d stale dedup index entries.\n", pruned)
		}
		return nil
	})
}

func cmdDebug(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("debug: missing subcommand (seed-file, dump-msg)")
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
	case "dump-msg":
		if len(args) < 2 {
			return errors.New("debug dump-msg: usage: dump-msg <message-id>")
		}
		msgID, err := strconv.Atoi(args[1])
		if err != nil {
			return fmt.Errorf("debug dump-msg: invalid id %q", args[1])
		}
		return cmdDebugDumpMsg(ctx, msgID)
	default:
		return fmt.Errorf("debug: unknown subcommand %q", args[0])
	}
}

// cmdDebugDumpMsg downloads the document attached to a channel
// message and writes its raw bytes to stdout. Used in the M7 smoke
// test to assert that a chunk's on-the-wire bytes don't equal the
// plaintext source.
func cmdDebugDumpMsg(ctx context.Context, msgID int) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if err := cfg.RequireChannel(); err != nil {
		return err
	}
	client, err := tg.New(cfg)
	if err != nil {
		return err
	}
	return client.RunSession(ctx, func(ctx context.Context, sess *tg.Session) error {
		_, err := sess.DownloadDocument(ctx, msgID, os.Stdout)
		return err
	})
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

		chunkSize, err := metaStore.ChunkSize(ctx)
		if err != nil {
			return err
		}
		chunker := chunk.NewChunker(bytes.NewReader(data), int(chunkSize))
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
