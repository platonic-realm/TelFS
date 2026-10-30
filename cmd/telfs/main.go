// Command telfs mounts a Telegram channel as a FUSE filesystem.
//
// The commands and their flags are listed in the usage text below
// (`telfs help`); README.md documents them for users, and academy/
// explains the design chapter by chapter.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/term"

	"telfs/internal/chunk"
	"telfs/internal/config"
	"telfs/internal/control"
	"telfs/internal/crypto"
	telfsfs "telfs/internal/fs"
	"telfs/internal/meta"
	"telfs/internal/metrics"
	"telfs/internal/mountctl"
	"telfs/internal/secret"
	"telfs/internal/sessionstore"
	"telfs/internal/snapshot"
	"telfs/internal/tg"
	"telfs/internal/trash"
	"telfs/internal/wire"
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
 Setup
  telfs login                       Authenticate against Telegram (interactive
                                      MTProto phone → code → 2FA). The first
                                      login SETS the profile's secret: the
                                      session is stored encrypted (session.enc).
  telfs channel list                List your channels (id, title, post permission).
  telfs channel set [--access-hash N] [--force] <id>
                                    Configure the backing channel.
                                      --access-hash overrides the automatic
                                      dialog-list discovery; --force rebinds a
                                      profile that already has data (dangerous).
  telfs channel info                Show the currently configured channel.
  telfs channel ping                Round-trip a test message (smoke test).
  telfs init [--chunk-size N] [--compression none|zstd] [--kdf-memory SIZE] [--kdf-time N]
                                    Create a NEW filesystem (format 2: always
                                      encrypted) on the bound channel; posts and
                                      pins its key message and the first anchor,
                                      prints the key export.
  telfs profile {list, show, create, delete, use, reset-auth, export, import}
                                    Manage multiple profiles (accounts/channels).
                                      Each profile is ONE identity; reset-auth
                                      wipes it (session + channel binding).

 Mount and service
  telfs mount [flags] <mountpoint>  Mount the filesystem (foreground daemon). A
                                      missing local DB is recovered from the
                                      channel (pinned key message + anchor).
      --readonly                    Write nothing to the channel (drills).
      --readonly --version NAME     Mount a version instead of the live tree;
                                      may run next to the live mount.
      --service                     systemd Type=notify: never prompts, waits
                                      and retries while the network/Telegram
                                      is unreachable, READY=1 once mounted,
                                      STOPPING=1 on stop.
      --key-header FILE             Recover with a key export (missing DB only).
      --metrics-addr ADDR           Also serve Prometheus /metrics over TCP
                                      (off by default).
      --allow-other                 Let other users in; the kernel checks the
                                      stored owners and modes
                                      (--no-default-permissions: no checks).
      --debug --no-recover
      --cache-cap --mem-cache-cap --prefetch-window --prefetch-concurrency
      --blob-prefetch               Cache and read tuning (telfs mount -h).
  telfs unmount [--timeout D] <mountpoint|profile>
                                    Stop the mount daemon and return when it
                                      has exited (asked over its control
                                      socket; default --timeout 2m). 5 = not
                                      mounted.

 Talking to a running mount (control socket <profile>/daemon.sock; no secret)
  telfs status [--json [--version NAME]]
                                    One-screen summary of the active profile;
                                      --json: one object (schema 1) for scripts,
                                      mounted or not; --version: that version's
                                      mount (5 if not mounted).
  telfs sync [--wait] [--timeout D] 0 when everything written through the mount
                                      (with --wait: before the call) is on the
                                      channel; 4 not (yet) / timeout, 5 not
                                      mounted. Format 2.
  telfs versions create NAME [--timeout D]
                                    Sync, then post a named, immutable version
                                      (format 2); 0 once on the channel, 4 sync
                                      timeout (nothing posted), 5, 6 Telegram.
  telfs versions list [--json]      The registry (from the mount, else local).
  telfs versions delete NAME        Delete a version (refused while mounted).
  telfs upload limit [RATE]         Show or change the running daemon's
                                      upload cap (e.g. 3MB/s, 0 = no cap)
                                      until it stops; [upload] max_rate
                                      applies again at the next start.
  telfs fsck --quick [--json]       Format 2: every referenced message exists
                                      with its document and every packed extent
                                      fits — nothing downloaded, 100 ids per
                                      request. Without a mount: own read-only
                                      session (secret). 0 ok, 6 Telegram,
                                      7 findings (first 20 on stderr).

 Restore and recovery
  telfs restore [--version NAME] [--workers N] SRC DEST
                                    Copy SRC (a path in the filesystem; a
                                      directory's contents) into the local
                                      directory DEST, from the live filesystem
                                      or a version: each message downloaded
                                      once (default 4 workers), every file
                                      re-read and checked against its keyed
                                      hashes; a re-run skips what is already
                                      right. 0 ok, 4 data the mount had not
                                      uploaded yet, 6 Telegram, 7 integrity
                                      (list: <profile>/restore-<time>.tsv).
  telfs recover --from-captions [--journal use|ignore] [--report FILE] [--key-header FILE]
                                    Rebuild the local DB from captions, pack
                                      indexes and journal — no snapshot needed.
  telfs snapshot {list [--max N], restore [--yes] <msg-id>}
                                    Inspect / roll back to a historical snapshot.

 Keys and secrets
  telfs secret test                 Check the profile's secret (exit 0 ok,
                                      2 source failed, 3 wrong secret).
  telfs secret change --new-from-prompt | --new-file PATH | --new-entry ENTRY
                                    Re-wrap the FS key + re-encrypt the session
                                      under a new secret (no data re-encrypted).
  telfs session encrypt             Convert a v0.22 plaintext session.json.
  telfs key export [--out FILE]     Print the key header export (keep it with
                                      the secret: KeePassXC entry, break-glass kit).
  telfs encrypt status              Show the filesystem's encryption state.

 Maintenance
  telfs fsck [--fix] [--stop-after N]
                                    Download and verify every chunk_map row;
                                      format 2: also the pinned key header.
  telfs gc [--yes] [--pages N] [--min-age D]
                                    Reclaim orphan chunks + old snapshots
                                      (dry run without --yes; refuses while
                                      mounted and on a filesystem with versions).
  telfs doctor                      Lint local profile + DB state for known invariants.
  telfs trash {enable [--ttl D], disable, status, list, empty}
                                    Manage the rm safety-net at /.trash.
  telfs ratelimit {show, set <N>, clear, mode adaptive|manual}
                                    View or change the TG requests/min caps
                                      (telfs ratelimit help); a running daemon
                                      picks a change up within 5 s.
  telfs web [--listen ADDR] [--token TOKEN] [--tls-cert F --tls-key F]
                                    HTTP management UI (default 127.0.0.1:8080).
  telfs bench [flags] <dir>         Micro-benchmark IO scenarios (stat storms,
                                      small files, streaming, random reads)
                                      against a directory — a TelFS mountpoint,
                                      or any local dir for a baseline. See
                                      telfs bench --help for the scenario list.
  telfs version                     Print the program version (also -v,
                                      --version); takes no arguments.

Global flags (anywhere on the line):
  --secret-from prompt|file|keepassxc
                   Read the secret from this source for one command
                   (the [secret] table of config.toml names the default).

Exit codes (every command):
  0 ok              1 usage or other error     2 secret source unavailable
  3 wrong secret    4 timeout / not synced yet 5 not mounted (no daemon answers)
  6 Telegram or the channel unreachable or refused (after the retries)
  7 integrity problem (fsck findings, recovery refused, restore mismatch)

config.toml (per profile; see README "Configuration"):
  [secret]   source = "prompt"|"file"|"keepassxc", file, entry
             (file = "${CREDENTIALS_DIRECTORY}/<name>" for systemd credentials)
  [staging]  dir, max_bytes (default 50GB), min_free (default 5GiB)
  [snapshot] every_ops (default 500000), every (default 24h)   — format 2
  [upload]   connections (default 8), parts_per_upload (default 2),
             max_rate (default 5MB/s)
  [read]     offline_timeout (default 60s): an uncached read with no
             progress that long fails with EIO ("0" = wait)

Environment:
  TELFS_PROFILE    Active profile name (overrides ~/.config/telfs/active)
  TELFS_API_ID     Telegram API ID (https://my.telegram.org/apps)
  TELFS_API_HASH   Telegram API hash
  TELFS_PHONE      Phone number for login (prompted if unset)
  TELFS_DC         Starting datacenter override (default: gotd's DC 2)
  TELFS_PASSPHRASE The secret of a prompt-source profile (skips the prompt).
                   Removed from TelFS's environment at start: no process
                   TelFS starts ever inherits it. Prefer a file or KeePassXC.
`

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "telfs:", err)
		os.Exit(exitCode(err))
	}
}

func run() error {
	args, err := extractGlobalFlags(os.Args)
	if err != nil {
		return err
	}
	os.Args = args
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	return dispatch(ctx, os.Args)
}

// dispatch runs the command named by args[1]. Usage errors return an
// error (exit 1, the v1 interface's "usage / other error"; they used to
// exit 2, which v1 gives to "secret source unavailable").
func dispatch(ctx context.Context, args []string) error {
	if len(args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		return errors.New("missing command (usage above)")
	}
	switch args[1] {
	case "-h", "--help", "help":
		fmt.Print(usage)
		return nil
	case "version", "-v", "--version":
		// The PROGRAM version only. Anything after it is an error, so a
		// script that typed `telfs version create x` (filesystem
		// versions are `telfs versions …`) fails instead of printing
		// the program version and exiting 0.
		if len(args) > 2 {
			return fmt.Errorf("`telfs %s` prints the program version and takes no arguments (filesystem versions: telfs versions create|list|delete)", args[1])
		}
		fmt.Printf("telfs %s (commit %s, built %s)\n", version, commit, buildDate)
		return nil
	case "versions":
		return cmdVersions(ctx, args[2:])
	case "sync":
		return cmdSync(ctx, args[2:])
	case "upload":
		return cmdUpload(ctx, args[2:])
	case "login":
		return cmdLogin(ctx, args[2:])
	case "channel":
		return cmdChannel(ctx, args[2:])
	case "mount":
		return cmdMount(ctx, args[2:])
	case "unmount", "umount":
		return cmdUnmount(ctx, args[2:])
	case "init":
		return cmdInit(ctx, args[2:])
	case "encrypt":
		return cmdEncrypt(ctx, args[2:])
	case "profile":
		return cmdProfile(ctx, args[2:])
	case "status":
		return cmdStatus(ctx, args[2:])
	case "web":
		return cmdWeb(ctx, args[2:])
	case "fsck":
		return cmdFsck(ctx, args[2:])
	case "gc":
		return cmdGC(ctx, args[2:])
	case "snapshot":
		return cmdSnapshot(ctx, args[2:])
	case "trash":
		return cmdTrash(ctx, args[2:])
	case "debug":
		return cmdDebug(ctx, args[2:])
	case "doctor":
		return cmdDoctor(ctx, args[2:])
	case "ratelimit":
		return cmdRateLimit(ctx, args[2:])
	case "bench":
		return cmdBench(ctx, args[2:])
	case "secret":
		return cmdSecret(ctx, args[2:])
	case "session":
		return cmdSession(ctx, args[2:])
	case "key":
		return cmdKey(ctx, args[2:])
	case "recover":
		return cmdRecover(ctx, args[2:])
	case "restore":
		return cmdRestore(ctx, args[2:])
	default:
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("unknown command %q (usage above)", args[1])
	}
}

func cmdLogin(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("login", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if cfg.RequireAPI() != nil {
		// Ask for the app credentials once (they identify the
		// application, not the account: my.telegram.org/apps).
		if err := promptAPI(cfg); err != nil {
			return err
		}
	}
	if cfg.HasLegacySession() {
		return sessionstore.ErrLegacy
	}
	// The first login of a profile SETS its secret (session.enc is
	// created with it): ask twice for a typed one and warn if short.
	purpose := secret.Unlock
	if !cfg.HasSession() {
		purpose = secret.SetNew
	}
	sec, err := readSecret(ctx, cfg, purpose)
	if err != nil {
		return err
	}
	defer sec.Zero()
	if purpose == secret.SetNew {
		secret.WarnIfWeak(os.Stderr, sec)
	}
	client, err := newClientWith(cfg, sec)
	if err != nil {
		return err
	}
	// Login records the authenticated identity into cfg and saves it
	// on success.
	return client.Login(ctx)
}

// promptAPI asks for api_id and api_hash on the terminal and saves them
// to the profile's config.toml.
func promptAPI(cfg *config.Config) error {
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return cfg.RequireAPI()
	}
	fmt.Println("This profile has no Telegram API credentials yet — get the pair once at https://my.telegram.org/apps.")
	r := bufio.NewReader(os.Stdin)
	fmt.Print("api_id: ")
	idLine, _ := r.ReadString('\n')
	id, err := strconv.Atoi(strings.TrimSpace(idLine))
	if err != nil || id <= 0 {
		return errors.New("api_id must be a positive number")
	}
	fmt.Print("api_hash: ")
	hash, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Println()
	if err != nil || len(strings.TrimSpace(string(hash))) == 0 {
		return errors.New("api_hash is required")
	}
	cfg.APIID, cfg.APIHash = id, strings.TrimSpace(string(hash))
	if err := cfg.Save(); err != nil {
		return err
	}
	fmt.Printf("Saved to %s.\n", cfg.ConfigPath())
	return nil
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
	client, err := newClient(ctx, cfg)
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
	accessHash := fs.Int64("access-hash", 0, "explicit channel access_hash — overrides the automatic dialog-list discovery")
	force := fs.Bool("force", false, "rebind even though this profile already has data on its current channel (DANGEROUS: reads break, writes/snapshots split across channels, and gc against the wrong channel deletes other data)")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: telfs channel set [--access-hash N] [--force] <channel-id>")
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
	// Rebind guard: once chunk_map references messages, they live in
	// the CURRENT channel — every tg_message_id resolves against
	// whatever channel is bound. Rebinding makes all reads dangle,
	// splits new writes/snapshots across channels, and turns `gc`
	// into a weapon against whatever the new channel holds.
	if !*force && cfg.Channel.ID != 0 && cfg.Channel.ID != id {
		if _, statErr := os.Stat(cfg.DBPath()); statErr == nil {
			if mstore, merr := meta.Open(cfg.DBPath()); merr == nil {
				has, rerr := mstore.HasAnyChunks(ctx)
				mstore.Close()
				if rerr == nil && has {
					return fmt.Errorf("channel set: this profile has data in the currently-bound channel (id=%d) — rebinding would orphan all of it.\nIf you really mean it, re-run with --force. To use a new channel cleanly, create a new profile instead", cfg.Channel.ID)
				}
			}
		}
	}
	client, err := newClient(ctx, cfg)
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
	client, err := newClient(ctx, cfg)
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
	compression := fs.String("compression", "", "chunk-payload compression codec: none (default), zstd")
	kdfMemory := fs.String("kdf-memory", "", "Argon2id memory for the filesystem key (default 2GiB, RFC 9106's first choice); lower only for machines without the RAM")
	kdfTime := fs.Uint("kdf-time", 0, "Argon2id passes (default 1 with 2 GiB)")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: telfs init [--chunk-size BYTES] [--compression none|zstd] [--kdf-memory SIZE] [--kdf-time N]")
		fmt.Fprintln(os.Stderr, "Creates a NEW filesystem (format 2: always encrypted) on the profile's bound channel.")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := meta.ValidateChunkSize(*chunkSize); err != nil {
		return err
	}
	params := crypto.DefaultArgonParamsV2()
	if *kdfMemory != "" {
		n, err := config.ParseHumanBytes(*kdfMemory)
		if err != nil || n < 64<<20 {
			return fmt.Errorf("--kdf-memory %q: want a size of at least 64MiB", *kdfMemory)
		}
		params.Memory = uint32(n >> 10)
		if *kdfTime == 0 && n < 1<<30 {
			params.Time = 3 // RFC 9106's low-memory option uses 3 passes
		}
	}
	if *kdfTime > 0 {
		params.Time = uint32(*kdfTime)
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if err := cfg.RequireChannel(); err != nil {
		return err
	}
	if !cfg.HasSession() {
		return errors.New("init: log in first (`telfs login`)")
	}
	// Setting up a filesystem SETS what its key is wrapped under: the
	// profile's secret (the same one that opens session.enc).
	sec, err := readSecret(ctx, cfg, secret.Unlock)
	if err != nil {
		return err
	}
	defer sec.Zero()
	secret.WarnIfWeak(os.Stderr, sec)
	client, err := newClientWith(cfg, sec)
	if err != nil {
		return err
	}
	err = client.RunSession(ctx, func(ctx context.Context, sess *tg.Session) error {
		return init2(ctx, cfg, sec, sess, *chunkSize, *compression, params)
	})
	if err != nil {
		return err
	}
	m, err := meta.Open(cfg.DBPath())
	if err != nil {
		return err
	}
	defer m.Close()
	h, _, err := keyHeaderOf(ctx, m)
	if err != nil {
		return err
	}
	fmt.Println()
	fmt.Println("Store this key header export with the filesystem's secret (its KeePassXC entry")
	fmt.Println("and your break-glass kit). Without the secret it opens nothing; with it, it is")
	fmt.Println("the whole key. `telfs key export` prints it again.")
	fmt.Println()
	fmt.Print(wire.Export(h, exportInfo(ctx, cfg, m)))
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

// describeUpload is [upload] in words, for the mount's startup lines.
func describeUpload(u config.UploadSettings) string {
	conns := "the main connection only"
	if u.Connections > 0 {
		parts := "parts"
		if u.PartsPerUpload == 1 {
			parts = "part"
		}
		conns = fmt.Sprintf("%d connections, %d %s in flight per upload", u.Connections, u.PartsPerUpload, parts)
	}
	capText := "no rate cap"
	if u.MaxRate > 0 {
		capText = "capped at " + strconv.FormatFloat(math.Round(float64(u.MaxRate)/1e4)/100, 'f', -1, 64) + " MB/s"
	}
	return conns + ", " + capText
}

// cmdUnmount stops a running telfs mount daemon and detaches its
// mountpoint. Accepts either the mountpoint path or a profile name.
// Works regardless of who started the daemon (CLI foreground, web
// supervisor, or a process orphaned by a dead session): the daemon is
// located via /proc, SIGTERMed so its orderly shutdown (upload drain +
// final journal post; format 1: final snapshot) runs, and the kernel
// mount is verified gone afterward — escalating to SIGKILL + lazy
// unmount only if it wedges.
func cmdUnmount(_ context.Context, args []string) error {
	fs := flag.NewFlagSet("unmount", flag.ContinueOnError)
	timeout := fs.Duration("timeout", 2*time.Minute, "how long to wait for the daemon to finish its orderly shutdown and exit")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: telfs unmount [--timeout D] <mountpoint | profile-name>")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return errors.New("unmount: expected exactly one <mountpoint | profile-name> argument")
	}
	target := fs.Arg(0)

	// Resolve to (pid, mountpoint): try as a mountpoint path first,
	// then as a profile name (the mount source "telfs:<profile>", or a
	// v0.22 daemon's TELFS_PROFILE) — its live mount; a version mount
	// is unmounted by its mountpoint.
	var mountpoint string
	var pid int
	if abs, err := filepath.Abs(target); err == nil && mountctl.IsMounted(abs) {
		// A bind view of a mount (S9: /mnt/telfs) is no daemon's mount:
		// stopping "its" daemon would take the mount away everywhere.
		if primary, view := mountctl.Views(mountctl.ScanMounts())[abs]; view {
			return fmt.Errorf("unmount: %s is a view (bind mount) of the TelFS mount at %s — unmount the view with umount, or stop the filesystem with `telfs unmount %s`", abs, primary, primary)
		}
		mountpoint = abs
		pid, _ = mountctl.FindDaemonPid(abs)
	} else if p, mp, ok, ferr := mountctl.FindLiveByProfile(target); ok {
		pid, mountpoint = p, mp
	} else if ferr != nil && config.ValidateProfileName(target) == nil {
		return fmt.Errorf("unmount: %q: %w", target, ferr)
	} else {
		mounts := mountctl.ScanProcMounts()
		if len(mounts) == 0 {
			return fmt.Errorf("unmount: %q is not a fuse.telfs mountpoint or a mounted profile (no telfs mounts are active): %w", target, errNotMounted)
		}
		return fmt.Errorf("unmount: %q is not a fuse.telfs mountpoint or a mounted profile: %w\nactive telfs mounts: %s", target, errNotMounted, strings.Join(mounts, ", "))
	}

	// A v1 daemon is asked over its control socket and the command waits
	// for THAT process to exit (no SIGKILL): "unmounted" means the daemon
	// is gone, not just the FUSE mount (spec v1 §3).
	if done, err := unmountViaControl(mountpoint, *timeout); done {
		if err != nil {
			return err
		}
		fmt.Printf("Unmounted %s\n", mountpoint)
		return nil
	}
	if pid > 0 {
		fmt.Printf("Stopping mount daemon (pid %d) for %s — waiting for orderly shutdown…\n", pid, mountpoint)
	} else {
		fmt.Printf("No daemon process found for %s — detaching the mount directly.\n", mountpoint)
	}
	if err := mountctl.StopDaemon(pid, mountpoint, 35*time.Second); err != nil {
		return fmt.Errorf("unmount: %w", err)
	}
	fmt.Printf("Unmounted %s\n", mountpoint)
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
func cmdMount(signalCtx context.Context, args []string) error {
	fs := flag.NewFlagSet("mount", flag.ContinueOnError)
	readonly := fs.Bool("readonly", false, "mount read-only (rejects all write ops with EROFS)")
	allowOther := fs.Bool("allow-other", false, "allow other users to access the mount (FUSE -o allow_other; requires user_allow_other in /etc/fuse.conf); the kernel then checks every access against the stored owners and modes (default_permissions)")
	noDefaultPerms := fs.Bool("no-default-permissions", false, "with --allow-other: do NOT let the kernel check owners and modes — every local user reads and writes everything (the behaviour before v1.1)")
	debug := fs.Bool("debug", false, "log every FUSE op")
	noRecover := fs.Bool("no-recover", false, "never recover from the channel: with no local DB the mount fails instead (a filesystem is created only by telfs init)")
	metricsAddr := fs.String("metrics-addr", "", "also serve Prometheus /metrics (and /telegram, /cache) over TCP at this address, e.g. 127.0.0.1:9090. Off by default: status, sync and the web console use the profile's owner-only control socket.")
	cacheCap := fs.String("cache-cap", "", "override the LRU disk-cache size cap (e.g. 8GiB, 512MiB, 2GB). Flag wins over config; empty falls through to config.toml's cache_cap; that falls through to 1 GiB.")
	prefetchWindow := fs.Int("prefetch-window", 0, "how many chunks past the current read cursor to speculatively warm (0 = default 8). Bigger = more parallel reads in flight for sequential streams; smaller = less wasted bandwidth on random reads.")
	prefetchConcurrency := fs.Int("prefetch-concurrency", 0, "max prefetchOne goroutines (0 = default 4). Each one can issue 4 concurrent upload.getFile RPCs through the parallel ranged fetcher.")
	memCacheCap := fs.String("mem-cache-cap", "", "in-RAM hot-tier cache size (e.g. 128MiB, 1GiB, 0 to disable). Empty falls through to 128 MiB. Reads hit the mem tier first, skipping os.ReadFile on hot chunks.")
	blobPrefetch := fs.String("blob-prefetch", "", "packed-blob locality prefetch span (e.g. 4MiB): a cold read of one packed small file warms its blob neighbors in one ranged download. Empty = default 4 MiB; 0 disables.")
	serviceFlag := fs.Bool("service", false, "run as a systemd service (Type=notify): never prompt (the secret comes from a file or systemd credentials), wait and retry while the network or Telegram is unreachable at boot, report READY/STATUS/STOPPING to systemd")
	keyHeader := fs.String("key-header", "", "recover a format-2 filesystem with this key header export (telfs key export) instead of the channel's pinned key message; only used when the local DB is missing")
	versionName := fs.String("version", "", "with --readonly: mount this version of the filesystem (telfs versions list) instead of the live tree — its own copy of the version's DB in the profile's versions/ directory; may run next to the live mount")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: telfs mount [--readonly [--version NAME]] [--allow-other] [--debug] [--metrics-addr ADDR] [--cache-cap SIZE] <mountpoint>")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	// Was --metrics-addr given explicitly? An explicit addr that can't
	// bind fails the mount loudly (the operator asked for it); the
	// on-by-default addr degrades to a warning instead, so a second
	// concurrent daemon (another profile) doesn't die on the port
	// already being owned by the first.
	metricsExplicit := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "metrics-addr" {
			metricsExplicit = true
		}
	})
	if fs.NArg() != 1 {
		fs.Usage()
		return errors.New("mount: expected exactly one mountpoint argument")
	}
	if *versionName != "" {
		switch {
		case !*readonly:
			return errors.New("mount: --version needs --readonly (a version is immutable)")
		case *keyHeader != "":
			return errors.New("mount: --key-header recovers the live filesystem's DB; it does not apply to --version")
		case *metricsAddr != "":
			return errors.New("mount: --metrics-addr is not available with --version")
		}
		if err := snapshot.ValidateVersionName(*versionName); err != nil {
			return fmt.Errorf("mount: %w", err)
		}
	}
	mountpoint := fs.Arg(0)
	// Normalize to an absolute path: /proc/mounts records the absolute
	// mountpoint, and every is-it-still-mounted check below compares
	// against that.
	if abs, err := filepath.Abs(mountpoint); err == nil {
		mountpoint = abs
	}
	if os.Args[len(os.Args)-1] != mountpoint {
		// The command line must name the mountpoint exactly (see
		// reexecAbsoluteMount); does not return on success.
		if err := reexecAbsoluteMount(mountpoint); err != nil {
			return err
		}
	}
	checkFusermount()

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if err := cfg.RequireChannel(); err != nil {
		return err
	}
	if *versionName != "" {
		return mountVersion(signalCtx, cfg, versionMountOpts{name: *versionName, mountpoint: mountpoint,
			allowOther: *allowOther, defaultPerms: ownerChecks(*allowOther, *noDefaultPerms), debug: *debug, service: *serviceFlag, cacheCap: *cacheCap,
			memCacheCap: *memCacheCap, blobPrefetch: *blobPrefetch,
			prefetchWindow: *prefetchWindow, prefetchConcurrency: *prefetchConcurrency})
	}
	// [snapshot] (format 2) and [upload]: checked before anything slow
	// happens.
	snapEveryOps, snapEvery, err := cfg.SnapshotPolicy()
	if err != nil {
		return err
	}
	uploadCfg, err := cfg.UploadSettings()
	if err != nil {
		return err
	}
	offlineTimeout, err := cfg.ReadOfflineTimeout()
	if err != nil {
		return err
	}

	// Single-writer guard (this machine): one daemon per profile, and no
	// second read-write mount of a filesystem already mounted — checked
	// by profile now, by fs_uuid once the DB is open. The profile lock
	// comes first: it closes the race of two mounts starting together,
	// and — released by the kernel when a daemon ends, however it ends —
	// holding it proves no daemon of this profile is alive, so this
	// profile's mounts the kernel reports dead are our own leftovers.
	lock, err := lockProfile(cfg)
	if err != nil {
		return err
	}
	defer lock.Close()
	clearDeadMounts(cfg.ProfileName(), mountpoint)
	if c := mountctl.WriterConflict(cfg.ProfileName(), "", *readonly); c != "" {
		return fmt.Errorf("mount: %s", c)
	}

	// --key-header recovers a MISSING local DB; with a DB present it
	// would be silently ignored, so say so before the slow secret read.
	_, statErr := os.Stat(cfg.DBPath())
	dbMissing := errors.Is(statErr, os.ErrNotExist)
	if *keyHeader != "" && !dbMissing {
		return fmt.Errorf("--key-header recovers a missing local DB, but %s exists — move it away first (or mount without --key-header)", cfg.DBPath())
	}

	noPrompt = *serviceFlag
	svc := newService(*serviceFlag)
	go svc.keepStarting(signalCtx)
	svc.status("starting")

	// The profile's secret, read ONCE: it opens session.enc (Argon2id
	// #1) and, below, the filesystem key (Argon2id #2 on format 2, the
	// passphrase of an encrypted format-1 FS). Zeroed as soon as both
	// are done.
	sec, err := readSecret(signalCtx, cfg, secret.Unlock)
	if err != nil {
		return err
	}
	defer sec.Zero()
	client, err := newClientWith(cfg, sec)
	if err != nil {
		return err
	}

	// Cold-mount recovery: if the local DB is missing, restore it from
	// the channel. A channel with no TelFS filesystem is an ERROR now
	// (decision N3): a filesystem is created only by `telfs init`.
	if dbMissing {
		if *noRecover {
			return errors.New("--no-recover: there is no local DB to mount — run `telfs init` to create a filesystem, or mount without --no-recover to restore one from the channel")
		}
		svc.status("recovering the filesystem from the channel")
		attempt := 0
		err := svc.retryAtBoot(signalCtx, "recovery", func(ctx context.Context) error {
			if attempt++; attempt > 1 {
				// The DB was missing at the start: whatever a failed
				// attempt left behind goes before the next one.
				for _, sfx := range []string{"", "-wal", "-shm"} {
					_ = os.Remove(cfg.DBPath() + sfx)
				}
			}
			return tryRecover(ctx, cfg, client, sec, *keyHeader, *readonly)
		})
		if err != nil {
			if retriable(err) {
				err = fmt.Errorf("%w: %w", errTelegram, err)
			}
			return fmt.Errorf("recovery: %w", err)
		}
	}

	// A recovery this mount refuses (checkRecoveredDB below) leaves no
	// DB behind. Registered before the store's Close so it runs after it.
	dropRestored := false
	defer func() {
		if dropRestored {
			for _, sfx := range []string{"", "-wal", "-shm"} {
				_ = os.Remove(cfg.DBPath() + sfx)
			}
		}
	}()
	metaStore, err := meta.Open(cfg.DBPath())
	if err != nil {
		return err
	}
	defer metaStore.Close()
	if !dbMissing {
		if blank, err := neverUsedDB(signalCtx, metaStore); err != nil {
			return err
		} else if blank {
			return fmt.Errorf("%s is not a filesystem: no `telfs init` and no recovery made it (empty, never snapshotted — something opened a missing DB, which creates one). Mounting it would post an empty filesystem to the channel. Move it away, then mount again to restore this profile's filesystem from the channel, or run `telfs init` to create a new one", cfg.DBPath())
		}
	}
	fsUUID, err := metaStore.FSUUID(signalCtx)
	if err != nil {
		return err
	}
	if c := mountctl.WriterConflict(cfg.ProfileName(), fsUUID, *readonly); c != "" {
		return fmt.Errorf("mount: %s", c)
	}

	// Startup repair: a previous daemon may have died mid-upload
	// (FLOOD_WAIT, kill, OOM, etc.) and left files whose chunk_map
	// has gaps. Truncate each such inode to its largest contiguous
	// prefix BEFORE the FUSE server starts so user-visible reads
	// don't EIO on the gap. Idempotent; cheap on a clean FS.
	//
	// Crucially, repair must know about wires still staged in the
	// pending dir: recoverPending (later in startup) re-submits them
	// and their chunk_map rows land shortly after mount. Repairing
	// through such a gap would truncate away fully-recoverable data —
	// which is exactly what the crash left behind.
	stagedSet := make(map[int64]map[int32]bool)
	if entries, err := chunk.ScanPending(cfg.StagingDir()); err == nil {
		for _, e := range entries {
			if stagedSet[e.Ino] == nil {
				stagedSet[e.Ino] = make(map[int32]bool)
			}
			stagedSet[e.Ino][e.Idx] = true
		}
	}
	stagedChecker := func(ino int64, idx int32) bool { return stagedSet[ino][idx] }
	// Format 2, just recovered: the repair may not shorten anything —
	// refuse the mount instead, naming the files (see repair.go).
	if dbMissing {
		if f, _ := metaStore.Format(signalCtx); f == 2 {
			if err := checkRecoveredDB(signalCtx, metaStore, os.Stderr, cfg.DataDir, stagedChecker); err != nil {
				dropRestored = true
				return fmt.Errorf("recovery: %w: %w", errIntegrity, err)
			}
		}
	}
	if repaired, err := metaStore.RepairPartialFiles(signalCtx, stagedChecker); err != nil {
		fmt.Fprintln(os.Stderr, "[repair] warning:", err)
	} else if len(repaired) > 0 {
		// Never shorten a file without saying so: count, the first
		// paths here, every path in the report file.
		if cut := shortened(repaired); len(cut) > 0 {
			fmt.Fprintf(os.Stderr, "[repair] %d file(s) left inconsistent by an earlier crash were cut to the part that reached the channel; the rest of their content is NOT on the channel:\n", len(cut))
			if path, err := writeRepairReport(signalCtx, metaStore, os.Stderr, cfg.DataDir, "repair", cut); err != nil {
				fmt.Fprintln(os.Stderr, "[repair] report not written:", err)
			} else {
				fmt.Fprintln(os.Stderr, "[repair] every file:", path)
			}
		}
		if n := len(repaired) - len(shortened(repaired)); n > 0 {
			fmt.Printf("[repair] dropped stray chunk rows past the end of %d file(s) (no data lost)\n", n)
		}
	}

	// Seed lifetime Telegram stats from the profile's DB so /telegram
	// totals continue across restarts instead of resetting to zero.
	// Persisted back periodically (and at shutdown) below.
	if b, err := metaStore.GetKV(signalCtx, tg.KVLifetimeStats); err == nil {
		if serr := client.Tracker().SeedState(b); serr != nil {
			fmt.Fprintln(os.Stderr, "[stats] seed lifetime stats:", serr)
		}
	}
	var pipeCounters *wire.Counters // set once the pipeline is open
	persistStats := func(ctx context.Context) {
		persistUses(ctx, metaStore, pipeCounters)
		if b, err := client.Tracker().MarshalState(); err == nil {
			// Uncounted: this fires every 60 s and is telemetry, not FS
			// content — routing it through PutKV made the snapshot
			// change-detector see a permanently dirty DB (idle daemons
			// re-uploaded identical snapshots forever, and the channel-
			// sync indicator could never report clean).
			_ = metaStore.PutKVUncounted(ctx, tg.KVLifetimeStats, b)
		}
	}

	// The gotd session must stay alive past signalCtx cancellation so
	// the final journal post (format 1: the final snapshot) can go out.
	// We give RunSession its own Background-derived context and let our
	// callback drive shutdown in the correct order (unmount FUSE → drain
	// → final post → return → gotd shuts down).
	// Resolve the cipher up front: plaintext (NoopCipher) unless this
	// FS was set up with `telfs encrypt init` (crypto_mode set in
	// meta_kv). For the encrypted case we prompt for the passphrase
	// here and refuse to mount if the canary doesn't decrypt.
	pipe, err := openPipeline(signalCtx, metaStore, sec)
	if err != nil {
		return err
	}
	sec.Zero() // both derivations are done
	pipeCounters = pipe.counters
	cipher := pipe.cipher
	if pipe.format == 2 {
		fmt.Fprintln(os.Stderr, "filesystem format: 2 (encrypted chunks, captions, snapshots and journal)")
	} else if cipher == nil || isNoop(cipher) {
		fmt.Fprintln(os.Stderr, "filesystem format: 1 — UNENCRYPTED: this profile's secret protects only the Telegram session, not your files")
	} else {
		fmt.Fprintln(os.Stderr, "filesystem format: 1 (encrypted chunks and snapshots; journal and names on the channel in clear)")
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
	codec := pipe.codec

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
	svc.status("connecting to Telegram")
	mgr := client.NewSessionManager()
	sess := mgr.Session()
	// --readonly never writes to the channel: the session itself refuses
	// every upload, post, deletion and pin (and below, the snapshot
	// manager, journal poster and staged-upload recovery don't start).
	sess.SetReadOnly(*readonly)
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
	// Boot without internet (v1.1 I2): a filesystem whose DB passed the
	// channel check at an earlier mount mounts at once — local data is
	// served while the network is still away; the first connection and
	// the check run in the background, and a definite refusal there stops
	// the daemon (exit 6), as it used to refuse the mount. A first mount,
	// or one that just recovered the DB, still waits for both.
	bootOffline := !dbMissing && channelCheckedBefore(signalCtx, metaStore)
	if bootOffline {
		fmt.Fprintln(os.Stderr, "channel checked at an earlier mount: mounting now; connecting to Telegram and checking the channel in the background")
		go func() {
			if err := backgroundPreflight(signalCtx, firstConnect, sess, metaStore); err != nil && signalCtx.Err() == nil {
				fmt.Fprintf(os.Stderr, "telfs: the channel refused this filesystem: %v — stopping\n", err)
				bgRefusal.Store(&err)
				_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
			}
		}()
	} else {
		select {
		case err := <-firstConnect:
			if err != nil {
				stopMgr()
				<-mgrDone
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("%w: %w", errTelegram, err)
			}
		case <-signalCtx.Done():
			stopMgr()
			<-mgrDone
			return nil
		}

		// Preflight: fail fast (with the actor and the fix named) when the
		// session identity doesn't match auth_mode or can't access the
		// bound channel — otherwise the mount comes up "fine" and every
		// channel RPC dies in a CHANNEL_INVALID retry storm only visible
		// in the log.
		svc.status("checking access to the channel")
		verr := svc.retryAtBoot(signalCtx, "channel access", func(ctx context.Context) error {
			verifyCtx, cancelVerify := context.WithTimeout(ctx, 30*time.Second)
			defer cancelVerify()
			return sess.VerifyAccess(verifyCtx)
		})
		if verr != nil {
			stopMgr()
			<-mgrDone
			if errors.Is(verr, context.Canceled) {
				return nil
			}
			return fmt.Errorf("mount preflight: %w: %w", errTelegram, verr)
		}
		markChannelChecked(signalCtx, metaStore)
	}

	err = func() error {
		// A read of uncached data fails (EIO) once it has made no progress
		// for [read] offline_timeout — it used to wait out the whole outage.
		fetcher := &chunk.TGFetcher{Session: sess, OfflineTimeout: offlineTimeout}
		// Cache cap resolution: --cache-cap flag wins, then config's
		// cache_cap, then chunk.DefaultCacheCapBytes. A parse error on
		// the flag aborts (operator typo, fail loudly); a parse error
		// in config is silent (treat as "use default" — config-edit
		// errors shouldn't fail mount when it would otherwise come
		// up cleanly).
		capBytes, err := cacheCapFrom(*cacheCap, cfg)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "cache cap: %s (%d bytes)\n", humanBytes(capBytes), capBytes)
		cache, err := chunk.NewCacheWire(cfg.CachePath(), capBytes, fetcher, pipe.wc)
		if err != nil {
			return err
		}
		// In-RAM hot tier. Default 128 MiB; "0" disables entirely.
		// Errors on the flag parse abort the mount — operator typo
		// shouldn't silently degrade to the default.
		memCap, err := memCacheCapFrom(*memCacheCap)
		if err != nil {
			return err
		}
		if memCap > 0 {
			cache.SetMemCache(chunk.NewMemCache(memCap))
			fmt.Fprintf(os.Stderr, "mem cache: %s (%d bytes)\n", humanBytes(memCap), memCap)
		} else {
			fmt.Fprintln(os.Stderr, "mem cache: disabled")
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
		// The staging directory ([staging] dir, default
		// <profile>/cache/pending) and its gate: written-but-not-yet-
		// uploaded data waits there, up to [staging] max_bytes, never
		// filling its filesystem below min_free (spec v1, row 9).
		pendingDir := cfg.StagingDir()
		if err := os.MkdirAll(pendingDir, 0o700); err != nil {
			return fmt.Errorf("staging dir: %w", err)
		}
		chunk.CleanupPendingTmp(pendingDir)
		pool.PendingDir = pendingDir
		stageMax, stageFree, err := cfg.StagingLimits()
		if err != nil {
			return err
		}
		stagingGate := chunk.NewDiskGate(pendingDir, stageMax, stageFree)
		pool.Disk = stagingGate
		fmt.Printf("Staging: %s (up to %s waiting for upload, keeps %s free)\n", pendingDir, humanBytes(stageMax), humanBytes(stageFree))
		if !*readonly {
			fmt.Printf("Upload: %s\n", describeUpload(uploadCfg))
		}
		// In-process pending index: accelerates Reader.tryPendingFallback
		// by skipping the O(N) Glob over the pending dir on every miss.
		// Writer populates it after PersistPending; packer drops entries
		// after DeletePending. Reader consults it before falling back
		// to a disk scan, so a stale entry just falls through harmlessly.
		pendingIndex := chunk.NewPendingIndex()
		pool.PendingIndex = pendingIndex
		reader := chunk.NewReader(metaStore, cache, pool.Pending, chunkSize)
		// Wire the staging-dir fallback so reads of freshly-written
		// chunks succeed even when chunk_map hasn't seen its row yet
		// (the wire is on disk, encoded the same way the Writer's
		// dispatch encoded it — same cipher, same codec).
		reader.SetPendingFallbackWire(pendingDir, pipe.wc)
		reader.SetPendingIndex(pendingIndex)
		// Prefetch knobs: CLI flag wins (forced override), otherwise
		// the defaults from chunk.DefaultPrefetch* take effect inside
		// NewReader. Reader.SetPrefetch leaves a zero/negative value
		// untouched so passing one direction doesn't accidentally
		// reset the other to the default.
		if *prefetchWindow > 0 || *prefetchConcurrency > 0 {
			reader.SetPrefetch(*prefetchWindow, *prefetchConcurrency)
		}
		// Packed-blob locality prefetch: a cold read of one packed
		// extent triggers ONE ranged download warming the neighboring
		// extents of the same blob (files packed together are read
		// together — tar order ≈ walk order). Turns per-file serial
		// round trips on cold directory walks (make menuconfig over
		// ~1,500 Kconfig files) into a handful of span fetches.
		var blobPF *chunk.BlobPrefetcher
		blobBudget, err := blobBudgetFrom(*blobPrefetch) // 0 disables below
		if err != nil {
			return err
		}
		if blobBudget > 0 {
			blobPF = chunk.NewBlobPrefetcherWire(metaStore, cache, fetcher, pipe.wc, blobBudget)
			reader.SetBlobPrefetcher(blobPF)
		}
		// Small-file packer: aggregates chunks smaller than
		// chunk.DefaultPackThreshold into shared TG blobs to amortize
		// the per-message RTT. Nil-safe — Writer falls back to
		// direct upload when no packer is wired.
		packer := chunk.NewPacker(metaStore, sess, 0, 0, 0)
		packer.SetWire(pipe.wc)
		packer.SetPendingIndex(pendingIndex)
		packer.SetDiskGate(stagingGate)
		// Cancel pending uploads on unlink. When a file is deleted while
		// some of its chunks still wait (tar overwriting an extracted
		// tree, rm -rf during an upload, a file manager's delete),
		// uploading them would only produce orphan TG messages that
		// PutChunk then FK-rejects: the packer's queued chunks are pulled
		// and every staged wire of the inode — the direct queue's too —
		// is removed (chunk.DropStagedInode).
		metaStore.SetInodeDeletedCallback(func(ino int64) {
			if n := chunk.DropStagedInode(packer, pendingIndex, stagingGate, ino); n > 0 {
				fmt.Fprintf(os.Stderr, "[staging] deleted ino=%d: dropped %d staged chunk(s) before upload\n", ino, n)
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
			Wire:      pipe.wc,
			ChunkSize: chunkSize,
			ReadOnly:  *readonly,
			TrashIno:  trashIno,
		}

		// Adaptive rate-limit controller. Created BEFORE the metrics
		// HTTP server so the /telegram handler can include its
		// snapshot in the JSON. The controller's Run goroutine is
		// started later (after meta_kv pin values are loaded), so
		// `adaptive.Snapshot()` returns zero-valued state until the
		// poller calls SetPinned* the first time.
		adaptive := tg.NewAdaptiveController(client.RateLimiter(), client.Tracker())

		// queueDrained reports whether every upload-side backlog is
		// empty: packer buffer, wires staged on disk, uploads anywhere
		// between dispatch and chunk_map commit (pool.Pending spans
		// Register→Release, so sem-queued goroutines count too), and
		// unposted journal rows. Shared by the snapshot manager's
		// quiesce trigger and the /telegram sync report.
		queueDrained := func(ctx context.Context) bool {
			if packer != nil {
				if n, _ := packer.QueueDepth(); n > 0 {
					return false
				}
			}
			if pool != nil && pool.Pending.Total() > 0 {
				return false
			}
			if pendingDir != "" {
				if ents, err := os.ReadDir(pendingDir); err == nil {
					for _, e := range ents {
						if !e.IsDir() {
							return false
						}
					}
				}
			}
			v, err := metaStore.Stats(ctx)
			return err == nil && v.JournalUnposted == 0
		}
		// Created here (started further down) so the /telegram handler
		// can report snapshot-coverage state from day one of the
		// process; the sync indicator keys off it.
		snapMgr := &snapshot.Manager{Meta: metaStore, Session: sess, Cipher: cipher, Keys: pipe.keys, QuiesceCheck: queueDrained,
			Policy: snapshot.Policy{EveryOps: snapEveryOps, Every: snapEvery}}

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
			metricsSrv.RegisterHandler("/telegram", telegramJSONHandler(sess.Tracker(), cfg, packer, pool, pendingDir, metaStore, adaptive, snapMgr))
			// /cache reports the LRU disk cache's live size + cap +
			// hit/miss counts so the /state web page can render the
			// cache panel without having to know about the metrics
			// scrape format. Cheap: in-memory atomic loads.
			metricsSrv.RegisterHandler("/cache", cacheJSONHandler(cache))
			// /status: who this daemon is. A non-dumpable daemon hides
			// its environ and fd/1 from other processes, so the web's
			// adopted-mount view asks the daemon itself (P1, point 1).
			metricsSrv.RegisterHandler("/status", daemonStatusHandler(cfg.ProfileName(), mountpoint, pipe.format))
			if err := metricsSrv.Start(); err != nil {
				if metricsExplicit {
					return err
				}
				// Default addr taken (most likely another profile's
				// daemon) — run without the endpoint rather than
				// failing the mount. The web UI's live panels will
				// show whichever daemon owns the port.
				fmt.Fprintf(os.Stderr, "metrics endpoint disabled: %v\n", err)
				metricsSrv = nil
			} else {
				fmt.Printf("Metrics: http://%s/metrics\n", metricsSrv.Addr())
				fmt.Printf("Telegram log: http://%s/telegram\n", metricsSrv.Addr())
			}
		}
		// The control socket is created BEFORE the FUSE mount: failing
		// here must not leave a mounted filesystem without its daemon.
		// It is served once everything it reports on exists (below).
		ctlPath := control.Path(cfg.DataDir)
		ctlLn, err := control.Listen(ctlPath)
		if err != nil {
			return err
		}
		ctlServed := false
		defer func() {
			if !ctlServed {
				ctlLn.Close()
				_ = os.Remove(ctlPath)
			}
		}()

		server, err := telfsfs.Mount(telfsfs.MountOptions{
			MountPoint:         mountpoint,
			AllowOther:         *allowOther,
			DefaultPermissions: ownerChecks(*allowOther, *noDefaultPerms),
			Debug:              *debug,
			Profile:            cfg.ProfileName(),
			FSUUID:             fsUUID,
			ReadOnly:           *readonly,
		}, backend)
		if err != nil {
			return err
		}

		// Snapshots. Format 1: one immediately (so a freshly-recovered
		// DB is re-baselined), then every interval. Format 2: the policy
		// (rare whole-DB snapshots + the pinned anchor; nothing at mount
		// when nothing changed). We pass signalCtx — the goroutine exits
		// when the gotd session tears down. The user-stop signal stops
		// it via snapCtx below.
		snapCtx, stopSnap := context.WithCancel(signalCtx)
		snapDone := make(chan struct{})
		go func() {
			if !*readonly {
				_ = snapMgr.Run(snapCtx)
			}
			close(snapDone)
		}()

		// Journal poster: drains the local journal table every
		// DefaultPosterInterval and uploads each pending op as a
		// journal-op message on the channel. With this running,
		// recovery can replay mutations made between snapshot
		// cadences — narrowing the crash data-loss window from
		// 5 min (snapshot interval) to ~5 s (poster interval).
		journalPoster := &snapshot.Poster{Meta: metaStore, Session: sess, Keys: pipe.keys}
		journalCtx, stopJournal := context.WithCancel(signalCtx)
		journalDone := make(chan struct{})
		go func() {
			if !*readonly {
				_ = journalPoster.Run(journalCtx)
			}
			close(journalDone)
		}()

		// Adaptive rate-limit controller. AIMD on the upload lane:
		// halve on FLOOD_WAIT, additive-increase during quiet windows
		// with active traffic. When the operator pins an explicit cap
		// via the CLI/web, the controller respects the pin and resumes
		// adaptive control when the pin is cleared. The poller below
		// converts meta_kv values into pin updates rather than calling
		// SetXPerMinute directly, so we have a single owner of the
		// limiter's settings.
		// (Constructed above at the metrics block so /telegram can
		// see it; start the Run loop here.)
		adaptiveCtx, stopAdaptive := context.WithCancel(signalCtx)
		adaptiveDone := make(chan struct{})
		go func() {
			defer close(adaptiveDone)
			adaptive.Run(adaptiveCtx)
		}()

		// Rate-limit poller: reads meta_kv every few seconds and
		// pushes the operator's pin state into the adaptive
		// controller. n > 0 pins the lane; n == 0 (or missing)
		// surrenders the lane to adaptive control. The poller
		// runs at 5 s; the controller's AIMD ticks at 30 s — both
		// see fresh state when the operator edits via CLI or web.
		var lastPinUp, lastPinDown int = -1, -1
		lastMode := ""
		applyRateLimit := func() {
			// Mode first, so a pin update below lands under the right
			// semantics (manual: 0 = unlimited; adaptive: 0 = AIMD).
			if mode, err := metaStore.TGRateLimitMode(signalCtx); err == nil && mode != lastMode {
				adaptive.SetManualMode(mode == "manual")
				fmt.Fprintf(os.Stderr, "[ratelimit] mode: %s\n", mode)
				lastMode = mode
				lastPinUp = -1 // force re-apply of the pin under the new mode
			}
			if n, err := metaStore.TGRateLimitPerMin(signalCtx); err == nil {
				if n != lastPinUp {
					adaptive.SetPinnedUpload(n)
					switch {
					case n > 0:
						fmt.Fprintf(os.Stderr, "[ratelimit] upload pinned to %d req/min\n", n)
					case lastMode == "manual":
						fmt.Fprintln(os.Stderr, "[ratelimit] upload unlimited (manual mode)")
					default:
						fmt.Fprintln(os.Stderr, "[ratelimit] upload pin cleared — adaptive control resumed")
					}
					lastPinUp = n
				}
			}
			if n, err := metaStore.TGDownloadRateLimitPerMin(signalCtx); err == nil {
				if n != lastPinDown {
					adaptive.SetPinnedDownload(n)
					if n == 0 {
						fmt.Fprintln(os.Stderr, "[ratelimit] download cleared (unlimited)")
					} else {
						fmt.Fprintf(os.Stderr, "[ratelimit] download pinned to %d req/min\n", n)
					}
					lastPinDown = n
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

		// Lifetime-stats persister: lands the tracker's cumulative
		// totals in meta_kv every minute. Cheap (one JSON marshal +
		// one UPSERT); the final write happens in the shutdown
		// sequence BEFORE the last snapshot, so the stats also ride
		// the snapshot blob into the channel and survive recovery.
		go func() {
			t := time.NewTicker(60 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-signalCtx.Done():
					return
				case <-t.C:
					persistStats(signalCtx)
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
		if *readonly {
			fmt.Fprintln(os.Stderr, "read-only: no snapshots, no journal, no uploads — nothing is written to the channel")
		} else {
			go recoverPending(signalCtx, pendingDir, packer, metaStore, pendingIndex, pool)
		}

		// Stall watchdog: if there's chunk work pending but no
		// successful RPC has registered for a while (and no server-
		// mandated FLOOD_WAIT window is in effect), fire the
		// SessionManager's Reconnect signal so a wedged MTProto
		// connection auto-recovers without manual restart.
		go stallWatchdog(signalCtx, pool, packer, mgr, client.Tracker())

		// Format 2: check that the channel's pinned key message exists
		// and matches the local key header — at mount and every 6 h.
		// Warn only; repairs are `fsck --fix` / `--key-header`.
		if pipe.format == 2 {
			go func() {
				check := func() {
					kc := checkPinnedHeader(signalCtx, sess, metaStore)
					storeKeyCheck(signalCtx, metaStore, kc)
					if !kc.OK {
						fmt.Fprintf(os.Stderr, "[key] WARNING: %s\n", kc.Detail)
					}
				}
				if !*readonly {
					deleteOldKeyMessages(signalCtx, sess, metaStore) // left by an interrupted secret change
				}
				check()
				t := time.NewTicker(6 * time.Hour)
				defer t.Stop()
				for {
					select {
					case <-signalCtx.Done():
						return
					case <-t.C:
						check()
					}
				}
			}()
		}

		// The control socket (spec v1 §3): status, sync, unmount and the
		// web console talk to this daemon through it, owner-only. The S9
		// services depend on it, so a mount without one is refused.
		ctl := &daemonCtl{cfg: cfg, mountpoint: mountpoint, readOnly: *readonly, format: pipe.format,
			meta: metaStore, sess: sess, pool: pool, poster: journalPoster, snapMgr: snapMgr, counters: pipe.counters,
			telegram: telegramJSONHandler(sess.Tracker(), cfg, packer, pool, pendingDir, metaStore, adaptive, snapMgr),
			cache:    cacheJSONHandler(cache),
			ctx:      signalCtx, keys: pipe.keys, ch: sess, upLimit: client.ByteLimiter()}
		ctlServed = true
		go control.Serve(signalCtx, ctlLn, ctlPath, ctl.handler())
		go ctl.sampleRate(signalCtx)

		if *allowOther {
			if ownerChecks(*allowOther, *noDefaultPerms) {
				fmt.Println("Other users: allowed; the kernel checks every access against the stored owners and modes (default_permissions).")
			} else {
				fmt.Println("WARNING: --no-default-permissions: every local user can read and write everything on this mount.")
			}
		}
		fmt.Printf("Mounted at %s. Press ^C to unmount.\n", mountpoint)
		svc.markReady(mountpoint)
		<-signalCtx.Done()
		svc.stopping()

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
			stopAdaptive()
			<-adaptiveDone
			// Unmount + drain BEFORE the final snapshot. Two-step:
			//   1. Unmount severs the kernel-to-userspace channel so
			//      no new FUSE writes can fire; server.Wait drains
			//      any FUSE request goroutines still in flight.
			//   2. backend.Shutdown drains the upload pool — the
			//      uploadOne goroutines that outlived their handle
			//      via async close. Until this returns, freshly-
			//      closed files may have inode.size persisted but
			//      no chunk_map row.
			// THEN take the snapshot (format 1) or post the last
			// journal ops (format 2), so the channel state reflects
			// a fully-quiescent DB. Snapshotting before drain would
			// ship a torn DB (file present, chunks missing) — fine
			// for warm restart (local DB is authoritative) but
			// catastrophic for recover-from-channel.
			fmt.Println("Unmounting…")
			if uerr := server.Unmount(); uerr != nil {
				// Two benign shapes hide behind a fusermount error:
				//  - the mount is already gone (unmounted externally)
				//    → nothing to do;
				//  - EBUSY from open files → lazy-detach so
				//    server.Wait below can actually drain instead of
				//    wedging until the 30 s watchdog SIGKILLs us
				//    (observed live: a file manager holding the mount
				//    open turned every clean shutdown into a
				//    watchdog force-exit, skipping the final
				//    snapshot).
				switch {
				case !mountctl.IsMounted(mountpoint):
					fmt.Println("mount already detached (unmounted externally)")
				default:
					if lerr := mountctl.LazyUnmount(mountpoint); lerr != nil {
						fmt.Fprintln(os.Stderr, "unmount:", uerr)
						fmt.Fprintln(os.Stderr, "lazy unmount:", lerr)
					} else {
						fmt.Println("unmount was busy — lazily detached (open files keep working until closed)")
					}
				}
			}
			server.Wait()
			// Stop the blob prefetcher before draining: its span
			// downloads share the gotd session and there's no point
			// warming a cache nobody will read from this process.
			if blobPF != nil {
				blobPF.Close()
			}
			drainCtx, cancelDrain := context.WithTimeout(context.Background(), 12*time.Second)
			backend.Shutdown(drainCtx)
			cancelDrain()
			// Format 2 takes no final snapshot: its journal carries every
			// change, chunk placements included. The poster's own final
			// drain ran when the stop signal came; uploads that landed
			// during the drain above journaled their placements after
			// it, so post those now (same 10 s budget the format-1 final
			// snapshot gets). Whatever doesn't make it stays in the local
			// journal and goes out at the next mount.
			if !*readonly && pipe.format == 2 {
				postCtx, postCancel := context.WithTimeout(context.Background(), 10*time.Second)
				if err := journalPoster.Drain(postCtx); err != nil {
					fmt.Fprintln(os.Stderr, "[journal-poster] final post:", err)
				}
				postCancel()
			}
			if metricsSrv != nil {
				mctx, mcancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = metricsSrv.Shutdown(mctx)
				mcancel()
			}
			// Final lifetime-stats write BEFORE the final snapshot so
			// the snapshot blob carries the up-to-date totals.
			persistStats(context.Background())
			// Format 1: final snapshot, post-drain. Uploads after
			// RunSession returns fail with "engine closed", so this MUST
			// run inside the callback. 10-second budget keeps the total
			// (12 drain + 10 snapshot + slack) under the 30s watchdog
			// below.
			if !*readonly && pipe.format == 1 {
				fmt.Println("Taking final snapshot…")
				finalCtx, finalCancel := context.WithTimeout(context.Background(), 10*time.Second)
				if err := snapMgr.Once(finalCtx); err != nil {
					fmt.Fprintln(os.Stderr, "[snapshot] final failed:", err)
				}
				finalCancel()
			}
		}()
		select {
		case <-done:
			// Orderly shutdown complete.
		case <-time.After(30 * time.Second):
			fmt.Fprintln(os.Stderr, "telfs: orderly shutdown timed out — forcing lazy unmount + exit")
			_ = mountctl.LazyUnmount(mountpoint)
			os.Exit(1)
		}
		return nil
	}()
	// Stop the session manager AFTER the shutdown sequence ran the
	// final post/snapshot. The manager's goroutine then unwinds; mgrDone
	// signals when the gotd client's run loop has fully torn down.
	stopMgr()
	<-mgrDone
	if p := bgRefusal.Load(); p != nil {
		return fmt.Errorf("mount preflight: %w: %w", errTelegram, *p)
	}
	// Treat clean cancellation as success — the user asked for unmount.
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// cacheCapFrom resolves the disk-cache cap: --cache-cap wins, then
// config.toml's cache_cap, then chunk.DefaultCacheCapBytes. A parse
// error on the flag aborts (operator typo, fail loudly); one in config
// is silent (treat as "use default").
func cacheCapFrom(flagVal string, cfg *config.Config) (int64, error) {
	capBytes := chunk.DefaultCacheCapBytes
	if flagVal != "" {
		n, err := config.ParseHumanBytes(flagVal)
		if err != nil {
			return 0, fmt.Errorf("--cache-cap: %w", err)
		}
		if n > 0 {
			capBytes = n
		}
	} else if n := cfg.CacheCapBytes(); n > 0 {
		capBytes = n
	}
	return capBytes, nil
}

// memCacheCapFrom resolves the in-RAM hot tier's size: default 128 MiB,
// "0" disables it; a bad flag aborts.
func memCacheCapFrom(flagVal string) (int64, error) {
	if flagVal == "" {
		return chunk.DefaultMemCacheCapBytes, nil
	}
	n, err := config.ParseHumanBytes(flagVal)
	if err != nil {
		return 0, fmt.Errorf("--mem-cache-cap: %w", err)
	}
	return n, nil
}

// blobBudgetFrom resolves the packed-blob prefetch span (0 disables).
func blobBudgetFrom(flagVal string) (int64, error) {
	if flagVal == "" {
		return chunk.DefaultBlobPrefetchBudget, nil
	}
	n, err := config.ParseHumanBytes(flagVal)
	if err != nil {
		return 0, fmt.Errorf("--blob-prefetch: %w", err)
	}
	return n, nil
}

// daemonStatusHandler serves the daemon's identity as JSON. The log
// path is this process's own stdout when that is a regular file (a
// non-dumpable process may still read its own /proc/self/fd).
func daemonStatusHandler(profile, mountpoint string, format int) http.HandlerFunc {
	logPath := ""
	if target, err := os.Readlink("/proc/self/fd/1"); err == nil && strings.HasPrefix(target, "/") && !strings.HasPrefix(target, "/dev/") {
		if st, err := os.Stat(target); err == nil && st.Mode().IsRegular() {
			logPath = target
		}
	}
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"profile": profile, "pid": os.Getpid(), "mountpoint": mountpoint,
			"log_path": logPath, "format": format,
		})
	}
}

// stallWatchdog watches the link while the mount runs:
//   - when chunk work is pending and no RPC has succeeded for
//     stallThreshold (outside a server-mandated FLOOD_WAIT), it forces an
//     MTProto reconnect, so a wedged connection recovers without a restart;
//   - when the link comes back after being down (v1.1 I6), staged uploads
//     and the packer retry at once instead of waiting out their backoff,
//     which grows to 5 min during a long outage (S9: the network returned
//     at 23:45:35, a staged chunk landed only at ~23:49:30).
func stallWatchdog(ctx context.Context, pool *chunk.UploadPool, packer *chunk.Packer, mgr *tg.SessionManager, tr *tg.Tracker) {
	const (
		sampleInterval    = 5 * time.Second
		stallThreshold    = 60 * time.Second
		reconnectCooldown = 90 * time.Second
	)
	var (
		lastOK        int64
		stalledSince  time.Time
		lastReconnect time.Time
		link          linkState
	)
	tick := time.NewTicker(sampleInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			pending := pool.Pending.Total()
			snap := tr.Snapshot()
			if link.observe(snap.LastSuccess, snap.LastError, pending > 0, time.Now()) {
				n := pool.KickRetries()
				if packer != nil {
					packer.KickRetries()
				}
				if n > 0 {
					fmt.Fprintf(os.Stderr, "[watchdog] link is back: retrying %d staged upload(s) now\n", n)
				}
			}
			if pending == 0 || snap.TotalOK != lastOK {
				// Idle (nothing pending) OR RPCs are landing.
				stalledSince = time.Time{}
				lastOK = snap.TotalOK
				continue
			}
			if time.Now().Before(snap.FloodWaitUntil) {
				// Server-mandated quiet period — silence is expected.
				stalledSince = time.Time{}
				continue
			}
			// pending > 0, no successful RPC since last sample.
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
				"[watchdog] STALL: %d chunks pending, no successful RPC for %v. "+
					"Forcing MTProto reconnect.\n",
				pending, elapsed.Round(time.Second))
			mgr.Reconnect()
			link.down = true
			lastReconnect = time.Now()
			stalledSince = time.Time{} // give the new session a chance
		}
	}
}

// linkState tells when the link to Telegram came back. It is down when the
// newest RPC event was an error, or when work was pending and nothing
// succeeded for linkDownAfter (calls into a dead connection may simply
// hang, recording no error); the first success after that is "back".
type linkState struct {
	seenOK time.Time
	down   bool
}

const linkDownAfter = 30 * time.Second

// observe takes one sample and reports whether the link just came back.
func (l *linkState) observe(lastOK, lastErr time.Time, pending bool, now time.Time) (back bool) {
	if lastOK.After(l.seenOK) {
		l.seenOK = lastOK
		if l.down {
			l.down = false
			return true
		}
	}
	if lastErr.After(lastOK) || (pending && now.Sub(lastOK) >= linkDownAfter) {
		l.down = true
	}
	return false
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
func telegramJSONHandler(tr *tg.Tracker, cfg *config.Config, packer *chunk.Packer, pool *chunk.UploadPool, pendingDir string, mstore *meta.Store, adaptive *tg.AdaptiveController, snapMgr *snapshot.Manager) http.HandlerFunc {
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
		var stagingCap int64
		var stagingBlocked bool
		var stagedWaiting int64
		var stagedRunning int32
		if pool != nil && pool.Disk != nil {
			// Queue mode: the gate and the in-memory index know the
			// staged bytes and files; listing a directory of hundreds of
			// thousands of wires on every request would not scale.
			pendingBytes, stagingCap, stagingBlocked = pool.Disk.Stats()
			if pool.PendingIndex != nil {
				pendingFiles = pool.PendingIndex.Len()
			}
			stagedWaiting, stagedRunning = pool.StagedQueue()
		} else if pendingDir != "" {
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
		var uploadsTotal int64
		if pool != nil {
			poolInFlight, poolCap = pool.InFlight(), pool.Cap()
			// Pending spans Register→Release, so uploads QUEUED for a
			// sem slot (async dispatch) count too — poolInFlight alone
			// only sees slot holders.
			uploadsTotal = pool.Pending.Total()
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
		fmt.Fprintf(w, "    \"uploads_total\": %d,\n", uploadsTotal)
		fmt.Fprintf(w, "    \"staging_dir\": %q,\n", pendingDir)
		fmt.Fprintf(w, "    \"staging_bytes\": %d,\n", pendingBytes)
		fmt.Fprintf(w, "    \"staging_cap_bytes\": %d,\n", stagingCap)
		fmt.Fprintf(w, "    \"staging_blocked\": %t,\n", stagingBlocked)
		fmt.Fprintf(w, "    \"staged_direct_waiting\": %d,\n", stagedWaiting)
		fmt.Fprintf(w, "    \"staged_direct_running\": %d,\n", stagedRunning)
		fmt.Fprintf(w, "    \"journal_unposted\": %d\n", journalUnposted)
		fmt.Fprintln(w, "  },")
		// Channel-sync verdict. channel_complete means the channel
		// alone is sufficient to recover this FS on fresh hardware:
		// every upload-side backlog is empty AND a snapshot covering
		// the current DB has been posted (chunk_map isn't journaled, so
		// queue-empty by itself leaves chunk mappings local-only until
		// the next snapshot lands — the quiesce trigger takes one
		// within seconds of the queue draining).
		queueEmpty := packerChunks == 0 && pendingFiles == 0 && uploadsTotal == 0 && journalUnposted == 0
		snapClean := snapMgr != nil && snapMgr.Synced(r.Context())
		fmt.Fprintln(w, "  \"sync\": {")
		fmt.Fprintf(w, "    \"queue_empty\": %t,\n", queueEmpty)
		fmt.Fprintf(w, "    \"snapshot_clean\": %t,\n", snapClean)
		fmt.Fprintf(w, "    \"channel_complete\": %t\n", queueEmpty && snapClean)
		fmt.Fprintln(w, "  },")
		// Adaptive rate-limiter state — pin status + current
		// effective cap + last-adjustment metadata. Cheap to read
		// (one mutex grab). Surfaced so the web tab can show
		// "auto, 240 req/min, raised 2 min ago" instead of just a
		// bare integer.
		if adaptive != nil {
			a := adaptive.Snapshot()
			fmt.Fprintln(w, "  \"adaptive\": {")
			fmt.Fprintf(w, "    \"manual\": %t,\n", a.Manual)
			fmt.Fprintf(w, "    \"upload_pinned\": %t,\n", a.UploadPinned)
			fmt.Fprintf(w, "    \"upload_effective\": %d,\n", a.UploadEffective)
			fmt.Fprintf(w, "    \"upload_adaptive_level\": %d,\n", a.UploadAdaptiveLevel)
			fmt.Fprintf(w, "    \"download_pinned\": %t,\n", a.DownloadPinned)
			fmt.Fprintf(w, "    \"download_effective\": %d,\n", a.DownloadEffective)
			fmt.Fprintf(w, "    \"last_flood_ago_sec\": %d,\n", a.LastFloodAgoSec)
			fmt.Fprintf(w, "    \"last_adjust_ago_sec\": %d,\n", a.LastAdjustAgoSec)
			fmt.Fprintf(w, "    \"last_action\": %q\n", a.LastAction)
			fmt.Fprintln(w, "  },")
		}
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
// than a hard failure. (Emergency-unmount paths themselves go through
// internal/mountctl, which does the same fusermount3-first lookup.)
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
func recoverPending(ctx context.Context, dir string, packer *chunk.Packer, mstore *meta.Store, pendingIndex *chunk.PendingIndex, pool *chunk.UploadPool) {
	entries, err := chunk.ScanPending(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pending: scan:", err)
		return
	}
	if len(entries) == 0 {
		return
	}
	// Duplicate-generation guard: a crash between staging a retry wire
	// and deleting its predecessor can leave TWO files for one
	// (ino, idx) slot (the filename embeds the plaintext size, which
	// may differ between generations). Submitting both would race
	// their chunk_map upserts in arbitrary order — keep only the
	// newest by mtime.
	type slot struct {
		ino int64
		idx int32
	}
	bySlot := make(map[slot]chunk.PendingEntry)
	mtime := func(p string) int64 {
		if info, err := os.Stat(p); err == nil {
			return info.ModTime().UnixNano()
		}
		return 0
	}
	for _, e := range entries {
		k := slot{e.Ino, e.Idx}
		prev, dup := bySlot[k]
		if !dup {
			bySlot[k] = e
			continue
		}
		if mtime(e.Path) >= mtime(prev.Path) {
			_ = chunk.DeletePending(prev.Path)
			bySlot[k] = e
		} else {
			_ = chunk.DeletePending(e.Path)
		}
	}
	if len(bySlot) < len(entries) {
		fmt.Printf("Dropped %d superseded pending wire generation(s)\n", len(entries)-len(bySlot))
		entries = entries[:0]
		for _, e := range bySlot {
			entries = append(entries, e)
		}
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
		// Populate the index so Reader fallbacks during the recovery
		// window go through the O(1) lookup instead of a Glob scan.
		// The packer will UnrecordPersist on each successful PutChunk.
		pendingIndex.RecordPersist(e.Ino, e.Idx, e.Path)
		info, err := os.Stat(e.Path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "pending: stat %s: %v\n", e.Path, err)
			continue
		}
		// By REFERENCE (spec v1, row 9): the packer reads the wire at
		// flush, so recovering 50 GB of staged data never loads it into
		// memory. Its bytes count against the staging cap from now
		// until its file is removed. A failure is retried from disk
		// (and, after a restart, re-found here). Format 2 keeps the
		// content hash (the wire's AAD) in the staged file's name; it
		// must reach chunk_map with the row.
		// Registered like a fresh write: `telfs sync` waits for it, and
		// opening this file for writing waits until it has landed (a
		// stale recovered wire must never overwrite a newer write).
		pool.Disk.Add(info.Size())
		res := pool.Register(e.Ino)
		packer.SubmitRecovered(e.Ino, e.Idx, e.PlaintextSize, info.Size(), e.Path, e.Hash, res.Release)
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
		// armForceExit is installed AFTER signal.NotifyContext already
		// consumed the FIRST signal (we're called post <-ctx.Done()),
		// so the next delivery on this channel IS the user's second
		// signal. An earlier version received twice here, which made
		// the promised "second ^C forces exit" actually require a
		// third.
		<-ch
		fmt.Fprintln(os.Stderr, "telfs: second signal — forcing lazy unmount + exit")
		_ = mountctl.LazyUnmount(mountpoint)
		os.Exit(130)
	}()
}

// neverUsedDB reports a DB that neither `telfs init` nor a recovery
// produced: format 1 (no format marker), never snapshotted, nothing but
// the root directory, no chunks, no journal. That is what meta.Open
// leaves behind when it is called on a missing path. (Every filesystem
// that was ever mounted has a snapshot record: the daemon snapshots at
// mount. Format 2 always carries its marker.)
func neverUsedDB(ctx context.Context, m *meta.Store) (bool, error) {
	if f, err := m.Format(ctx); err != nil || f != 1 {
		return false, err
	}
	for _, k := range []string{snapshot.KVCurrentMsgID, snapshot.KVSnapMsgIDs} {
		if _, err := m.GetKV(ctx, k); err == nil {
			return false, nil
		} else if !errors.Is(err, meta.ErrNotFound) {
			return false, err
		}
	}
	st, err := m.Stats(ctx)
	if err != nil {
		return false, err
	}
	return st.InodesTotal <= 1 && st.ChunkRows == 0 && st.JournalRows == 0, nil
}

// tryRecover restores a missing local DB from the channel. Format 2
// first (pinned key message, or the --key-header export → secret →
// newest snapshot → encrypted journal); else format 1 (newest snapshot
// caption, envelope decrypted with the profile's secret → journal).
func tryRecover(ctx context.Context, cfg *config.Config, client *tg.Client, sec *secret.Secret, keyHeaderPath string, readOnly bool) error {
	return client.RunSession(ctx, func(ctx context.Context, sess *tg.Session) error {
		sess.SetReadOnly(readOnly) // a read-only drill restores locally, never writes the channel
		fmt.Println("Local DB missing — looking for the filesystem on the channel…")
		found2, err := recover2(ctx, cfg, sess, sec, keyHeaderPath)
		if found2 || err != nil {
			return err
		}
		if keyHeaderPath != "" {
			return errors.New("--key-header given, but the export does not open with this secret")
		}
		latest, err := sess.FindLatestSnapshot(ctx, "", 200)
		if err != nil {
			return err
		}
		if latest == nil {
			// "No snapshot found" must NOT silently become "start
			// empty" when the channel plainly holds TelFS data: the
			// mount would immediately post an empty-DB snapshot as
			// the NEWEST snapshot, poisoning every future recovery,
			// and a later `gc --yes` (empty chunk_map) would delete
			// every chunk in the channel. Probe one page — any
			// document there means data exists beyond our scan.
			hasMessages := false
			_ = sess.WalkChannelMessages(ctx, 1, func(cm tg.ChannelMessage) error {
				hasMessages = true
				return tg.ErrStopWalk
			})
			if hasMessages {
				return fmt.Errorf("no snapshot or key message found within the scan window, but the channel is NOT empty — refusing to start with an empty DB (that would poison recovery and make gc destructive).\n" +
					"Inspect the channel (telfs snapshot list); for a format-2 filesystem whose key message is gone, mount with --key-header <export>")
			}
			return errors.New("this channel holds no TelFS filesystem — run `telfs init` to create one")
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
		// embedded KDF state + the profile's secret, decrypt to the
		// plaintext gzipped DB.
		plaintext, err := maybeUnwrapEncrypted(data, sec)
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
			// A failed replay is NOT safe to shrug off: silently
			// proceeding with snapshot-only state hides the loss of
			// every mutation since the snapshot. Remove the restored
			// DB so the next attempt re-enters recovery cleanly, and
			// tell the operator how to accept the degraded state on
			// purpose if they want it.
			_ = os.Remove(cfg.DBPath())
			for _, sidecar := range []string{cfg.DBPath() + "-wal", cfg.DBPath() + "-shm"} {
				_ = os.Remove(sidecar)
			}
			return fmt.Errorf("journal replay failed: %w\n"+
				"The snapshot itself restored fine, but mutations made after it could not be replayed.\n"+
				"Retry when the network is stable, or run `telfs snapshot restore` explicitly to accept snapshot-only state", err)
		}
		return nil
	})
}

// isNoop reports a plaintext format-1 cipher.
func isNoop(c crypto.Cipher) bool {
	_, ok := c.(crypto.NoopCipher)
	return ok
}

// replayJournalSince loads journal-op messages from the channel whose
// seq is strictly greater than sinceSeq (the snapshot's high-water
// mark) and applies them in ascending order to the just-restored
// local DB.
//
// A failed op ABORTS the replay (error out, caller discards the DB):
// ops after a skipped one can apply to the wrong state — most
// dangerously, a skipped `create` used to shift every subsequently
// auto-assigned inode number, silently attaching sizes and hardlinks
// to the wrong files. ReplayOp itself tolerates the benign idempotent
// cases (already-created, already-deleted), so an error here is a
// genuine inconsistency, not replay-over-existing-state noise.
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
	for _, m := range msgs {
		op, err := meta.DecodeOp(m.Caption.Payload)
		if err != nil {
			return fmt.Errorf("decode journal op seq=%d: %w", m.Caption.Seq, err)
		}
		if err := mstore.ReplayOp(ctx, op); err != nil {
			return fmt.Errorf("apply journal op seq=%d (%s): %w", m.Caption.Seq, op.Op, err)
		}
	}
	// Continue the sequence after the replayed ops (see AdvanceJournalSeq).
	if err := mstore.AdvanceJournalSeq(ctx, msgs[len(msgs)-1].Caption.Seq); err != nil {
		return err
	}
	fmt.Printf("Replayed %d journal ops.\n", len(msgs))
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
func maybeUnwrapEncrypted(data []byte, sec *secret.Secret) ([]byte, error) {
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
	if sec == nil {
		return nil, errors.New("encrypted snapshot: the profile's secret is needed")
	}
	derived := crypto.DeriveKey(sec.Bytes(), hdr.Salt, params)
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

// classify2 maps a format-2 message onto gc's kinds by decrypting its
// caption, and returns its caption kind ("" when it has none of ours).
// Anything that doesn't open under OUR caption key — text messages (the
// key message), format-1 leftovers, foreign documents — is KindUnknown,
// which gc never touches; so are the anchor and versions.
func classify2(cm tg.ChannelMessage, keys *wire.Keys) (tg.ChannelMessage, string) {
	out := tg.ChannelMessage{ID: cm.ID, Date: cm.Date, HasDoc: cm.HasDoc, RawCaption: cm.RawCaption}
	if !cm.HasDoc || !wire.LooksLikeCaption(cm.RawCaption) {
		return out, ""
	}
	c, err := keys.OpenCaption(cm.RawCaption)
	if err != nil {
		return out, ""
	}
	switch c.K {
	case wire.KindChunk, wire.KindPack:
		out.Kind = tg.KindChunk
	case wire.KindSnapshot:
		out.Kind = tg.KindSnapshot
		out.SnapCap = tg.SnapshotCaption{Kind: "snap", Seq: c.Seq, TSUnix: c.TS, FSUUID: c.FS}
	case wire.KindJournal:
		out.Kind = tg.KindJournal
		out.JnlCap = tg.JournalCaption{Kind: tg.MessageKindJournal, FSUUID: c.FS, FirstSeq: c.First, LastSeq: c.Last, Count: c.N}
	}
	// Anything else — the pinned anchor, a version, a kind a newer telfs
	// posts — stays KindUnknown, which gc never deletes.
	return out, c.K
}

// journalPruneBound is the highest journal seq gc may prune up to (-1:
// none). minRetained is the lowest seq of the retained snapshots the scan
// saw. The anchored snapshot may lie beyond the scanned history (a newer
// snapshot whose anchor post failed), and the journal after ITS seq is
// what a recovery replays: bound by its recorded seq (anchorSeq), or
// prune nothing when that is unknown (-1). As before, a scan that saw no
// retained snapshot prunes nothing.
func journalPruneBound(minRetained int64, anchorSnap int, anchorSeen bool, anchorSeq int64) int64 {
	if minRetained < 0 || anchorSnap <= 0 || anchorSeen {
		return minRetained
	}
	if anchorSeq < 0 {
		return -1
	}
	return min(minRetained, anchorSeq)
}

// gcVersionsRefusal refuses gc on a filesystem with versions (spec v1
// §2): v1 has no version-aware reclamation, and a chunk the live tree no
// longer references may still be one a version needs — gc would call it
// an orphan and delete it.
func gcVersionsRefusal(ctx context.Context, m *meta.Store) error {
	vs, err := snapshot.LoadVersions(ctx, m)
	if err != nil {
		return fmt.Errorf("gc: %w", err)
	}
	if len(vs) == 0 {
		return nil
	}
	return fmt.Errorf("gc: this filesystem has %d version(s) (newest: %s) — gc would delete chunks they still need (it only knows the live tree's). A version-aware gc comes later; until then gc refuses while versions exist (telfs versions list)", len(vs), versionsStatus(vs).Newest.Name)
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
	minAge := gcfs.Duration("min-age", time.Hour, "never touch messages younger than this — protects chunks whose chunk_map row hasn't landed yet")
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
	// HARD guard: gc against a live mount is a data-destruction race.
	// The referenced-set below is a point-in-time read of chunk_map; a
	// daemon actively uploading has a multi-minute window (packer
	// batching, FLOOD_WAIT backoff) where a chunk's message exists on
	// the channel but its chunk_map row doesn't. gc would classify it
	// as an orphan and delete freshly-written data.
	if _, mp, ok, ferr := mountctl.FindByProfile(cfg.ProfileName()); ferr != nil {
		return fmt.Errorf("gc: %w", ferr) // fail closed
	} else if ok {
		return fmt.Errorf("gc: profile %s is mounted at %s — unmount first (gc scans chunk_map at a point in time and would delete chunks whose rows haven't landed yet)", cfg.ProfileName(), mp)
	}
	if _, err := os.Stat(cfg.DBPath()); err != nil {
		return fmt.Errorf("gc: no local DB at %s", cfg.DBPath())
	}
	metaStore, err := meta.Open(cfg.DBPath())
	if err != nil {
		return err
	}
	defer metaStore.Close()

	if err := gcVersionsRefusal(ctx, metaStore); err != nil {
		return err
	}
	referenced, err := metaStore.AllChunkMessageIDs(ctx)
	if err != nil {
		return err
	}
	localUUID, err := metaStore.FSUUID(ctx)
	if err != nil {
		return fmt.Errorf("gc: read fs_uuid: %w", err)
	}
	var currentSnap, anchorSnap int
	if v, err := metaStore.GetKV(ctx, snapshot.KVCurrentMsgID); err == nil {
		if id, err := strconv.Atoi(string(v)); err == nil {
			currentSnap = id
		}
	}
	// Format 2: the snapshot the pinned anchor names is what a recovery
	// starts from — never stale, even when a newer one exists — and the
	// journal after its seq is what the recovery replays.
	anchorSeq := int64(-1) // unknown
	if v, err := metaStore.GetKV(ctx, snapshot.KVAnchorSnapID); err == nil {
		if id, err := strconv.Atoi(string(v)); err == nil {
			anchorSnap = id
		}
	}
	if v, err := metaStore.GetKV(ctx, snapshot.KVAnchorSnapSeq); err == nil {
		if n, err := strconv.ParseInt(string(v), 10, 64); err == nil {
			anchorSeq = n
		}
	}

	sec, err := readSecret(ctx, cfg, secret.Unlock)
	if err != nil {
		return err
	}
	defer sec.Zero()
	client, err := newClientWith(cfg, sec)
	if err != nil {
		return err
	}
	// Format 2: a message's kind is known only by decrypting its caption.
	var keys2 *wire.Keys
	if f, _ := metaStore.Format(ctx); f == 2 {
		if keys2, _, err = unlock2(ctx, metaStore, sec); err != nil {
			return err
		}
	}
	sec.Zero()
	return client.RunSession(ctx, func(ctx context.Context, sess *tg.Session) error {
		var orphanChunks, staleSnaps []int
		var totalChunks, totalSnaps, tooYoung, foreignSnaps, keptRetention int
		// Journal-message pruning state: a journal batch is garbage
		// once EVERY retained snapshot already includes its ops (its
		// last seq <= the oldest retained snapshot's seq) — no restore
		// from any snapshot we keep could ever need to replay it.
		type jnlMsg struct {
			id      int
			lastSeq int64
			date    int
		}
		var journalMsgs []jnlMsg
		minRetainedSeq := int64(-1) // min Seq across our retained snapshots
		anchorSeen := false         // the anchored snapshot was among the messages scanned
		var versionMsgs []int       // version messages (the registry may have lost them)
		ageCutoff := time.Now().Add(-*minAge).Unix()
		fmt.Printf("Scanning channel (up to %d pages of 100 messages)…\n", *pages)
		if err := sess.WalkChannelMessages(ctx, *pages, func(cm tg.ChannelMessage) error {
			if keys2 != nil {
				var k string
				if cm, k = classify2(cm, keys2); k == wire.KindVersion {
					versionMsgs = append(versionMsgs, cm.ID)
				}
			}
			switch cm.Kind {
			case tg.KindJournal:
				if cm.JnlCap.FSUUID != localUUID {
					return nil
				}
				eff := cm.JnlCap.LastSeq
				if eff == 0 {
					eff = cm.JnlCap.Seq // legacy single-op caption
				}
				journalMsgs = append(journalMsgs, jnlMsg{id: cm.ID, lastSeq: eff, date: cm.Date})
			case tg.KindChunk:
				totalChunks++
				if _, ref := referenced[int64(cm.ID)]; ref {
					return nil
				}
				// Age gate: a chunk message younger than --min-age may
				// belong to an upload whose chunk_map row is still in
				// flight (or a mount we failed to detect). Skip it —
				// the next gc run reaps it once it's provably stale.
				if cm.Date > 0 && int64(cm.Date) > ageCutoff {
					tooYoung++
					return nil
				}
				orphanChunks = append(orphanChunks, cm.ID)
			case tg.KindSnapshot:
				// NEVER touch another TelFS instance's snapshots. A
				// channel that hosts (or hosted) a second instance —
				// e.g. after a channel rebind — is exactly when blind
				// snapshot deletion destroys someone's recovery anchor.
				if cm.SnapCap.FSUUID != "" && cm.SnapCap.FSUUID != localUUID {
					foreignSnaps++
					return nil
				}
				totalSnaps++
				keepSnap := func(seq int64) {
					if minRetainedSeq < 0 || seq < minRetainedSeq {
						minRetainedSeq = seq
					}
				}
				if anchorSnap > 0 && cm.ID == anchorSnap {
					anchorSeen = true
				}
				if cm.ID == currentSnap || (anchorSnap > 0 && cm.ID == anchorSnap) {
					keepSnap(cm.SnapCap.Seq)
					return nil
				}
				// Keep the retention window the snapshot manager
				// maintains (newest-first walk ⇒ the first N we see
				// are the newest N). Only snapshots beyond it are
				// stale. The age gate applies here too.
				if totalSnaps <= snapshot.DefaultRetention {
					keptRetention++
					keepSnap(cm.SnapCap.Seq)
					return nil
				}
				if cm.Date > 0 && int64(cm.Date) > ageCutoff {
					tooYoung++
					keepSnap(cm.SnapCap.Seq)
					return nil
				}
				staleSnaps = append(staleSnaps, cm.ID)
			}
			return nil
		}); err != nil {
			return err
		}

		// A version the registry does not list (lost with a local DB
		// rebuilt from an anchor that never got its version list) still
		// needs its chunks: refuse before deleting anything.
		if len(versionMsgs) > 0 {
			return fmt.Errorf("gc: the channel holds %d version message(s) of this filesystem (e.g. message %d) that the local registry does not list — gc would delete chunks they need; refusing (a version-aware gc comes later)", len(versionMsgs), versionMsgs[0])
		}

		// Journal batches wholly covered by EVERY retained snapshot are
		// garbage: no restore from a snapshot we keep could ever need
		// to replay them. If the scan saw no retained snapshot of ours,
		// prune nothing — those messages may be the only recovery
		// record there is.
		// The anchored snapshot may lie beyond the scanned history (a
		// newer snapshot whose anchor post failed): the journal after its
		// seq is still what a recovery replays. Bound by its recorded
		// seq; when that is unknown, prune no journal at all.
		if anchorSnap > 0 && !anchorSeen && anchorSeq < 0 {
			fmt.Printf("The anchored snapshot (message %d) is beyond the scanned history and its seq is not recorded — no journal batch is pruned.\n", anchorSnap)
		}
		minRetainedSeq = journalPruneBound(minRetainedSeq, anchorSnap, anchorSeen, anchorSeq)
		var staleJournal []int
		if minRetainedSeq >= 0 {
			for _, jm := range journalMsgs {
				if jm.lastSeq <= 0 || jm.lastSeq > minRetainedSeq {
					continue
				}
				if jm.date > 0 && int64(jm.date) > ageCutoff {
					tooYoung++
					continue
				}
				staleJournal = append(staleJournal, jm.id)
			}
		}

		// fs_uuid sanity: if the channel shows snapshots from a foreign
		// instance and NONE from us, this profile is almost certainly
		// pointed at the wrong channel (rebind mistake). Deleting
		// "orphan" chunks here would destroy the other instance's data
		// — every one of its chunks is unreferenced by OUR chunk_map.
		if foreignSnaps > 0 && totalSnaps == 0 {
			return fmt.Errorf("gc: channel contains snapshots from a different TelFS instance (fs_uuid mismatch, %d seen) and none from this one — refusing to delete anything; check the channel binding", foreignSnaps)
		}

		fmt.Printf("\nChunks  in channel: %d   referenced in chunk_map: %d   orphan: %d   (skipped %d younger than %s)\n",
			totalChunks, len(referenced), len(orphanChunks), tooYoung, *minAge)
		fmt.Printf("Snapshots in channel (ours): %d   current msg_id: %d   kept for retention: %d   stale: %d\n",
			totalSnaps, currentSnap, keptRetention, len(staleSnaps))
		fmt.Printf("Journal messages (ours): %d   superseded by retained snapshots: %d\n",
			len(journalMsgs), len(staleJournal))
		if foreignSnaps > 0 {
			fmt.Printf("Snapshots from other TelFS instances (left untouched): %d\n", foreignSnaps)
		}
		if len(orphanChunks)+len(staleSnaps)+len(staleJournal) == 0 {
			fmt.Println("\nNothing to do.")
			return nil
		}

		fmt.Printf("\nOrphan chunk msg_ids:   %v\n", orphanChunks)
		fmt.Printf("Stale snapshot msg_ids: %v\n", staleSnaps)
		fmt.Printf("Stale journal msg_ids:  %v\n", staleJournal)

		if !*doDelete {
			fmt.Println("\nDry-run. Re-run with --yes to delete.")
			return nil
		}

		// Re-check chunk_map immediately before deleting: the channel
		// scan above can take minutes, and even with the mount guard
		// (best-effort /proc/mounts) something may have committed rows
		// in the meantime. One extra SELECT is cheap insurance.
		refreshed, err := metaStore.AllChunkMessageIDs(ctx)
		if err != nil {
			return fmt.Errorf("gc: refresh referenced set: %w", err)
		}
		toDelete := make([]int, 0, len(orphanChunks)+len(staleSnaps)+len(staleJournal))
		rescued := 0
		for _, id := range orphanChunks {
			if _, ref := refreshed[int64(id)]; ref {
				rescued++
				continue
			}
			toDelete = append(toDelete, id)
		}
		toDelete = append(toDelete, staleSnaps...)
		toDelete = append(toDelete, staleJournal...)
		if rescued > 0 {
			fmt.Printf("Rescued %d chunk(s) that became referenced during the scan.\n", rescued)
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
		return errors.New("debug: missing subcommand (dump-msg, observe)")
	}
	switch args[0] {
	case "observe":
		return cmdDebugObserve(ctx, args[1:])
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
	client, err := newClient(ctx, cfg)
	if err != nil {
		return err
	}
	return client.RunSession(ctx, func(ctx context.Context, sess *tg.Session) error {
		_, err := sess.DownloadDocument(ctx, msgID, os.Stdout)
		return err
	})
}

// clearDeadMounts handles this profile's mounts left behind by a daemon
// that died (crash, OOM, systemctl kill -s KILL): the kernel answers
// ENOTCONN for them, and the next mount used to refuse with "profile … is
// already mounted" (v1.1 I7). Called with the profile lock held, so no
// daemon of this profile is alive. The dead mount on our own mountpoint is
// lazily unmounted so we can mount there again; any other (a bind-mount
// view of it) no longer blocks the mount — the guard skips dead mounts —
// and is named for its owner to remount once this mount is up.
func clearDeadMounts(profile, mountpoint string) {
	for _, m := range mountctl.ScanMounts() {
		if p, ok := mountctl.ProfileOf(m); !ok || p != profile || mountctl.VersionOf(m) != "" || !mountctl.Dead(m) {
			continue
		}
		if m.Point != mountpoint {
			fmt.Fprintf(os.Stderr, "note: %s is a dead view of profile %s's previous mount (its daemon is gone) — remount it once this mount is up\n", m.Point, profile)
			continue
		}
		if err := mountctl.LazyUnmount(m.Point); err != nil {
			fmt.Fprintf(os.Stderr, "profile %s's dead mount at %s could not be cleared: %v\n", profile, m.Point, err)
			continue
		}
		fmt.Fprintf(os.Stderr, "cleared profile %s's dead mount at %s (its daemon was gone)\n", profile, m.Point)
	}
}

// lockProfile takes an exclusive lock on <profile>/daemon.lock for the
// life of the mount daemon (single-writer guard against two mounts of
// one profile racing). The kernel drops it when the process ends.
func lockProfile(cfg *config.Config) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(cfg.DataDir, "daemon.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("profile lock: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("mount: another telfs daemon for profile %s is starting or running (profile lock held)", cfg.ProfileName())
	}
	return f, nil
}

// unmountViaControl stops the daemon of mountpoint through its profile's
// control socket and waits up to timeout for the process to exit. done is
// false when there is no such daemon to talk to (fall back to signals).
func unmountViaControl(mountpoint string, timeout time.Duration) (done bool, err error) {
	var profile, version string
	for _, m := range mountctl.ScanMounts() {
		if m.Point == mountpoint {
			profile, _ = mountctl.ProfileOf(m)
			version = mountctl.VersionOf(m)
		}
	}
	if profile == "" {
		return false, nil
	}
	dir, derr := config.ProfileDir(profile)
	if derr != nil {
		return false, nil
	}
	// A version mount has its own socket: the profile's is the live
	// mount's, which must not be the one stopped.
	sock := control.Path(dir)
	if version != "" {
		sock = control.VersionPath(dir, version)
	}
	resp, perr := control.Client(sock, 10*time.Second).Post(control.BaseURL+"/stop", "application/json", nil)
	if perr != nil {
		return false, nil
	}
	var body struct {
		PID int `json:"pid"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	resp.Body.Close()
	fmt.Printf("Stopping mount daemon (pid %d) for %s — waiting for it to finish and exit…\n", body.PID, mountpoint)
	deadline := time.Now().Add(timeout)
	for syscall.Kill(body.PID, 0) == nil {
		if time.Now().After(deadline) {
			return true, fmt.Errorf("unmount: the daemon (pid %d) is still running after %s — its log says why; `telfs unmount --timeout` waits longer", body.PID, timeout)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if mountctl.IsMounted(mountpoint) {
		if err := mountctl.LazyUnmount(mountpoint); err != nil {
			return true, fmt.Errorf("unmount: daemon exited but %s is still mounted: %w", mountpoint, err)
		}
	}
	return true, nil
}
