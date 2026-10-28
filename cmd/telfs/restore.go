package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gotd/td/tgerr"

	"telfs/internal/chunk"
	"telfs/internal/compress"
	"telfs/internal/config"
	"telfs/internal/control"
	"telfs/internal/meta"
	"telfs/internal/mountctl"
	"telfs/internal/restore"
	"telfs/internal/secret"
	"telfs/internal/snapshot"
	"telfs/internal/tg"
	"telfs/internal/wire"
)

const restoreUsage = "usage: telfs restore [--version NAME] [--workers N] SRC DEST"

// errRestoreFormat1: restore is a format-2 feature (decision D1).
var errRestoreFormat1 = errors.New("restore needs a format-2 filesystem (format 1 keeps working as in v0.22, without `telfs restore`: copy the files out of a mount, e.g. with rsync)")

// restoreConnectTimeout bounds the wait for the first Telegram
// connection (gotd's own first connect waits forever).
const restoreConnectTimeout = 2 * time.Minute

// cmdRestore is `telfs restore [--version NAME] SRC DEST` (spec v1 row 6
// + addition 4): SRC (a path in the filesystem; a directory's contents
// become DEST's) is restored into the local directory DEST from the live
// filesystem's DB — read consistently, beside a running mount — or from
// a version's. Each needed channel message is downloaded once, every
// restored file is re-read and checked against its keyed hashes, and a
// re-run skips what is already right. Exit 0 all restored and verified,
// 6 Telegram failed (re-run to resume), 7 integrity problems (files on
// stderr and in <profile>/restore-<time>.tsv), 4 a live restore met files
// the mount had not uploaded yet, 1 DEST could not be written.
func cmdRestore(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("restore", flag.ContinueOnError)
	versionName := fs.String("version", "", "restore from this version (telfs versions list) instead of the live filesystem")
	workers := fs.Int("workers", 4, "channel messages downloaded at once")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return errors.New(restoreUsage)
	}
	if *workers < 1 || *workers > 64 {
		return fmt.Errorf("--workers %d: want 1 to 64", *workers)
	}
	if *versionName != "" {
		if err := snapshot.ValidateVersionName(*versionName); err != nil {
			return err
		}
	}
	src, destArg := pos[0], pos[1]
	if !strings.HasPrefix(src, "/") {
		return fmt.Errorf("SRC %q must be an absolute path inside the filesystem (e.g. / or /home/arash)\n%s", src, restoreUsage)
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	dest, err := checkRestoreDest(cfg, destArg, mountctl.ScanProcMounts())
	if err != nil {
		return err
	}
	if _, err := os.Stat(cfg.DBPath()); err != nil {
		return fmt.Errorf("restore needs the profile's local DB (%s): mount the profile once — it recovers the DB from the channel", cfg.DBPath())
	}
	live, err := meta.OpenReadOnly(cfg.DBPath(), false)
	if err != nil {
		return err
	}
	defer live.Close()
	if f, err := live.Format(ctx); err != nil {
		return err
	} else if f != 2 {
		return errRestoreFormat1
	}
	var v wire.AnchorVersion
	if *versionName != "" {
		vs, err := snapshot.LoadVersions(ctx, live)
		if err != nil {
			return err
		}
		var ok bool
		if v, ok = snapshot.FindVersion(vs, *versionName); !ok {
			return fmt.Errorf("%w: %q (telfs versions list)", snapshot.ErrNoVersion, *versionName)
		}
	}
	fsUUID, err := live.FSUUID(ctx)
	if err != nil {
		return err
	}
	// The live tree is planned before the secret and the network: a
	// wrong SRC fails at once.
	var plan *restore.Plan
	if *versionName == "" {
		if plan, err = planRestore(ctx, live, src, "the live filesystem"); err != nil {
			return err
		}
	}
	if err := cfg.RequireChannel(); err != nil {
		return err
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
	// The live DB's key header is the current one (a version's copy is
	// as old as the version).
	keys, _, err := unlock2(ctx, live, sec)
	if err != nil {
		return err
	}
	sec.Zero()

	sess, stop, err := connectReadOnly(ctx, client)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return errors.New("restore interrupted")
		}
		return err
	}
	defer stop()

	store := live
	if *versionName != "" {
		if err := ensureVersionDB(ctx, cfg, sess, keys, v); err != nil {
			return err
		}
		vstore, err := meta.OpenReadOnly(versionDBPath(cfg, v.Name), true)
		if err != nil {
			return fmt.Errorf("%w: version %s: %w", errIntegrity, v.Name, err)
		}
		defer vstore.Close()
		if f, _ := vstore.Format(ctx); f != 2 {
			return fmt.Errorf("%w: version %s: its DB is not a format-2 filesystem", errIntegrity, v.Name)
		}
		if got, _ := vstore.FSUUID(ctx); got != fsUUID {
			return fmt.Errorf("%w: version %s holds filesystem %q, this profile's is %q", errIntegrity, v.Name, got, fsUUID)
		}
		store = vstore
		if plan, err = planRestore(ctx, store, src, "version "+v.Name); err != nil {
			return err
		}
	}
	codecName, err := store.CompressionCodec(ctx)
	if err != nil {
		return err
	}
	codec, err := compress.New(codecName)
	if err != nil {
		return err
	}
	wc, err := chunk.NewFormat2Codec(keys, codec, nil)
	if err != nil {
		return err
	}

	var inFlight func(context.Context, *restore.Entry) bool
	if *versionName == "" {
		inFlight = inFlightCheck(cfg, live)
	}
	start := time.Now()
	res, err := restore.Run(ctx, plan, restore.Options{
		Dest:      dest,
		Codec:     wc,
		Download:  tgDownloader{f: &chunk.TGFetcher{Session: sess}},
		Workers:   *workers,
		Retriable: retriable,
		Telegram:  telegramFailure,
		InFlight:  inFlight,
		Chown:     os.Geteuid() == 0,
		Logf:      func(f string, a ...any) { fmt.Printf(f+"\n", a...) },
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return errors.New("restore interrupted — run it again to resume (what is already right is skipped)")
		}
		return err
	}
	return reportRestore(cfg, src, dest, res, time.Since(start), os.Stdout, os.Stderr)
}

// planRestore reads the plan and says what it holds.
func planRestore(ctx context.Context, m *meta.Store, src, from string) (*restore.Plan, error) {
	fmt.Printf("Reading %s of %s…\n", src, from)
	plan, err := restore.BuildPlan(ctx, m, src)
	if err != nil {
		return nil, err
	}
	dirs, files, symlinks, links, bytes := plan.Totals()
	fmt.Printf("  %d file(s) (%s), %d director(y/ies), %d symlink(s), %d hard link(s)\n", files, humanBytes(bytes), dirs, symlinks, links)
	return plan, nil
}

// checkRestoreDest resolves DEST (it may not exist yet) and refuses a
// DEST inside a TelFS mount — restore writes to local storage — or one
// that holds, or lies inside, the profile directory (the report goes
// there, never into DEST; and restoring over the profile's own state
// would be destructive).
func checkRestoreDest(cfg *config.Config, dest string, mounts []string) (string, error) {
	if dest == "" {
		return "", errors.New(restoreUsage)
	}
	abs, err := filepath.Abs(dest)
	if err != nil {
		return "", err
	}
	resolved := resolveExisting(abs)
	for _, mp := range mounts {
		if within(resolved, resolveExisting(mp)) {
			return "", fmt.Errorf("DEST %s is inside the TelFS mount %s: restore into a local directory", dest, mp)
		}
	}
	profile := resolveExisting(cfg.DataDir)
	switch {
	case within(profile, resolved):
		return "", fmt.Errorf("DEST %s holds the profile directory %s: choose a directory outside it", dest, cfg.DataDir)
	case within(resolved, profile):
		return "", fmt.Errorf("DEST %s is inside the profile directory %s: choose a directory outside it", dest, cfg.DataDir)
	}
	return resolved, nil
}

// resolveExisting resolves the symlinks of p's longest existing prefix
// and appends the rest.
func resolveExisting(p string) string {
	p = filepath.Clean(p)
	rest := ""
	for {
		if r, err := filepath.EvalSymlinks(p); err == nil {
			return filepath.Join(r, rest)
		}
		parent := filepath.Dir(p)
		if parent == p {
			return filepath.Join(p, rest)
		}
		rest = filepath.Join(filepath.Base(p), rest)
		p = parent
	}
}

// within reports whether p is root or below it.
func within(p, root string) bool {
	return p == root || root == "/" || strings.HasPrefix(p, strings.TrimSuffix(root, "/")+"/")
}

// connectReadOnly starts a long-lived session (it reconnects) that
// refuses every write to the channel, and checks the channel is
// reachable. stop ends it.
func connectReadOnly(ctx context.Context, client *tg.Client) (*tg.Session, func(), error) {
	mgr := client.NewSessionManager()
	sess := mgr.Session()
	sess.SetReadOnly(true)
	mgrCtx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	done := make(chan error, 1)
	go func() {
		err := mgr.Run(mgrCtx, func() error {
			select {
			case first <- nil:
			default:
			}
			return nil
		})
		select {
		case first <- err:
		default:
		}
		done <- err
	}()
	stop := func() {
		cancel()
		<-done
	}
	fmt.Println("Connecting to Telegram…")
	select {
	case err := <-first:
		if err != nil {
			stop()
			return nil, nil, fmt.Errorf("%w: %w", errTelegram, err)
		}
	case <-ctx.Done():
		stop()
		return nil, nil, ctx.Err()
	case <-time.After(restoreConnectTimeout):
		stop()
		return nil, nil, fmt.Errorf("%w: no connection within %s", errTelegram, restoreConnectTimeout)
	}
	vctx, vcancel := context.WithTimeout(ctx, 30*time.Second)
	defer vcancel()
	if err := sess.VerifyAccess(vctx); err != nil {
		stop()
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		return nil, nil, fmt.Errorf("%w: %w", errTelegram, err)
	}
	return sess, stop, nil
}

// ensureVersionDB makes <profile>/versions/NAME.sqlite the version's DB:
// the local copy while it matches the registry (size + SHA-256), else a
// fresh download under the version's lock (a version mount takes it).
func ensureVersionDB(ctx context.Context, cfg *config.Config, sess *tg.Session, keys *wire.Keys, v wire.AnchorVersion) error {
	dbPath := versionDBPath(cfg, v.Name)
	created := time.Unix(v.Created, 0).UTC().Format(time.RFC3339)
	if snapshot.VersionFileMatches(dbPath, v) {
		fmt.Printf("Version %s (%s): using the local copy %s\n", v.Name, created, dbPath)
		return nil
	}
	if err := os.MkdirAll(versionsDir(cfg), 0o700); err != nil {
		return fmt.Errorf("versions dir: %w", err)
	}
	lock, err := os.OpenFile(filepath.Join(versionsDir(cfg), v.Name+".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("version lock: %w", err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if snapshot.VersionFileMatches(dbPath, v) {
			return nil
		}
		return fmt.Errorf("version %s is being downloaded by another telfs process (a version mount starting?): try again in a moment", v.Name)
	}
	if snapshot.VersionFileMatches(dbPath, v) {
		return nil
	}
	fmt.Printf("Version %s (%s): downloading message %d (%s DB) into %s…\n", v.Name, created, v.Msg, humanBytes(v.Size), dbPath)
	if err := snapshot.DownloadVersion(ctx, sess, keys, v, dbPath); err != nil {
		if errors.Is(err, snapshot.ErrChannel) || retriable(err) {
			return fmt.Errorf("%w: %w", errTelegram, err)
		}
		return fmt.Errorf("%w: %w", errIntegrity, err)
	}
	return nil
}

// tgDownloader is restore.Downloader over the session.
type tgDownloader struct{ f *chunk.TGFetcher }

func (d tgDownloader) Fetch(ctx context.Context, msg int64) ([]byte, error) {
	return d.f.Fetch(ctx, chunk.Key{}, msg)
}

func (d tgDownloader) FetchRange(ctx context.Context, msg, off, n int64) ([]byte, error) {
	return d.f.FetchRange(ctx, chunk.Key{}, msg, off, n)
}

// telegramFailure: a download error that is Telegram's or the network's
// (an RPC error, or what the retries were for) — not a message that is
// gone or does not hold what the DB says (integrity).
func telegramFailure(err error) bool {
	if _, ok := tgerr.As(err); ok {
		return true
	}
	return retriable(err) || errors.Is(err, tg.ErrNotConnected)
}

// inFlightCheck tells, for a restore of the live filesystem, whether a
// file the DB showed with missing or short chunks is data the mount had
// not uploaded yet rather than lost: the file changed since the read
// (its rows landed, or it was written again), or uploads are pending —
// in the running daemon, or staged for the next mount when none runs.
func inFlightCheck(cfg *config.Config, live *meta.Store) func(context.Context, *restore.Entry) bool {
	var once sync.Once
	var uploading bool
	return func(ctx context.Context, e *restore.Entry) bool {
		once.Do(func() { uploading = uploadsPending(ctx, cfg) })
		return uploading || changedSince(ctx, live, e)
	}
}

// uploadsPending: the profile's daemon has uploads open, or — no daemon
// answering — its staging directory holds wires.
func uploadsPending(ctx context.Context, cfg *config.Config) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, control.BaseURL+"/sync", nil)
	if err == nil {
		if resp, err := ctlClient(cfg, 5*time.Second).Do(req); err == nil {
			defer resp.Body.Close()
			var rep syncReply
			if json.NewDecoder(resp.Body).Decode(&rep) == nil {
				return rep.PendingUploads > 0
			}
		}
	}
	ents, err := os.ReadDir(cfg.StagingDir())
	return err == nil && len(ents) > 0
}

// changedSince re-reads e's inode and chunk rows: did the file change
// after the restore's read?
func changedSince(ctx context.Context, m *meta.Store, e *restore.Entry) bool {
	changed := false
	err := m.Read(ctx, func(v *meta.ReadView) error {
		in, err := v.GetInode(ctx, e.Inode.Ino)
		if err != nil || in.Size != e.Inode.Size || in.Mtime != e.Inode.Mtime {
			changed = true // rewritten, or gone
			return nil
		}
		rows, err := v.Chunks(ctx, e.Inode.Ino)
		if err != nil {
			return err
		}
		now := map[int32]meta.Chunk{}
		for _, c := range rows {
			now[c.Idx] = c
		}
		for _, idx := range e.Missing {
			if _, ok := now[idx]; ok {
				changed = true
			}
		}
		for _, c := range e.Chunks {
			if n, ok := now[c.Idx]; !ok || n.TGMessageID != c.TGMessageID || n.Size != c.Size || n.PackOffset != c.PackOffset {
				changed = true
			}
		}
		return nil
	})
	return changed || err != nil
}

// reportListMax is how many problem files stderr names.
const reportListMax = 20

// reportRestore prints the outcome and maps it to the exit code: 7 when
// any file has an integrity problem, else 6 when Telegram failed, else 4
// when a live restore met data not uploaded yet, else 1 when DEST could
// not be written; every problem file goes to
// <profile>/restore-<time>.tsv (never into DEST), the first 20 to stderr.
func reportRestore(cfg *config.Config, src, dest string, res *restore.Result, took time.Duration, stdout, stderr io.Writer) error {
	restored := res.Files - res.Identical
	fmt.Fprintf(stdout, "Restore of %s into %s: %d file(s) written, %d already identical; %d director(y/ies), %d symlink(s), %d hard link(s).\n",
		src, dest, restored, res.Identical, res.Dirs, res.Symlinks, res.Links)
	fmt.Fprintf(stdout, "  %s downloaded in %d message(s), %s written, %d file(s) re-read and verified, in %s.\n",
		humanBytes(res.Downloaded), res.Messages, humanBytes(res.Written), res.Verified, took.Round(time.Second))
	if res.Stray > 0 {
		fmt.Fprintf(stdout, "  (%d chunk row(s) past the end of their file ignored)\n", res.Stray)
	}
	if len(res.Problems) == 0 {
		fmt.Fprintln(stdout, "OK — every restored file matches its keyed hashes.")
		return nil
	}
	tsv := filepath.Join(cfg.DataDir, "restore-"+time.Now().Format("20060102-150405")+".tsv")
	werr := writeRestoreTSV(tsv, src, dest, res.Problems)
	fmt.Fprintf(stderr, "%d file(s) not restored exactly:\n", len(res.Problems))
	for i, p := range res.Problems {
		if i == reportListMax {
			fmt.Fprintf(stderr, "  … and %d more\n", len(res.Problems)-reportListMax)
			break
		}
		fmt.Fprintf(stderr, "  %-9s %s: %s\n", p.Kind, p.Path, p.Detail)
	}
	if werr != nil {
		fmt.Fprintf(stderr, "could not write the list to %s: %v\n", tsv, werr)
	} else {
		fmt.Fprintf(stderr, "All of them: %s\n", tsv)
	}
	n := len(res.Problems)
	switch {
	case res.Has(restore.KindIntegrity):
		return fmt.Errorf("%w: %d file(s) do not match the filesystem's metadata or keyed hashes (list: %s)", errIntegrity, n, tsv)
	case res.Has(restore.KindTelegram):
		return fmt.Errorf("%w: %d file(s) not downloaded — run the restore again to resume (list: %s)", errTelegram, n, tsv)
	case res.Has(restore.KindPending):
		return fmt.Errorf("%w: %d file(s) were still being uploaded by the mount — `telfs sync --wait`, then run the restore again (list: %s)", errNotSynced, n, tsv)
	default:
		return fmt.Errorf("%d file(s) could not be written into %s (list: %s)", n, dest, tsv)
	}
}

// writeRestoreTSV writes the problem list: kind, path in the filesystem,
// what is wrong (tabs, newlines and backslashes escaped).
func writeRestoreTSV(path, src, dest string, ps []restore.Problem) error {
	esc := strings.NewReplacer(`\`, `\\`, "\t", `\t`, "\n", `\n`, "\r", `\r`)
	var b strings.Builder
	fmt.Fprintf(&b, "# telfs restore %s -> %s, %s\n", esc.Replace(src), esc.Replace(dest), time.Now().UTC().Format(time.RFC3339))
	b.WriteString("kind\tpath\tproblem\n")
	for _, p := range ps {
		fmt.Fprintf(&b, "%s\t%s\t%s\n", p.Kind, esc.Replace(p.Path), esc.Replace(p.Detail))
	}
	return os.WriteFile(path, []byte(b.String()), 0o600)
}
