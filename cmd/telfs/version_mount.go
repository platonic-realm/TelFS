package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"telfs/internal/chunk"
	"telfs/internal/compress"
	"telfs/internal/config"
	"telfs/internal/control"
	telfsfs "telfs/internal/fs"
	"telfs/internal/meta"
	"telfs/internal/mountctl"
	"telfs/internal/secret"
	"telfs/internal/snapshot"
	"telfs/internal/tg"
	"telfs/internal/wire"
)

// versionMountOpts are the `telfs mount` flags a version mount uses.
type versionMountOpts struct {
	name, mountpoint                    string
	allowOther, debug, service          bool
	cacheCap, memCacheCap, blobPrefetch string
	prefetchWindow, prefetchConcurrency int
}

// lockVersion takes <profile>/versions/NAME.lock for the life of a
// version mount: one mount per version. The profile's daemon.lock is the
// live mount's and is not taken — a version mount runs next to it.
func lockVersion(cfg *config.Config, name string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(versionsDir(cfg), name+".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("version lock: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if mp, ok := mountctl.VersionMounts(cfg.ProfileName())[name]; ok {
			return nil, fmt.Errorf("mount: version %s of profile %s is already mounted at %s", name, cfg.ProfileName(), mp)
		}
		return nil, fmt.Errorf("mount: another telfs daemon for version %s of profile %s is starting or running (version lock held)", name, cfg.ProfileName())
	}
	return f, nil
}

// versionToMount reads from the live filesystem's DB — read-only, beside
// a running daemon — what a version mount needs: the registry entry, the
// fs_uuid, and the store whose (current) key header unlocks the keys.
// The caller closes the store.
func versionToMount(ctx context.Context, cfg *config.Config, name string) (*meta.Store, wire.AnchorVersion, string, error) {
	var v wire.AnchorVersion
	if _, err := os.Stat(cfg.DBPath()); err != nil {
		return nil, v, "", fmt.Errorf("a version mount needs the profile's local DB (%s: its key header and version registry) — mount the profile once: it recovers the DB from the channel", cfg.DBPath())
	}
	live, err := meta.OpenReadOnly(cfg.DBPath(), false)
	if err != nil {
		return nil, v, "", err
	}
	fail := func(err error) (*meta.Store, wire.AnchorVersion, string, error) {
		live.Close()
		return nil, v, "", err
	}
	if f, err := live.Format(ctx); err != nil {
		return fail(err)
	} else if f != 2 {
		return fail(snapshot.ErrVersionFormat1)
	}
	vs, err := snapshot.LoadVersions(ctx, live)
	if err != nil {
		return fail(err)
	}
	v, ok := snapshot.FindVersion(vs, name)
	if !ok {
		return fail(fmt.Errorf("%w: %q (telfs versions list)", snapshot.ErrNoVersion, name))
	}
	fsUUID, err := live.FSUUID(ctx)
	if err != nil {
		return fail(err)
	}
	return live, v, fsUUID, nil
}

// versionDownloadErr gives a failed version download its exit code: 7
// when the version is damaged or not what the registry says (a restart
// would download the same), 6 when Telegram or the network failed.
func versionDownloadErr(err error) error {
	switch {
	case errors.Is(err, snapshot.ErrVersionIntegrity):
		return fmt.Errorf("%w: %w", errIntegrity, err)
	case errors.Is(err, snapshot.ErrChannel) || retriable(err):
		return fmt.Errorf("%w: %w", errTelegram, err)
	}
	return err
}

// mountVersion is `telfs mount --readonly --version NAME <mp>` (spec v1
// §2): the version's DB is downloaded into <profile>/versions/NAME.sqlite
// (kept, and reused while it matches the registry's size and SHA-256),
// opened immutable and served read-only. Nothing is written to the
// channel (the session refuses it) and nothing to the live filesystem's
// DB, staging directory or cache: no snapshot manager, journal poster or
// staged-upload recovery runs. Its own lock, cache, control socket and
// mount source ("…:v=NAME", ignored by the single-writer guard) let it
// run next to the live read-write mount of the same profile.
func mountVersion(signalCtx context.Context, cfg *config.Config, o versionMountOpts) error {
	if err := snapshot.ValidateVersionName(o.name); err != nil {
		return err
	}
	if err := os.MkdirAll(versionsDir(cfg), 0o700); err != nil {
		return fmt.Errorf("versions dir: %w", err)
	}
	lock, err := lockVersion(cfg, o.name)
	if err != nil {
		return err
	}
	defer lock.Close()

	live, v, fsUUID, err := versionToMount(signalCtx, cfg, o.name)
	if err != nil {
		return err
	}
	defer live.Close()

	noPrompt = o.service
	svc := newService(o.service)
	go svc.keepStarting(signalCtx)
	svc.status("starting")
	sec, err := readSecret(signalCtx, cfg, secret.Unlock)
	if err != nil {
		return err
	}
	defer sec.Zero()
	client, err := newClientWith(cfg, sec)
	if err != nil {
		return err
	}
	// The live DB's key header is the current one (a version's own copy
	// is as old as the version: a later secret change re-wrapped the
	// same key under another secret).
	keys, _, err := unlock2(signalCtx, live, sec)
	if err != nil {
		return err
	}
	sec.Zero()
	live.Close()

	dbPath := versionDBPath(cfg, o.name)
	created := time.Unix(v.Created, 0).UTC().Format(time.RFC3339)
	if snapshot.VersionFileMatches(dbPath, v) {
		fmt.Printf("Version %s (%s): using the local copy %s\n", o.name, created, dbPath)
	} else {
		svc.status("downloading version " + o.name)
		fmt.Printf("Version %s (%s): downloading message %d (%s DB) into %s…\n", o.name, created, v.Msg, humanBytes(v.Size), dbPath)
		err := svc.retryAtBoot(signalCtx, "version download", func(ctx context.Context) error {
			return client.RunSession(ctx, func(ctx context.Context, sess *tg.Session) error {
				sess.SetReadOnly(true)
				return snapshot.DownloadVersion(ctx, sess, keys, v, dbPath)
			})
		})
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return versionDownloadErr(err)
		}
	}
	vstore, err := meta.OpenReadOnly(dbPath, true)
	if err != nil {
		return fmt.Errorf("%w: version %s: %w", errIntegrity, o.name, err)
	}
	defer vstore.Close()
	if f, _ := vstore.Format(signalCtx); f != 2 {
		return fmt.Errorf("%w: version %s: its DB is not a format-2 filesystem", errIntegrity, o.name)
	}
	if got, _ := vstore.FSUUID(signalCtx); got != fsUUID {
		return fmt.Errorf("%w: version %s holds filesystem %q, this profile's is %q", errIntegrity, o.name, got, fsUUID)
	}
	codecName, err := vstore.CompressionCodec(signalCtx)
	if err != nil {
		return err
	}
	codec, err := compress.New(codecName)
	if err != nil {
		return err
	}
	wc, err := chunk.NewFormat2Codec(keys, codec, vstore)
	if err != nil {
		return err
	}
	chunkSize, err := vstore.ChunkSize(signalCtx)
	if err != nil {
		return err
	}

	svc.status("connecting to Telegram")
	mgr := client.NewSessionManager()
	sess := mgr.Session()
	sess.SetReadOnly(true) // refuses every upload, post, deletion and pin
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
		select {
		case firstConnect <- err:
		default:
		}
		mgrDone <- err
	}()
	stop := func() {
		stopMgr()
		<-mgrDone
	}
	select {
	case err := <-firstConnect:
		if err != nil {
			stop()
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("%w: %w", errTelegram, err)
		}
	case <-signalCtx.Done():
		stop()
		return nil
	}
	svc.status("checking access to the channel")
	if verr := svc.retryAtBoot(signalCtx, "channel access", func(ctx context.Context) error {
		vctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		return sess.VerifyAccess(vctx)
	}); verr != nil {
		stop()
		if errors.Is(verr, context.Canceled) {
			return nil
		}
		return fmt.Errorf("mount preflight: %w: %w", errTelegram, verr)
	}

	cacheDir, err := prepareVersionCache(cfg, v)
	if err != nil {
		stop()
		return err
	}
	err = serveVersion(signalCtx, cfg, o, sess, vstore, wc, codec, chunkSize, fsUUID, v, cacheDir, svc)
	stop()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// serveVersion mounts the version's DB read-only and serves it until the
// stop signal.
func serveVersion(signalCtx context.Context, cfg *config.Config, o versionMountOpts, sess *tg.Session, vstore *meta.Store,
	wc chunk.WireCodec, codec compress.Codec, chunkSize int64, fsUUID string, v wire.AnchorVersion, cacheDir string, svc *service) error {
	fetcher := &chunk.TGFetcher{Session: sess}
	capBytes, err := cacheCapFrom(o.cacheCap, cfg)
	if err != nil {
		return err
	}
	// Its own cache: the cache is keyed by (inode, index), and the
	// version's inode 5 may hold other content than the live tree's
	// (prepareVersionCache empties it when it was another version's).
	cache, err := chunk.NewCacheWire(cacheDir, capBytes, fetcher, wc)
	if err != nil {
		return err
	}
	memCap, err := memCacheCapFrom(o.memCacheCap)
	if err != nil {
		return err
	}
	if memCap > 0 {
		cache.SetMemCache(chunk.NewMemCache(memCap))
	}
	reader := chunk.NewReader(vstore, cache, nil, chunkSize)
	if o.prefetchWindow > 0 || o.prefetchConcurrency > 0 {
		reader.SetPrefetch(o.prefetchWindow, o.prefetchConcurrency)
	}
	var blobPF *chunk.BlobPrefetcher
	blobBudget, err := blobBudgetFrom(o.blobPrefetch)
	if err != nil {
		return err
	}
	if blobBudget > 0 {
		blobPF = chunk.NewBlobPrefetcherWire(vstore, cache, fetcher, wc, blobBudget)
		reader.SetBlobPrefetcher(blobPF)
	}
	backend := &telfsfs.Backend{Meta: vstore, Reader: reader, Cache: cache, Wire: wc, Codec: codec,
		ChunkSize: chunkSize, ReadOnly: true}

	ctlPath := control.VersionPath(cfg.DataDir, o.name)
	ctlLn, err := control.Listen(ctlPath)
	if err != nil {
		return err
	}
	served := false
	defer func() {
		if !served {
			ctlLn.Close()
			_ = os.Remove(ctlPath)
		}
	}()
	server, err := telfsfs.Mount(telfsfs.MountOptions{
		MountPoint: o.mountpoint,
		AllowOther: o.allowOther,
		Debug:      o.debug,
		Profile:    cfg.ProfileName(),
		FSUUID:     fsUUID,
		Version:    o.name,
		ReadOnly:   true,
	}, backend)
	if err != nil {
		return err
	}
	ctl := &daemonCtl{cfg: cfg, mountpoint: o.mountpoint, readOnly: true, format: 2, meta: vstore, sess: sess,
		version: o.name, cache: cacheJSONHandler(cache), ctx: signalCtx}
	served = true
	go control.Serve(signalCtx, ctlLn, ctlPath, ctl.handler())
	go ctl.sampleRate(signalCtx)

	fmt.Printf("Version %s of profile %s (journal seq %d, created %s) mounted read-only at %s. Press ^C to unmount.\n",
		o.name, cfg.ProfileName(), v.Seq, time.Unix(v.Created, 0).UTC().Format(time.RFC3339), o.mountpoint)
	svc.markReady(o.mountpoint)
	<-signalCtx.Done()
	svc.stopping()
	armForceExit(o.mountpoint)
	done := make(chan struct{})
	go func() {
		defer close(done)
		fmt.Println("Unmounting…")
		if uerr := server.Unmount(); uerr != nil && mountctl.IsMounted(o.mountpoint) {
			if lerr := mountctl.LazyUnmount(o.mountpoint); lerr != nil {
				fmt.Fprintln(os.Stderr, "unmount:", uerr)
				fmt.Fprintln(os.Stderr, "lazy unmount:", lerr)
			}
		}
		server.Wait()
		if blobPF != nil {
			blobPF.Close()
		}
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		fmt.Fprintln(os.Stderr, "telfs: orderly shutdown timed out — forcing lazy unmount + exit")
		_ = mountctl.LazyUnmount(o.mountpoint)
		os.Exit(1)
	}
	return nil
}
