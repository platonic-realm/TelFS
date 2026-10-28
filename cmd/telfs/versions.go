package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"telfs/internal/config"
	"telfs/internal/control"
	"telfs/internal/meta"
	"telfs/internal/mountctl"
	"telfs/internal/snapshot"
	"telfs/internal/wire"
)

const versionsUsage = "create NAME [--timeout D] | list [--json] | delete NAME"

// cmdVersions is `telfs versions …` — named, immutable versions of a
// format-2 filesystem (spec v1 §2, §3). create and delete go through the
// running daemon (no secret needed here); list asks the daemon, else
// reads the local registry. A missing or unknown subcommand is an error
// (exit 1): a script must never mistake it for success.
func cmdVersions(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("telfs versions: missing subcommand (" + versionsUsage + ")")
	}
	switch args[0] {
	case "create":
		return cmdVersionsCreate(ctx, args[1:])
	case "list":
		return cmdVersionsList(ctx, args[1:])
	case "delete":
		return cmdVersionsDelete(ctx, args[1:])
	default:
		return fmt.Errorf("telfs versions: unknown subcommand %q (%s)", args[0], versionsUsage)
	}
}

// parseInterleaved parses flags given before, between or after the
// positional arguments (`versions create NAME --timeout 5m`).
func parseInterleaved(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		if fs.NArg() == 0 {
			return pos, nil
		}
		pos = append(pos, fs.Arg(0))
		args = fs.Args()[1:]
	}
}

// versionReply is the body of the daemon's /versions routes.
type versionReply struct {
	Token           uint64              `json:"token,omitempty"`
	State           string              `json:"state,omitempty"` // syncing | posting | done | failed | canceled
	Phase           string              `json:"phase,omitempty"` // while syncing: uploads | journal
	PendingUploads  int                 `json:"pending_uploads,omitempty"`
	JournalUnposted int64               `json:"journal_unposted,omitempty"`
	Version         *wire.AnchorVersion `json:"version,omitempty"`
	Warning         string              `json:"warning,omitempty"`
	Error           string              `json:"error,omitempty"`
	// Kind classifies Error: "telegram" (exit 6), "exists", "missing",
	// "refused", "usage" (exit 1).
	Kind string `json:"kind,omitempty"`
}

// Version job states.
const (
	verSyncing  = "syncing"
	verPosting  = "posting"
	verDone     = "done"
	verFailed   = "failed"
	verCanceled = "canceled"
)

// versionJob is one `versions create` in the daemon: the sync barrier
// first (advanced by the client's waits — nobody waiting, nothing
// posted), then the post, which runs to its end on the daemon's context.
// A post that finds uploads in flight or files without all their chunks
// posts nothing and goes back to syncing (at most versionAttempts takes).
type versionJob struct {
	name     string
	tok      *syncTok
	created  time.Time
	state    string
	rep      versionReply  // the result once done or failed
	done     chan struct{} // closed when the post ends
	attempts int           // takes that came out incomplete
	// A job lives as long as a client waits for it (not by the clock):
	// waiters is the number of /versions/wait calls running, lastSeen
	// when one last started or ended, gone that the last one ended
	// because its client went away.
	waiters  int
	lastSeen time.Time
	gone     bool
}

// versionAttempts bounds the takes of one `versions create`: each one
// that is not complete syncs again first; after the last the create
// fails with exit 4, nothing posted.
const versionAttempts = 5

// versionIdle is how long a job no client waits for is kept (a client
// polls without pause; one that has not come back for this long is gone).
const versionIdle = 2 * time.Minute

// abandoned: no client waits for the job, and the last one went away (or
// none has come for versionIdle). A job being posted is never abandoned.
func (j *versionJob) abandoned(now time.Time) bool {
	if j.state == verPosting || j.waiters > 0 {
		return false
	}
	return j.gone || now.Sub(j.lastSeen) > versionIdle
}

// versionsList is the body of GET /versions and of `versions list --json`.
type versionsList struct {
	Versions []versionView `json:"versions"`
}

// versionView is one version as `versions list --json` shows it.
type versionView struct {
	Name        string  `json:"name"`
	MsgID       int     `json:"msg_id"`
	CreatedUnix int64   `json:"created_unix"`
	Seq         int64   `json:"seq"`
	Size        int64   `json:"size"`
	SHA256      string  `json:"sha256,omitempty"`
	Mountpoint  *string `json:"mountpoint"` // a version mount of it on this machine
}

func viewVersions(vs []wire.AnchorVersion, mounts map[string]string) versionsList {
	out := versionsList{Versions: []versionView{}}
	for _, v := range vs {
		vv := versionView{Name: v.Name, MsgID: v.Msg, CreatedUnix: v.Created, Seq: v.Seq, Size: v.Size, SHA256: v.SHA256}
		if mp, ok := mounts[v.Name]; ok {
			vv.Mountpoint = &mp
		}
		out.Versions = append(out.Versions, vv)
	}
	return out
}

// errKind maps a version operation's error onto the reply's Kind.
func errKind(err error) string {
	switch {
	case errors.Is(err, snapshot.ErrChannel):
		return "telegram"
	case errors.Is(err, snapshot.ErrVersionExists):
		return "exists"
	case errors.Is(err, snapshot.ErrNoVersion):
		return "missing"
	case errors.Is(err, snapshot.ErrVersionFormat1):
		return "refused"
	case errors.Is(err, snapshot.ErrVersionIncomplete):
		return "incomplete"
	}
	return ""
}

// versionsRefusal says why this daemon cannot change versions ("" if it
// can).
func (d *daemonCtl) versionsRefusal() string {
	switch {
	case d.format != 2:
		return snapshot.ErrVersionFormat1.Error()
	case d.version != "":
		return "this is a version mount; versions are made and deleted through the live mount"
	case d.readOnly:
		return "the mount is read-only (--readonly): it writes nothing to the channel"
	case d.keys == nil || d.ch == nil:
		return "this daemon cannot post versions"
	}
	return ""
}

func (d *daemonCtl) daemonCtx() context.Context {
	if d.ctx != nil {
		return d.ctx
	}
	return context.Background()
}

func (d *daemonCtl) logf(format string, a ...any) {
	fmt.Printf(format+"\n", a...)
}

// versionsRoutes are the live daemon's version routes.
func (d *daemonCtl) versionsRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /versions", func(w http.ResponseWriter, r *http.Request) {
		vs, err := snapshot.LoadVersions(r.Context(), d.meta)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, versionReply{Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, viewVersions(vs, mountctl.VersionMounts(d.cfg.ProfileName())))
	})
	mux.HandleFunc("POST /versions/create", d.versionCreate)
	mux.HandleFunc("GET /versions/wait", d.versionWait)
	mux.HandleFunc("POST /versions/cancel", d.versionCancel)
	mux.HandleFunc("POST /versions/delete", d.versionDelete)
}

// POST /versions/create?name=N: check the request, then start the sync
// barrier (as `sync --wait`); GET /versions/wait advances it.
func (d *daemonCtl) versionCreate(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if msg := d.versionsRefusal(); msg != "" {
		writeJSON(w, http.StatusConflict, versionReply{Error: msg, Kind: "refused"})
		return
	}
	if err := snapshot.ValidateVersionName(name); err != nil {
		writeJSON(w, http.StatusBadRequest, versionReply{Error: err.Error(), Kind: "usage"})
		return
	}
	vs, err := snapshot.LoadVersions(r.Context(), d.meta)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, versionReply{Error: err.Error()})
		return
	}
	exists := fmt.Sprintf("%v: %q (telfs versions list)", snapshot.ErrVersionExists, name)
	if _, dup := snapshot.FindVersion(vs, name); dup {
		writeJSON(w, http.StatusConflict, versionReply{Error: exists, Kind: "exists"})
		return
	}
	var reg uint64
	if d.pool != nil {
		reg = d.pool.LastRegistered()
	}
	d.verMu.Lock()
	if d.verJobs == nil {
		d.verJobs = map[uint64]*versionJob{}
	}
	now := time.Now()
	// Jobs no client waits for any more go (a create whose client was
	// killed must not block its name, nor a long sync be purged while
	// its client still waits).
	for id, j := range d.verJobs {
		if j.abandoned(now) {
			delete(d.verJobs, id)
		}
	}
	for _, j := range d.verJobs {
		if j.name == name && (j.state == verSyncing || j.state == verPosting) {
			d.verMu.Unlock()
			writeJSON(w, http.StatusConflict, versionReply{Error: exists + " — being created", Kind: "exists"})
			return
		}
	}
	d.verNext++
	id := d.verNext
	d.verJobs[id] = &versionJob{name: name, tok: &syncTok{reg: reg, created: now}, created: now,
		state: verSyncing, done: make(chan struct{}), lastSeen: now}
	d.verMu.Unlock()
	if d.poster != nil {
		d.poster.Kick()
	}
	writeJSON(w, http.StatusOK, versionReply{Token: id, State: verSyncing})
}

// advanceVersion moves a job on: once its sync barrier is reached it
// starts the post. It returns the job's state as a reply.
func (d *daemonCtl) advanceVersion(ctx context.Context, j *versionJob) versionReply {
	d.verMu.Lock()
	state, rep := j.state, j.rep
	d.verMu.Unlock()
	switch state {
	case verDone, verFailed:
		return rep
	case verPosting:
		return versionReply{State: verPosting}
	}
	s := d.syncCheck(ctx, j.tok)
	if s.Error != "" {
		return versionReply{State: verSyncing, Error: s.Error}
	}
	if !s.Synced {
		return versionReply{State: verSyncing, Phase: s.Phase, PendingUploads: s.PendingUploads, JournalUnposted: s.JournalUnposted}
	}
	d.verMu.Lock()
	if j.state == verSyncing {
		j.state = verPosting
		go d.postVersion(j)
	}
	d.verMu.Unlock()
	return versionReply{State: verPosting}
}

// uploadGate is the upload pool a version checks (nil without one).
func (d *daemonCtl) uploadGate() snapshot.UploadGate {
	if d.pool == nil {
		return nil
	}
	return d.pool
}

// postVersion posts the version (snapshot, message, registry, anchor).
// A take that was not complete posted nothing: the job syncs again — up
// to the uploads registered by now — and takes it again.
func (d *daemonCtl) postVersion(j *versionJob) {
	v, warn, err := snapshot.CreateVersion(d.daemonCtx(), d.ch, d.meta, d.keys, j.name, d.uploadGate(), d.logf)
	d.verMu.Lock()
	var inc *snapshot.IncompleteError
	if errors.As(err, &inc) && j.attempts+1 < versionAttempts {
		j.attempts++
		var reg uint64
		if d.pool != nil {
			reg = d.pool.LastRegistered()
		}
		j.tok = &syncTok{reg: reg, created: time.Now()}
		j.state = verSyncing
		d.verMu.Unlock()
		d.logf("[versions] version %q not taken (%v) — syncing again (take %d of %d)", j.name, err, j.attempts+1, versionAttempts)
		if d.poster != nil {
			d.poster.Kick()
		}
		return
	}
	if inc != nil {
		err = fmt.Errorf("%w (after %d takes)", err, versionAttempts)
	}
	if err != nil {
		j.state = verFailed
		j.rep = versionReply{State: verFailed, Error: err.Error(), Kind: errKind(err)}
		d.logf("[versions] version %q not created: %v", j.name, err)
	} else {
		j.state = verDone
		j.rep = versionReply{State: verDone, Version: &v, Warning: warn}
		d.logf("[versions] version %q posted (message %d, journal seq %d)", v.Name, v.Msg, v.Seq)
	}
	close(j.done)
	d.verMu.Unlock()
}

func (d *daemonCtl) job(r *http.Request) (uint64, *versionJob) {
	id, _ := strconv.ParseUint(r.URL.Query().Get("token"), 10, 64)
	d.verMu.Lock()
	defer d.verMu.Unlock()
	return id, d.verJobs[id]
}

// GET /versions/wait?token=N&max=D: long-poll until the version is
// posted (or failed) or max has passed.
func (d *daemonCtl) versionWait(w http.ResponseWriter, r *http.Request) {
	id, j := d.job(r)
	if j == nil {
		writeJSON(w, http.StatusNotFound, versionReply{Error: "unknown or expired version token"})
		return
	}
	d.verMu.Lock()
	j.waiters++
	j.lastSeen, j.gone = time.Now(), false
	d.verMu.Unlock()
	defer func() {
		d.verMu.Lock()
		j.waiters--
		j.lastSeen = time.Now()
		j.gone = r.Context().Err() != nil // the client went away mid-wait
		d.verMu.Unlock()
	}()
	maxWait, err := time.ParseDuration(r.URL.Query().Get("max"))
	if err != nil || maxWait <= 0 || maxWait > 5*time.Minute {
		maxWait = 30 * time.Second
	}
	deadline := time.Now().Add(maxWait)
	for {
		rep := d.advanceVersion(r.Context(), j)
		rep.Token = id
		if rep.State == verDone || rep.State == verFailed {
			d.verMu.Lock()
			delete(d.verJobs, id)
			d.verMu.Unlock()
			writeJSON(w, http.StatusOK, rep)
			return
		}
		if rep.Error != "" || time.Now().After(deadline) {
			writeJSON(w, http.StatusOK, rep)
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-j.done:
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// POST /versions/cancel?token=N: give up a create whose sync has not
// finished (nothing was posted). A post already under way is not
// stopped: the reply says so and the client waits for it.
func (d *daemonCtl) versionCancel(w http.ResponseWriter, r *http.Request) {
	id, j := d.job(r)
	if j == nil {
		writeJSON(w, http.StatusNotFound, versionReply{Error: "unknown or expired version token"})
		return
	}
	d.verMu.Lock()
	rep := versionReply{Token: id, State: j.state}
	switch j.state {
	case verSyncing:
		j.state = verCanceled
		delete(d.verJobs, id)
		rep.State = verCanceled
	case verDone, verFailed:
		rep = j.rep
		rep.Token = id
		delete(d.verJobs, id)
	}
	d.verMu.Unlock()
	writeJSON(w, http.StatusOK, rep)
}

// POST /versions/delete?name=N: delete a version (message, registry
// entry, anchor) and its local copy. Refused while it is mounted.
func (d *daemonCtl) versionDelete(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if msg := d.versionsRefusal(); msg != "" {
		writeJSON(w, http.StatusConflict, versionReply{Error: msg, Kind: "refused"})
		return
	}
	if err := snapshot.ValidateVersionName(name); err != nil {
		writeJSON(w, http.StatusBadRequest, versionReply{Error: err.Error(), Kind: "usage"})
		return
	}
	if mp, ok := mountctl.VersionMounts(d.cfg.ProfileName())[name]; ok {
		writeJSON(w, http.StatusConflict, versionReply{Error: fmt.Sprintf("version %q is mounted at %s — unmount it first (telfs unmount %s)", name, mp, mp), Kind: "refused"})
		return
	}
	// The daemon's context, not the request's: a client that goes away
	// must not leave a half-done delete.
	v, warn, err := snapshot.DeleteVersion(d.daemonCtx(), d.ch, d.meta, d.keys, name, d.logf)
	if err != nil {
		code := http.StatusConflict
		if errKind(err) == "telegram" {
			code = http.StatusBadGateway
		}
		writeJSON(w, code, versionReply{Error: err.Error(), Kind: errKind(err)})
		return
	}
	removeVersionCopy(d.cfg, name)
	d.logf("[versions] version %q deleted (message %d)", v.Name, v.Msg)
	writeJSON(w, http.StatusOK, versionReply{State: verDone, Version: &v, Warning: warn})
}

// versionsDir is where a profile keeps its downloaded versions.
func versionsDir(cfg *config.Config) string { return filepath.Join(cfg.DataDir, "versions") }

// versionDBPath is the local copy of version name.
func versionDBPath(cfg *config.Config, name string) string {
	return filepath.Join(versionsDir(cfg), name+".sqlite")
}

// versionCacheDir is version name's chunk cache. It is its own: the
// cache is keyed by (inode, index), and a version's inode 5 may hold
// other content than the live filesystem's.
func versionCacheDir(cfg *config.Config, name string) string {
	return filepath.Join(versionsDir(cfg), name+".cache")
}

// versionCacheID is what a version cache holds the chunks of: the
// version's message and DB SHA-256 (a version deleted and made again
// under the same name is another version).
const versionCacheID = ".version"

// prepareVersionCache returns version v's cache directory, emptied when
// it was filled for another version of that name: the cache holds
// plaintext by (inode, index) and is not re-checked on a hit.
func prepareVersionCache(cfg *config.Config, v wire.AnchorVersion) (string, error) {
	dir := versionCacheDir(cfg, v.Name)
	id := fmt.Sprintf("%d %s %d\n", v.Msg, v.SHA256, v.Size)
	if b, err := os.ReadFile(filepath.Join(dir, versionCacheID)); err == nil && string(b) == id {
		return dir, nil
	}
	if err := os.RemoveAll(dir); err != nil {
		return "", fmt.Errorf("version cache %s: %w", dir, err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("version cache %s: %w", dir, err)
	}
	tmp := filepath.Join(dir, versionCacheID+".tmp")
	if err := os.WriteFile(tmp, []byte(id), 0o600); err != nil {
		return "", fmt.Errorf("version cache %s: %w", dir, err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, versionCacheID)); err != nil {
		return "", fmt.Errorf("version cache %s: %w", dir, err)
	}
	return dir, nil
}

// removeVersionCopy deletes a deleted version's local copy and cache.
func removeVersionCopy(cfg *config.Config, name string) {
	db := versionDBPath(cfg, name)
	for _, p := range []string{db, db + "-wal", db + "-shm", db + ".download"} {
		_ = os.Remove(p)
	}
	_ = os.RemoveAll(versionCacheDir(cfg, name))
}

// ctlCall sends one request to the profile's daemon and decodes the
// reply into out. A daemon that does not answer is errNotMounted.
func ctlCall(ctx context.Context, c *http.Client, cfg *config.Config, method, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, control.BaseURL+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.Do(req)
	if err != nil {
		return fmt.Errorf("%w: profile %s: %v", errNotMounted, cfg.ProfileName(), err)
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("daemon reply (HTTP %d): %w", resp.StatusCode, err)
	}
	return nil
}

// replyErr turns a reply's error into the command's error.
func replyErr(rep versionReply) error {
	if rep.Error == "" {
		return nil
	}
	switch rep.Kind {
	case "telegram":
		return fmt.Errorf("%w: %s", errTelegram, strings.TrimPrefix(rep.Error, snapshot.ErrChannel.Error()+": "))
	case "incomplete":
		return fmt.Errorf("%w: %s — no version was created", errNotSynced, rep.Error)
	}
	return errors.New(rep.Error)
}

// cmdVersionsCreate is `telfs versions create NAME [--timeout D]`: sync
// (as `sync --wait`), then post the version; exit 0 once it is on the
// channel, 4 when the sync did not finish within D (nothing posted), 5
// without a running daemon, 6 when Telegram failed.
func cmdVersionsCreate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("versions create", flag.ContinueOnError)
	timeout := fs.Duration("timeout", 0, "give up when the sync before the version has not finished after this long (exit 4, nothing posted); 0 = as long as it takes")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return errors.New("usage: telfs versions create NAME [--timeout D]")
	}
	name := pos[0]
	if err := snapshot.ValidateVersionName(name); err != nil {
		return err
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	short := ctlClient(cfg, 10*time.Second)
	var begin versionReply
	if err := ctlCall(ctx, short, cfg, http.MethodPost, "/versions/create?name="+url.QueryEscape(name), &begin); err != nil {
		return err
	}
	if err := replyErr(begin); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "Syncing: waiting until everything written before this call is on the channel…\n")
	var deadline time.Time
	if *timeout > 0 {
		deadline = time.Now().Add(*timeout)
	}
	long := ctlClient(cfg, 0)
	posting := false
	// Stopped (SIGTERM, ^C) while syncing: give the job up, so a retry
	// of the same name is not answered "being created". A fresh context:
	// ctx is the one that ended.
	defer func() {
		if ctx.Err() == nil || posting {
			return
		}
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var rep versionReply
		_ = ctlCall(cctx, short, cfg, http.MethodPost, fmt.Sprintf("/versions/cancel?token=%d", begin.Token), &rep)
	}()
	for {
		poll := 30 * time.Second
		if !deadline.IsZero() && !posting {
			left := time.Until(deadline)
			if left <= 0 {
				var rep versionReply
				if err := ctlCall(ctx, short, cfg, http.MethodPost, fmt.Sprintf("/versions/cancel?token=%d", begin.Token), &rep); err != nil {
					return err
				}
				switch rep.State {
				case verCanceled:
					return fmt.Errorf("%w after %s — no version was created", errNotSynced, *timeout)
				case verDone:
					return versionCreated(rep)
				case verFailed:
					return replyErr(rep)
				}
				fmt.Fprintln(os.Stderr, "The timeout came while the version was being posted — waiting for the post to finish.")
				posting = true
				continue
			}
			if left < poll {
				poll = left
			}
		}
		var rep versionReply
		if err := ctlCall(ctx, long, cfg, http.MethodGet, fmt.Sprintf("/versions/wait?token=%d&max=%s", begin.Token, poll), &rep); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		switch {
		case rep.State == verDone:
			return versionCreated(rep)
		case rep.State == verFailed || rep.Error != "":
			if rep.Error == "" {
				rep.Error = "the version was not created"
			}
			return replyErr(rep)
		case rep.State == verPosting && !posting:
			posting = true
			fmt.Fprintln(os.Stderr, "Synced. Posting the version…")
		case rep.State == verSyncing && posting:
			// The take found uploads in flight or files without all
			// their chunks: nothing posted, syncing again (--timeout
			// applies again).
			posting = false
			fmt.Fprintln(os.Stderr, "Writes were still landing — syncing again before the version is taken…")
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func versionCreated(rep versionReply) error {
	if rep.Version == nil {
		return errors.New("the daemon reported the version done but did not name it")
	}
	v := rep.Version
	fmt.Printf("Version %s is on the channel (message %d, journal seq %d, DB %s).\n", v.Name, v.Msg, v.Seq, humanBytes(v.Size))
	if rep.Warning != "" {
		fmt.Fprintln(os.Stderr, "warning:", rep.Warning)
	}
	return nil
}

// cmdVersionsList is `telfs versions list [--json]`: the daemon's
// registry when the profile is mounted, else the local DB's (opened
// read-only; never created). No secret.
func cmdVersionsList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("versions list", flag.ContinueOnError)
	jsonOut := fs.Bool("json", false, `one JSON object: {"versions": [{name, msg_id, created_unix, seq, size, sha256, mountpoint}]}, oldest first`)
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 0 {
		return errors.New("usage: telfs versions list [--json]")
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	var list versionsList
	if err := ctlCall(ctx, ctlClient(cfg, 10*time.Second), cfg, http.MethodGet, "/versions", &list); err != nil {
		if !errors.Is(err, errNotMounted) {
			return err
		}
		if list, err = localVersions(ctx, cfg); err != nil {
			return err
		}
	}
	if list.Versions == nil {
		list.Versions = []versionView{}
	}
	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(list)
	}
	if len(list.Versions) == 0 {
		fmt.Println("No versions.")
		return nil
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tCREATED (UTC)\tJOURNAL SEQ\tDB SIZE\tMESSAGE\tMOUNTED AT")
	for _, v := range list.Versions {
		mp := "-"
		if v.Mountpoint != nil {
			mp = *v.Mountpoint
		}
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%d\t%s\n", v.Name, time.Unix(v.CreatedUnix, 0).UTC().Format("2006-01-02 15:04:05"),
			v.Seq, humanBytes(v.Size), v.MsgID, mp)
	}
	return tw.Flush()
}

// localVersions reads the registry from the local DB, read-only.
func localVersions(ctx context.Context, cfg *config.Config) (versionsList, error) {
	if _, err := os.Stat(cfg.DBPath()); err != nil {
		return versionsList{}, fmt.Errorf("no running daemon and no local DB at %s (mount the profile: it recovers the DB from the channel)", cfg.DBPath())
	}
	m, err := meta.OpenReadOnly(cfg.DBPath(), false)
	if err != nil {
		return versionsList{}, err
	}
	defer m.Close()
	vs, err := snapshot.LoadVersions(ctx, m)
	if err != nil {
		return versionsList{}, err
	}
	return viewVersions(vs, mountctl.VersionMounts(cfg.ProfileName())), nil
}

// cmdVersionsDelete is `telfs versions delete NAME`, through the daemon.
func cmdVersionsDelete(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("versions delete", flag.ContinueOnError)
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return errors.New("usage: telfs versions delete NAME")
	}
	name := pos[0]
	if err := snapshot.ValidateVersionName(name); err != nil {
		return err
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	var rep versionReply
	if err := ctlCall(ctx, ctlClient(cfg, 5*time.Minute), cfg, http.MethodPost, "/versions/delete?name="+url.QueryEscape(name), &rep); err != nil {
		return err
	}
	if err := replyErr(rep); err != nil {
		return err
	}
	if rep.Version != nil {
		fmt.Printf("Version %s deleted (message %d).\n", rep.Version.Name, rep.Version.Msg)
	}
	if rep.Warning != "" {
		fmt.Fprintln(os.Stderr, "warning:", rep.Warning)
	}
	return nil
}
