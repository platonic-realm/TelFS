package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	"telfs/internal/config"
	"telfs/internal/control"
	"telfs/internal/fsck"
	"telfs/internal/meta"
	"telfs/internal/secret"
	"telfs/internal/tg"
)

// errFsckQuickFormat1: the quick check is a format-2 feature (decision D1).
var errFsckQuickFormat1 = errors.New("fsck --quick needs a format-2 filesystem (format 1 keeps working as in v0.22: `telfs fsck` checks it by downloading every chunk)")

// fsckReply is the body of the daemon's POST /fsck/quick: the report, or
// an error.
type fsckReply struct {
	*fsck.Report
	Error string `json:"error,omitempty"`
	// Kind classifies Error: "telegram" (exit 6), "refused", "busy".
	Kind string `json:"kind,omitempty"`
}

// fsckQuick serves POST /fsck/quick (spec v1 row 7): the quick check of
// the live filesystem, with the daemon's session — the caller needs no
// secret. It runs while the request lasts (a client that goes away
// stops it) and ends with the daemon.
func (d *daemonCtl) fsckQuick(w http.ResponseWriter, r *http.Request) {
	if d.format != 2 {
		writeJSON(w, http.StatusConflict, fsckReply{Error: errFsckQuickFormat1.Error(), Kind: "refused"})
		return
	}
	if !d.fsckMu.TryLock() {
		writeJSON(w, http.StatusConflict, fsckReply{Error: "a quick check is already running in this daemon: wait for it", Kind: "busy"})
		return
	}
	defer d.fsckMu.Unlock()
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	defer context.AfterFunc(d.daemonCtx(), cancel)()
	var msgs fsck.Messages = d.sess
	if d.msgs != nil {
		msgs = d.msgs
	}
	rep, err := fsck.Quick(ctx, d.meta, msgs, fsck.Options{Retriable: retriable})
	if err != nil {
		code, kind := http.StatusInternalServerError, ""
		if errors.Is(err, fsck.ErrChannel) {
			code, kind = http.StatusBadGateway, "telegram"
		}
		d.logf("[fsck] quick check failed: %v", err)
		writeJSON(w, code, fsckReply{Error: err.Error(), Kind: kind})
		return
	}
	d.logf("[fsck] quick check: %d message(s), %d chunk row(s): %d missing, %d too small (%.1f s)",
		rep.MessagesChecked, rep.RowsChecked, len(rep.Missing), len(rep.TooSmall), rep.TookS)
	writeJSON(w, http.StatusOK, fsckReply{Report: rep})
}

// cmdFsckQuick is `telfs fsck --quick [--json]` (spec v1 §3): through the
// profile's running daemon when one answers on the control socket (no
// secret), else with this command's own read-only session (the secret,
// as `telfs fsck`). Exit 0 when every referenced message exists with its
// document and every packed extent fits, 7 when not (the first 20 on
// stderr, all of them in --json), 6 when Telegram failed.
func cmdFsckQuick(ctx context.Context, asJSON bool, stdout, stderr io.Writer) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	info := stdout
	if asJSON {
		info = stderr // stdout is the JSON alone
	} else {
		fmt.Fprintf(info, "Quick check of profile %s: every referenced message, 100 per request, nothing downloaded…\n", cfg.ProfileName())
	}
	via := "the running daemon"
	rep, answered, err := fsckQuickDaemon(ctx, cfg)
	if err != nil {
		return err
	}
	if !answered {
		if rep, via, err = fsckQuickStandalone(ctx, cfg, info, !asJSON); err != nil {
			return err
		}
	}
	return reportFsckQuick(rep, via, asJSON, stdout, stderr)
}

// fsckQuickDaemon asks the profile's daemon for the quick check.
// answered is false when no daemon answers on the control socket (or it
// predates the route).
func fsckQuickDaemon(ctx context.Context, cfg *config.Config) (rep *fsck.Report, answered bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, control.BaseURL+"/fsck/quick", nil)
	if err != nil {
		return nil, false, err
	}
	// No client timeout: a large filesystem takes minutes (one channel
	// request per 100 messages); ^C ends the check in the daemon too.
	resp, err := ctlClient(cfg, 0).Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, true, ctx.Err()
		}
		return nil, false, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed {
		return nil, false, nil // a daemon older than the route
	}
	var r fsckReply
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		if ctx.Err() != nil {
			return nil, true, ctx.Err()
		}
		return nil, true, fmt.Errorf("daemon reply (HTTP %d): %w", resp.StatusCode, err)
	}
	switch {
	case r.Kind == "telegram":
		return nil, true, fmt.Errorf("%w: %s", errTelegram, r.Error)
	case r.Error != "":
		return nil, true, errors.New(r.Error)
	case r.Report == nil:
		return nil, true, fmt.Errorf("daemon reply (HTTP %d) without a report", resp.StatusCode)
	}
	return r.Report, true, nil
}

// fsckQuickStandalone runs the quick check without a daemon: the local
// DB (read-only) and a session of its own, which needs the secret. via
// says how it ran.
func fsckQuickStandalone(ctx context.Context, cfg *config.Config, info io.Writer, progress bool) (rep *fsck.Report, via string, err error) {
	if _, err := os.Stat(cfg.DBPath()); err != nil {
		return nil, "", fmt.Errorf("fsck --quick: no local DB at %s (mount once to recover it from the channel)", cfg.DBPath())
	}
	m, err := meta.OpenReadOnly(cfg.DBPath(), false)
	if err != nil {
		return nil, "", err
	}
	defer m.Close()
	if f, err := m.Format(ctx); err != nil {
		return nil, "", err
	} else if f != 2 {
		return nil, "", errFsckQuickFormat1
	}
	if has, err := m.HasAnyChunks(ctx); err != nil {
		return nil, "", err
	} else if !has {
		return &fsck.Report{Missing: []fsck.Missing{}, TooSmall: []fsck.TooSmall{}}, "the local DB (it references no message)", nil
	}
	if err := cfg.RequireChannel(); err != nil {
		return nil, "", err
	}
	fmt.Fprintf(info, "No daemon of profile %s answered: checking with this command's own Telegram session.\n", cfg.ProfileName())
	msgs, stop, err := fsckConnect(ctx, cfg, info)
	if err != nil {
		return nil, "", err
	}
	defer stop()
	opt := fsck.Options{Retriable: retriable}
	if progress {
		opt.Progress = func(n, rows int64) {
			if n%10000 == 0 {
				fmt.Fprintf(info, "  …%d message(s), %d chunk row(s) checked\n", n, rows)
			}
		}
	}
	rep, err = fsck.Quick(ctx, m, msgs, opt)
	if errors.Is(err, fsck.ErrChannel) {
		return nil, "", fmt.Errorf("%w: %w", errTelegram, err)
	}
	return rep, "this command's own session", err
}

// fsckConnect opens this command's own read-only Telegram session for a
// standalone quick check; stop ends it. A variable so tests can stand in
// a fake channel.
var fsckConnect = func(ctx context.Context, cfg *config.Config, info io.Writer) (fsck.Messages, func(), error) {
	if !cfg.HasSession() {
		return nil, nil, tg.ErrNotAuthorized
	}
	sec, err := readSecret(ctx, cfg, secret.Unlock)
	if err != nil {
		return nil, nil, err
	}
	defer sec.Zero()
	client, err := newClientWith(cfg, sec)
	if err != nil {
		return nil, nil, err
	}
	sec.Zero()
	sess, stop, err := connectReadOnly(ctx, client, info)
	if err != nil {
		return nil, nil, err
	}
	return sess, stop, nil
}

// reportFsckQuick prints the report — as one JSON object with asJSON —
// and maps it to the exit code: 7 when anything is missing or too small
// (the first 20 findings on stderr either way).
func reportFsckQuick(rep *fsck.Report, via string, asJSON bool, stdout, stderr io.Writer) error {
	if asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rep); err != nil {
			return err
		}
	} else {
		fmt.Fprintf(stdout, "Quick check through %s: %d channel message(s) referenced by %d chunk row(s), in %.1f s (nothing downloaded).\n",
			via, rep.MessagesChecked, rep.RowsChecked, rep.TookS)
	}
	if rep.OK() {
		if !asJSON {
			fmt.Fprintln(stdout, "OK — every referenced message exists with its document, and every packed extent fits.")
		}
		return nil
	}
	fmt.Fprintf(stderr, "%d referenced message(s) missing, %d too small for their packed extents:\n", len(rep.Missing), len(rep.TooSmall))
	shown := 0
	for _, p := range rep.Missing {
		if shown == reportListMax {
			break
		}
		fmt.Fprintf(stderr, "  missing    msg %d (%s): %d chunk row(s), e.g. %s\n", p.MsgID, p.Reason, p.Rows, examplePath(p.ExamplePath, p.ExampleIno))
		shown++
	}
	for _, p := range rep.TooSmall {
		if shown == reportListMax {
			break
		}
		fmt.Fprintf(stderr, "  too small  msg %d (document %d B, extents need %d B): %d chunk row(s), e.g. %s\n", p.MsgID, p.DocSize, p.Need, p.Rows, examplePath(p.ExamplePath, p.ExampleIno))
		shown++
	}
	if total := len(rep.Missing) + len(rep.TooSmall); total > shown {
		fmt.Fprintf(stderr, "  … and %d more (telfs fsck --quick --json lists all)\n", total-shown)
	}
	return fmt.Errorf("%w: %d referenced message(s) missing, %d too small — %d chunk row(s) affected",
		errIntegrity, len(rep.Missing), len(rep.TooSmall), rep.RowsAffected())
}

// examplePath names a finding's example file (its inode when it has no
// name).
func examplePath(path string, ino int64) string {
	if path == "" {
		return fmt.Sprintf("inode %d (no name)", ino)
	}
	return path
}
