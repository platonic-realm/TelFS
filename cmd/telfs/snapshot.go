package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"text/tabwriter"
	"time"

	"telfs/internal/config"
	"telfs/internal/meta"
	"telfs/internal/mountctl"
	"telfs/internal/secret"
	"telfs/internal/snapshot"
	"telfs/internal/tg"
	"telfs/internal/wire"
)

// localKeys2 unlocks the local DB's keys when it is format 2; nil (no
// error) for format 1 or no local DB.
func localKeys2(ctx context.Context, cfg *config.Config, sec *secret.Secret) (*wire.Keys, error) {
	if _, err := os.Stat(cfg.DBPath()); err != nil {
		return nil, nil
	}
	m, err := meta.Open(cfg.DBPath())
	if err != nil {
		return nil, err
	}
	defer m.Close()
	if f, err := m.Format(ctx); err != nil || f != 2 {
		return nil, err
	}
	keys, _, err := unlock2(ctx, m, sec)
	return keys, err
}

// listSnapshots2 lists format-2 snapshots newest first by decrypting
// captions.
func listSnapshots2(ctx context.Context, sess *tg.Session, keys *wire.Keys, max int) ([]tg.LatestSnapshot, error) {
	var out []tg.LatestSnapshot
	err := sess.WalkChannelMessages(ctx, snapshot.ScanPages, func(cm tg.ChannelMessage) error {
		if !cm.HasDoc || !wire.LooksLikeCaption(cm.RawCaption) {
			return nil
		}
		c, err := keys.OpenCaption(cm.RawCaption)
		if err != nil || c.K != wire.KindSnapshot {
			return nil
		}
		out = append(out, tg.LatestSnapshot{MessageID: cm.ID, Caption: tg.SnapshotCaption{Kind: "snap", Seq: c.Seq, TSUnix: c.TS, FSUUID: c.FS}})
		if len(out) >= max {
			return tg.ErrStopWalk
		}
		return nil
	})
	return out, err
}

// readFSUUIDFromMetaKV opens the local DB (read-only OK) and reads the
// FS uuid that snapshots are filtered by. Returns "" if the DB isn't
// present yet — in that case ListSnapshots will return all snapshots
// regardless of which TelFS instance wrote them.
func readFSUUIDFromMetaKV(ctx context.Context, cfg *config.Config) (string, error) {
	if _, err := os.Stat(cfg.DBPath()); err != nil {
		return "", nil
	}
	m, err := meta.Open(cfg.DBPath())
	if err != nil {
		return "", err
	}
	defer m.Close()
	return m.FSUUID(ctx)
}

func cmdSnapshot(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return snapshotUsage()
	}
	switch args[0] {
	case "list":
		return cmdSnapshotList(ctx, args[1:])
	case "restore":
		return cmdSnapshotRestore(ctx, args[1:])
	case "-h", "--help", "help":
		return snapshotUsage()
	default:
		return fmt.Errorf("unknown snapshot subcommand %q (try `telfs snapshot`)", args[0])
	}
}

func snapshotUsage() error {
	fmt.Print(`telfs snapshot — inspect and roll back to historical snapshots

The mount daemon posts snapshots of the local SQLite metadata to the
backing channel: format 2 when 500,000 journal ops or 24 hours (with
changes) have passed since the last ([snapshot] in config.toml) — the
journal in between holds every change; format 1 every 5 minutes. The
last 12 snapshots are retained on the channel; older ones are deleted
as new ones land.

Usage:
  telfs snapshot list             Show the snapshots currently on the
                                  channel for the active profile,
                                  newest first.
  telfs snapshot restore [--yes] <msg-id>
                                  Roll the active profile's local DB
                                  back to the given snapshot. The
                                  current DB is renamed to
                                  db.sqlite.pre-restore-<ts> as a
                                  safety net. Requires the daemon to
                                  NOT be mounted (will refuse
                                  otherwise). Add --yes to skip the
                                  confirmation prompt.

Workflow for "I deleted something 30 minutes ago, undo":
  telfs umount  (or kill the mount daemon)
  telfs snapshot list                # pick an msg-id from ~30 min ago
  telfs snapshot restore <msg-id>
  telfs mount /your/mountpoint       # files now reflect that state
`)
	return nil
}

func cmdSnapshotList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("snapshot list", flag.ContinueOnError)
	max := fs.Int("max", 50, "maximum snapshots to enumerate")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if err := cfg.RequireChannel(); err != nil {
		return err
	}
	fsUUID, err := readFSUUIDFromMetaKV(ctx, cfg)
	if err != nil {
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
	keys, err := localKeys2(ctx, cfg, sec)
	if err != nil {
		return err
	}
	sec.Zero()
	return client.RunSession(ctx, func(ctx context.Context, sess *tg.Session) error {
		var snaps []tg.LatestSnapshot
		var err error
		if keys != nil {
			snaps, err = listSnapshots2(ctx, sess, keys, *max)
		} else {
			snaps, err = sess.ListSnapshots(ctx, fsUUID, *max)
		}
		if err != nil {
			return err
		}
		if len(snaps) == 0 {
			fmt.Println("No snapshots found.")
			return nil
		}
		tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "MSG-ID\tAGE\tTS (UTC)\tJOURNAL-SEQ")
		now := time.Now()
		for _, s := range snaps {
			ts := time.Unix(s.Caption.TSUnix, 0).UTC()
			fmt.Fprintf(tw, "%d\t%s\t%s\t%d\n",
				s.MessageID,
				shortAge(now.Sub(ts)),
				ts.Format("2006-01-02 15:04:05"),
				s.Caption.Seq)
		}
		return tw.Flush()
	})
}

func cmdSnapshotRestore(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("snapshot restore", flag.ContinueOnError)
	yes := fs.Bool("yes", false, "skip the y/N confirmation prompt")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: telfs snapshot restore [--yes] <msg-id>")
	}
	msgID, err := strconv.Atoi(fs.Arg(0))
	if err != nil {
		return fmt.Errorf("invalid msg-id %q: %w", fs.Arg(0), err)
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if err := cfg.RequireChannel(); err != nil {
		return err
	}
	// Refuse to restore while the mount is live — restoring the DB
	// from under a running daemon would lead to inconsistent state.
	if _, mp, ok, ferr := mountctl.FindByProfile(cfg.ProfileName()); ferr != nil {
		return ferr // fail closed
	} else if ok {
		return fmt.Errorf("profile %s is mounted at %s; unmount it first (`telfs unmount %s`)", cfg.ProfileName(), mp, cfg.ProfileName())
	}
	if !*yes {
		fmt.Printf("This will replace %s with the contents of snapshot msg=%d.\n", cfg.DBPath(), msgID)
		fmt.Printf("The current DB will be saved as %s.pre-restore-<ts>.\n", cfg.DBPath())
		fmt.Print("Proceed? [y/N] ")
		r := bufio.NewReader(os.Stdin)
		line, _ := r.ReadString('\n')
		if len(line) == 0 || (line[0] != 'y' && line[0] != 'Y') {
			fmt.Println("Aborted.")
			return nil
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
	keys, err := localKeys2(ctx, cfg, sec)
	if err != nil {
		return err
	}
	// Format 2: a snapshot from before a secret change carries the OLD
	// key header; keep the CURRENT one so the restored DB still opens
	// with today's secret.
	var currentHeader, currentAnchor, currentVersions []byte
	var currentSeq int64
	if keys != nil {
		if m, merr := meta.Open(cfg.DBPath()); merr == nil {
			_, currentHeader, _ = keyHeaderOf(ctx, m)
			currentAnchor, _ = m.GetKV(ctx, snapshot.KVAnchorMsgID)
			currentSeq, _ = m.LastJournalSeq(ctx)
			// Versions live on the channel, not in the tree: the
			// rolled-back DB keeps today's registry.
			currentVersions, _ = m.GetKV(ctx, snapshot.KVVersions)
			m.Close()
		}
	}
	return client.RunSession(ctx, func(ctx context.Context, sess *tg.Session) error {
		fmt.Printf("Downloading snapshot msg=%d…\n", msgID)
		var plaintext []byte
		if keys != nil {
			plaintext, err = snapshot.DownloadSnapshot2(ctx, sess, keys, msgID)
		} else {
			var data []byte
			if data, err = sess.DownloadSnapshot(ctx, msgID); err == nil {
				plaintext, err = maybeUnwrapEncrypted(data, sec)
			}
		}
		if err != nil {
			return fmt.Errorf("snapshot %d: %w", msgID, err)
		}
		// Backup the existing DB before overwriting. Move the WAL/SHM
		// sidecars along with the main file — a WAL left behind would
		// (a) make the backup incomplete (its newest frames live in
		// the WAL) and (b) get replayed by sqlite into the RESTORED
		// database on next open, silently corrupting it. Restore()
		// also defensively deletes leftover sidecars.
		if _, err := os.Stat(cfg.DBPath()); err == nil {
			backup := fmt.Sprintf("%s.pre-restore-%d", cfg.DBPath(), time.Now().Unix())
			if err := os.Rename(cfg.DBPath(), backup); err != nil {
				return fmt.Errorf("backup %s → %s: %w", cfg.DBPath(), backup, err)
			}
			for _, suffix := range []string{"-wal", "-shm"} {
				if err := os.Rename(cfg.DBPath()+suffix, backup+suffix); err != nil && !os.IsNotExist(err) {
					return fmt.Errorf("backup sidecar %s: %w", cfg.DBPath()+suffix, err)
				}
			}
			fmt.Printf("Saved current DB to %s\n", backup)
		}
		if err := snapshot.Restore(ctx, plaintext, cfg.DBPath()); err != nil {
			return err
		}
		if len(currentHeader) > 0 {
			if m, merr := meta.Open(cfg.DBPath()); merr == nil {
				_ = m.PutKV(ctx, meta.KVKeyHeader, currentHeader)
				_ = snapshot.RecordSnapID(ctx, m, msgID)
				// Format 2 snapshots rarely: the channel's anchor still
				// names the newest snapshot, not this rolled-back state.
				// The next mount takes a snapshot at once, and its anchor
				// replaces the current one.
				// The rolled-back journal continues after every seq the
				// abandoned one reached, so the two never share a number
				// on the channel.
				_ = m.AdvanceJournalSeq(ctx, currentSeq)
				_ = m.PutKV(ctx, snapshot.KVSnapDue, []byte(fmt.Sprintf("restored from snapshot %d", msgID)))
				if len(currentAnchor) > 0 {
					_ = m.PutKV(ctx, snapshot.KVAnchorMsgID, currentAnchor)
				}
				if len(currentVersions) > 0 {
					_ = m.PutKV(ctx, snapshot.KVVersions, currentVersions)
				} else {
					_ = m.DeleteKV(ctx, snapshot.KVVersions)
				}
				m.Close()
			}
		}
		fmt.Printf("Restored snapshot msg=%d → %s\n", msgID, cfg.DBPath())
		fmt.Println("Mount the profile to see the rolled-back state.")
		return nil
	})
}

// shortAge prints a duration as "5m", "3h", "12d" — a single
// magnitude suitable for tabular display.
func shortAge(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}
