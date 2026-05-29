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
	"telfs/internal/snapshot"
	"telfs/internal/tg"
)

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

The mount daemon posts a snapshot of the local SQLite metadata to the
backing channel every 5 minutes (configurable). The last 12 snapshots
are retained on the channel by default — about an hour of recoverable
state. Older ones are auto-deleted as new ones land.

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
	client, err := tg.New(cfg)
	if err != nil {
		return err
	}
	return client.RunSession(ctx, func(ctx context.Context, sess *tg.Session) error {
		snaps, err := sess.ListSnapshots(ctx, fsUUID, *max)
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
	if isProfileMounted(cfg) {
		return fmt.Errorf("the profile's mount daemon is running; unmount it first (`fusermount -u <mountpoint>` or stop the daemon)")
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
	client, err := tg.New(cfg)
	if err != nil {
		return err
	}
	return client.RunSession(ctx, func(ctx context.Context, sess *tg.Session) error {
		fmt.Printf("Downloading snapshot msg=%d…\n", msgID)
		data, err := sess.DownloadSnapshot(ctx, msgID)
		if err != nil {
			return err
		}
		plaintext, err := maybeUnwrapEncrypted(data)
		if err != nil {
			return fmt.Errorf("decrypt snapshot: %w", err)
		}
		// Backup the existing DB before overwriting.
		if _, err := os.Stat(cfg.DBPath()); err == nil {
			backup := fmt.Sprintf("%s.pre-restore-%d", cfg.DBPath(), time.Now().Unix())
			if err := os.Rename(cfg.DBPath(), backup); err != nil {
				return fmt.Errorf("backup %s → %s: %w", cfg.DBPath(), backup, err)
			}
			fmt.Printf("Saved current DB to %s\n", backup)
		}
		if err := snapshot.Restore(ctx, plaintext, cfg.DBPath()); err != nil {
			return err
		}
		fmt.Printf("Restored snapshot msg=%d → %s\n", msgID, cfg.DBPath())
		fmt.Println("Mount the profile to see the rolled-back state.")
		return nil
	})
}

// isProfileMounted checks /proc/mounts for any fuse.telfs entry whose
// mountpoint is a child of the active profile's data dir. Strictly
// best-effort — we only need to detect the common case.
func isProfileMounted(cfg *config.Config) bool {
	f, err := os.Open("/proc/mounts")
	if err != nil {
		return false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if len(line) == 0 {
			continue
		}
		// fields: src mountpoint fstype options ...
		var src, mp, fstype string
		fmt.Sscanf(line, "%s %s %s", &src, &mp, &fstype)
		if fstype == "fuse.telfs" {
			// Any fuse.telfs mount blocks restore — we can't reliably
			// tell from /proc/mounts which profile a mount belongs to,
			// and rolling back any profile's DB while it's live is
			// unsafe.
			return true
		}
	}
	return false
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
