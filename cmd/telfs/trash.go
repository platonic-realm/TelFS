package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"
	"time"

	"telfs/internal/config"
	"telfs/internal/meta"
	"telfs/internal/trash"
)

func cmdTrash(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return trashUsage()
	}
	switch args[0] {
	case "enable":
		return cmdTrashEnable(ctx, args[1:])
	case "disable":
		return cmdTrashDisable(ctx, args[1:])
	case "status":
		return cmdTrashStatus(ctx, args[1:])
	case "list":
		return cmdTrashList(ctx, args[1:])
	case "empty":
		return cmdTrashEmpty(ctx, args[1:])
	case "-h", "--help", "help":
		return trashUsage()
	default:
		return fmt.Errorf("unknown trash subcommand %q (try `telfs trash`)", args[0])
	}
}

func trashUsage() error {
	fmt.Print(`telfs trash — manage the rm safety-net at /.trash

Usage:
  telfs trash enable [--ttl D]    Turn on the safety-net (default ttl: 7d).
                                  When on, every kernel-issued unlink/rmdir
                                  reroutes into /.trash/; a background GC
                                  unlinks entries older than ttl.
  telfs trash disable             Turn it off. /.trash and its contents
                                  remain in place — unlinks immediately
                                  delete again. Existing trashed entries
                                  are NOT auto-purged; use 'trash empty'.
  telfs trash status              Show enabled/ttl/count.
  telfs trash list                Show what's in the trash, oldest first.
  telfs trash empty               Permanently delete every trash entry.

Examples:
  telfs trash enable                  # 7-day default
  telfs trash enable --ttl 30d        # custom retention
  telfs trash status
`)
	return nil
}

func openMeta(ctx context.Context) (*meta.Store, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(cfg.DBPath()); err != nil {
		return nil, fmt.Errorf("metadata DB not found at %s — run `telfs mount` at least once first", cfg.DBPath())
	}
	return meta.Open(cfg.DBPath())
}

func cmdTrashEnable(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("trash enable", flag.ContinueOnError)
	ttlStr := fs.String("ttl", "", "retention (e.g. 7d, 24h, 2h30m). Empty = 7 days (default).")
	if err := fs.Parse(args); err != nil {
		return err
	}
	mstore, err := openMeta(ctx)
	if err != nil {
		return err
	}
	defer mstore.Close()

	if *ttlStr != "" {
		d, err := parseDurationLoose(*ttlStr)
		if err != nil {
			return fmt.Errorf("--ttl %q: %w", *ttlStr, err)
		}
		if err := mstore.SetTrashTTL(ctx, d); err != nil {
			return err
		}
	}
	if err := mstore.SetTrashEnabled(ctx, true); err != nil {
		return err
	}
	// Bootstrap the dir up-front so the next mount doesn't need to do
	// it. Also makes `trash status` see it immediately.
	if _, err := trash.EnsureRootDir(ctx, mstore); err != nil {
		return err
	}
	d, _ := mstore.TrashTTL(ctx)
	fmt.Printf("Trash enabled. TTL=%s. /.trash will appear on the next mount (or already exists).\n", d)
	return nil
}

func cmdTrashDisable(ctx context.Context, args []string) error {
	if len(args) > 0 {
		return fmt.Errorf("trash disable takes no flags")
	}
	mstore, err := openMeta(ctx)
	if err != nil {
		return err
	}
	defer mstore.Close()
	if err := mstore.SetTrashEnabled(ctx, false); err != nil {
		return err
	}
	fmt.Println("Trash disabled. /.trash kept in place; run `telfs trash empty` to purge.")
	return nil
}

func cmdTrashStatus(ctx context.Context, args []string) error {
	if len(args) > 0 {
		return fmt.Errorf("trash status takes no flags")
	}
	mstore, err := openMeta(ctx)
	if err != nil {
		return err
	}
	defer mstore.Close()

	enabled, _ := mstore.TrashEnabled(ctx)
	ttl, _ := mstore.TrashTTL(ctx)
	state := "disabled"
	if enabled {
		state = "enabled"
	}

	count := 0
	if in, err := mstore.Lookup(ctx, meta.RootIno, trash.DirName); err == nil {
		ents, err := mstore.Readdir(ctx, in.Ino)
		if err == nil {
			count = len(ents)
		}
	}
	fmt.Printf("Trash: %s\n", state)
	fmt.Printf("  TTL:     %s\n", ttl)
	fmt.Printf("  Entries: %d\n", count)
	return nil
}

func cmdTrashList(ctx context.Context, args []string) error {
	if len(args) > 0 {
		return fmt.Errorf("trash list takes no flags")
	}
	mstore, err := openMeta(ctx)
	if err != nil {
		return err
	}
	defer mstore.Close()

	in, err := mstore.Lookup(ctx, meta.RootIno, trash.DirName)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			fmt.Println("(/.trash does not exist yet)")
			return nil
		}
		return err
	}
	ents, err := mstore.Readdir(ctx, in.Ino)
	if err != nil {
		return err
	}
	type row struct {
		Name  string
		Kind  meta.Kind
		Size  int64
		Mtime time.Time
	}
	rows := make([]row, 0, len(ents))
	for _, e := range ents {
		inn, err := mstore.GetInode(ctx, e.ChildIno)
		if err != nil {
			continue
		}
		rows = append(rows, row{
			Name:  e.Name,
			Kind:  inn.Kind,
			Size:  inn.Size,
			Mtime: time.Unix(inn.Mtime, 0),
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Mtime.Before(rows[j].Mtime) })

	if len(rows) == 0 {
		fmt.Println("(empty)")
		return nil
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "AGE\tKIND\tSIZE\tNAME")
	for _, r := range rows {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\n",
			shortDuration(time.Since(r.Mtime)), r.Kind, r.Size, r.Name)
	}
	return tw.Flush()
}

func cmdTrashEmpty(ctx context.Context, args []string) error {
	if len(args) > 0 {
		return fmt.Errorf("trash empty takes no flags")
	}
	mstore, err := openMeta(ctx)
	if err != nil {
		return err
	}
	defer mstore.Close()

	in, err := mstore.Lookup(ctx, meta.RootIno, trash.DirName)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			fmt.Println("(/.trash does not exist yet)")
			return nil
		}
		return err
	}
	ents, err := mstore.Readdir(ctx, in.Ino)
	if err != nil {
		return err
	}
	removed := 0
	for _, e := range ents {
		if err := mstore.Unlink(ctx, in.Ino, e.Name); err == nil {
			removed++
		}
	}
	fmt.Printf("Removed %d trash entries.\n", removed)
	return nil
}

// parseDurationLoose accepts `7d`, `24h`, `2h30m`, etc. Go's
// time.ParseDuration doesn't understand the `d` suffix; we translate
// it to multiples of 24h.
func parseDurationLoose(s string) (time.Duration, error) {
	if n := len(s); n > 1 && (s[n-1] == 'd' || s[n-1] == 'D') {
		days, err := time.ParseDuration(s[:n-1] + "h")
		if err != nil {
			return 0, err
		}
		return days * 24, nil
	}
	return time.ParseDuration(s)
}

// shortDuration renders a duration as "5m", "3h", "12d" — a single
// magnitude. Used for the trash list AGE column.
func shortDuration(d time.Duration) string {
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
