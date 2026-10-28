package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"telfs/internal/meta"
)

// The startup repair must never empty or shorten a file without saying
// so, and after a recovery it must not do it at all (Arash, decision 9).
// A file that comes back from the channel with a size but without its
// chunk placements was either restored from a snapshot + journal that
// don't hold them, or its data never reached the channel; truncating it
// to "fix" the mismatch would make an incomplete recovery look complete
// (the P1 1M test emptied 921,691 files that way).

// repairListed is how many repaired files a mount names on its output;
// the report file names all of them.
const repairListed = 20

// shortened keeps the repairs that cut a file's size (the other shape,
// stray rows past the end, loses nothing).
func shortened(reps []meta.RepairReport) []meta.RepairReport {
	var out []meta.RepairReport
	for _, r := range reps {
		if r.NewSize < r.OldSize {
			out = append(out, r)
		}
	}
	return out
}

// writeRepairReport writes every file of reps to <dir>/<name>-<time>.tsv
// (0600) and names the first repairListed of them on w. Returns the
// report's path.
func writeRepairReport(ctx context.Context, m *meta.Store, w io.Writer, dir, name string, reps []meta.RepairReport) (string, error) {
	path := filepath.Join(dir, fmt.Sprintf("%s-%s.tsv", name, time.Now().Format("20060102-150405")))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", err
	}
	defer f.Close()
	fmt.Fprintln(f, "path\tino\tsize\tsize_after\tchunks\tchunks_after\tfirst_gap_idx")
	dirs := map[int64]string{}
	for i, r := range reps {
		p, ok := m.PathOfCached(ctx, r.Ino, dirs)
		if !ok {
			p = fmt.Sprintf("(ino %d, no path)", r.Ino)
		}
		fmt.Fprintf(f, "%s\t%d\t%d\t%d\t%d\t%d\t%d\n", p, r.Ino, r.OldSize, r.NewSize, r.OldChunks, r.NewChunks, r.FirstGapAt)
		if i < repairListed {
			fmt.Fprintf(w, "  %s: %d → %d bytes (%d chunk(s) present)\n", p, r.OldSize, r.NewSize, r.OldChunks)
		}
	}
	if len(reps) > repairListed {
		fmt.Fprintf(w, "  … and %d more\n", len(reps)-repairListed)
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	return path, nil
}

// checkRecoveredDB is the dry run that guards a format-2 recovery: if
// the startup repair would shorten any file of the DB just restored
// from the channel, it names them (report file in dir) and returns an
// error; the caller then refuses the mount and removes the restored DB.
func checkRecoveredDB(ctx context.Context, m *meta.Store, w io.Writer, dir string, staged meta.StagedChecker) error {
	scan, err := m.ScanPartialFiles(ctx, staged)
	if err != nil {
		return fmt.Errorf("check the recovered DB: %w", err)
	}
	cut := shortened(scan)
	if len(cut) == 0 {
		return nil
	}
	fmt.Fprintf(w, "%d file(s) came back from the channel with a size but without all their chunks:\n", len(cut))
	path, err := writeRepairReport(ctx, m, w, dir, "recovery-incomplete", cut)
	if err != nil {
		return fmt.Errorf("%d file(s) came back incomplete (report not written: %v)", len(cut), err)
	}
	return fmt.Errorf("%d file(s) came back from the channel without all their chunks (list: %s). "+
		"Not emptying them, and not keeping this recovery. `telfs recover --from-captions` "+
		"rebuilds the chunk placements from the chunk captions; files whose data never reached "+
		"the channel are listed there as incomplete", len(cut), path)
}
