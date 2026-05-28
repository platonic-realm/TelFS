package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"telfs/internal/config"
	"telfs/internal/meta"
	"telfs/internal/tg"
)

// fsckIssue records a single inconsistency found by the integrity walk.
// Categorized so a future --fix path can dispatch on Kind.
type fsckIssue struct {
	Kind string // "missing" | "size_mismatch" | "decrypt_failed"
	C    meta.Chunk
	Want int32
	Got  int
	Err  error
}

// cmdFsck walks every chunk_map row and verifies the corresponding
// channel message: exists, downloads, decrypts under the FS key (if
// encryption is on), and has the recorded size. Reports findings;
// --fix removes unreachable chunks from chunk_map (the affected files
// become truncated at the gap on next read).
//
// Cost: O(chunk count) Telegram round-trips. Slow for large FSes.
// Mostly useful after a suspicious crash, a manual channel-message
// deletion mishap, or before/after a `telfs gc --yes` to confirm
// nothing was over-zealously reaped.
func cmdFsck(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("fsck", flag.ContinueOnError)
	doFix := fs.Bool("fix", false, "remove unreachable chunks from chunk_map after the report (affected files will end at the gap)")
	stopOn := fs.Int("stop-after", 0, "stop after N issues (0 = walk everything)")
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
	metaStore, err := meta.Open(cfg.DBPath())
	if err != nil {
		return err
	}
	defer metaStore.Close()

	chunks, err := metaStore.AllChunks(ctx)
	if err != nil {
		return err
	}
	if len(chunks) == 0 {
		fmt.Println("chunk_map is empty — nothing to check.")
		return nil
	}

	cipher, err := loadCipher(ctx, metaStore)
	if err != nil {
		return err
	}

	client, err := tg.New(cfg)
	if err != nil {
		return err
	}

	var issues []fsckIssue
	err = client.RunSession(ctx, func(ctx context.Context, sess *tg.Session) error {
		fmt.Printf("Walking %d chunks…\n", len(chunks))
		for i, c := range chunks {
			if i > 0 && i%50 == 0 {
				fmt.Printf("  …%d/%d\n", i, len(chunks))
			}
			var buf bytes.Buffer
			if _, err := sess.DownloadDocument(ctx, int(c.TGMessageID), &buf); err != nil {
				issues = append(issues, fsckIssue{Kind: "missing", C: c, Err: err})
				if *stopOn > 0 && len(issues) >= *stopOn {
					return nil
				}
				continue
			}
			body, err := cipher.Open(c.Ino, c.Idx, buf.Bytes())
			if err != nil {
				issues = append(issues, fsckIssue{Kind: "decrypt_failed", C: c, Err: err})
				if *stopOn > 0 && len(issues) >= *stopOn {
					return nil
				}
				continue
			}
			if int32(len(body)) != c.Size {
				issues = append(issues, fsckIssue{Kind: "size_mismatch", C: c, Want: c.Size, Got: len(body)})
				if *stopOn > 0 && len(issues) >= *stopOn {
					return nil
				}
				continue
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	fmt.Println()
	if len(issues) == 0 {
		fmt.Printf("OK — %d chunks all reachable and authenticated.\n", len(chunks))
		return nil
	}
	fmt.Printf("Found %d issue(s):\n", len(issues))
	byKind := map[string]int{}
	for _, is := range issues {
		byKind[is.Kind]++
		fmt.Printf("  ino=%d idx=%d msg=%d  %s", is.C.Ino, is.C.Idx, is.C.TGMessageID, is.Kind)
		if is.Kind == "size_mismatch" {
			fmt.Printf(" (recorded=%d, got=%d)", is.Want, is.Got)
		}
		if is.Err != nil {
			fmt.Printf("  %v", is.Err)
		}
		fmt.Println()
	}
	fmt.Println()
	for k, n := range byKind {
		fmt.Printf("  %-16s  %d\n", k, n)
	}

	if !*doFix {
		fmt.Println("\nRe-run with --fix to remove unreachable chunks from chunk_map.")
		return nil
	}

	// --fix path: drop chunk_map rows for any "missing" issue. We
	// deliberately do NOT drop on "decrypt_failed" or "size_mismatch"
	// — those usually mean a wrong key or a TG storage anomaly, not
	// a chunk we should evict. Human can manually intervene with
	// sqlite3 if they're sure.
	var fixed int
	for _, is := range issues {
		if is.Kind != "missing" {
			continue
		}
		if err := metaStore.DeleteChunk(ctx, is.C.Ino, is.C.Idx); err != nil {
			fmt.Fprintf(os.Stderr, "  could not drop (ino=%d, idx=%d): %v\n", is.C.Ino, is.C.Idx, err)
			continue
		}
		fixed++
	}
	fmt.Printf("\nRemoved %d unreachable chunk_map row(s).\n", fixed)
	fmt.Println("Affected files now end at the first removed chunk; readers see EOF past that point.")
	return nil
}

// silence unused-import linter when neither errors nor flag is used in the future
var _ = errors.New
var _ = flag.Bool
