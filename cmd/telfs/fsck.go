package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"

	"telfs/internal/config"
	"telfs/internal/crypto"
	"telfs/internal/meta"
	"telfs/internal/secret"
	"telfs/internal/snapshot"
	"telfs/internal/tg"
	"telfs/internal/wire"
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
	doFix := fs.Bool("fix", false, "remove unreachable chunks from chunk_map after the report (affected files will end at the gap); on format 2 also re-pin / re-post the key message")
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
	if _, err := os.Stat(cfg.DBPath()); err != nil {
		return fmt.Errorf("fsck: no local DB at %s (mount once to recover it from the channel)", cfg.DBPath())
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

	sec, err := readSecret(ctx, cfg, secret.Unlock)
	if err != nil {
		return err
	}
	defer sec.Zero()
	client, err := newClientWith(cfg, sec)
	if err != nil {
		return err
	}
	pipe, err := openPipeline(ctx, metaStore, sec)
	if err != nil {
		return err
	}
	sec.Zero()

	var issues []fsckIssue
	var keyOK = true
	err = client.RunSession(ctx, func(ctx context.Context, sess *tg.Session) error {
		if pipe.format == 2 {
			kc := checkPinnedHeader(ctx, sess, metaStore)
			storeKeyCheck(ctx, metaStore, kc)
			keyOK = kc.OK
			fmt.Printf("Key header: %s\n", kc.Detail)
			if !kc.OK && *doFix {
				if ferr := fixKeyMessage(ctx, sess, metaStore); ferr != nil {
					fmt.Fprintf(os.Stderr, "  could not repair the key message: %v\n", ferr)
				} else if kc2 := checkPinnedHeader(ctx, sess, metaStore); kc2.OK {
					keyOK = true
					storeKeyCheck(ctx, metaStore, kc2)
					fmt.Printf("  repaired: %s\n", kc2.Detail)
				}
			}
			printUses(pipe.counters)
		}
		if len(chunks) == 0 {
			fmt.Println("chunk_map is empty — nothing to check.")
			return nil
		}
		// One download per MESSAGE: a packed blob holds many chunks, and
		// each chunk is its extent of the blob (the old fsck decrypted
		// the whole blob as one chunk and flagged every packed file).
		byMsg := map[int64][]meta.Chunk{}
		var order []int64
		for _, c := range chunks {
			if _, ok := byMsg[c.TGMessageID]; !ok {
				order = append(order, c.TGMessageID)
			}
			byMsg[c.TGMessageID] = append(byMsg[c.TGMessageID], c)
		}
		fmt.Printf("Walking %d chunks in %d messages…\n", len(chunks), len(order))
		for n, msgID := range order {
			if n > 0 && n%50 == 0 {
				fmt.Printf("  …%d/%d messages\n", n, len(order))
			}
			var buf bytes.Buffer
			if _, err := sess.DownloadDocument(ctx, int(msgID), &buf); err != nil {
				for _, c := range byMsg[msgID] {
					issues = append(issues, fsckIssue{Kind: "missing", C: c, Err: err})
				}
				if *stopOn > 0 && len(issues) >= *stopOn {
					return nil
				}
				continue
			}
			doc := buf.Bytes()
			for _, c := range byMsg[msgID] {
				w := doc
				if c.PackSize > 0 {
					if c.PackOffset+int64(c.PackSize) > int64(len(doc)) {
						issues = append(issues, fsckIssue{Kind: "size_mismatch", C: c, Want: c.Size, Got: len(doc)})
						continue
					}
					w = doc[c.PackOffset : c.PackOffset+int64(c.PackSize)]
				}
				plain, err := pipe.wc.Decode(c.Ino, c.Idx, c.Hash, w)
				if err != nil {
					issues = append(issues, fsckIssue{Kind: "decrypt_failed", C: c, Err: err})
				} else if int32(len(plain)) != c.Size {
					issues = append(issues, fsckIssue{Kind: "size_mismatch", C: c, Want: c.Size, Got: len(plain)})
				}
				if *stopOn > 0 && len(issues) >= *stopOn {
					return nil
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	fmt.Println()
	if len(issues) == 0 {
		if len(chunks) > 0 {
			fmt.Printf("OK — %d chunks all reachable and authenticated.\n", len(chunks))
		}
		if !keyOK {
			return errors.New("fsck: the key header check failed (see above; --fix repairs it)")
		}
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
		return fmt.Errorf("fsck: %d issue(s)", len(issues))
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

// fixKeyMessage makes the channel's pinned message the key message that
// matches the local header: re-pin the recorded one if it still matches,
// else post the local header anew and pin it.
func fixKeyMessage(ctx context.Context, sess *tg.Session, m *meta.Store) error {
	h, local, err := keyHeaderOf(ctx, m)
	if err != nil {
		return err
	}
	if v, err := m.GetKV(ctx, meta.KVKeyMsgID); err == nil {
		if id, err := strconv.Atoi(string(v)); err == nil && id > 0 {
			if text, err := sess.GetMessageText(ctx, id); err == nil && wire.IsKeyMessage(text) {
				if _, js, err := wire.ParseKeyMessage(text); err == nil && bytes.Equal(js, local) {
					return sess.PinMessage(ctx, id)
				}
			}
		}
	}
	id, err := snapshot.PostKeyMessage(ctx, sess, h)
	if err != nil {
		return err
	}
	return m.PutKV(ctx, meta.KVKeyMsgID, []byte(strconv.Itoa(id)))
}

// printUses shows the per-key encryption counts against AES-GCM's
// 2^32 limit (spec P1 §1).
func printUses(c *wire.Counters) {
	if c == nil {
		return
	}
	for _, k := range []struct {
		name string
		n    uint64
	}{{"chunk", c.Chunk.Load()}, {"meta", c.Meta.Load()}, {"caption", c.Caption.Load()}} {
		fmt.Printf("Key uses:   %-8s %d of 2^32 (%.6f %%)\n", k.name, k.n, 100*float64(k.n)/float64(crypto.NonceLimit))
	}
}
