package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"telfs/internal/config"
	"telfs/internal/meta"
	"telfs/internal/mountctl"
	"telfs/internal/rebuild"
	"telfs/internal/secret"
	"telfs/internal/snapshot"
	"telfs/internal/tg"
	"telfs/internal/wire"
)

// cmdRecover is `telfs recover --from-captions`: rebuild the local DB
// from the channel's captions, pack indexes and journal alone — the last
// resort when every snapshot is gone (feature 10, spec P1 §8).
func cmdRecover(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("recover", flag.ContinueOnError)
	fromCaptions := fs.Bool("from-captions", false, "rebuild from chunk captions, pack indexes and journal batches (no snapshot needed)")
	journal := fs.String("journal", "use", "use | ignore — replay the journal batches found (names, folders, links, renames, deletions, dedup refs) or rely on captions only")
	reportPath := fs.String("report", "", "write the per-file report (TSV) here; default <profile>/recover-report-<time>.tsv")
	keyHeader := fs.String("key-header", "", "use this key header export instead of the channel's key message")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !*fromCaptions {
		return errors.New("recover: say how: --from-captions (a normal mount recovers from the newest snapshot by itself)")
	}
	if *journal != "use" && *journal != "ignore" {
		return fmt.Errorf("recover: --journal %q: use or ignore", *journal)
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if err := cfg.RequireChannel(); err != nil {
		return err
	}
	if _, mp, ok, ferr := mountctl.FindByProfile(cfg.ProfileName()); ferr != nil {
		return ferr
	} else if ok {
		return fmt.Errorf("recover: profile %s is mounted at %s — unmount first", cfg.ProfileName(), mp)
	}
	if _, err := os.Stat(cfg.DBPath()); err == nil {
		return fmt.Errorf("recover: %s exists — move it (and its -wal/-shm) away first; the rebuild writes a fresh one", cfg.DBPath())
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
	if *reportPath == "" {
		*reportPath = filepath.Join(cfg.DataDir, fmt.Sprintf("recover-report-%s.tsv", time.Now().Format("20060102-150405")))
	}
	ok := false
	defer func() {
		if !ok {
			for _, p := range []string{cfg.DBPath(), cfg.DBPath() + "-wal", cfg.DBPath() + "-shm"} {
				os.Remove(p)
			}
		}
	}()
	err = client.RunSession(ctx, func(ctx context.Context, sess *tg.Session) error {
		var cands []snapshot.KeyFound
		if *keyHeader != "" {
			text, err := os.ReadFile(*keyHeader)
			if err != nil {
				return err
			}
			h, _, err := wire.ParseExport(string(text))
			if err != nil {
				return err
			}
			cands = []snapshot.KeyFound{{Header: h, JSON: h.JSON()}}
		} else if cands, err = snapshot.FindKeyMessages(ctx, sess); err != nil {
			return err
		}
		if len(cands) == 0 {
			return errors.New("recover: no key message on the channel — pass --key-header <export>")
		}
		k, dek, fsUUID, err := snapshot.UnlockAny(cands, sec.Bytes())
		if err != nil {
			return err
		}
		defer zero(dek)
		sec.Zero()
		keys, err := wire.DeriveKeys(dek, fsUUID, nil)
		if err != nil {
			return err
		}
		m, err := meta.Open(cfg.DBPath())
		if err != nil {
			return err
		}
		defer m.Close()
		now := time.Now().UTC().Format(time.RFC3339)
		for key, v := range map[string][]byte{
			meta.KVFSUUID:    []byte(fsUUID), // the rebuilt FS keeps its identity (captions name it)
			meta.KVFormat:    []byte("2"),
			meta.KVKeyHeader: k.JSON,
			meta.KVCreated:   []byte(now),
		} {
			if err := m.PutKV(ctx, key, v); err != nil {
				return err
			}
		}
		if k.MsgID > 0 {
			_ = m.PutKV(ctx, meta.KVKeyMsgID, []byte(strconv.Itoa(k.MsgID)))
		}
		fmt.Printf("Rebuilding filesystem %s from the channel (journal: %s)…\n", fsUUID, *journal)
		res, err := rebuild.Run(ctx, &channelSource{sess: sess}, keys, m, rebuild.Options{
			UseJournal: *journal == "use",
			Logf:       func(f string, a ...any) { fmt.Printf("  "+f+"\n", a...) },
		})
		if err != nil {
			return err
		}
		// Filesystem defaults from what the captions say.
		best, bestN := meta.DefaultChunkSize, 0
		for size, n := range res.ChunkSizes {
			if size > 0 && n > bestN {
				best, bestN = size, n
			}
		}
		_ = m.SetChunkSize(ctx, best)
		if res.Codecs["zstd"] > 0 {
			_ = m.SetCompressionCodec(ctx, "zstd")
		}
		if err := seedUsesFromRebuild(ctx, m, res.Stats); err != nil {
			return err
		}
		// The channel's pinned anchor (if any) still names a snapshot of
		// the state before the rebuild. Until the mount posts a snapshot
		// of this one (at once: it is owed), a recovery through that
		// anchor must refuse rather than replay the old journal and this
		// DB's new ops as one: the journal continues one seq past the
		// newest the channel holds, and that gap makes it refuse.
		last, err := m.LastJournalSeq(ctx)
		if err != nil {
			return err
		}
		if err := m.UpdateKV(ctx, func(k *meta.KVTx) error {
			if err := k.Put(snapshot.KVSnapDue, []byte("rebuilt from captions ("+time.Now().UTC().Format(time.RFC3339Nano)+")")); err != nil {
				return err
			}
			return k.AdvanceJournalSeq(last + 1)
		}); err != nil {
			return err
		}
		// The versions found by their captions are the registry (gc
		// refuses while any exist; the next anchor lists them).
		if err := snapshot.StoreVersions(ctx, m, res.Versions); err != nil {
			return err
		}
		if err := writeReport(*reportPath, res); err != nil {
			return err
		}
		st := res.Stats
		fmt.Printf("\nRead %d messages: %d chunk captions, %d packs (%d files inside), %d journal batches, %d snapshots (ignored), %d version(s) (kept).\n",
			st.Messages, st.ChunkCaptions, st.Packs, st.PackEntries, st.JournalBatches, st.Snapshots, len(res.Versions))
		if *journal == "use" {
			fmt.Printf("Journal: %d op(s) replayed, %d skipped (their parent folder was unknown); %d deleted file(s) kept deleted.\n",
				st.JournalOps, st.JournalOpsSkipped, st.DeletedByJournal)
		}
		bySrc := map[string]int{}
		for _, f := range res.Files {
			bySrc[f.NameSource]++
		}
		fmt.Printf("Files: %d (", st.Files)
		var parts []string
		for _, s := range []string{rebuild.SrcJournal, rebuild.SrcHint, rebuild.SrcHintRsync, rebuild.SrcLostFound, rebuild.SrcLostFoundDu} {
			if bySrc[s] > 0 {
				parts = append(parts, fmt.Sprintf("%d %s", bySrc[s], s))
			}
		}
		fmt.Printf("%s); %d incomplete.\n", strings.Join(parts, ", "), st.Incomplete)
		if *journal == "ignore" || st.JournalBatches == 0 {
			fmt.Println("Captions alone cannot bring back: empty files, symlinks, empty folders, xattrs, extra")
			fmt.Println("hard-link names, renames after a file's last upload, deduplicated copies, and")
			fmt.Println("deletions (a deleted file whose messages were not collected yet comes back).")
		}
		fmt.Printf("Report: %s\n", *reportPath)
		fmt.Println("Mount the profile: the next snapshot makes the channel whole again.")
		return nil
	})
	if err != nil {
		return err
	}
	ok = true
	return nil
}

func writeReport(path string, res *rebuild.Result) error {
	var b bytes.Buffer
	b.WriteString("path\tino\tsize\tchunks_found\tchunks_expected\tname_source\tnotes\n")
	for _, f := range res.Files {
		fmt.Fprintf(&b, "%s\t%d\t%d\t%d\t%d\t%s\t%s\n", f.Path, f.Ino, f.Size, f.Found, f.Expected, f.NameSource, strings.Join(f.Notes, "; "))
	}
	return os.WriteFile(path, b.Bytes(), 0o600)
}

// channelSource is the real channel for the rebuild: it must walk ALL
// of the history, so a scan cut short by the page cap is an error.
type channelSource struct{ sess *tg.Session }

func (c *channelSource) Walk(ctx context.Context, fn func(rebuild.Message) error) error {
	const pages = 1 << 20 // 100 M messages: effectively "all"
	n := 0
	exhausted, err := c.sess.WalkChannelMessagesExhaust(ctx, pages, func(cm tg.ChannelMessage) error {
		n++
		if n%10000 == 0 {
			fmt.Printf("  …%d messages\n", n)
		}
		return fn(rebuild.Message{ID: cm.ID, HasDoc: cm.HasDoc, Caption: cm.RawCaption})
	})
	if err != nil {
		return err
	}
	if !exhausted {
		return errors.New("the channel history walk stopped before the beginning of the channel")
	}
	return nil
}

func (c *channelSource) ReadRange(ctx context.Context, id int, off, n int64) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := c.sess.DownloadDocumentRange(ctx, id, &buf, off, n); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *channelSource) ReadAll(ctx context.Context, id int) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := c.sess.DownloadDocument(ctx, id, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
