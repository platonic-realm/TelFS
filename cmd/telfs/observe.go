package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"strings"

	"telfs/internal/config"
	"telfs/internal/tg"
	"telfs/internal/wire"
)

// multiFlag collects repeated --marker values.
type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

// cmdDebugObserve looks at the channel the way an observer without the
// filesystem key does (spec P1 §10, the observer test): it downloads
// EVERY message raw and checks that documents are named `blob`, that
// every caption is "tfs2" + exactly 1020 base64 characters, that the
// only text message of the filesystem is its key message, and that none
// of the planted marker strings appears in any byte. Exit non-zero on
// any finding.
func cmdDebugObserve(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("debug observe", flag.ContinueOnError)
	var markers multiFlag
	fs.Var(&markers, "marker", "a string planted in file names / contents / xattrs / symlink targets that must not appear on the channel (repeatable)")
	pages := fs.Int("pages", 2000, "history pages to scan (100 messages each)")
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
	client, err := newClient(ctx, cfg)
	if err != nil {
		return err
	}
	var problems []string
	var docs, texts, keyMsgs int
	var bytesSeen int64
	err = client.RunSession(ctx, func(ctx context.Context, sess *tg.Session) error {
		return sess.WalkChannelMessages(ctx, *pages, func(cm tg.ChannelMessage) error {
			check := func(what string, b []byte) {
				for _, mk := range markers {
					if bytes.Contains(b, []byte(mk)) {
						problems = append(problems, fmt.Sprintf("msg %d: marker %q in its %s", cm.ID, mk, what))
					}
				}
			}
			check("caption/text", []byte(cm.RawCaption))
			if !cm.HasDoc {
				texts++
				if wire.IsKeyMessage(cm.RawCaption) {
					keyMsgs++
				}
				return nil
			}
			docs++
			if cm.DocName != wire.DocName {
				problems = append(problems, fmt.Sprintf("msg %d: document named %q (want %q)", cm.ID, cm.DocName, wire.DocName))
			}
			check("filename", []byte(cm.DocName))
			if len(cm.RawCaption) != wire.CaptionLen || !strings.HasPrefix(cm.RawCaption, wire.CaptionTag) {
				problems = append(problems, fmt.Sprintf("msg %d: caption is %d characters (want %d, %q + base64)", cm.ID, len(cm.RawCaption), wire.CaptionLen, wire.CaptionTag))
			}
			var buf bytes.Buffer
			if _, err := sess.DownloadDocument(ctx, cm.ID, &buf); err != nil {
				problems = append(problems, fmt.Sprintf("msg %d: download: %v", cm.ID, err))
				return nil
			}
			bytesSeen += int64(buf.Len())
			check("body", buf.Bytes())
			return nil
		})
	})
	if err != nil {
		return err
	}
	fmt.Printf("Observed %d documents (%d bytes) and %d text messages (%d key message(s)).\n", docs, bytesSeen, texts, keyMsgs)
	if keyMsgs != 1 {
		problems = append(problems, fmt.Sprintf("%d key messages on the channel (want exactly 1)", keyMsgs))
	}
	if len(problems) == 0 {
		fmt.Println("OK — nothing readable without the key but the key header, sizes and times.")
		return nil
	}
	for _, p := range problems {
		fmt.Println("  " + p)
	}
	return fmt.Errorf("observe: %d finding(s)", len(problems))
}
