package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"telfs/internal/crypto"
	"telfs/internal/meta"
	"telfs/internal/rebuild"
	"telfs/internal/secret"
	"telfs/internal/snapshot"
	"telfs/internal/wire"
)

func TestExtractGlobalFlags(t *testing.T) {
	defer func() { secretFrom, secretFD = "", 0 }()
	out, err := extractGlobalFlags([]string{"telfs", "mount", "--secret-from", "keepassxc", "--readonly", "/mnt", "--secret-fd=4"})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(out) != "[telfs mount --readonly /mnt]" || secretFrom != "keepassxc" || secretFD != 4 {
		t.Fatalf("out=%v from=%q fd=%d", out, secretFrom, secretFD)
	}
	for _, bad := range [][]string{
		{"telfs", "--secret-from", "env"},
		{"telfs", "--secret-from"},
		{"telfs", "--secret-fd", "1"},
	} {
		if _, err := extractGlobalFlags(bad); err == nil {
			t.Errorf("%v: want an error", bad)
		}
	}
}

func TestExitCodes(t *testing.T) {
	if exitCode(fmt.Errorf("x: %w", secret.ErrSource)) != 2 {
		t.Error("source failure must exit 2")
	}
	if exitCode(fmt.Errorf("session.enc: %w", crypto.ErrWrongSecret)) != 3 {
		t.Error("wrong secret must exit 3")
	}
	if exitCode(errors.New("other")) != 1 {
		t.Error("other errors exit 1")
	}
}

// A DB that meta.Open created on a missing path is not a filesystem:
// mount refuses it (the P1 key-header drill mounted one and posted its
// empty snapshot). Anything init, a recovery or a mount leaves behind is.
func TestNeverUsedDB(t *testing.T) {
	ctx := context.Background()
	open := func(t *testing.T) *meta.Store {
		m, err := meta.Open(filepath.Join(t.TempDir(), "db.sqlite"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { m.Close() })
		return m
	}
	check := func(t *testing.T, m *meta.Store, want bool) {
		t.Helper()
		got, err := neverUsedDB(ctx, m)
		if err != nil || got != want {
			t.Fatalf("neverUsedDB = %v, %v; want %v", got, err, want)
		}
	}
	t.Run("fresh", func(t *testing.T) { check(t, open(t), true) })
	t.Run("format2", func(t *testing.T) {
		m := open(t)
		if err := m.PutKV(ctx, meta.KVFormat, []byte("2")); err != nil {
			t.Fatal(err)
		}
		check(t, m, false)
	})
	t.Run("snapshotted", func(t *testing.T) {
		m := open(t)
		if err := m.PutKV(ctx, snapshot.KVCurrentMsgID, []byte("7")); err != nil {
			t.Fatal(err)
		}
		check(t, m, false)
	})
	t.Run("has a file", func(t *testing.T) {
		m := open(t)
		if _, err := m.CreateInode(ctx, meta.Inode{Kind: meta.KindFile, Mode: 0o644}); err != nil {
			t.Fatal(err)
		}
		check(t, m, false)
	})
}

// A rebuilt DB starts with the use counts the channel shows, not zero,
// and the mount's loadUses reads them back.
func TestSeedUsesFromRebuild(t *testing.T) {
	ctx := context.Background()
	m, err := meta.Open(filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	st := rebuild.Stats{ChunkCaptions: 18, Packs: 5, PackEntries: 747, Snapshots: 3, JournalBatches: 6}
	if err := seedUsesFromRebuild(ctx, m, st); err != nil {
		t.Fatal(err)
	}
	var c wire.Counters
	loadUses(ctx, m, &c)
	if c.Chunk.Load() != 765 || c.Meta.Load() != 14 || c.Caption.Load() != 32 {
		t.Fatalf("chunk %d meta %d caption %d; want 765 14 32", c.Chunk.Load(), c.Meta.Load(), c.Caption.Load())
	}
}

// The TEST-ONLY --snapshot-interval stays out of `telfs mount --help`.
func TestPrintDefaultsExceptHidesFlag(t *testing.T) {
	fs := flag.NewFlagSet("mount", flag.ContinueOnError)
	fs.Bool("readonly", false, "mount read-only")
	fs.Duration("snapshot-interval", 0, "TEST-ONLY")
	var b strings.Builder
	fs.SetOutput(&b)
	printDefaultsExcept(fs, "snapshot-interval")
	if out := b.String(); strings.Contains(out, "snapshot-interval") || !strings.Contains(out, "-readonly") {
		t.Fatalf("help output:\n%s", out)
	}
}
