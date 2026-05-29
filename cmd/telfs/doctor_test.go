package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"telfs/internal/meta"
)

// openTestMeta opens an in-memory-like meta store in a tempdir for
// doctor checks that need a real *meta.Store with FK enforcement.
func openTestMeta(t *testing.T) *meta.Store {
	t.Helper()
	dir := t.TempDir()
	m, err := meta.Open(filepath.Join(dir, "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

// TestCheckChunkMapHealthSurfacesDedup: two chunk_map rows pointing at
// the same tg_message_id should be reported as a single distinct
// message with a "collapsed by dedup" message.
func TestCheckChunkMapHealthSurfacesDedup(t *testing.T) {
	m := openTestMeta(t)
	ctx := context.Background()
	ino1, _ := m.CreateChild(ctx, meta.RootIno, "a", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	ino2, _ := m.CreateChild(ctx, meta.RootIno, "b", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	for _, ino := range []int64{ino1, ino2} {
		if err := m.PutChunk(ctx, meta.Chunk{Ino: ino, Idx: 0, TGMessageID: 42, Size: 1024}); err != nil {
			t.Fatal(err)
		}
	}
	findings := checkChunkMapHealth(ctx, m)
	if len(findings) == 0 {
		t.Fatal("expected at least one finding")
	}
	var sawDedup bool
	for _, f := range findings {
		if strings.Contains(f.Message, "collapsed by dedup") {
			sawDedup = true
		}
		if f.Severity == sevErr {
			t.Fatalf("unexpected error finding: %s", f.Message)
		}
	}
	if !sawDedup {
		t.Fatalf("expected a 'collapsed by dedup' finding; got: %+v", findings)
	}
}

// TestCheckChunkMapHealthFlagsBadRow: a chunk_map row with size <= 0
// or tg_message_id <= 0 surfaces as an error.
func TestCheckChunkMapHealthFlagsBadRow(t *testing.T) {
	m := openTestMeta(t)
	ctx := context.Background()
	ino, _ := m.CreateChild(ctx, meta.RootIno, "f", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	// Direct SQL insert with size=0 (PutChunk doesn't reject it; the
	// invariant is a doctor concern, not a meta concern).
	if _, err := m.DB().ExecContext(ctx,
		`INSERT INTO chunk_map(ino, idx, tg_message_id, size) VALUES (?,?,?,?)`,
		ino, 0, 42, 0); err != nil {
		t.Fatal(err)
	}
	findings := checkChunkMapHealth(ctx, m)
	var sawErr bool
	for _, f := range findings {
		if f.Severity == sevErr && strings.Contains(f.Message, "non-positive size") {
			sawErr = true
		}
	}
	if !sawErr {
		t.Fatalf("expected non-positive-size error; got: %+v", findings)
	}
}

// TestCheckCacheVsMapFlagsOrphan: a cache file whose (ino, idx)
// doesn't match any chunk_map row is reported as an orphan.
func TestCheckCacheVsMapFlagsOrphan(t *testing.T) {
	m := openTestMeta(t)
	ctx := context.Background()
	ino, _ := m.CreateChild(ctx, meta.RootIno, "f", meta.Inode{Kind: meta.KindFile, Mode: 0o100644})
	if err := m.PutChunk(ctx, meta.Chunk{Ino: ino, Idx: 0, TGMessageID: 42, Size: 4}); err != nil {
		t.Fatal(err)
	}
	cacheDir := t.TempDir()
	// One legit cache file (matches ino/idx=0) and one orphan.
	for _, name := range []string{
		// legit
		filepath.Join(cacheDir, "X-0.bin"), // wrong ino so it's orphan
	} {
		if err := os.WriteFile(name, []byte("data"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	legit := filepath.Join(cacheDir, formatCacheName(ino, 0))
	if err := os.WriteFile(legit, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(cacheDir, "999-7.bin")
	if err := os.WriteFile(orphan, []byte("orphan"), 0o600); err != nil {
		t.Fatal(err)
	}

	findings := checkCacheVsMap(ctx, m, cacheDir)
	var sawOrphan, sawJunk bool
	for _, f := range findings {
		if f.Severity == sevWarn && strings.Contains(f.Message, "orphan cache file") {
			sawOrphan = true
		}
		if f.Severity == sevWarn && strings.Contains(f.Message, "unparseable cache filename") {
			sawJunk = true
		}
	}
	if !sawOrphan {
		t.Fatalf("expected orphan warning; got: %+v", findings)
	}
	if !sawJunk {
		t.Fatalf("expected unparseable-name warning for 'X-0.bin'; got: %+v", findings)
	}
}

func formatCacheName(ino int64, idx int32) string {
	return strings.Join([]string{itoa(ino), "-", itoa(int64(idx)), ".bin"}, "")
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	out := []byte{}
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		out = append([]byte{byte('0' + n%10)}, out...)
		n /= 10
	}
	if neg {
		out = append([]byte{'-'}, out...)
	}
	return string(out)
}
