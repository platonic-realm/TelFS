package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"telfs/internal/chunk"
	"telfs/internal/compress"
	"telfs/internal/config"
	"telfs/internal/meta"
	"telfs/internal/restore"
	"telfs/internal/wire"
)

// `telfs restore` usage errors exit 1 before anything is read.
func TestRestoreCLIUsage(t *testing.T) {
	isolate(t, "t")
	ctx := context.Background()
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"telfs", "restore"}, restoreUsage},
		{[]string{"telfs", "restore", "/"}, restoreUsage},
		{[]string{"telfs", "restore", "/", "a", "b"}, restoreUsage},
		{[]string{"telfs", "restore", "--workers", "0", "/", "out"}, "--workers 0"},
		{[]string{"telfs", "restore", "--version", "a/b", "/", "out"}, "invalid version name"},
		{[]string{"telfs", "restore", "home", "out"}, "must be an absolute path"},
		{[]string{"telfs", "restore", "/", "out"}, "needs the profile's local DB"},
	} {
		err := dispatch(ctx, tc.args)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%v: err %v, want %q", tc.args, err, tc.want)
			continue
		}
		if c := exitCode(err); c != 1 {
			t.Errorf("%v: exit %d, want 1", tc.args, c)
		}
	}
}

// A format-1 filesystem is refused with a clear message (decision D1).
func TestRestoreRefusesFormat1(t *testing.T) {
	dir := isolate(t, "t")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	m, err := meta.Open((&config.Config{DataDir: dir}).DBPath())
	if err != nil {
		t.Fatal(err)
	}
	m.Close()
	err = dispatch(context.Background(), []string{"telfs", "restore", "/", filepath.Join(t.TempDir(), "out")})
	if !errors.Is(err, errRestoreFormat1) || exitCode(err) != 1 {
		t.Fatalf("err %v (exit %d), want the format-1 refusal", err, exitCode(err))
	}
}

// DEST must be local: not inside a TelFS mount (also through a symlink),
// not holding or inside the profile directory.
func TestCheckRestoreDest(t *testing.T) {
	base := t.TempDir()
	profile := filepath.Join(base, "cfg", "telfs", "profiles", "p")
	mnt := filepath.Join(base, "mnt")
	for _, d := range []string{profile, mnt, filepath.Join(base, "out")} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(mnt, filepath.Join(base, "via-link")); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{DataDir: profile}
	mounts := []string{mnt}
	for _, bad := range []string{mnt, filepath.Join(mnt, "x/y"), filepath.Join(base, "via-link", "z"),
		base, filepath.Join(base, "cfg"), filepath.Join(profile, "restore")} {
		if _, err := checkRestoreDest(cfg, bad, mounts); err == nil {
			t.Errorf("DEST %s accepted", bad)
		}
	}
	got, err := checkRestoreDest(cfg, filepath.Join(base, "out", "new", "dir"), mounts)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(resolveExisting(base), "out", "new", "dir"); got != want {
		t.Errorf("resolved %s, want %s", got, want)
	}
	if _, err := checkRestoreDest(cfg, filepath.Join(base, "mntx"), mounts); err != nil {
		t.Errorf("a sibling of the mount refused: %v", err)
	}
}

// The report's exit codes weigh integrity (7) over Telegram (6) over a
// local write failure (1); every problem file is in the TSV in the
// profile directory (never in DEST), the first 20 on stderr.
func TestReportRestore(t *testing.T) {
	profile := t.TempDir()
	dest := t.TempDir()
	cfg := &config.Config{DataDir: profile}
	var ps []restore.Problem
	for i := 0; i < 25; i++ {
		ps = append(ps, restore.Problem{Kind: restore.KindLocal, Path: fmt.Sprintf("/d/f%02d", i), Detail: "disk full"})
	}
	ps[3].Kind, ps[3].Path = restore.KindIntegrity, "/d/tab\there"
	ps[7].Kind = restore.KindTelegram
	ps[9].Kind = restore.KindPending
	for _, tc := range []struct {
		problems []restore.Problem
		exit     int
	}{
		{nil, 0},
		{ps[:3], 1},
		{ps[8:10], 4},
		{ps[4:10], 6},
		{ps, 7},
	} {
		var out, errOut bytes.Buffer
		res := &restore.Result{Files: 25, Problems: tc.problems}
		err := reportRestore(cfg, "/d", dest, res, 0, &out, &errOut)
		c := 0
		if err != nil {
			c = exitCode(err)
		}
		if c != tc.exit {
			t.Errorf("%d problems: exit %d, want %d (%v)", len(tc.problems), c, tc.exit, err)
		}
		if len(tc.problems) == 25 {
			if n := strings.Count(errOut.String(), "/d/f"); n != reportListMax-1 { // one of the 20 is /d/tab…
				t.Errorf("stderr names %d of the files, want %d:\n%s", n, reportListMax-1, errOut.String())
			}
			if !strings.Contains(errOut.String(), "and 5 more") {
				t.Errorf("stderr: %s", errOut.String())
			}
		}
	}
	entries, _ := os.ReadDir(dest)
	if len(entries) != 0 {
		t.Errorf("the report wrote into DEST: %v", entries)
	}
	tsvs, _ := filepath.Glob(filepath.Join(profile, "restore-*.tsv"))
	if len(tsvs) == 0 {
		t.Fatal("no TSV in the profile directory")
	}
	// Runs within one second share a name: the last one (25 rows) wins.
	b, _ := os.ReadFile(tsvs[len(tsvs)-1])
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 2+25 || !strings.Contains(string(b), "integrity\t/d/tab\\there\t") {
		t.Errorf("TSV (%d lines):\n%s", len(lines), b)
	}
}

// restoreFake serves a restore's downloads from memory.
type restoreFake struct{ docs map[int64][]byte }

func (f restoreFake) Fetch(_ context.Context, msg int64) ([]byte, error) {
	if d, ok := f.docs[msg]; ok {
		return d, nil
	}
	return nil, fmt.Errorf("message %d is empty (deleted?)", msg)
}

func (f restoreFake) FetchRange(ctx context.Context, msg, off, n int64) ([]byte, error) {
	d, err := f.Fetch(ctx, msg)
	if err != nil || off+n > int64(len(d)) {
		return nil, fmt.Errorf("range: %v", err)
	}
	return d[off : off+n], nil
}

// flipCodec damages the bytes of one file after they decrypted, as a bad
// disk would after the write.
type flipCodec struct {
	restore.Codec
	ino int64
}

func (c flipCodec) Decode(ino int64, idx int32, h, w []byte) ([]byte, error) {
	b, err := c.Codec.Decode(ino, idx, h, w)
	if err == nil && ino == c.ino {
		b = append([]byte(nil), b...)
		b[0] ^= 0x80
	}
	return b, err
}

// End to end below the network: a byte that lands wrong in DEST makes
// the restore exit 7 and names the file — on stderr and in the TSV.
func TestRestoreCorruptByteExits7(t *testing.T) {
	ctx := context.Background()
	profile := t.TempDir()
	m, err := meta.Open(filepath.Join(profile, "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	m.PutKV(ctx, meta.KVFormat, []byte("2"))
	fsUUID, _ := m.FSUUID(ctx)
	dek := make([]byte, 32)
	rand.Read(dek)
	keys, _ := wire.DeriveKeys(dek, fsUUID, nil)
	wc, err := chunk.NewFormat2Codec(keys, compress.None{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	fake := restoreFake{docs: map[int64][]byte{}}
	var pack []byte
	inos := map[string]int64{}
	for _, name := range []string{"good.txt", "bad.txt"} {
		data := []byte("contents of " + name)
		ino, err := m.CreateChild(ctx, meta.RootIno, name, meta.Inode{Kind: meta.KindFile, Mode: 0o100644, Size: int64(len(data))})
		if err != nil {
			t.Fatal(err)
		}
		h := wc.Hash(data)
		w, _, _ := wc.Encode(ino, 0, data, h)
		m.PutChunk(ctx, meta.Chunk{Ino: ino, Idx: 0, TGMessageID: 9, Size: int32(len(data)), PackOffset: int64(len(pack)), PackSize: int32(len(w)), Hash: h})
		pack = append(pack, w...)
		inos[name] = ino
	}
	fake.docs[9] = pack
	plan, err := restore.BuildPlan(ctx, m, "/")
	if err != nil {
		t.Fatal(err)
	}
	dest := t.TempDir()
	res, err := restore.Run(ctx, plan, restore.Options{Dest: dest, Codec: flipCodec{Codec: wc, ino: inos["bad.txt"]}, Download: fake})
	if err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	err = reportRestore(&config.Config{DataDir: profile}, "/", dest, res, 0, &out, &errOut)
	if exitCode(err) != 7 {
		t.Fatalf("exit %d (%v), want 7", exitCode(err), err)
	}
	if !strings.Contains(errOut.String(), "/bad.txt") || strings.Contains(errOut.String(), "/good.txt") {
		t.Errorf("stderr:\n%s", errOut.String())
	}
	tsvs, _ := filepath.Glob(filepath.Join(profile, "restore-*.tsv"))
	if len(tsvs) != 1 {
		t.Fatalf("TSVs %v", tsvs)
	}
	b, _ := os.ReadFile(tsvs[0])
	if !strings.Contains(string(b), "integrity\t/bad.txt\t") {
		t.Errorf("TSV:\n%s", b)
	}
	if got, _ := os.ReadFile(filepath.Join(dest, "good.txt")); string(got) != "contents of good.txt" {
		t.Errorf("good.txt = %q", got)
	}
}

// A live restore tells data still in flight from data lost: a file that
// changed after the read (its missing row landed), or uploads pending —
// staged wires when no daemon answers.
func TestRestoreInFlightCheck(t *testing.T) {
	ctx := context.Background()
	profile := isolate(t, "t")
	if err := os.MkdirAll(profile, 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{DataDir: profile}
	m, err := meta.Open(cfg.DBPath())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ino, err := m.CreateChild(ctx, meta.RootIno, "f", meta.Inode{Kind: meta.KindFile, Mode: 0o100644, Size: 10, ChunkSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	m.PutChunk(ctx, meta.Chunk{Ino: ino, Idx: 0, TGMessageID: 5, Size: 4, Hash: []byte{1}})
	m.PutChunk(ctx, meta.Chunk{Ino: ino, Idx: 2, TGMessageID: 7, Size: 2, Hash: []byte{3}})
	plan, err := restore.BuildPlan(ctx, m, "/f")
	if err != nil {
		t.Fatal(err)
	}
	e := &plan.Entries[0]
	if len(e.Missing) != 1 || e.Missing[0] != 1 {
		t.Fatalf("missing %v", e.Missing)
	}
	check := inFlightCheck(cfg, m)
	if check(ctx, e) {
		t.Fatal("unchanged file, nothing staged: reported in flight")
	}
	// The row lands after the read.
	m.PutChunk(ctx, meta.Chunk{Ino: ino, Idx: 1, TGMessageID: 6, Size: 4, Hash: []byte{2}})
	if !changedSince(ctx, m, e) {
		t.Error("a row that landed after the read: not seen")
	}
	// No daemon, but wires staged for the next mount.
	if err := os.MkdirAll(cfg.StagingDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	if uploadsPending(ctx, cfg) {
		t.Error("empty staging dir: uploads pending")
	}
	os.WriteFile(filepath.Join(cfg.StagingDir(), "9-1-4-00.bin"), []byte("wire"), 0o600)
	if !uploadsPending(ctx, cfg) {
		t.Error("a staged wire: no uploads pending")
	}
}
