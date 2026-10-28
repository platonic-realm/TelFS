package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"telfs/internal/control"
	"telfs/internal/meta"
	"telfs/internal/restore"
)

// A live restore decides "in flight" per inode: a file with a missing
// chunk is pending only when ITS uploads were open (or it changed since
// the plan) — another file's upload does not hide data loss (exit 7) —
// and a whole file rewritten in place with uploads open is pending too.
func TestInFlightPerInode(t *testing.T) {
	ctx := context.Background()
	d, m, pool := newSyncDaemon(t)
	lost, _ := m.CreateChild(ctx, meta.RootIno, "lost", meta.Inode{Kind: meta.KindFile, Mode: 0o100644, Size: 8, ChunkSize: 4})
	m.PutChunk(ctx, meta.Chunk{Ino: lost, Idx: 0, TGMessageID: 5, Size: 4, Hash: []byte{1}})
	whole, _ := m.CreateChild(ctx, meta.RootIno, "whole", meta.Inode{Kind: meta.KindFile, Mode: 0o100644, Size: 4, ChunkSize: 4})
	m.PutChunk(ctx, meta.Chunk{Ino: whole, Idx: 0, TGMessageID: 6, Size: 4, Hash: []byte{2}})
	plan, err := restore.BuildPlan(ctx, m, "/")
	if err != nil {
		t.Fatal(err)
	}
	var files []*restore.Entry
	for i := range plan.Entries {
		if plan.Entries[i].Inode.Kind == meta.KindFile {
			files = append(files, &plan.Entries[i])
		}
	}
	// Another file is uploading: the lost chunk stays an integrity problem.
	got := inFlightCheck(m, map[int64]string{999: "its uploads were open in the mount"})(ctx, files)
	if len(got) != 0 {
		t.Fatalf("an unrelated upload made %v pending", got)
	}
	// The whole file's uploads were open at the plan: pending.
	got = inFlightCheck(m, map[int64]string{whole: "its uploads were open in the mount"})(ctx, files)
	if len(got) != 1 || got[whole] == "" {
		t.Fatalf("open uploads of the file: %v", got)
	}
	// Its new chunk lands while restoring (in-place rewrite): changed.
	m.PutChunk(ctx, meta.Chunk{Ino: whole, Idx: 0, TGMessageID: 66, Size: 4, Hash: []byte{3}})
	if got := changedSince(ctx, m, files); len(got) != 1 || got[whole] == "" {
		t.Fatalf("a replaced row: %v", got)
	}

	// The daemon names the inodes with uploads open.
	path := control.Path(d.cfg.DataDir)
	ln, err := control.Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go control.Serve(sctx, ln, path, d.handler())
	res := pool.Register(whole)
	defer res.Release()
	resp, err := control.Client(path, 5*time.Second).Get(control.BaseURL + "/uploads/open")
	if err != nil {
		t.Fatal(err)
	}
	var open openUploads
	json.NewDecoder(resp.Body).Decode(&open)
	resp.Body.Close()
	if len(open.Inos) != 1 || open.Inos[0] != whole {
		t.Fatalf("open uploads %v, want [%d]", open.Inos, whole)
	}
	if got := openUploadInos(ctx, d.cfg); len(got) != 1 || got[whole] == "" {
		t.Fatalf("openUploadInos %v", got)
	}
}
