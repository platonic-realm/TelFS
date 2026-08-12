package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testBenchEnv(t *testing.T) *benchEnv {
	t.Helper()
	return &benchEnv{
		work:      t.TempDir(),
		files:     8,
		smallSize: 1 << 10,
		bigSize:   1 << 20,
		bs:        64 << 10,
		iters:     32,
		jobs:      2,
		randOps:   16,
		randBS:    4 << 10,
	}
}

// TestBenchScenariosRun drives every registered scenario, in registry
// order (matching "all"), against a tempdir and sanity-checks the
// results. Fixture reuse across scenarios is exercised implicitly.
func TestBenchScenariosRun(t *testing.T) {
	env := testBenchEnv(t)
	ctx := context.Background()
	for _, s := range benchScenarios {
		res, err := s.run(ctx, env)
		if err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		if res.ops <= 0 {
			t.Fatalf("%s: no ops recorded", s.name)
		}
		if len(res.lats) != res.ops {
			t.Fatalf("%s: %d latencies for %d ops", s.name, len(res.lats), res.ops)
		}
		for i := 1; i < len(res.lats); i++ {
			if res.lats[i] < res.lats[i-1] {
				t.Fatalf("%s: latencies not sorted", s.name)
			}
		}
		if res.wall <= 0 {
			t.Fatalf("%s: zero wall time", s.name)
		}
	}
	// unlink ran last: the small files must actually be gone.
	if _, err := os.Stat(env.smallPath(0)); !os.IsNotExist(err) {
		t.Fatalf("small file survived unlink scenario: %v", err)
	}
}

// TestBenchStandaloneFixtures verifies scenarios that depend on
// fixtures build them when run in isolation.
func TestBenchStandaloneFixtures(t *testing.T) {
	ctx := context.Background()
	for _, name := range []string{"read-small", "readdir", "unlink", "seq-read", "rand-read"} {
		env := testBenchEnv(t) // fresh empty workdir each time
		sel, err := selectBenchScenarios(name)
		if err != nil {
			t.Fatalf("select %s: %v", name, err)
		}
		if _, err := sel[0].run(ctx, env); err != nil {
			t.Fatalf("standalone %s: %v", name, err)
		}
	}
}

func TestBenchSeqWritePartialTail(t *testing.T) {
	env := testBenchEnv(t)
	env.bigSize = env.bs*3 + 17 // deliberately not block-aligned
	if _, err := benchSeqWrite(context.Background(), env); err != nil {
		t.Fatalf("seq-write: %v", err)
	}
	st, err := os.Stat(env.bigPath())
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != env.bigSize {
		t.Fatalf("big file is %d bytes, want %d", st.Size(), env.bigSize)
	}
	// seq-read must accept the odd-sized file too.
	if _, err := benchSeqRead(context.Background(), env); err != nil {
		t.Fatalf("seq-read: %v", err)
	}
}

func TestSelectBenchScenarios(t *testing.T) {
	all, err := selectBenchScenarios("all")
	if err != nil || len(all) != len(benchScenarios) {
		t.Fatalf("all: %d scenarios, err=%v", len(all), err)
	}
	two, err := selectBenchScenarios("seq-read, stat-hot")
	if err != nil {
		t.Fatal(err)
	}
	if len(two) != 2 || two[0].name != "seq-read" || two[1].name != "stat-hot" {
		t.Fatalf("expected user-ordered [seq-read stat-hot], got %v", two)
	}
	if _, err := selectBenchScenarios("no-such"); err == nil {
		t.Fatal("unknown scenario should error")
	}
	if _, err := selectBenchScenarios(","); err == nil {
		t.Fatal("empty selection should error")
	}
}

func TestBenchPct(t *testing.T) {
	lats := []time.Duration{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if got := benchPct(lats, 0.50); got != 5 {
		t.Fatalf("p50 = %d, want 5", got)
	}
	if got := benchPct(lats, 1.0); got != 10 {
		t.Fatalf("max = %d, want 10", got)
	}
	if got := benchPct(nil, 0.5); got != 0 {
		t.Fatalf("empty pct = %d, want 0", got)
	}
}

func TestBenchStatMissRejectsExisting(t *testing.T) {
	env := testBenchEnv(t)
	// Plant the "missing" file — the scenario must fail loudly instead
	// of quietly benchmarking successful stats as misses.
	dir := filepath.Join(env.work, "inc", "inc0")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "missing.h"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := benchStatMiss(context.Background(), env); err == nil {
		t.Fatal("expected error when the probed file exists")
	}
}
