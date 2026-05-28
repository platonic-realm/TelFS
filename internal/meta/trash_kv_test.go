package meta

import (
	"testing"
	"time"
)

func TestTrashEnabledDefault(t *testing.T) {
	s := newTestStore(t)
	on, err := s.TrashEnabled(ctxT(t))
	if err != nil {
		t.Fatal(err)
	}
	if on {
		t.Errorf("fresh FS should default to trash disabled")
	}
}

func TestTrashEnabledRoundtrip(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	if err := s.SetTrashEnabled(ctx, true); err != nil {
		t.Fatal(err)
	}
	on, _ := s.TrashEnabled(ctx)
	if !on {
		t.Errorf("after SetTrashEnabled(true), expected true")
	}
	if err := s.SetTrashEnabled(ctx, false); err != nil {
		t.Fatal(err)
	}
	on, _ = s.TrashEnabled(ctx)
	if on {
		t.Errorf("after SetTrashEnabled(false), expected false")
	}
}

func TestTrashTTLDefault(t *testing.T) {
	s := newTestStore(t)
	d, err := s.TrashTTL(ctxT(t))
	if err != nil {
		t.Fatal(err)
	}
	want := time.Duration(DefaultTrashTTLSecs) * time.Second
	if d != want {
		t.Errorf("default ttl: got %s, want %s", d, want)
	}
}

func TestTrashTTLRoundtrip(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	if err := s.SetTrashTTL(ctx, 3*time.Hour); err != nil {
		t.Fatal(err)
	}
	d, _ := s.TrashTTL(ctx)
	if d != 3*time.Hour {
		t.Errorf("ttl roundtrip: got %s, want 3h", d)
	}
}

func TestTrashTTLZeroBecomesDefault(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	if err := s.SetTrashTTL(ctx, 0); err != nil {
		t.Fatal(err)
	}
	d, _ := s.TrashTTL(ctx)
	want := time.Duration(DefaultTrashTTLSecs) * time.Second
	if d != want {
		t.Errorf("SetTrashTTL(0) should mean default; got %s, want %s", d, want)
	}
}

func TestTrashTTLNegativeRejected(t *testing.T) {
	s := newTestStore(t)
	if err := s.SetTrashTTL(ctxT(t), -time.Second); err == nil {
		t.Errorf("expected error on negative ttl, got nil")
	}
}
