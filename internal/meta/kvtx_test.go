package meta

import (
	"errors"
	"testing"
)

// UpdateKV is all or nothing; GetKVs reads several keys at once.
func TestUpdateKVAtomic(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	_ = s.PutKV(ctx, "a", []byte("1"))
	boom := errors.New("boom")
	err := s.UpdateKV(ctx, func(k *KVTx) error {
		if err := k.Put("a", []byte("2")); err != nil {
			return err
		}
		if err := k.Put("b", []byte("2")); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err %v", err)
	}
	kv, err := s.GetKVs(ctx, "a", "b", "c")
	if err != nil {
		t.Fatal(err)
	}
	if string(kv["a"]) != "1" || len(kv) != 1 {
		t.Fatalf("a failed update left %q", kv)
	}
	if err := s.UpdateKV(ctx, func(k *KVTx) error {
		if v, err := k.Get("a"); err != nil || string(v) != "1" {
			t.Errorf("get in tx: %q %v", v, err)
		}
		if _, err := k.Get("zz"); !errors.Is(err, ErrNotFound) {
			t.Errorf("missing key: %v", err)
		}
		if err := k.Delete("a"); err != nil {
			return err
		}
		if err := k.Delete("never-there"); err != nil {
			return err
		}
		if err := k.Put("b", []byte("x")); err != nil {
			return err
		}
		return k.AdvanceJournalSeq(41)
	}); err != nil {
		t.Fatal(err)
	}
	kv, _ = s.GetKVs(ctx, "a", "b")
	if _, ok := kv["a"]; ok || string(kv["b"]) != "x" {
		t.Fatalf("after update %q", kv)
	}
	if seq, _ := s.LastJournalSeq(ctx); seq != 41 {
		t.Fatalf("journal seq %d", seq)
	}
}
