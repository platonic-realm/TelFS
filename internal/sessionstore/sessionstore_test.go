package sessionstore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/gotd/td/session"

	"telfs/internal/crypto"
)

// Cheap params so the tests don't spend 2 GiB per derivation.
var testParams = crypto.ArgonParams{Time: 1, Memory: 8 * 1024, Threads: 1}

func TestRoundTripAndWrongSecret(t *testing.T) {
	ctx := context.Background()
	p := filepath.Join(t.TempDir(), "session.enc")
	st, err := Open(p, []byte("right"), testParams)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.LoadSession(ctx); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("empty store: %v", err)
	}
	data := []byte(`{"Version":1,"Data":{"AuthKey":"c2VjcmV0"}}`)
	if err := st.StoreSession(ctx, data); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(p)
	if bytes.Contains(raw, []byte("AuthKey")) || bytes.Contains(raw, []byte("c2VjcmV0")) {
		t.Fatal("session.enc contains the plaintext")
	}
	if fi, _ := os.Stat(p); fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v, want 0600", fi.Mode().Perm())
	}
	// Reopen with the right secret.
	st2, err := Open(p, []byte("right"), testParams)
	if err != nil {
		t.Fatal(err)
	}
	got, err := st2.LoadSession(ctx)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("reload: %q %v", got, err)
	}
	// A second write keeps working and bumps the counter.
	if err := st2.StoreSession(ctx, append(data, ' ')); err != nil {
		t.Fatal(err)
	}
	raw2, _ := os.ReadFile(p)
	h, _, _, _ := parse(raw2)
	if h.N != 2 {
		t.Fatalf("write counter = %d, want 2", h.N)
	}
	// Wrong secret fails at Open, before anything else.
	if _, err := Open(p, []byte("wrong"), testParams); !errors.Is(err, crypto.ErrWrongSecret) {
		t.Fatalf("wrong secret: %v", err)
	}
	// Tampering with the header (AAD) is detected.
	raw2[len(magic)+5] ^= 1
	os.WriteFile(p, raw2, 0o600)
	if _, err := Open(p, []byte("right"), testParams); err == nil {
		t.Fatal("tampered header must not open")
	}
}

func TestAtomicWriteLeavesNoTornFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	if err := WriteAtomic(p, []byte("one"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := WriteAtomic(p, []byte("two"), 0o600); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(p)
	if string(b) != "two" {
		t.Fatalf("got %q", b)
	}
	ents, _ := os.ReadDir(dir)
	if len(ents) != 1 {
		t.Fatalf("temp files left behind: %v", ents)
	}
}

func TestConvertLegacy(t *testing.T) {
	dir := t.TempDir()
	legacy := filepath.Join(dir, "session.json")
	enc := filepath.Join(dir, "session.enc")
	data := []byte(`{"Version":1,"Data":{"DC":2,"AuthKey":"a2V5"}}`)
	os.WriteFile(legacy, data, 0o600)
	if err := Convert(legacy, enc, []byte("pw"), testParams); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Fatal("plaintext session.json must be removed after conversion")
	}
	st, err := Open(enc, []byte("pw"), testParams)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := st.LoadSession(context.Background()); !bytes.Equal(got, data) {
		t.Fatalf("converted content: %q", got)
	}
	// Not a gotd file → refused.
	os.WriteFile(legacy, []byte("hello"), 0o600)
	os.Remove(enc)
	if err := Convert(legacy, enc, []byte("pw"), testParams); err == nil {
		t.Fatal("a non-session file must not be converted")
	}
}
