package chunk

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// TestPersistScanDeleteRoundTrip exercises the basic lifecycle:
// stage a wire on disk → scan finds it with the right metadata →
// delete removes it. No upload involved.
func TestPersistScanDeleteRoundTrip(t *testing.T) {
	dir, err := EnsurePendingDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	want := []byte("wire-bytes-for-some-chunk")
	path, err := PersistPending(dir, 42, 7, int32(len(want)), want)
	if err != nil {
		t.Fatal(err)
	}
	// Confirm the file is there with the expected content.
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("persisted bytes mismatch: %q vs %q", got, want)
	}
	// ScanPending should return one entry with the metadata we encoded.
	ents, err := ScanPending(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 1 {
		t.Fatalf("scan returned %d entries, want 1", len(ents))
	}
	e := ents[0]
	if e.Ino != 42 || e.Idx != 7 || e.PlaintextSize != int32(len(want)) {
		t.Fatalf("scan entry mismatch: %+v", e)
	}
	if e.Path != path {
		t.Fatalf("scan path %q != persist path %q", e.Path, path)
	}
	// Delete and confirm scan returns empty.
	if err := DeletePending(path); err != nil {
		t.Fatal(err)
	}
	ents, err = ScanPending(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 0 {
		t.Fatalf("scan after delete = %d entries, want 0", len(ents))
	}
}

// TestPersistAtomicVsCrash: a tmp-file left behind by an interrupted
// PersistPending must NOT show up in ScanPending, and CleanupPendingTmp
// must remove it.
func TestPersistAtomicVsCrash(t *testing.T) {
	dir, err := EnsurePendingDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Simulate an interrupted persist: a .tmp- file exists but the
	// final .bin doesn't.
	tmpPath := filepath.Join(dir, "1-2-3.bin.tmp-99999")
	if err := os.WriteFile(tmpPath, []byte("partial wire"), 0o600); err != nil {
		t.Fatal(err)
	}
	// ScanPending must ignore it (would re-submit garbage to packer).
	ents, _ := ScanPending(dir)
	if len(ents) != 0 {
		t.Fatalf("scan picked up tmp file: %+v", ents)
	}
	// CleanupPendingTmp should remove it.
	CleanupPendingTmp(dir)
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatalf("CleanupPendingTmp didn't remove %s: %v", tmpPath, err)
	}
}

// TestDeletePendingIdempotent: re-deleting an already-gone path is
// not an error (the packer's flush calls it best-effort).
func TestDeletePendingIdempotent(t *testing.T) {
	dir, _ := EnsurePendingDir(t.TempDir())
	path, err := PersistPending(dir, 1, 1, 5, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if err := DeletePending(path); err != nil {
		t.Fatalf("first delete: %v", err)
	}
	if err := DeletePending(path); err != nil {
		t.Fatalf("second delete (should be noop): %v", err)
	}
}
