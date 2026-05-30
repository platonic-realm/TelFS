package chunk

import (
	"fmt"
	"os"
	"path/filepath"
)

// PendingDirName is the subdirectory under the cache root where
// wire bytes awaiting upload + chunk_map land are staged. Existence
// of a file here is the source-of-truth answer to "did this chunk
// land?" — restart scans this directory and re-submits any leftover
// wires to the packer so a hard daemon kill never loses cp'd data.
const PendingDirName = "pending"

// PendingEntry describes one wire staged on disk for a chunk that
// hasn't yet been committed to chunk_map. The file holds the wire
// bytes (post-compress, post-encrypt); metadata travels in the
// filename so a scan doesn't need to open every file.
//
// Filename format: <ino>-<idx>-<plaintextSize>.bin
type PendingEntry struct {
	Ino           int64
	Idx           int32
	PlaintextSize int32
	Path          string
}

// EnsurePendingDir creates the pending directory under cacheDir if
// it doesn't exist. Idempotent.
func EnsurePendingDir(cacheDir string) (string, error) {
	dir := filepath.Join(cacheDir, PendingDirName)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("pending: mkdir %s: %w", dir, err)
	}
	return dir, nil
}

// PersistPending writes a wire to disk under the given pending dir.
// Returns the on-disk path on success.
//
// We do NOT fsync — the cost is significant for many small chunks,
// and the failure mode that matters for TelFS is "daemon SIGKILL
// while machine keeps running," which the page cache covers
// without fsync. Power-loss recovery is best-effort: any wire that
// hadn't reached disk before the crash is lost (same as a sync
// daemon's in-flight buffer would be).
func PersistPending(dir string, ino int64, idx int32, plaintextSize int32, wire []byte) (string, error) {
	name := fmt.Sprintf("%d-%d-%d.bin", ino, idx, plaintextSize)
	path := filepath.Join(dir, name)
	// Write-then-rename so a partial write never appears as a valid
	// pending entry to ScanPending. Tmp filename includes a process-
	// specific suffix to avoid clashes between concurrent uploadOne
	// goroutines that might (rarely) target the same (ino, idx).
	tmp := path + fmt.Sprintf(".tmp-%d", os.Getpid())
	if err := os.WriteFile(tmp, wire, 0o600); err != nil {
		return "", fmt.Errorf("pending: write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return "", fmt.Errorf("pending: rename %s: %w", path, err)
	}
	return path, nil
}

// DeletePending removes a staged wire. Called after the packer's
// flush has committed the chunk_map row, so the chunk is durable
// on TG + indexed locally. Errors are swallowed by callers (the
// next scan would re-submit; idempotent on PutChunk's ON CONFLICT).
func DeletePending(path string) error {
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// ScanPending walks the pending dir and returns each staged wire
// in arbitrary order. Files whose names don't parse are skipped
// (e.g., orphan .tmp-* from interrupted writes — callers can clean
// those separately).
func ScanPending(dir string) ([]PendingEntry, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("pending: readdir %s: %w", dir, err)
	}
	var out []PendingEntry
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		// Strict suffix check: Sscanf("%d-%d-%d.bin", ...) accepts
		// trailing garbage like ".tmp-N", so we'd otherwise rescan
		// half-written files. Insist on exactly ".bin" at the end.
		if !endsWith(name, ".bin") {
			continue
		}
		stem := name[:len(name)-len(".bin")]
		var ino, idx, plaintextSize int64
		n, err := fmt.Sscanf(stem, "%d-%d-%d", &ino, &idx, &plaintextSize)
		if err != nil || n != 3 {
			continue
		}
		out = append(out, PendingEntry{
			Ino:           ino,
			Idx:           int32(idx),
			PlaintextSize: int32(plaintextSize),
			Path:          filepath.Join(dir, name),
		})
	}
	return out, nil
}

func endsWith(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}

// CleanupPendingTmp removes any .tmp-* leftovers from a previous
// daemon's interrupted PersistPending. Called once at startup
// before ScanPending so a partially-written wire doesn't get re-
// submitted (the .bin form wouldn't exist for those anyway, but
// the .tmp- files waste disk).
func CleanupPendingTmp(dir string) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		// .tmp- markers anywhere in the name flag interrupted writes.
		if idx := indexOfTmpSuffix(e.Name()); idx >= 0 {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}

func indexOfTmpSuffix(name string) int {
	for i := 0; i+4 < len(name); i++ {
		if name[i] == '.' && name[i+1] == 't' && name[i+2] == 'm' && name[i+3] == 'p' && name[i+4] == '-' {
			return i
		}
	}
	return -1
}
