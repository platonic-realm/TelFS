package chunk

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// PendingIndex is an in-process map (Ino, Idx) → on-disk path for
// pending wires. The Reader consults it on chunk_map miss to find
// the staged wire in O(1), replacing a filepath.Glob over the entire
// pending dir (which scales with queue depth and was measurable on
// kernel-untar workloads).
//
// The index is a CACHE in front of the disk source of truth: the
// disk is authoritative, the index is best-effort acceleration. A
// missing index entry MUST fall back to the disk scan — recovery
// state or an unsynced concurrent write could leave a wire on disk
// that the index doesn't know about. Reader.tryPendingFallback
// preserves the Glob fallback on index miss.
//
// Concurrency: a single sync.RWMutex; reads dominate (every pending-
// chunk read does one), writes are rare (one per upload).
type PendingIndex struct {
	mu sync.RWMutex
	m  map[Key]string
}

// NewPendingIndex returns an empty index. Hand it to the daemon's
// reader (Reader.SetPendingIndex) and to the writer/packer paths
// that update it (RecordPersist, UnrecordPersist, DropByIno).
func NewPendingIndex() *PendingIndex {
	return &PendingIndex{m: make(map[Key]string)}
}

// RecordPersist registers a freshly-persisted wire's path. Called
// from the writer after PersistPending succeeds.
func (p *PendingIndex) RecordPersist(ino int64, idx int32, path string) {
	if p == nil || path == "" {
		return
	}
	p.mu.Lock()
	p.m[Key{Ino: ino, Idx: idx}] = path
	p.mu.Unlock()
}

// UnrecordPersist drops the index entry for (ino, idx). Called from
// the packer after DeletePending lands (chunk_map row committed or
// FK-violation drop).
func (p *PendingIndex) UnrecordPersist(ino int64, idx int32) {
	if p == nil {
		return
	}
	p.mu.Lock()
	delete(p.m, Key{Ino: ino, Idx: idx})
	p.mu.Unlock()
}

// Lookup returns the on-disk path for (ino, idx) if known. Reader
// uses this in the fallback fast path before falling back to a Glob
// of the pending dir (which costs O(N) in queue depth).
func (p *PendingIndex) Lookup(ino int64, idx int32) (string, bool) {
	if p == nil {
		return "", false
	}
	p.mu.RLock()
	path, ok := p.m[Key{Ino: ino, Idx: idx}]
	p.mu.RUnlock()
	return path, ok
}

// DropByIno removes every entry for ino. Called by Packer.DropInode
// when an unlink discards in-flight wires; mirrors what
// DropInode + DeletePending do on disk so the next Reader fallback
// doesn't return a stale path that points at a just-deleted file.
func (p *PendingIndex) DropByIno(ino int64) {
	if p == nil {
		return
	}
	p.mu.Lock()
	for k := range p.m {
		if k.Ino == ino {
			delete(p.m, k)
		}
	}
	p.mu.Unlock()
}

// Len reports the current entry count. Used by /cache JSON and tests.
func (p *PendingIndex) Len() int {
	if p == nil {
		return 0
	}
	p.mu.RLock()
	n := len(p.m)
	p.mu.RUnlock()
	return n
}

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
