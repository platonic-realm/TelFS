package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"telfs/internal/config"
	"telfs/internal/crypto"
	"telfs/internal/meta"
	"telfs/internal/snapshot"
)

// cmdDoctor runs a battery of read-only local-state invariant checks
// against the active profile. Complements `telfs status` (descriptive,
// not interpretive) and `telfs fsck` (channel-side integrity) by
// validating that the things that have to be true about the LOCAL
// state actually are. Never writes to disk.
//
// Exit code reflects severity: 0 if everything is ok or only warnings,
// 1 if any error-level finding surfaces — so doctor is usable as a
// monitoring probe.
func cmdDoctor(ctx context.Context, args []string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	var report []finding
	report = append(report, checkProfileDir(cfg)...)

	if !fileExistsLocal(cfg.DBPath()) {
		report = append(report, infof("meta", "db.sqlite missing — FS not initialized yet (`telfs init` or first mount creates it)"))
		printReport(report)
		// Missing DB is informational on a fresh profile, not an error.
		return nil
	}

	mstore, err := meta.Open(cfg.DBPath())
	if err != nil {
		report = append(report, errorf("meta", "cannot open db.sqlite: %v", err))
		printReport(report)
		return errors.New("doctor: errors found")
	}
	defer mstore.Close()

	report = append(report, checkMetaKV(ctx, mstore)...)
	report = append(report, checkCryptoState(ctx, mstore)...)
	report = append(report, checkChunkMapHealth(ctx, mstore)...)
	report = append(report, checkSnapshotJournal(ctx, mstore)...)
	report = append(report, checkCacheVsMap(ctx, mstore, cfg.CachePath())...)

	hasErr := printReport(report)
	if hasErr {
		return errors.New("doctor: integrity errors found")
	}
	return nil
}

// finding is a single doctor diagnostic. Severity drives the report
// summary and the process exit code.
type finding struct {
	Severity severity
	Area     string
	Message  string
}

type severity int

const (
	sevOK severity = iota
	sevInfo
	sevWarn
	sevErr
)

func (s severity) tag() string {
	switch s {
	case sevOK:
		return "  ok "
	case sevInfo:
		return "info"
	case sevWarn:
		return "warn"
	case sevErr:
		return " err"
	}
	return "????"
}

func okf(area, msg string, a ...any) finding {
	return finding{Severity: sevOK, Area: area, Message: fmt.Sprintf(msg, a...)}
}
func infof(area, msg string, a ...any) finding {
	return finding{Severity: sevInfo, Area: area, Message: fmt.Sprintf(msg, a...)}
}
func warnf(area, msg string, a ...any) finding {
	return finding{Severity: sevWarn, Area: area, Message: fmt.Sprintf(msg, a...)}
}
func errorf(area, msg string, a ...any) finding {
	return finding{Severity: sevErr, Area: area, Message: fmt.Sprintf(msg, a...)}
}

func printReport(report []finding) (hasErr bool) {
	var okN, infoN, warnN, errN int
	for _, f := range report {
		fmt.Printf("[%s] %-10s %s\n", f.Severity.tag(), f.Area, f.Message)
		switch f.Severity {
		case sevOK:
			okN++
		case sevInfo:
			infoN++
		case sevWarn:
			warnN++
		case sevErr:
			errN++
		}
	}
	fmt.Printf("\nSummary: %d ok, %d info, %d warning, %d error\n", okN, infoN, warnN, errN)
	return errN > 0
}

// ── checks ──────────────────────────────────────────────────────────

func checkProfileDir(cfg *config.Config) []finding {
	var out []finding
	// config.toml mode — must be 0600 to keep api_hash out of other
	// users' eyes on a multi-user host.
	if info, err := os.Stat(cfg.ConfigPath()); err == nil {
		mode := info.Mode().Perm()
		if mode&0o077 != 0 {
			out = append(out, warnf("profile", "config.toml mode is %#o; consider chmod 600 (contains api_hash)", mode))
		} else {
			out = append(out, okf("profile", "config.toml present, mode %#o", mode))
		}
	} else {
		out = append(out, errorf("profile", "config.toml missing: %v", err))
	}

	// session.json: optional (e.g. on a freshly-created profile before
	// login), but if present, mode must also be tight.
	if info, err := os.Stat(cfg.SessionPath()); err == nil {
		mode := info.Mode().Perm()
		if mode&0o077 != 0 {
			out = append(out, warnf("profile", "session.json mode is %#o; consider chmod 600 (contains MTProto auth key)", mode))
		} else {
			out = append(out, okf("profile", "session.json present, mode %#o", mode))
		}
	} else if os.IsNotExist(err) {
		out = append(out, infof("profile", "session.json absent — `telfs login` first"))
	} else {
		out = append(out, errorf("profile", "session.json stat: %v", err))
	}

	// Cache dir: not strictly required (NewCache creates it), but if
	// it exists, must be writable.
	if info, err := os.Stat(cfg.CachePath()); err == nil {
		if !info.IsDir() {
			out = append(out, errorf("cache", "%s is not a directory", cfg.CachePath()))
		} else {
			out = append(out, okf("cache", "cache directory exists: %s", cfg.CachePath()))
		}
	}
	return out
}

var uuidRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func checkMetaKV(ctx context.Context, m *meta.Store) []finding {
	var out []finding
	if uuid, err := m.FSUUID(ctx); err != nil {
		out = append(out, errorf("kv", "fs_uuid missing — meta_kv is in an inconsistent state: %v", err))
	} else if !uuidRE.MatchString(uuid) {
		out = append(out, errorf("kv", "fs_uuid %q is not a valid UUID; channel snapshots cannot be filtered correctly", uuid))
	} else {
		out = append(out, okf("kv", "fs_uuid %s", uuid))
	}
	if size, err := m.ChunkSize(ctx); err != nil {
		out = append(out, errorf("kv", "chunk_size missing: %v", err))
	} else {
		// Must be a power of two within MinChunkSize..MaxChunkSize.
		switch {
		case size < meta.MinChunkSize:
			out = append(out, errorf("kv", "chunk_size %d below MinChunkSize %d", size, meta.MinChunkSize))
		case size > meta.MaxChunkSize:
			out = append(out, errorf("kv", "chunk_size %d above MaxChunkSize %d", size, meta.MaxChunkSize))
		case size&(size-1) != 0:
			out = append(out, errorf("kv", "chunk_size %d is not a power of two", size))
		default:
			out = append(out, okf("kv", "chunk_size %d bytes (%s)", size, humanBytes(size)))
		}
	}
	return out
}

func checkCryptoState(ctx context.Context, m *meta.Store) []finding {
	var out []finding
	mode, err := m.GetKV(ctx, crypto.KVMode)
	if errors.Is(err, meta.ErrNotFound) {
		out = append(out, infof("crypto", "encryption disabled (plaintext FS)"))
		return out
	}
	if err != nil {
		out = append(out, errorf("crypto", "crypto_mode read failed: %v", err))
		return out
	}
	out = append(out, okf("crypto", "mode %s", string(mode)))

	salt, err := m.GetKV(ctx, crypto.KVSalt)
	if err != nil {
		out = append(out, errorf("crypto", "crypto_salt missing: %v", err))
	} else if len(salt) != 16 {
		out = append(out, errorf("crypto", "crypto_salt length %d, want 16", len(salt)))
	}

	argonBytes, err := m.GetKV(ctx, crypto.KVArgon)
	if err != nil {
		out = append(out, errorf("crypto", "crypto_argon_params missing: %v", err))
	} else if _, err := crypto.UnmarshalArgonParams(argonBytes); err != nil {
		out = append(out, errorf("crypto", "crypto_argon_params unparseable: %v", err))
	}

	if _, err := m.GetKV(ctx, crypto.KVCanary); err != nil {
		out = append(out, errorf("crypto", "crypto_canary missing: %v", err))
	}

	switch string(mode) {
	case crypto.ModeAESGCMv1:
		// v1 has no wrapped DEK by design.
	case crypto.ModeAESGCMv2:
		wrapped, err := m.GetKV(ctx, crypto.KVWrappedDEK)
		if err != nil {
			out = append(out, errorf("crypto", "v2 FS missing crypto_wrapped_dek: %v", err))
		} else if len(wrapped) < 12+32 {
			out = append(out, errorf("crypto", "crypto_wrapped_dek length %d too short for nonce+DEK+tag", len(wrapped)))
		} else {
			out = append(out, okf("crypto", "wrapped DEK present (%d bytes; %s nonce)", len(wrapped), hex.EncodeToString(wrapped[:4])+"..."))
		}
	default:
		out = append(out, errorf("crypto", "unknown crypto_mode %q", string(mode)))
	}
	return out
}

func checkChunkMapHealth(ctx context.Context, m *meta.Store) []finding {
	var out []finding
	chunks, err := m.AllChunks(ctx)
	if err != nil {
		out = append(out, errorf("chunks", "AllChunks: %v", err))
		return out
	}
	if len(chunks) == 0 {
		out = append(out, infof("chunks", "chunk_map is empty (no data written yet)"))
		return out
	}
	msgIDs := make(map[int64]int)
	var totalBytes int64
	for _, c := range chunks {
		msgIDs[c.TGMessageID]++
		totalBytes += int64(c.Size)
		if c.Size <= 0 {
			out = append(out, errorf("chunks", "chunk_map row ino=%d idx=%d has non-positive size %d", c.Ino, c.Idx, c.Size))
		}
		if c.TGMessageID <= 0 {
			out = append(out, errorf("chunks", "chunk_map row ino=%d idx=%d has non-positive tg_message_id %d", c.Ino, c.Idx, c.TGMessageID))
		}
	}
	dedupSaved := len(chunks) - len(msgIDs)
	if dedupSaved > 0 {
		out = append(out, okf("chunks", "%d row(s) reference %d distinct channel message(s); %d collapsed by dedup",
			len(chunks), len(msgIDs), dedupSaved))
	} else {
		out = append(out, okf("chunks", "%d row(s), %d distinct channel message(s)", len(chunks), len(msgIDs)))
	}
	out = append(out, okf("chunks", "total chunk payload: %s", humanBytes(totalBytes)))

	if blobs, err := m.CountChunkBlobs(ctx); err == nil && blobs > 0 {
		out = append(out, okf("chunks", "dedup index has %d blob(s)", blobs))
	}
	return out
}

func checkSnapshotJournal(ctx context.Context, m *meta.Store) []finding {
	var out []finding
	if id, err := m.GetKV(ctx, snapshot.KVCurrentMsgID); err == nil {
		if n, perr := strconv.Atoi(string(id)); perr == nil && n > 0 {
			out = append(out, okf("snap", "last snapshot msg_id: %d", n))
		} else {
			out = append(out, warnf("snap", "snap_msg_id value %q is not a positive integer", string(id)))
		}
	} else if errors.Is(err, meta.ErrNotFound) {
		out = append(out, infof("snap", "no snapshot recorded yet (will be taken on next mount)"))
	} else {
		out = append(out, errorf("snap", "read snap_msg_id: %v", err))
	}
	if seq, err := m.LastJournalSeq(ctx); err == nil {
		if seq > 0 {
			out = append(out, okf("snap", "journal high-water seq: %d", seq))
		} else {
			out = append(out, infof("snap", "journal is empty"))
		}
	}
	return out
}

func checkCacheVsMap(ctx context.Context, m *meta.Store, cacheDir string) []finding {
	var out []finding
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		// Cache dir absent is fine — it's lazily created.
		if !os.IsNotExist(err) {
			out = append(out, warnf("cache", "ReadDir(%s): %v", cacheDir, err))
		}
		return out
	}
	// Build the set of (ino, idx) keys present in chunk_map.
	chunks, err := m.AllChunks(ctx)
	if err != nil {
		out = append(out, errorf("cache", "AllChunks for cache crosscheck: %v", err))
		return out
	}
	live := make(map[struct {
		ino int64
		idx int32
	}]struct{}, len(chunks))
	for _, c := range chunks {
		live[struct {
			ino int64
			idx int32
		}{c.Ino, c.Idx}] = struct{}{}
	}

	var cached, orphan int
	var orphanBytes, totalBytes int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		var ino int64
		var idx int32
		if _, err := fmt.Sscanf(e.Name(), "%d-%d.bin", &ino, &idx); err != nil {
			out = append(out, warnf("cache", "unparseable cache filename %q (junk left from a crash?)", e.Name()))
			continue
		}
		cached++
		info, err := e.Info()
		if err != nil {
			continue
		}
		totalBytes += info.Size()
		if _, ok := live[struct {
			ino int64
			idx int32
		}{ino, idx}]; !ok {
			orphan++
			orphanBytes += info.Size()
		}
	}
	if cached == 0 {
		out = append(out, infof("cache", "cache is empty"))
		return out
	}
	out = append(out, okf("cache", "%d cached chunk(s), %s on disk", cached, humanBytes(totalBytes)))
	if orphan > 0 {
		out = append(out, warnf("cache", "%d orphan cache file(s) (%s) reference (ino, idx) tuples not in chunk_map — safe to delete from %s",
			orphan, humanBytes(orphanBytes), filepath.Clean(cacheDir)))
	}
	return out
}

func fileExistsLocal(path string) bool {
	if path == "" {
		return false
	}
	_, err := os.Stat(path)
	return err == nil
}
