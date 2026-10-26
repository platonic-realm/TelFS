package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"telfs/internal/chunk"
	"telfs/internal/compress"
	"telfs/internal/config"
	"telfs/internal/crypto"
	"telfs/internal/meta"
	"telfs/internal/secret"
	"telfs/internal/snapshot"
	"telfs/internal/tg"
	"telfs/internal/wire"
)

// usesState is meta_kv[crypto2_uses]: the per-key encryption counts
// (nonce limit, spec P1 §1).
type usesState struct {
	Chunk   uint64 `json:"chunk"`
	Meta    uint64 `json:"meta"`
	Caption uint64 `json:"caption"`
}

func loadUses(ctx context.Context, m *meta.Store, c *wire.Counters) {
	b, err := m.GetKV(ctx, meta.KVUses)
	if err != nil {
		return
	}
	var u usesState
	if json.Unmarshal(b, &u) == nil {
		c.Chunk.Seed(u.Chunk)
		c.Meta.Seed(u.Meta)
		c.Caption.Seed(u.Caption)
	}
}

// persistUses writes the counts. Uncounted: like the stats blob, it
// must not make the snapshot change-detector see a dirty DB forever.
func persistUses(ctx context.Context, m *meta.Store, c *wire.Counters) {
	if c == nil {
		return
	}
	b, _ := json.Marshal(usesState{Chunk: c.Chunk.Load(), Meta: c.Meta.Load(), Caption: c.Caption.Load()})
	_ = m.PutKVUncounted(ctx, meta.KVUses, b)
}

// keyHeaderOf reads the local copy of a format-2 key header.
func keyHeaderOf(ctx context.Context, m *meta.Store) (wire.KeyHeader, []byte, error) {
	var h wire.KeyHeader
	js, err := m.GetKV(ctx, meta.KVKeyHeader)
	if err != nil {
		return h, nil, fmt.Errorf("format-2 filesystem without a local key header: %w", err)
	}
	if err := json.Unmarshal(js, &h); err != nil {
		return h, nil, fmt.Errorf("local key header: %w", err)
	}
	return h, js, nil
}

// unlock2 opens a format-2 filesystem's keys with the secret: the local
// key header → KEK (Argon2id) → DEK → working keys, with the persisted
// encryption counts seeded.
func unlock2(ctx context.Context, m *meta.Store, sec *secret.Secret) (*wire.Keys, *wire.Counters, error) {
	h, _, err := keyHeaderOf(ctx, m)
	if err != nil {
		return nil, nil, err
	}
	fmt.Fprintf(os.Stderr, "Unlocking the filesystem key (Argon2id, %d MiB)…\n", h.M/1024)
	dek, fsUUID, err := h.Unlock(sec.Bytes())
	if err != nil {
		return nil, nil, err
	}
	defer zero(dek)
	local, err := m.FSUUID(ctx)
	if err != nil {
		return nil, nil, err
	}
	if local != fsUUID {
		return nil, nil, fmt.Errorf("the key header belongs to filesystem %s, the local DB is %s", fsUUID, local)
	}
	counters := &wire.Counters{}
	loadUses(ctx, m, counters)
	warn := func(name string) func(uint64) {
		return func(n uint64) {
			fmt.Fprintf(os.Stderr, "WARNING: the %s key has been used %d times — half of AES-GCM's 2^32 limit; plan a move to a new filesystem\n", name, n)
		}
	}
	counters.Chunk.OnWarn, counters.Meta.OnWarn, counters.Caption.OnWarn = warn("chunk"), warn("meta"), warn("caption")
	keys, err := wire.DeriveKeys(dek, fsUUID, counters)
	if err != nil {
		return nil, nil, err
	}
	return keys, counters, nil
}

// pipeline is what a mount needs from the unlocked filesystem.
type pipeline struct {
	format   int
	wc       chunk.WireCodec
	cipher   crypto.Cipher // format 1 only
	codec    compress.Codec
	keys     *wire.Keys // format 2 only
	counters *wire.Counters
}

// openPipeline unlocks the filesystem in m with the profile's secret.
// Format 1 uses the secret as the passphrase of an encrypted FS (and
// ignores it on a plaintext one); format 2 always needs it.
func openPipeline(ctx context.Context, m *meta.Store, sec *secret.Secret) (*pipeline, error) {
	format, err := m.Format(ctx)
	if err != nil {
		return nil, err
	}
	codecName, err := m.CompressionCodec(ctx)
	if err != nil {
		return nil, err
	}
	codec, err := compress.New(codecName)
	if err != nil {
		return nil, err
	}
	p := &pipeline{format: format, codec: codec}
	switch format {
	case 1:
		cipher, err := loadCipher(ctx, m, sec)
		if err != nil {
			return nil, err
		}
		p.cipher = cipher
		p.wc = chunk.NewFormat1Codec(cipher, codec)
	case 2:
		keys, counters, err := unlock2(ctx, m, sec)
		if err != nil {
			return nil, err
		}
		wc, err := chunk.NewFormat2Codec(keys, codec, m)
		if err != nil {
			return nil, err
		}
		p.keys, p.counters, p.wc = keys, counters, wc
		m.SetJournalRefs(true)
	default:
		return nil, fmt.Errorf("filesystem format %d is newer than this telfs — upgrade", format)
	}
	return p, nil
}

// recover2 restores a format-2 filesystem from the channel: key message
// (or an export) → secret → newest snapshot → journal replay.
// found2=false means "no format-2 key header here" (try format 1).
func recover2(ctx context.Context, cfg *config.Config, sess *tg.Session, sec *secret.Secret, keyHeaderPath string) (found2 bool, err error) {
	var cands []snapshot.KeyFound
	var onChannel []snapshot.KeyFound
	onChannel, err = snapshot.FindKeyMessages(ctx, sess)
	if err != nil {
		return false, err
	}
	if keyHeaderPath != "" {
		text, rerr := os.ReadFile(keyHeaderPath)
		if rerr != nil {
			return true, fmt.Errorf("--key-header: %w", rerr)
		}
		h, _, perr := wire.ParseExport(string(text))
		if perr != nil {
			return true, perr
		}
		cands = []snapshot.KeyFound{{Header: h, JSON: h.JSON()}}
	} else {
		cands = onChannel
	}
	if len(cands) == 0 {
		return false, nil
	}
	k, dek, fsUUID, err := snapshot.UnlockAny(cands, sec.Bytes())
	if err != nil {
		return true, err
	}
	defer zero(dek)
	keys, err := wire.DeriveKeys(dek, fsUUID, nil)
	if err != nil {
		return true, err
	}
	fmt.Printf("Key header opened (filesystem %s). Looking for the newest snapshot…\n", fsUUID)
	snap, err := snapshot.FindLatestSnapshot2(ctx, sess, keys)
	if err != nil {
		return true, err
	}
	if snap == nil {
		return true, fmt.Errorf("the key header opens, but no snapshot of filesystem %s was found in the newest %d messages", fsUUID, snapshot.ScanPages*100)
	}
	fmt.Printf("Found snapshot msg=%d (journal seq %d, %s)\n", snap.MsgID, snap.Caption.Seq,
		time.Unix(snap.Caption.TS, 0).UTC().Format(time.RFC3339))
	gz, err := snapshot.DownloadSnapshot2(ctx, sess, keys, snap.MsgID)
	if err != nil {
		return true, err
	}
	if err := snapshot.Restore(ctx, gz, cfg.DBPath()); err != nil {
		return true, err
	}
	fail := func(e error) (bool, error) {
		_ = os.Remove(cfg.DBPath())
		_ = os.Remove(cfg.DBPath() + "-wal")
		_ = os.Remove(cfg.DBPath() + "-shm")
		return true, e
	}
	m, err := meta.Open(cfg.DBPath())
	if err != nil {
		return fail(err)
	}
	defer m.Close()
	if got, _ := m.FSUUID(ctx); got != fsUUID {
		return fail(fmt.Errorf("restored snapshot is filesystem %s, the key header says %s", got, fsUUID))
	}
	_ = snapshot.RecordSnapID(ctx, m, snap.MsgID)
	ops, err := snapshot.ListJournal2(ctx, sess, keys, snap.Caption.Seq)
	if err != nil {
		return fail(fmt.Errorf("journal: %w", err))
	}
	if len(ops) > 0 {
		fmt.Printf("Replaying %d journal op(s) since seq %d…\n", len(ops), snap.Caption.Seq)
	}
	for _, e := range ops {
		op, derr := meta.DecodeOp(e.Op)
		if derr != nil {
			return fail(fmt.Errorf("decode journal op seq=%d: %w", e.Seq, derr))
		}
		if rerr := m.ReplayOp(ctx, op); rerr != nil {
			return fail(fmt.Errorf("apply journal op seq=%d (%s): %w", e.Seq, op.Op, rerr))
		}
	}
	// The header that opened is the current one.
	if err := m.PutKV(ctx, meta.KVKeyHeader, k.JSON); err != nil {
		return fail(err)
	}
	switch {
	case k.MsgID > 0:
		_ = m.PutKV(ctx, meta.KVKeyMsgID, []byte(fmt.Sprint(k.MsgID)))
		if !k.Pinned {
			if perr := sess.PinMessage(ctx, k.MsgID); perr == nil {
				fmt.Printf("Re-pinned the key message (msg %d).\n", k.MsgID)
			}
		}
	case len(onChannel) == 0:
		// Recovered from an export and the channel has no key message:
		// put it back (spec §4.2).
		id, perr := snapshot.PostKeyMessage(ctx, sess, k.Header)
		if perr != nil {
			fmt.Fprintf(os.Stderr, "warning: could not re-post the key message: %v\n", perr)
		} else {
			_ = m.PutKV(ctx, meta.KVKeyMsgID, []byte(fmt.Sprint(id)))
			fmt.Printf("Re-posted and pinned the key message (msg %d).\n", id)
		}
	default:
		// The channel has a key message the export doesn't match (the
		// secret was changed after this export): do NOT post the old
		// header back — that would revive the old secret.
		fmt.Fprintln(os.Stderr, "warning: the channel's key message differs from this export (was the secret changed since?) — not re-posting the export")
	}
	return true, nil
}

// init2 creates a format-2 filesystem on the bound channel (shared with
// the web console: snapshot.Init2).
func init2(ctx context.Context, cfg *config.Config, sec *secret.Secret, sess *tg.Session, chunkSize int64, codecName string, params crypto.ArgonParams) error {
	fsUUID, err := snapshot.Init2(ctx, cfg.DBPath(), sec.Bytes(), sess, chunkSize, codecName, params)
	if err != nil {
		return err
	}
	fmt.Printf("Filesystem %s created (format 2, chunk size %s, compression %s).\n", fsUUID, humanBytes(chunkSize), orNone(codecName))
	return nil
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

// exportInfo builds the export context for a format-2 filesystem.
func exportInfo(ctx context.Context, cfg *config.Config, m *meta.Store) wire.ExportInfo {
	fs, _ := m.FSUUID(ctx)
	info := wire.ExportInfo{FSUUID: fs, ChannelID: cfg.Channel.ID, ChannelTitle: cfg.Channel.Title, Profile: cfg.ProfileName()}
	if v, err := m.GetKV(ctx, meta.KVCreated); err == nil {
		info.Created, _ = time.Parse(time.RFC3339, string(v))
	}
	if v, err := m.GetKV(ctx, meta.KVSecretSet); err == nil {
		info.SecretSet, _ = time.Parse(time.RFC3339, string(v))
	}
	return info
}

// keyCheck compares the channel's pinned message with the local key
// header (status / fsck / the daemon, spec §4.2).
type keyCheck struct {
	TS     int64  `json:"ts"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
}

func checkPinnedHeader(ctx context.Context, sess *tg.Session, m *meta.Store) keyCheck {
	kc := keyCheck{TS: time.Now().Unix()}
	_, local, err := keyHeaderOf(ctx, m)
	if err != nil {
		kc.Detail = err.Error()
		return kc
	}
	pinned, err := sess.PinnedMessageID(ctx)
	if err != nil {
		kc.Detail = "cannot read the pinned message: " + err.Error()
		return kc
	}
	if pinned == 0 {
		kc.Detail = "no message is pinned — the key message must be pinned (fsck --fix re-pins it)"
		return kc
	}
	text, err := sess.GetMessageText(ctx, pinned)
	if err != nil {
		kc.Detail = fmt.Sprintf("cannot read pinned message %d: %v", pinned, err)
		return kc
	}
	if !wire.IsKeyMessage(text) {
		kc.Detail = fmt.Sprintf("pinned message %d is not a key message (fsck --fix re-pins the key message)", pinned)
		return kc
	}
	_, js, err := wire.ParseKeyMessage(text)
	if err != nil {
		kc.Detail = fmt.Sprintf("pinned key message %d is damaged: %v", pinned, err)
		return kc
	}
	if !bytes.Equal(js, local) {
		kc.Detail = fmt.Sprintf("pinned key message %d differs from the local key header (stale after an interrupted secret change? fsck --fix replaces it)", pinned)
		return kc
	}
	kc.OK = true
	kc.Detail = fmt.Sprintf("pinned key message %d matches the local key header", pinned)
	return kc
}

func storeKeyCheck(ctx context.Context, m *meta.Store, kc keyCheck) {
	b, _ := json.Marshal(kc)
	_ = m.PutKVUncounted(ctx, meta.KVKeyCheck, b)
}

func loadKeyCheck(ctx context.Context, m *meta.Store) (keyCheck, bool) {
	var kc keyCheck
	b, err := m.GetKV(ctx, meta.KVKeyCheck)
	if err != nil || json.Unmarshal(b, &kc) != nil {
		return kc, false
	}
	return kc, true
}

// sortedSeqs is a helper for reports.
func sortedKeys(m map[int64]bool) []int64 {
	out := make([]int64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
