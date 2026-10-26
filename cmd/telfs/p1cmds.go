package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"telfs/internal/config"
	"telfs/internal/crypto"
	"telfs/internal/meta"
	"telfs/internal/mountctl"
	"telfs/internal/secret"
	"telfs/internal/sessionstore"
	"telfs/internal/snapshot"
	"telfs/internal/tg"
	"telfs/internal/wire"
)

// ── telfs secret {test, change} ───────────────────────────────────

func cmdSecret(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("secret: missing subcommand (test, change)")
	}
	switch args[0] {
	case "test":
		return cmdSecretTest(ctx)
	case "change":
		return cmdSecretChange(ctx, args[1:])
	default:
		return fmt.Errorf("secret: unknown subcommand %q (test, change)", args[0])
	}
}

// cmdSecretTest reads the profile's secret and checks that it opens
// session.enc and (when there is a local DB) the filesystem key. Exit
// codes: 0 ok, 2 the source failed, 3 the secret is wrong.
func cmdSecretTest(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	sec, err := readSecret(ctx, cfg, secret.Unlock)
	if err != nil {
		return err
	}
	defer sec.Zero()
	fmt.Printf("source:   %s (%d characters)\n", sec.Source, len([]rune(string(sec.Bytes()))))
	if cfg.HasSession() {
		if _, err := openSessionStore(cfg, sec); err != nil {
			return fmt.Errorf("session: %w", err)
		}
		fmt.Println("session:  opens")
	} else {
		fmt.Println("session:  none yet (log in first)")
	}
	if _, err := os.Stat(cfg.DBPath()); err == nil {
		m, err := meta.Open(cfg.DBPath())
		if err != nil {
			return err
		}
		defer m.Close()
		p, err := openPipeline(ctx, m, sec)
		if err != nil {
			return fmt.Errorf("filesystem key: %w", err)
		}
		switch {
		case p.format == 2:
			fmt.Println("fs key:   opens (format 2)")
		case p.cipher == nil || isNoop(p.cipher):
			fmt.Println("fs key:   none — format-1 UNENCRYPTED filesystem (the secret protects only the session)")
		default:
			fmt.Println("fs key:   opens (format 1, encrypted)")
		}
	}
	fmt.Println("ok")
	return nil
}

// cmdSecretChange re-wraps the filesystem key and re-encrypts
// session.enc under a new secret, replaces the key message, and prints
// a new export. Nothing is re-encrypted: data-key rotation is out of
// scope (spec P1 §1) — an OLD secret together with any OLD key header
// copy still opens the filesystem.
func cmdSecretChange(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("secret change", flag.ContinueOnError)
	newPrompt := fs.Bool("new-from-prompt", false, "type the new secret (asked twice)")
	newFile := fs.String("new-file", "", "read the new secret from this file (the rules of a secret file apply); [secret] then points at it")
	newEntry := fs.String("new-entry", "", "read the new secret from this KeePassXC entry; [secret] then points at it")
	if err := fs.Parse(args); err != nil {
		return err
	}
	n := 0
	for _, b := range []bool{*newPrompt, *newFile != "", *newEntry != ""} {
		if b {
			n++
		}
	}
	if n != 1 {
		return errors.New("secret change: give exactly one of --new-from-prompt, --new-file PATH, --new-entry ENTRY")
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if !cfg.HasSession() {
		return errors.New("secret change: this profile has no session yet — the first login sets the secret")
	}
	if _, mp, ok, ferr := mountctl.FindByProfile(cfg.ProfileName()); ferr != nil {
		return ferr
	} else if ok {
		return fmt.Errorf("secret change: profile %s is mounted at %s — unmount first", cfg.ProfileName(), mp)
	}

	// Current secret: proves it opens everything before anything changes.
	cur, err := readSecret(ctx, cfg, secret.Unlock)
	if err != nil {
		return err
	}
	defer cur.Zero()
	store, err := openSessionStore(cfg, cur)
	if err != nil {
		return fmt.Errorf("current secret: %w", err)
	}
	sessionBytes, err := store.LoadSession(ctx)
	if err != nil {
		return fmt.Errorf("read session: %w", err)
	}
	var m *meta.Store
	format := 0
	if _, err := os.Stat(cfg.DBPath()); err == nil {
		if m, err = meta.Open(cfg.DBPath()); err != nil {
			return err
		}
		defer m.Close()
		if format, err = m.Format(ctx); err != nil {
			return err
		}
	}

	// New secret.
	newConf := cfg.Secret
	req := secret.Request{Purpose: secret.SetNew, Prompt: "New secret: "}
	switch {
	case *newPrompt:
		newConf = config.SecretConfig{Source: config.SecretSourcePrompt}
	case *newFile != "":
		newConf = config.SecretConfig{Source: config.SecretSourceFile, File: *newFile}
	default:
		newConf = config.SecretConfig{Source: config.SecretSourceKeePassXC, Entry: *newEntry}
	}
	next, err := secret.Read(ctx, newConf, req)
	if err != nil {
		return err
	}
	defer next.Zero()
	if string(next.Bytes()) == string(cur.Bytes()) {
		return errors.New("secret change: the new secret is the same as the current one")
	}
	secret.WarnIfWeak(os.Stderr, next)

	client, err := newClientWith(cfg, cur)
	if err != nil {
		return err
	}
	encNew := cfg.SessionPath() + ".new"
	writeNewSession := func() error {
		os.Remove(encNew)
		st, err := sessionstore.Open(encNew, next.Bytes(), crypto.DefaultArgonParamsV2())
		if err != nil {
			return err
		}
		return st.StoreSession(ctx, sessionBytes)
	}

	switch {
	case format == 2:
		err = client.RunSession(ctx, func(ctx context.Context, sess *tg.Session) error {
			oldH, _, err := keyHeaderOf(ctx, m)
			if err != nil {
				return err
			}
			dek, fsUUID, err := oldH.Unlock(cur.Bytes())
			if err != nil {
				return fmt.Errorf("current secret does not open the filesystem key: %w", err)
			}
			defer zero(dek)
			fmt.Println("Wrapping the filesystem key under the new secret…")
			newH, err := wire.NewKeyHeader(next.Bytes(), dek, fsUUID, oldH.Params())
			if err != nil {
				return err
			}
			// Crash-safe order (spec §9). Every step leaves a state the
			// next start can finish: the session opener picks up a
			// leftover session.enc.new, and old key-message ids sit in
			// meta_kv until deleted.
			found, _ := snapshot.FindKeyMessages(ctx, sess)
			var old []string
			for _, k := range found {
				old = append(old, strconv.Itoa(k.MsgID))
			}
			if v, err := m.GetKV(ctx, meta.KVKeyMsgID); err == nil && !contains(old, string(v)) {
				old = append(old, string(v))
			}
			newID, err := sess.PostMessage(ctx, newH.Message())
			if err != nil {
				return fmt.Errorf("post new key message: %w", err)
			}
			if err := writeNewSession(); err != nil {
				return fmt.Errorf("write new session: %w", err)
			}
			now := time.Now().UTC().Format(time.RFC3339)
			for k, v := range map[string][]byte{
				meta.KVKeyMsgOld: []byte(strings.Join(old, ",")),
				meta.KVKeyHeader: newH.JSON(),
				meta.KVKeyMsgID:  []byte(strconv.Itoa(newID)),
				meta.KVSecretSet: []byte(now),
			} {
				if err := m.PutKV(ctx, k, v); err != nil {
					return err
				}
			}
			if err := sess.PinMessage(ctx, newID); err != nil {
				return err
			}
			if err := os.Rename(encNew, cfg.SessionPath()); err != nil {
				return err
			}
			deleteOldKeyMessages(ctx, sess, m)
			// A snapshot carries the new header to the channel too.
			keys, err := wire.DeriveKeys(dek, fsUUID, nil)
			if err == nil {
				_ = (&snapshot.Manager{Meta: m, Session: sess, Keys: keys}).Once(ctx)
			}
			fmt.Printf("Secret changed. New key message %d is pinned; the old one(s) are deleted.\n", newID)
			fmt.Println("Destroy every OLDER export: with the old secret it still opens this filesystem")
			fmt.Println("(the data key itself does not change — README, \"Changing the secret\").")
			fmt.Println()
			fmt.Print(wire.Export(newH, exportInfo(ctx, cfg, m)))
			return nil
		})
	case format == 1:
		cipher, cerr := loadCipher(ctx, m, cur)
		if cerr != nil {
			return cerr
		}
		if !isNoop(cipher) {
			if err := rewrapFormat1(ctx, m, cur.Bytes(), next.Bytes()); err != nil {
				return err
			}
		}
		if err = writeNewSession(); err == nil {
			err = os.Rename(encNew, cfg.SessionPath())
		}
		if err == nil {
			if isNoop(cipher) {
				fmt.Println("Session re-encrypted under the new secret (format-1 UNENCRYPTED filesystem: the secret protects only the session).")
			} else {
				fmt.Println("Format-1 filesystem key re-wrapped and session re-encrypted under the new secret.")
			}
		}
	default:
		if err = writeNewSession(); err == nil {
			err = os.Rename(encNew, cfg.SessionPath())
		}
		if err == nil {
			fmt.Println("Session re-encrypted under the new secret.")
		}
	}
	if err != nil {
		return err
	}
	if newConf.Source != config.SecretSourcePrompt || cfg.Secret.EffectiveSource() != config.SecretSourcePrompt {
		cfg.Secret = newConf
		if err := cfg.Save(); err != nil {
			return fmt.Errorf("secret changed, but saving [secret] failed (fix config.toml by hand): %w", err)
		}
		fmt.Printf("[secret] now: source = %q", newConf.EffectiveSource())
		if newConf.File != "" {
			fmt.Printf(", file = %q", newConf.File)
		}
		if newConf.Entry != "" {
			fmt.Printf(", entry = %q", newConf.Entry)
		}
		fmt.Println()
	}
	return nil
}

// deleteOldKeyMessages deletes the key messages recorded in
// KVKeyMsgOld (left by `secret change`), then clears the record.
func deleteOldKeyMessages(ctx context.Context, sess *tg.Session, m *meta.Store) {
	v, err := m.GetKV(ctx, meta.KVKeyMsgOld)
	if err != nil || len(v) == 0 {
		return
	}
	cur, _ := m.GetKV(ctx, meta.KVKeyMsgID)
	var ids []int
	for _, f := range strings.Split(string(v), ",") {
		if f == "" || f == string(cur) {
			continue
		}
		if id, err := strconv.Atoi(f); err == nil && id > 0 {
			ids = append(ids, id)
		}
	}
	if len(ids) > 0 {
		if err := sess.DeleteChannelMessages(ctx, ids...); err != nil {
			fmt.Fprintf(os.Stderr, "warning: could not delete old key message(s) %v yet: %v (retried at the next mount)\n", ids, err)
			return
		}
	}
	_ = m.PutKV(ctx, meta.KVKeyMsgOld, nil)
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// ── telfs session encrypt ─────────────────────────────────────────

func cmdSession(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "encrypt" {
		return errors.New("session: usage: telfs session encrypt")
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if !cfg.HasLegacySession() {
		return errors.New("session encrypt: no plaintext session.json in this profile — nothing to convert")
	}
	fmt.Println("Converting session.json (a full login to the Telegram account, in the clear) to session.enc.")
	fmt.Println("If this profile's filesystem is encrypted, use ITS passphrase: one secret per profile.")
	sec, err := readSecret(ctx, cfg, secret.SetNew)
	if err != nil {
		return err
	}
	defer sec.Zero()
	secret.WarnIfWeak(os.Stderr, sec)
	if err := sessionstore.Convert(cfg.LegacySessionPath(), cfg.SessionPath(), sec.Bytes(), crypto.DefaultArgonParamsV2()); err != nil {
		return err
	}
	fmt.Println("Done: session.enc written and session.json removed.")
	fmt.Println("Copies of the old session.json in backups or snapshots remain a valid login: end that")
	fmt.Println("session in Telegram (Settings → Devices) and log in again if you can't vouch for them.")
	return nil
}

// ── telfs key export ──────────────────────────────────────────────

func cmdKey(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "export" {
		return errors.New("key: usage: telfs key export [--out FILE]")
	}
	fs := flag.NewFlagSet("key export", flag.ContinueOnError)
	out := fs.String("out", "", "write the export to this file (mode 0600) instead of stdout")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if _, err := os.Stat(cfg.DBPath()); err != nil {
		return fmt.Errorf("key export: no local DB at %s", cfg.DBPath())
	}
	m, err := meta.Open(cfg.DBPath())
	if err != nil {
		return err
	}
	defer m.Close()
	if f, _ := m.Format(ctx); f != 2 {
		return errors.New("key export: only format-2 filesystems have a key header (a format-1 encrypted filesystem keeps its key state in every snapshot)")
	}
	h, _, err := keyHeaderOf(ctx, m)
	if err != nil {
		return err
	}
	text := wire.Export(h, exportInfo(ctx, cfg, m))
	if *out == "" {
		fmt.Print(text)
		return nil
	}
	if err := sessionstore.WriteAtomic(*out, []byte(text), 0o600); err != nil {
		return err
	}
	fmt.Printf("Key header export written to %s (0600).\n", *out)
	return nil
}
