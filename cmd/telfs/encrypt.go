package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"telfs/internal/config"
	"telfs/internal/crypto"
	"telfs/internal/meta"
	"telfs/internal/secret"
)

// cmdEncrypt dispatches `telfs encrypt {init,status,rotate}`.
func cmdEncrypt(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("encrypt: missing subcommand (init, status, rotate)")
	}
	switch args[0] {
	case "init":
		return errors.New("encrypt init: new filesystems are always encrypted (format 2) — create one with `telfs init`. " +
			"An existing format-1 filesystem cannot be encrypted in place; rsync its files into a new one")
	case "status":
		return cmdEncryptStatus(ctx)
	case "rotate":
		return errors.New("encrypt rotate: replaced by `telfs secret change` (it re-wraps the filesystem key AND re-encrypts the session — one secret per profile)")
	default:
		return fmt.Errorf("encrypt: unknown subcommand %q", args[0])
	}
}

// cmdEncryptStatus prints whether encryption is enabled on this FS.
func cmdEncryptStatus(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if _, err := os.Stat(cfg.DBPath()); err != nil {
		return fmt.Errorf("encrypt status: no local DB at %s", cfg.DBPath())
	}
	metaStore, err := meta.Open(cfg.DBPath())
	if err != nil {
		return err
	}
	defer metaStore.Close()

	if f, _ := metaStore.Format(ctx); f == 2 {
		fmt.Println("Encryption: always on (format 2) — chunks, captions, snapshots and journal.")
		if h, _, err := keyHeaderOf(ctx, metaStore); err == nil {
			fmt.Printf("KDF params:  Argon2id %d pass, %d MiB, %d lanes\n", h.T, h.M/1024, h.P)
		}
		fmt.Println("Secret:      `telfs secret change` changes it; `telfs key export` prints the key header export.")
		return nil
	}
	mode, err := metaStore.GetKV(ctx, crypto.KVMode)
	if errors.Is(err, meta.ErrNotFound) {
		fmt.Println("Encryption: disabled (format 1: chunks are uploaded as plaintext).")
		fmt.Println("This profile's secret protects only the Telegram session, not your files.")
		return nil
	}
	if err != nil {
		return err
	}
	fmt.Printf("Encryption: enabled (mode=%s)\n", string(mode))
	if argon, err := metaStore.GetKV(ctx, crypto.KVArgon); err == nil {
		fmt.Printf("KDF params:  %s\n", string(argon))
	}
	switch string(mode) {
	case crypto.ModeAESGCMv2, crypto.ModeAESGCMv3:
		fmt.Println("Rotation:    supported — `telfs secret change`.")
	case crypto.ModeAESGCMv1:
		fmt.Println("Rotation:    NOT supported on v1 — passphrase change requires re-encrypting all chunks.")
	}
	if string(mode) == crypto.ModeAESGCMv3 {
		fmt.Println("Dedup:       enabled (convergent) — identical chunks share one channel message.")
	}
	fmt.Println("The profile's secret is this filesystem's passphrase ([secret] in config.toml).")
	return nil
}

// loadCipher resolves which Cipher to use for a mount. Returns a
// NoopCipher when crypto_mode isn't set, an AESGCM cipher with a
// verified canary when it is, or an error if the user can't be
// authenticated (wrong passphrase, missing kv state, etc).
//
// Routes on mode:
//   - "" (no key)        → NoopCipher.
//   - "aes-gcm-v1"       → passphrase → Argon2id → key → cipher.
//   - "aes-gcm-v2"       → passphrase → KEK → unwrap DEK → cipher.
//
// The passphrase of an encrypted format-1 filesystem is the profile's
// secret (one secret per profile, decision D2): sec, already read from
// the profile's source.
func loadCipher(ctx context.Context, m *meta.Store, sec *secret.Secret) (crypto.Cipher, error) {
	mode, err := m.GetKV(ctx, crypto.KVMode)
	if errors.Is(err, meta.ErrNotFound) {
		return crypto.NoopCipher{}, nil
	}
	if err != nil {
		return nil, err
	}
	salt, err := m.GetKV(ctx, crypto.KVSalt)
	if err != nil {
		return nil, fmt.Errorf("loadCipher: salt missing: %w", err)
	}
	argonBytes, err := m.GetKV(ctx, crypto.KVArgon)
	if err != nil {
		return nil, fmt.Errorf("loadCipher: argon params missing: %w", err)
	}
	params, err := crypto.UnmarshalArgonParams(argonBytes)
	if err != nil {
		return nil, err
	}
	canary, err := m.GetKV(ctx, crypto.KVCanary)
	if err != nil {
		return nil, fmt.Errorf("loadCipher: canary missing: %w", err)
	}
	if sec == nil {
		return nil, errors.New("loadCipher: this filesystem is encrypted and needs the profile's secret")
	}
	derived := crypto.DeriveKey(sec.Bytes(), salt, params)
	defer zero(derived)

	switch string(mode) {
	case crypto.ModeAESGCMv1:
		cipher, err := crypto.NewAESGCM(derived)
		if err != nil {
			return nil, err
		}
		if err := crypto.VerifyCanary(cipher, canary); err != nil {
			return nil, err
		}
		return cipher, nil
	case crypto.ModeAESGCMv2, crypto.ModeAESGCMv3:
		wrappedDEK, err := m.GetKV(ctx, crypto.KVWrappedDEK)
		if err != nil {
			return nil, fmt.Errorf("loadCipher: wrapped_dek missing on %s FS: %w", string(mode), err)
		}
		dek, err := crypto.UnwrapDEK(derived, wrappedDEK)
		if err != nil {
			return nil, err
		}
		defer zero(dek)
		cipher, err := cipherForMode(string(mode), dek)
		if err != nil {
			return nil, err
		}
		if err := crypto.VerifyCanary(cipher, canary); err != nil {
			return nil, err
		}
		return cipher, nil
	default:
		return nil, fmt.Errorf("loadCipher: unsupported crypto_mode %q (this binary may be older than the FS — upgrade telfs)", string(mode))
	}
}

// zero clobbers a byte slice. Best-effort hygiene — Go's GC may have
// moved the buffer already, but it costs nothing to overwrite.
func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
