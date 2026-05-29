package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/term"

	"telfs/internal/config"
	"telfs/internal/crypto"
	"telfs/internal/meta"
)

// envPassphrase lets non-interactive runs (CI, tests, daemons) skip
// the stdin prompt. The value is read as-is; trailing whitespace is
// NOT trimmed (matches `echo -n`).
const envPassphrase = "TELFS_PASSPHRASE"

// cmdEncrypt dispatches `telfs encrypt {init,status,rotate}`.
func cmdEncrypt(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("encrypt: missing subcommand (init, status, rotate)")
	}
	switch args[0] {
	case "init":
		return cmdEncryptInit(ctx)
	case "status":
		return cmdEncryptStatus(ctx)
	case "rotate":
		return cmdEncryptRotate(ctx)
	default:
		return fmt.Errorf("encrypt: unknown subcommand %q", args[0])
	}
}

// cmdEncryptInit enables encryption on the local TelFS instance.
// Hard rule: refuses if any chunk already exists. This is a one-way
// configuration change.
func cmdEncryptInit(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	metaStore, err := meta.Open(cfg.DBPath())
	if err != nil {
		return err
	}
	defer metaStore.Close()

	if _, err := metaStore.GetKV(ctx, crypto.KVMode); err == nil {
		return errors.New("encrypt init: encryption is already enabled for this filesystem")
	}
	refs, err := metaStore.AllChunkMessageIDs(ctx)
	if err != nil {
		return err
	}
	if len(refs) > 0 {
		return fmt.Errorf("encrypt init: refuses to enable encryption when chunks already exist (%d chunks in chunk_map). "+
			"Start a fresh TelFS instance with a new channel and `cp -r` your data over to encrypt existing files.", len(refs))
	}

	pass, err := promptNewPassphrase()
	if err != nil {
		return err
	}
	defer zero(pass)

	salt, err := crypto.NewSalt()
	if err != nil {
		return err
	}
	params := crypto.DefaultArgonParams()
	fmt.Printf("Deriving key with Argon2id (time=%d, memory=%d MiB, threads=%d)…\n",
		params.Time, params.Memory/1024, params.Threads)
	kek := crypto.DeriveKey(pass, salt, params)
	defer zero(kek)

	// v2: generate a fresh DEK and wrap it under the KEK. Chunks +
	// snapshots use the DEK, not the KEK directly, so rotation can
	// re-wrap the DEK with a new KEK without touching any encrypted
	// data on the channel.
	dek, err := crypto.NewDEK()
	if err != nil {
		return err
	}
	defer zero(dek)
	wrappedDEK, err := crypto.WrapDEK(kek, dek)
	if err != nil {
		return err
	}
	cipher, err := crypto.NewAESGCM(dek)
	if err != nil {
		return err
	}
	canary, err := crypto.SealCanary(cipher)
	if err != nil {
		return err
	}
	argonJSON, err := crypto.MarshalArgonParams(params)
	if err != nil {
		return err
	}
	for _, kv := range []struct {
		k string
		v []byte
	}{
		{crypto.KVMode, []byte(crypto.ModeAESGCMv2)},
		{crypto.KVSalt, salt},
		{crypto.KVArgon, argonJSON},
		{crypto.KVCanary, canary},
		{crypto.KVWrappedDEK, wrappedDEK},
	} {
		if err := metaStore.PutKV(ctx, kv.k, kv.v); err != nil {
			return fmt.Errorf("persist %s: %w", kv.k, err)
		}
	}
	fmt.Println("\nEncryption enabled (aes-gcm-v2).")
	fmt.Println("Chunks + snapshots encrypted with a per-FS DEK; your passphrase wraps the DEK.")
	fmt.Println("Run `telfs encrypt rotate` to change the passphrase later without re-encrypting chunks.")
	return nil
}

// cmdEncryptRotate changes the passphrase without re-encrypting any
// chunks. Only available on v2 FSes (those initialized after v0.14):
//
//   1. Prompt for current passphrase, derive old KEK, unwrap DEK.
//   2. Prompt for new passphrase (+ confirmation), derive new KEK
//      with a fresh salt, re-wrap the same DEK.
//   3. Persist new salt + new argon (in case defaults changed) +
//      new wrapped DEK + new canary (encrypted under DEK — unchanged
//      semantics, but we re-seal so it's consistent with anyone who
//      later inspects).
//
// Atomicity: each PutKV is its own SQL statement. If we crash mid-
// rotation, the FS may end up with a hybrid of old-mode/new-mode
// keys. Since rotation is rare and the user is interactively driving
// it, we accept that risk — the recovery is "re-run rotate".
func cmdEncryptRotate(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	metaStore, err := meta.Open(cfg.DBPath())
	if err != nil {
		return err
	}
	defer metaStore.Close()

	mode, err := metaStore.GetKV(ctx, crypto.KVMode)
	if errors.Is(err, meta.ErrNotFound) {
		return errors.New("encrypt rotate: this FS is not encrypted")
	}
	if err != nil {
		return err
	}
	if string(mode) != crypto.ModeAESGCMv2 {
		return fmt.Errorf("encrypt rotate: only supported on aes-gcm-v2 filesystems "+
			"(this one is %q); migrate by `cp -r` to a fresh profile if rotation is needed", string(mode))
	}

	// Step 1: unwrap the DEK with the current passphrase.
	oldSalt, err := metaStore.GetKV(ctx, crypto.KVSalt)
	if err != nil {
		return fmt.Errorf("read salt: %w", err)
	}
	argonBytes, err := metaStore.GetKV(ctx, crypto.KVArgon)
	if err != nil {
		return fmt.Errorf("read argon: %w", err)
	}
	oldParams, err := crypto.UnmarshalArgonParams(argonBytes)
	if err != nil {
		return err
	}
	wrappedDEK, err := metaStore.GetKV(ctx, crypto.KVWrappedDEK)
	if err != nil {
		return fmt.Errorf("read wrapped DEK: %w", err)
	}
	oldPass, err := readPassphrase("Current passphrase: ")
	if err != nil {
		return err
	}
	defer zero(oldPass)
	oldKEK := crypto.DeriveKey(oldPass, oldSalt, oldParams)
	defer zero(oldKEK)
	dek, err := crypto.UnwrapDEK(oldKEK, wrappedDEK)
	if err != nil {
		return fmt.Errorf("encrypt rotate: %w", err)
	}
	defer zero(dek)

	// Step 2: derive the new KEK with a fresh salt.
	newPass, err := promptNewPassphrase()
	if err != nil {
		return err
	}
	defer zero(newPass)
	newSalt, err := crypto.NewSalt()
	if err != nil {
		return err
	}
	newParams := crypto.DefaultArgonParams()
	fmt.Printf("Re-wrapping DEK with new passphrase (Argon2id time=%d, memory=%d MiB, threads=%d)…\n",
		newParams.Time, newParams.Memory/1024, newParams.Threads)
	newKEK := crypto.DeriveKey(newPass, newSalt, newParams)
	defer zero(newKEK)
	newWrappedDEK, err := crypto.WrapDEK(newKEK, dek)
	if err != nil {
		return err
	}

	// Re-seal the canary too (same plaintext, fresh nonce — keeps the
	// stored state consistent with a fresh init).
	cipher, err := crypto.NewAESGCM(dek)
	if err != nil {
		return err
	}
	newCanary, err := crypto.SealCanary(cipher)
	if err != nil {
		return err
	}
	newArgonJSON, err := crypto.MarshalArgonParams(newParams)
	if err != nil {
		return err
	}

	// Step 3: persist. PutKV is upsert; updates the existing values.
	for _, kv := range []struct {
		k string
		v []byte
	}{
		{crypto.KVSalt, newSalt},
		{crypto.KVArgon, newArgonJSON},
		{crypto.KVCanary, newCanary},
		{crypto.KVWrappedDEK, newWrappedDEK},
	} {
		if err := metaStore.PutKV(ctx, kv.k, kv.v); err != nil {
			return fmt.Errorf("persist %s: %w", kv.k, err)
		}
	}
	fmt.Println("\nPassphrase rotated. Existing chunks remain valid (the DEK didn't change).")
	fmt.Println("Older snapshots on the channel still decrypt with the OLD passphrase — they'll")
	fmt.Println("age out of retention naturally. Use the new passphrase from now on.")
	return nil
}

// cmdEncryptStatus prints whether encryption is enabled on this FS.
func cmdEncryptStatus(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	metaStore, err := meta.Open(cfg.DBPath())
	if err != nil {
		return err
	}
	defer metaStore.Close()

	mode, err := metaStore.GetKV(ctx, crypto.KVMode)
	if errors.Is(err, meta.ErrNotFound) {
		fmt.Println("Encryption: disabled (chunks are uploaded as plaintext).")
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
	case crypto.ModeAESGCMv2:
		fmt.Println("Rotation:    supported — `telfs encrypt rotate`.")
	case crypto.ModeAESGCMv1:
		fmt.Println("Rotation:    NOT supported on v1 — passphrase change requires re-encrypting all chunks.")
	}
	fmt.Println("Passphrase is required to mount. Set TELFS_PASSPHRASE to skip the prompt.")
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
func loadCipher(ctx context.Context, m *meta.Store) (crypto.Cipher, error) {
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
	pass, err := readPassphrase("Passphrase: ")
	if err != nil {
		return nil, err
	}
	defer zero(pass)
	derived := crypto.DeriveKey(pass, salt, params)
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
	case crypto.ModeAESGCMv2:
		wrappedDEK, err := m.GetKV(ctx, crypto.KVWrappedDEK)
		if err != nil {
			return nil, fmt.Errorf("loadCipher: wrapped_dek missing on v2 FS: %w", err)
		}
		dek, err := crypto.UnwrapDEK(derived, wrappedDEK)
		if err != nil {
			return nil, err
		}
		defer zero(dek)
		cipher, err := crypto.NewAESGCM(dek)
		if err != nil {
			return nil, err
		}
		if err := crypto.VerifyCanary(cipher, canary); err != nil {
			return nil, err
		}
		return cipher, nil
	default:
		return nil, fmt.Errorf("loadCipher: unsupported crypto_mode %q", string(mode))
	}
}

// promptNewPassphrase prompts for a passphrase + confirmation, or
// reads TELFS_PASSPHRASE directly when set.
func promptNewPassphrase() ([]byte, error) {
	if env, ok := os.LookupEnv(envPassphrase); ok {
		return []byte(env), nil
	}
	fmt.Print("New passphrase: ")
	p1, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Println()
	if err != nil {
		return nil, fmt.Errorf("read passphrase: %w", err)
	}
	if len(p1) == 0 {
		return nil, errors.New("encrypt init: passphrase must not be empty")
	}
	fmt.Print("Confirm passphrase: ")
	p2, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Println()
	if err != nil {
		return nil, fmt.Errorf("read confirmation: %w", err)
	}
	if string(p1) != string(p2) {
		zero(p1)
		zero(p2)
		return nil, errors.New("encrypt init: passphrases do not match")
	}
	zero(p2)
	return p1, nil
}

// readPassphrase reads a single passphrase: from TELFS_PASSPHRASE if
// set, else interactively from the terminal (with no-echo if stdin
// is a tty, plain line read otherwise).
func readPassphrase(prompt string) ([]byte, error) {
	if env, ok := os.LookupEnv(envPassphrase); ok {
		return []byte(env), nil
	}
	fd := int(os.Stdin.Fd())
	if term.IsTerminal(fd) {
		fmt.Print(prompt)
		b, err := term.ReadPassword(fd)
		fmt.Println()
		if err != nil {
			return nil, fmt.Errorf("read passphrase: %w", err)
		}
		return b, nil
	}
	fmt.Print(prompt)
	r := bufio.NewReader(os.Stdin)
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("read passphrase: %w", err)
	}
	return []byte(strings.TrimRight(line, "\n")), nil
}

// zero clobbers a byte slice. Best-effort hygiene — Go's GC may have
// moved the buffer already, but it costs nothing to overwrite.
func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
