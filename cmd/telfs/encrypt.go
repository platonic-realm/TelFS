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

// cmdEncrypt dispatches `telfs encrypt {init,status}`.
func cmdEncrypt(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("encrypt: missing subcommand (init, status)")
	}
	switch args[0] {
	case "init":
		return cmdEncryptInit(ctx)
	case "status":
		return cmdEncryptStatus(ctx)
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
	key := crypto.DeriveKey(pass, salt, params)
	defer zero(key)
	cipher, err := crypto.NewAESGCM(key)
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
		{crypto.KVMode, []byte(crypto.ModeAESGCMv1)},
		{crypto.KVSalt, salt},
		{crypto.KVArgon, argonJSON},
		{crypto.KVCanary, canary},
	} {
		if err := metaStore.PutKV(ctx, kv.k, kv.v); err != nil {
			return fmt.Errorf("persist %s: %w", kv.k, err)
		}
	}
	fmt.Println("\nEncryption enabled (aes-gcm-v1).")
	fmt.Println("All future chunk uploads will be encrypted; channel observers see only ciphertext.")
	fmt.Println("Filenames, sizes, and directory structure are NOT encrypted — the snapshot blob")
	fmt.Println("in the channel still contains your metadata in the clear.")
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
	fmt.Println("Passphrase is required to mount. Set TELFS_PASSPHRASE to skip the prompt.")
	return nil
}

// loadCipher resolves which Cipher to use for a mount. Returns a
// NoopCipher when crypto_mode isn't set, an AESGCM cipher with a
// verified canary when it is, or an error if the user can't be
// authenticated (wrong passphrase, missing kv state, etc).
func loadCipher(ctx context.Context, m *meta.Store) (crypto.Cipher, error) {
	mode, err := m.GetKV(ctx, crypto.KVMode)
	if errors.Is(err, meta.ErrNotFound) {
		return crypto.NoopCipher{}, nil
	}
	if err != nil {
		return nil, err
	}
	if string(mode) != crypto.ModeAESGCMv1 {
		return nil, fmt.Errorf("loadCipher: unsupported crypto_mode %q", string(mode))
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
	key := crypto.DeriveKey(pass, salt, params)
	defer zero(key)
	cipher, err := crypto.NewAESGCM(key)
	if err != nil {
		return nil, err
	}
	if err := crypto.VerifyCanary(cipher, canary); err != nil {
		return nil, err
	}
	return cipher, nil
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
