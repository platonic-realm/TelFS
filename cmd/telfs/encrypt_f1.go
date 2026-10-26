package main

import (
	"context"
	"errors"
	"fmt"

	"telfs/internal/crypto"
	"telfs/internal/meta"
)

// cipherForMode returns the right Cipher implementation for the
// declared mode + a 32-byte DEK. Centralizes the mode -> cipher
// mapping so loadCipher / rewrap / snapshot recovery all agree.
func cipherForMode(mode string, dek []byte) (crypto.Cipher, error) {
	switch mode {
	case crypto.ModeAESGCMv1, crypto.ModeAESGCMv2:
		return crypto.NewAESGCM(dek)
	case crypto.ModeAESGCMv3:
		return crypto.NewAESGCMConvergent(dek)
	default:
		return nil, fmt.Errorf("cipherForMode: unsupported crypto_mode %q", mode)
	}
}

// rewrapFormat1 re-wraps a format-1 (aes-gcm-v2/v3) filesystem's DEK
// under a new passphrase without re-encrypting any chunk — what
// `telfs secret change` does on a format-1 profile (it was
// `telfs encrypt rotate`):
//
//  1. Derive the old KEK from the current passphrase, unwrap the DEK.
//  2. Derive a new KEK with a fresh salt, re-wrap the same DEK.
//  3. Persist the new salt + argon params + wrapped DEK + canary.
//
// Atomicity: each PutKV is its own statement; a crash mid-way can leave
// a hybrid — re-run the change. A plaintext format-1 FS has nothing to
// re-wrap (nil).
func rewrapFormat1(ctx context.Context, metaStore *meta.Store, oldPass, newPass []byte) error {
	mode, err := metaStore.GetKV(ctx, crypto.KVMode)
	if errors.Is(err, meta.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if string(mode) != crypto.ModeAESGCMv2 && string(mode) != crypto.ModeAESGCMv3 {
		return fmt.Errorf("secret change: an %q filesystem cannot change its passphrase (its key IS the passphrase) — move the data to a new filesystem", string(mode))
	}
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
	oldKEK := crypto.DeriveKey(oldPass, oldSalt, oldParams)
	defer zero(oldKEK)
	dek, err := crypto.UnwrapDEK(oldKEK, wrappedDEK)
	if err != nil {
		return fmt.Errorf("secret change: %w", err)
	}
	defer zero(dek)
	newSalt, err := crypto.NewSalt()
	if err != nil {
		return err
	}
	newParams := crypto.DefaultArgonParams()
	newKEK := crypto.DeriveKey(newPass, newSalt, newParams)
	defer zero(newKEK)
	newWrappedDEK, err := crypto.WrapDEK(newKEK, dek)
	if err != nil {
		return err
	}
	cipher, err := cipherForMode(string(mode), dek)
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
	fmt.Println("Existing chunks remain valid (the DEK didn't change). Older snapshots on the")
	fmt.Println("channel still decrypt with the OLD passphrase until they age out of retention.")
	return nil
}
