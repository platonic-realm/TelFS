package web

import (
	"errors"
	"fmt"
	"net/http"
	"os"

	"telfs/internal/config"
	"telfs/internal/crypto"
	"telfs/internal/meta"
)

// ── /encrypt ──────────────────────────────────────────────────────

type encryptShowData struct {
	pageBase
	HasMeta bool
	Enabled bool
	Mode    string
	Argon   string
}

func (s *Server) encryptShow(w http.ResponseWriter, r *http.Request) {
	d := encryptShowData{pageBase: s.basePage(w, r)}
	cfg, err := config.Load()
	if err != nil {
		s.renderTemplate(w, "crypto/show.html", d)
		return
	}
	if _, err := os.Stat(cfg.DBPath()); err != nil {
		s.renderTemplate(w, "crypto/show.html", d)
		return
	}
	mstore, err := meta.Open(cfg.DBPath())
	if err != nil {
		s.renderTemplate(w, "crypto/show.html", d)
		return
	}
	defer mstore.Close()
	d.HasMeta = true
	ctx := r.Context()
	if mode, err := mstore.GetKV(ctx, crypto.KVMode); err == nil {
		d.Enabled = true
		d.Mode = string(mode)
	}
	if a, err := mstore.GetKV(ctx, crypto.KVArgon); err == nil {
		d.Argon = string(a)
	}
	s.renderTemplate(w, "crypto/show.html", d)
}

type encryptInitData struct {
	pageBase
	HasChunks bool
}

func (s *Server) encryptInitForm(w http.ResponseWriter, r *http.Request) {
	d := encryptInitData{pageBase: s.basePage(w, r)}
	if cfg, err := config.Load(); err == nil {
		if _, err := os.Stat(cfg.DBPath()); err == nil {
			if mstore, err := meta.Open(cfg.DBPath()); err == nil {
				if has, err := mstore.HasAnyChunks(r.Context()); err == nil && has {
					d.HasChunks = true
				}
				mstore.Close()
			}
		}
	}
	s.renderTemplate(w, "crypto/init.html", d)
}

func (s *Server) encryptInitSubmit(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	pass1 := r.FormValue("passphrase")
	pass2 := r.FormValue("passphrase_confirm")
	if pass1 == "" {
		SetFlash(w, "error", "passphrase is required")
		http.Redirect(w, r, "/encrypt/init", http.StatusSeeOther)
		return
	}
	if pass1 != pass2 {
		SetFlash(w, "error", "passphrases do not match")
		http.Redirect(w, r, "/encrypt/init", http.StatusSeeOther)
		return
	}
	cfg, err := config.Load()
	if err != nil {
		SetFlash(w, "error", "config: "+err.Error())
		http.Redirect(w, r, "/encrypt/init", http.StatusSeeOther)
		return
	}
	mstore, err := meta.Open(cfg.DBPath())
	if err != nil {
		SetFlash(w, "error", "meta.Open: "+err.Error())
		http.Redirect(w, r, "/encrypt/init", http.StatusSeeOther)
		return
	}
	defer mstore.Close()
	ctx := r.Context()
	if _, err := mstore.GetKV(ctx, crypto.KVMode); err == nil {
		SetFlash(w, "error", "encryption is already enabled")
		http.Redirect(w, r, "/encrypt", http.StatusSeeOther)
		return
	} else if !errors.Is(err, meta.ErrNotFound) {
		SetFlash(w, "error", "read crypto_mode: "+err.Error())
		http.Redirect(w, r, "/encrypt/init", http.StatusSeeOther)
		return
	}
	has, err := mstore.HasAnyChunks(ctx)
	if err != nil {
		SetFlash(w, "error", "scan chunk_map: "+err.Error())
		http.Redirect(w, r, "/encrypt/init", http.StatusSeeOther)
		return
	}
	if has {
		SetFlash(w, "error", "refuses to enable encryption when chunks exist. Start a fresh TelFS with `cp -r` your data.")
		http.Redirect(w, r, "/encrypt/init", http.StatusSeeOther)
		return
	}
	// Same guard the CLI applies: a non-empty dedup index from a prior
	// plaintext lifetime points at messages we'd now be encrypting —
	// leaving stale rows is unsafe.
	if n, err := mstore.CountChunkBlobs(ctx); err == nil && n > 0 {
		SetFlash(w, "error", fmt.Sprintf("chunk_blob index has %d entries from a prior plaintext lifetime; clear local state before enabling encryption", n))
		http.Redirect(w, r, "/encrypt/init", http.StatusSeeOther)
		return
	}
	// v2 scheme, matching `telfs encrypt init`: the passphrase derives
	// a KEK which wraps a random per-FS DEK. The DEK encrypts chunks;
	// rotation re-wraps the DEK without re-encrypting anything. The
	// web path used to write the legacy v1 layout (passphrase-derived
	// key used directly — no rotation possible, ever), silently giving
	// wizard users a worse filesystem than the CLI promised.
	salt, err := crypto.NewSalt()
	if err != nil {
		SetFlash(w, "error", "salt: "+err.Error())
		http.Redirect(w, r, "/encrypt/init", http.StatusSeeOther)
		return
	}
	params := crypto.DefaultArgonParams()
	kek := crypto.DeriveKey([]byte(pass1), salt, params)
	defer zeroBytes(kek)
	dek, err := crypto.NewDEK()
	if err != nil {
		SetFlash(w, "error", "dek: "+err.Error())
		http.Redirect(w, r, "/encrypt/init", http.StatusSeeOther)
		return
	}
	defer zeroBytes(dek)
	wrappedDEK, err := crypto.WrapDEK(kek, dek)
	if err != nil {
		SetFlash(w, "error", "wrap dek: "+err.Error())
		http.Redirect(w, r, "/encrypt/init", http.StatusSeeOther)
		return
	}
	cipher, err := crypto.NewAESGCM(dek)
	if err != nil {
		SetFlash(w, "error", "cipher: "+err.Error())
		http.Redirect(w, r, "/encrypt/init", http.StatusSeeOther)
		return
	}
	canary, err := crypto.SealCanary(cipher)
	if err != nil {
		SetFlash(w, "error", "canary: "+err.Error())
		http.Redirect(w, r, "/encrypt/init", http.StatusSeeOther)
		return
	}
	argonJSON, err := crypto.MarshalArgonParams(params)
	if err != nil {
		SetFlash(w, "error", "argon json: "+err.Error())
		http.Redirect(w, r, "/encrypt/init", http.StatusSeeOther)
		return
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
		if err := mstore.PutKV(ctx, kv.k, kv.v); err != nil {
			SetFlash(w, "error", "persist "+kv.k+": "+err.Error())
			http.Redirect(w, r, "/encrypt/init", http.StatusSeeOther)
			return
		}
	}
	SetFlash(w, "ok", "encryption enabled (aes-gcm-v2). Save your passphrase — there's no recovery for lost passphrases.")
	http.Redirect(w, r, "/encrypt", http.StatusSeeOther)
}

func zeroBytes(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
