package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"telfs/internal/config"
	"telfs/internal/crypto"
	"telfs/internal/meta"
	"telfs/internal/secret"
	"telfs/internal/sessionstore"
	"telfs/internal/snapshot"
	"telfs/internal/tg"
)

// The web console holds NO key between actions (decision D5 + Arash's
// point 2: "derive, act, drop" for every source). Each request that
// talks to Telegram reads the profile's secret — from its file or
// KeePassXC source, or for prompt profiles from the form field
// "secret" — opens session.enc with it, drops the secret, acts, and
// lets the derived key go with the request. (Go cannot zero the AES key
// schedule inside crypto/cipher; "drop" means no reference survives the
// request.) A login is one action spanning its phone/code/2FA pages: its
// session store lives in the login goroutine until the flow ends.

// errNeedSecret: a prompt profile's action arrived without the field.
var errNeedSecret = errors.New("this profile's secret is typed in (source = \"prompt\") — enter it in the form")

// secretIsPrompt reports whether the active profile types its secret.
func secretIsPrompt(cfg *config.Config) bool {
	return cfg != nil && cfg.Secret.EffectiveSource() == config.SecretSourcePrompt
}

// actionSecret reads the secret for one action. purpose SetNew (a
// profile's first login sets its secret) needs the field twice.
func actionSecret(r *http.Request, cfg *config.Config, purpose secret.Purpose) (*secret.Secret, error) {
	if !secretIsPrompt(cfg) {
		return secret.Read(r.Context(), cfg.Secret, secret.Request{NoPrompt: true, Stderr: os.Stderr})
	}
	v := r.PostFormValue("secret")
	if v == "" {
		return nil, errNeedSecret
	}
	if purpose == secret.SetNew && r.PostFormValue("secret2") != v {
		return nil, fmt.Errorf("%w: the two entries of the new secret do not match", secret.ErrSource)
	}
	return secret.New([]byte(v), "form"), nil
}

// actionClient derives the session key for one action and returns a
// Telegram client over session.enc. The secret is zeroed before return.
func actionClient(r *http.Request, cfg *config.Config, purpose secret.Purpose) (*tg.Client, error) {
	if err := cfg.RequireAPI(); err != nil {
		return nil, err
	}
	if _, err := os.Stat(cfg.SessionPath()); errors.Is(err, os.ErrNotExist) && cfg.HasLegacySession() {
		return nil, sessionstore.ErrLegacy
	}
	sec, err := actionSecret(r, cfg, purpose)
	if err != nil {
		return nil, err
	}
	defer sec.Zero()
	if purpose == secret.SetNew && len(sec.Bytes()) < secret.WeakLength {
		// The CLI prints the long warning; here a flash line suffices.
		fmt.Fprintf(os.Stderr, "[web] warning: the new secret for %s is only %d characters\n", cfg.ProfileName(), len(sec.Bytes()))
	}
	st, err := sessionstore.Open(cfg.SessionPath(), sec.Bytes(), crypto.DefaultArgonParamsV2())
	if err != nil {
		return nil, err
	}
	return tg.New(cfg, st)
}

// profilesSetSecret saves WHERE the named profile's secret comes from
// ([secret] in config.toml). It never sees the secret itself; a file
// source is checked against the secret-file rules right away.
func (s *Server) profilesSetSecret(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	name := r.PathValue("name")
	back := setupReturn(r, "/profiles/"+name)
	dir, err := config.ProfileDir(name)
	if err != nil {
		http.Error(w, "bad profile name", http.StatusBadRequest)
		return
	}
	cfg, err := config.LoadFromDir(dir)
	if err != nil {
		SetFlash(w, "error", err.Error())
		http.Redirect(w, r, back, http.StatusSeeOther)
		return
	}
	sc := config.SecretConfig{
		Source: r.FormValue("source"),
		File:   strings.TrimSpace(r.FormValue("file")),
		Entry:  strings.Trim(strings.TrimSpace(r.FormValue("entry")), "/"),
	}
	if sc.Source != config.SecretSourceFile {
		sc.File = ""
	}
	if sc.Source != config.SecretSourceKeePassXC {
		sc.Entry = ""
	}
	if err := sc.Validate(); err != nil {
		SetFlash(w, "error", err.Error())
		http.Redirect(w, r, back, http.StatusSeeOther)
		return
	}
	// A ${CREDENTIALS_DIRECTORY} path (systemd encrypted credentials) is
	// readable only inside the unit that loads it — the console cannot
	// test-read it; Validate above checked its form.
	path, credErr := sc.ResolvedFile()
	if sc.Source == config.SecretSourceFile && credErr == nil {
		if b, err := secret.ReadFile(path); err != nil {
			SetFlash(w, "error", err.Error())
			http.Redirect(w, r, back, http.StatusSeeOther)
			return
		} else {
			for i := range b {
				b[i] = 0
			}
		}
	}
	cfg.Secret = sc
	if err := cfg.Save(); err != nil {
		SetFlash(w, "error", "save: "+err.Error())
		http.Redirect(w, r, back, http.StatusSeeOther)
		return
	}
	msg := fmt.Sprintf("secret source for %s: %s", name, sc.EffectiveSource())
	if cfg.HasSession() {
		msg += " — this only says WHERE the secret comes from; it must still be the same secret (change the secret itself with `telfs secret change`)"
	}
	SetFlash(w, "ok", msg)
	http.Redirect(w, r, back, http.StatusSeeOther)
}

// setupInit creates a format-2 filesystem on the active profile's
// channel (the wizard's "Create the filesystem" step).
func (s *Server) setupInit(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	cfg, err := config.Load()
	if err != nil {
		SetFlash(w, "error", err.Error())
		http.Redirect(w, r, "/setup", http.StatusSeeOther)
		return
	}
	chunkSize := meta.DefaultChunkSize
	if v := r.FormValue("chunk_size"); v != "" {
		if n, perr := config.ParseHumanBytes(v); perr == nil {
			chunkSize = n
		}
	}
	codec := r.FormValue("codec")
	if codec != "zstd" {
		codec = "none"
	}
	sec, err := actionSecret(r, cfg, secret.Unlock)
	if err != nil {
		SetFlash(w, "error", err.Error())
		http.Redirect(w, r, "/setup", http.StatusSeeOther)
		return
	}
	defer sec.Zero()
	st, err := sessionstore.Open(cfg.SessionPath(), sec.Bytes(), crypto.DefaultArgonParamsV2())
	if err != nil {
		SetFlash(w, "error", err.Error())
		http.Redirect(w, r, "/setup", http.StatusSeeOther)
		return
	}
	client, err := tg.New(cfg, st)
	if err != nil {
		SetFlash(w, "error", err.Error())
		http.Redirect(w, r, "/setup", http.StatusSeeOther)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Minute)
	defer cancel()
	var fsUUID string
	err = client.RunSession(ctx, func(ctx context.Context, sess *tg.Session) error {
		var ierr error
		fsUUID, ierr = snapshot.Init2(ctx, cfg.DBPath(), sec.Bytes(), sess, chunkSize, codec, crypto.DefaultArgonParamsV2())
		return ierr
	})
	if err != nil {
		SetFlash(w, "error", err.Error())
		http.Redirect(w, r, "/setup", http.StatusSeeOther)
		return
	}
	SetFlash(w, "ok", fmt.Sprintf("filesystem %s created — print its key header export with `telfs key export` and keep it with the secret", fsUUID))
	http.Redirect(w, r, "/setup", http.StatusSeeOther)
}
