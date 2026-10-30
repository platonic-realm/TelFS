package web

import (
	"net/http"
	"os"

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
	Format  int
}

func (s *Server) encryptShow(w http.ResponseWriter, r *http.Request) {
	d := encryptShowData{pageBase: s.basePage(w, r)}
	cfg, err := loadCfg(r)
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
	d.Format, _ = mstore.Format(ctx)
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
	if cfg, err := loadCfg(r); err == nil {
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
	// New filesystems are always encrypted (format 2); an existing
	// format-1 filesystem cannot be encrypted in place.
	SetFlash(w, "error", "new filesystems are always encrypted — create one in the setup wizard (or `telfs init`); an existing unencrypted filesystem cannot be encrypted in place: rsync its files into a new one")
	http.Redirect(w, r, "/setup", http.StatusSeeOther)
}

func zeroBytes(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
