package web

import (
	"net/http"
	"os"

	"telfs/internal/compress"
	"telfs/internal/config"
	"telfs/internal/meta"
)

// ── /compression ──────────────────────────────────────────────────────

type compressShowData struct {
	pageBase
	HasMeta    bool
	Codec      string // "none" or "zstd"
	HasChunks  bool   // disables the change-codec form
	ChunkCount int
}

func (s *Server) compressShow(w http.ResponseWriter, r *http.Request) {
	d := compressShowData{pageBase: s.basePage(w, r)}
	cfg, err := config.Load()
	if err != nil {
		s.renderTemplate(w, "compress/show.html", d)
		return
	}
	if _, err := os.Stat(cfg.DBPath()); err != nil {
		s.renderTemplate(w, "compress/show.html", d)
		return
	}
	mstore, err := meta.Open(cfg.DBPath())
	if err != nil {
		s.renderTemplate(w, "compress/show.html", d)
		return
	}
	defer mstore.Close()
	d.HasMeta = true
	ctx := r.Context()
	if codec, err := mstore.CompressionCodec(ctx); err == nil {
		d.Codec = codec
	} else {
		d.Codec = compress.NameNone
	}
	if n, err := mstore.CountChunks(ctx); err == nil {
		d.ChunkCount = int(n)
		d.HasChunks = n > 0
	}
	s.renderTemplate(w, "compress/show.html", d)
}

type compressInitData struct {
	pageBase
	HasChunks    bool
	CurrentCodec string
}

func (s *Server) compressInitForm(w http.ResponseWriter, r *http.Request) {
	d := compressInitData{pageBase: s.basePage(w, r), CurrentCodec: compress.NameNone}
	if cfg, err := config.Load(); err == nil {
		if _, err := os.Stat(cfg.DBPath()); err == nil {
			if mstore, err := meta.Open(cfg.DBPath()); err == nil {
				if has, err := mstore.HasAnyChunks(r.Context()); err == nil && has {
					d.HasChunks = true
				}
				if codec, err := mstore.CompressionCodec(r.Context()); err == nil {
					d.CurrentCodec = codec
				}
				mstore.Close()
			}
		}
	}
	s.renderTemplate(w, "compress/init.html", d)
}

func (s *Server) compressInitSubmit(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	// The codec is chosen when the filesystem is CREATED (the wizard's
	// "Create the filesystem" step / `telfs init --compression`) and is
	// immutable afterwards. This handler used to write it into a DB it
	// could mint by opening a missing file — which foreclosed recovery.
	SetFlash(w, "error", "compression is chosen when the filesystem is created (setup wizard or `telfs init --compression zstd`) and cannot change afterwards")
	http.Redirect(w, r, "/setup", http.StatusSeeOther)
}
