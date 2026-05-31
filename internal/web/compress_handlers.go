package web

import (
	"fmt"
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
	if refs, err := mstore.AllChunkMessageIDs(ctx); err == nil {
		d.ChunkCount = len(refs)
		d.HasChunks = len(refs) > 0
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
				if refs, err := mstore.AllChunkMessageIDs(r.Context()); err == nil && len(refs) > 0 {
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
	codec := r.FormValue("codec")
	if _, err := compress.New(codec); err != nil {
		SetFlash(w, "error", "codec: "+err.Error())
		http.Redirect(w, r, "/compression/init", http.StatusSeeOther)
		return
	}
	cfg, err := config.Load()
	if err != nil {
		SetFlash(w, "error", "config: "+err.Error())
		http.Redirect(w, r, "/compression/init", http.StatusSeeOther)
		return
	}
	mstore, err := meta.Open(cfg.DBPath())
	if err != nil {
		SetFlash(w, "error", "meta.Open: "+err.Error())
		http.Redirect(w, r, "/compression/init", http.StatusSeeOther)
		return
	}
	defer mstore.Close()
	ctx := r.Context()
	current, err := mstore.CompressionCodec(ctx)
	if err != nil {
		SetFlash(w, "error", "read current codec: "+err.Error())
		http.Redirect(w, r, "/compression/init", http.StatusSeeOther)
		return
	}
	if current == codec {
		SetFlash(w, "ok", fmt.Sprintf("compression already set to %q. No change.", codec))
		http.Redirect(w, r, "/compression", http.StatusSeeOther)
		return
	}
	refs, err := mstore.AllChunkMessageIDs(ctx)
	if err != nil {
		SetFlash(w, "error", "scan chunk_map: "+err.Error())
		http.Redirect(w, r, "/compression/init", http.StatusSeeOther)
		return
	}
	if len(refs) > 0 {
		SetFlash(w, "error", fmt.Sprintf("refuses to change codec from %q to %q while %d chunk(s) exist. Start a fresh TelFS with `cp -r` your data.", current, codec, len(refs)))
		http.Redirect(w, r, "/compression/init", http.StatusSeeOther)
		return
	}
	if err := mstore.SetCompressionCodec(ctx, codec); err != nil {
		SetFlash(w, "error", "persist codec: "+err.Error())
		http.Redirect(w, r, "/compression/init", http.StatusSeeOther)
		return
	}
	SetFlash(w, "ok", fmt.Sprintf("compression set to %q. Remount for the change to take effect.", codec))
	http.Redirect(w, r, "/compression", http.StatusSeeOther)
}
