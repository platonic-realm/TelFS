package web

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"telfs/internal/chunk"
	"telfs/internal/config"
	"telfs/internal/meta"
	"telfs/internal/snapshot"
)

// ── /state ────────────────────────────────────────────────────────────

type stateShowData struct {
	pageBase
	HasMeta      bool
	Profile      string
	DataDir      string
	DBPath       string
	DBSize       int64
	CachePath    string
	CacheSize    int64
	CacheN       int
	PendingDir   string
	PendingCount int
	PendingBytes int64

	Stats meta.Stats
	KV    []meta.MetaKVEntry

	// Partial-file repair candidates from the read-only scan.
	Partials []meta.RepairReport

	// Warning when a fuse.telfs mount is live (repair while writes are
	// happening can collide). Best-effort: /proc/mounts can't attribute
	// mounts to specific profiles.
	MountActive bool
}

func (s *Server) stateShow(w http.ResponseWriter, r *http.Request) {
	d := stateShowData{pageBase: s.basePage(w, r)}
	cfg, err := config.Load()
	if err != nil {
		SetFlash(w, "error", "config: "+err.Error())
		s.renderTemplate(w, "state/show.html", d)
		return
	}
	d.Profile = config.ActiveProfile()
	d.DataDir = cfg.DataDir
	d.DBPath = cfg.DBPath()
	d.CachePath = cfg.CachePath()
	d.PendingDir = filepath.Join(cfg.CachePath(), chunk.PendingDirName)

	if info, err := os.Stat(cfg.DBPath()); err == nil {
		d.DBSize = info.Size()
	}
	if total, n, err := dirStats(cfg.CachePath()); err == nil {
		d.CacheSize = total
		d.CacheN = n
	}
	if pendings, err := chunk.ScanPending(d.PendingDir); err == nil {
		d.PendingCount = len(pendings)
		for _, p := range pendings {
			if info, err := os.Stat(p.Path); err == nil {
				d.PendingBytes += info.Size()
			}
		}
	}
	d.MountActive = len(activeMounts()) > 0

	if _, err := os.Stat(cfg.DBPath()); err != nil {
		s.renderTemplate(w, "state/show.html", d)
		return
	}
	mstore, err := meta.Open(cfg.DBPath())
	if err != nil {
		SetFlash(w, "error", "meta.Open: "+err.Error())
		s.renderTemplate(w, "state/show.html", d)
		return
	}
	defer mstore.Close()
	d.HasMeta = true
	ctx := r.Context()
	if st, err := mstore.Stats(ctx); err == nil {
		d.Stats = st
	}
	if id, err := mstore.GetKV(ctx, snapshot.KVCurrentMsgID); err == nil {
		d.Stats.LastSnapshotMsgID = string(id)
	}
	if kv, err := mstore.MetaKVDump(ctx); err == nil {
		d.KV = kv
	}
	if parts, err := mstore.ScanPartialFiles(ctx); err == nil {
		d.Partials = parts
	}

	s.renderTemplate(w, "state/show.html", d)
}

// stateRepair runs RepairPartialFiles synchronously and flashes a
// summary. Refuses when a telfs mount is active to avoid racing with
// the daemon's writes (best-effort — /proc/mounts can't attribute
// mounts to specific profiles, so this is a coarse safety net).
func (s *Server) stateRepair(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	force := r.FormValue("force") == "1"
	if !force && len(activeMounts()) > 0 {
		SetFlash(w, "error", "refuses to repair while a fuse.telfs mount is active. Stop the mount first, or re-submit with the 'force anyway' checkbox.")
		http.Redirect(w, r, "/state", http.StatusSeeOther)
		return
	}
	cfg, err := config.Load()
	if err != nil {
		SetFlash(w, "error", "config: "+err.Error())
		http.Redirect(w, r, "/state", http.StatusSeeOther)
		return
	}
	mstore, err := meta.Open(cfg.DBPath())
	if err != nil {
		SetFlash(w, "error", "meta.Open: "+err.Error())
		http.Redirect(w, r, "/state", http.StatusSeeOther)
		return
	}
	defer mstore.Close()
	reports, err := mstore.RepairPartialFiles(r.Context())
	if err != nil {
		SetFlash(w, "error", "repair: "+err.Error())
		http.Redirect(w, r, "/state", http.StatusSeeOther)
		return
	}
	if len(reports) == 0 {
		SetFlash(w, "ok", "No partial files to repair.")
	} else {
		var saved int64
		for _, r := range reports {
			saved += r.OldSize - r.NewSize
		}
		SetFlash(w, "ok", fmt.Sprintf("Repaired %d file(s); truncated %s of unreachable tail bytes.", len(reports), humanBytesTpl(saved)))
	}
	http.Redirect(w, r, "/state", http.StatusSeeOther)
}
