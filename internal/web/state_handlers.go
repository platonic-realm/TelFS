package web

import (
	"context"
	"encoding/json"
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

	// CacheLive holds the daemon's live cache numbers (size, cap,
	// hit ratio). Nil when the daemon's --metrics-addr endpoint is
	// unreachable, in which case the template falls back to showing
	// just the on-disk size from CacheSize / CacheN above.
	CacheLive *cacheLiveStats

	Stats meta.Stats
	KV    []meta.MetaKVEntry

	// Partial-file repair candidates from the read-only scan.
	Partials []meta.RepairReport

	// Warning when a fuse.telfs mount is live (repair while writes are
	// happening can collide). Best-effort: /proc/mounts can't attribute
	// mounts to specific profiles.
	MountActive bool
	Mounts      []stateMountRow
	External    []string

	// Repair is the snapshot of the current/last RepairPartialFiles
	// run, or nil if one has never been started this server session.
	// The template uses it to render the live progress card.
	Repair *repairRunSnapshot
}

// cacheLiveStats is the parsed shape of the daemon's /cache JSON.
// Template-friendly: percentages are pre-multiplied by 100 so the
// HTML can render "{{printf "%.1f"}}%" without further math.
type cacheLiveStats struct {
	SizeBytes int64
	CapBytes  int64
	UsedPct   float64 // 0..100
	Hits      int64
	Misses    int64
	HitPct    float64 // 0..100; meaningless when Hits+Misses == 0
}

// fetchCacheLive pulls /cache from the running daemon and parses it.
// Returns nil on any failure (network, JSON, non-200) — the caller
// treats nil as "fall back to on-disk stats from dirStats".
func fetchCacheLive() *cacheLiveStats {
	resp, err := httpClient.Get("http://127.0.0.1:9090/cache")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil
	}
	var raw struct {
		SizeBytes int64   `json:"size_bytes"`
		CapBytes  int64   `json:"cap_bytes"`
		UsedRatio float64 `json:"used_ratio"`
		Hits      int64   `json:"hits"`
		Misses    int64   `json:"misses"`
		HitRatio  float64 `json:"hit_ratio"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil
	}
	return &cacheLiveStats{
		SizeBytes: raw.SizeBytes,
		CapBytes:  raw.CapBytes,
		UsedPct:   raw.UsedRatio * 100,
		Hits:      raw.Hits,
		Misses:    raw.Misses,
		HitPct:    raw.HitRatio * 100,
	}
}

// stateMountRow is a thin view of one mount for the Stop shortcut
// on /state — covers both web-supervised and CLI-adopted procs so
// the user sees one unified list regardless of how the daemon was
// started.
type stateMountRow struct {
	Profile    string
	Mountpoint string
	Status     string
	Origin     string
}

func (s *Server) stateShow(w http.ResponseWriter, r *http.Request) {
	d := stateShowData{pageBase: s.basePage(w, r)}
	cfg, err := config.Load()
	if err != nil {
		SetFlash(w, "error", "config: "+err.Error())
		s.renderTemplateWith(w, "state/show.html", d, "templates/partials/cache.html")
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
	d.CacheLive = fetchCacheLive()
	d.MountActive = len(activeMounts()) > 0
	d.Repair = s.repair.Snapshot()
	for _, mp := range s.sup.List() {
		origin := "supervised"
		if mp.Adopted {
			origin = "adopted"
		}
		d.Mounts = append(d.Mounts, stateMountRow{
			Profile:    mp.Profile,
			Mountpoint: mp.Mountpoint,
			Status:     mp.Status(),
			Origin:     origin,
		})
	}
	d.External = s.sup.ExternalMounts()

	if _, err := os.Stat(cfg.DBPath()); err != nil {
		s.renderTemplateWith(w, "state/show.html", d, "templates/partials/cache.html")
		return
	}
	mstore, err := meta.Open(cfg.DBPath())
	if err != nil {
		SetFlash(w, "error", "meta.Open: "+err.Error())
		s.renderTemplateWith(w, "state/show.html", d, "templates/partials/cache.html")
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

	s.renderTemplateWith(w, "state/show.html", d, "templates/state/repair_progress.html", "templates/partials/cache.html")
}

// stateRepair kicks off a RepairPartialFiles run in the background
// and redirects to /state so the page can show live progress via
// HTMX poll. Refuses to start a new run if one is already in flight
// (the tracker is single-slot) or if a telfs mount is active and
// the user didn't explicitly tick the force checkbox.
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
	if !s.repair.Begin() {
		SetFlash(w, "error", "a repair run is already in flight — wait for it to finish")
		http.Redirect(w, r, "/state", http.StatusSeeOther)
		return
	}
	// Fire-and-forget goroutine. Uses a background ctx so the handler
	// returning doesn't cancel the work; the daemon-log is the only
	// safety-net if everything crashes mid-run, but partial repairs
	// are themselves idempotent.
	go func() {
		mstore, err := meta.Open(cfg.DBPath())
		if err != nil {
			s.repair.Finish(fmt.Errorf("meta.Open: %w", err))
			s.opts.Logger.Printf("repair: meta.Open: %v", err)
			return
		}
		defer mstore.Close()
		_, err = mstore.RepairPartialFilesProgress(context.Background(), s.repair.OnProgress)
		if err != nil {
			s.opts.Logger.Printf("repair: %v", err)
		}
		s.repair.Finish(err)
	}()
	http.Redirect(w, r, "/state", http.StatusSeeOther)
}

// stateRepairProgress returns the HTMX-pollable repair progress
// fragment. While the run is in flight it re-arms itself
// (hx-trigger="every 500ms") so the user sees a live counter; once
// finished the partial returns with no further trigger and the
// final summary stays visible.
func (s *Server) stateRepairProgress(w http.ResponseWriter, r *http.Request) {
	d := struct {
		Repair *repairRunSnapshot
	}{Repair: s.repair.Snapshot()}
	s.renderPartialBlock(w, "state/repair_progress.html", "repair_progress", d)
}
