package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"telfs/internal/config"
	"telfs/internal/control"
	"telfs/internal/crypto"
	"telfs/internal/snapshot"
	"telfs/internal/wire"
)

// Per-profile monitoring: the dashboard's profile, channel and filesystem
// tables and the masthead lamps. Several profiles can run at once (on S9
// the backup and cloud daemons), so each profile is read on its own — from
// its daemon's control socket when one runs, from its local state
// otherwise. Nothing here depends on the selected (active) profile: the
// selection only decides what the Setup, Browse and State pages act on.

// liveStatus is the part of a daemon's GET /status (schema 1, spec v1 §3)
// the dashboard shows.
type liveStatus struct {
	ReadOnly   *bool   `json:"read_only"`
	Mountpoint *string `json:"mountpoint"`
	PID        *int    `json:"pid"`
	Sync       struct {
		PendingUploads  int    `json:"pending_uploads"`
		PendingBytes    int64  `json:"pending_bytes"`
		JournalUnposted int64  `json:"journal_unposted"`
		SnapshotDue     string `json:"snapshot_due"`
	} `json:"sync"`
	Upload *struct {
		Connections      int   `json:"connections"`
		RateCapBytesPerS int64 `json:"rate_cap_bytes_per_s"`
		RateBytesPerS    int64 `json:"rate_bytes_per_s"`
	} `json:"upload"`
	Staging struct {
		PendingBytes int64 `json:"pending_bytes"`
		CapBytes     int64 `json:"cap_bytes"`
		Blocked      bool  `json:"blocked"`
	} `json:"staging"`
	LastError *struct {
		Text          string `json:"text"`
		Unix          int64  `json:"unix"`
		RecoveredUnix *int64 `json:"recovered_unix"`
	} `json:"last_error"`
}

// CurrentError is the daemon's last error while the link has not
// recovered from it (nil once an RPC succeeded after it — a past error is
// no current trouble).
func (l *liveStatus) CurrentError() *struct {
	Text          string `json:"text"`
	Unix          int64  `json:"unix"`
	RecoveredUnix *int64 `json:"recovered_unix"`
} {
	if l == nil || l.LastError == nil || l.LastError.RecoveredUnix != nil {
		return nil
	}
	return l.LastError
}

// StagingPct is the staged share of the staging cap, 0 without a cap.
func (l *liveStatus) StagingPct() float64 {
	if l == nil || l.Staging.CapBytes <= 0 {
		return 0
	}
	return 100 * float64(l.Staging.PendingBytes) / float64(l.Staging.CapBytes)
}

// fsView is a profile's filesystem as its local DB describes it.
type fsView struct {
	UUID          string
	Format        int
	ChunkSize     int64
	Codec         string
	Encryption    string
	SnapMsg       int64
	SnapUnix      int64
	Versions      int
	NewestVersion string
	DBBytes       int64
	CacheBytes    int64
	CacheFiles    int
	CacheKnown    bool
}

// ShortUUID is the fs_uuid's first block — enough to tell filesystems
// apart in a table; the full one is in the cell's title.
func (f fsView) ShortUUID() string {
	if i := strings.IndexByte(f.UUID, '-'); i > 0 {
		return f.UUID[:i]
	}
	return f.UUID
}

// profileMonitor is one profile's row.
type profileMonitor struct {
	Name     string
	Selected bool
	Identity string
	Channel  channelView
	Err      string // the profile's config could not be read
	HasDB    bool
	FS       fsView
	Online   bool        // the profile's daemon answers on its control socket
	Live     *liveStatus // its /status (monLive and up); nil otherwise

	LinkKind, LinkLabel, LinkTip string
	SyncKind, SyncLabel, SyncTip string
}

// Mountpoint is where the profile's daemon mounted it ("" if unknown).
func (p profileMonitor) Mountpoint() string {
	if p.Live == nil || p.Live.Mountpoint == nil {
		return ""
	}
	return *p.Live.Mountpoint
}

// ReadOnly reports a --readonly mount.
func (p profileMonitor) ReadOnly() bool {
	return p.Live != nil && p.Live.ReadOnly != nil && *p.Live.ReadOnly
}

// monLevel is how much monitorProfiles reads: the polls stay cheap.
type monLevel int

const (
	monLamps monLevel = iota // masthead (every 5 s): the two lamps
	monLive                  // dashboard table (every 5 s): + daemon /status
	monFull                  // page load: + the DB's filesystem facts + cache walk
)

// monitorProfiles reads every profile, concurrently (one wedged daemon
// costs its 2 s timeout once, not once per profile).
func monitorProfiles(ctx context.Context, level monLevel) []profileMonitor {
	names := listProfileNames()
	selected := config.ActiveProfile()
	out := make([]profileMonitor, len(names))
	var wg sync.WaitGroup
	for i, name := range names {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out[i] = monitorProfile(ctx, name, name == selected, level)
		}()
	}
	wg.Wait()
	return out
}

func monitorProfile(ctx context.Context, name string, selected bool, level monLevel) profileMonitor {
	pm := profileMonitor{Name: name, Selected: selected}
	dir, err := config.ProfileDir(name)
	if err != nil {
		pm.Err = err.Error()
		return pm
	}
	cfg, err := config.LoadFromDir(dir)
	if err != nil {
		pm.Err = err.Error()
		return pm
	}
	pm.Identity = cfg.Identity.Label()
	if cfg.Channel.ID != 0 {
		pm.Channel = channelView{Set: true, ID: cfg.Channel.ID, Title: cfg.Channel.Title}
	}
	if level == monFull {
		pm.FS, pm.HasDB = readFSView(ctx, cfg)
	} else {
		_, err := os.Stat(cfg.DBPath())
		pm.HasDB = err == nil
	}

	// The lamps follow the masthead's rules (classifyHealth/classifySync).
	tg := tgPageData{NowUnix: time.Now().Unix(), Source: controlSource}
	populateTGState(&tg, cfg, controlSource)
	if !tg.Online {
		loadOfflineSync(ctx, cfg, &tg)
	}
	classifySync(&tg)
	pm.LinkKind, pm.LinkLabel, pm.LinkTip = classifyHealth(tg)
	if !tg.Online && !pm.HasDB {
		// Nothing to watch yet: no filesystem and no daemon.
		pm.LinkKind, pm.LinkLabel, pm.LinkTip = "muted", "no fs", "This profile has no local filesystem yet (Setup → Guided wizard)."
	}
	pm.SyncKind, pm.SyncLabel, pm.SyncTip = tg.SyncKind, tg.SyncLabel, tg.SyncTip
	pm.Online = tg.Online
	if tg.Online && level >= monLive {
		pm.Live = fetchLiveStatus(cfg)
	}
	return pm
}

// fetchLiveStatus reads the daemon's /status; nil if it doesn't answer.
func fetchLiveStatus(cfg *config.Config) *liveStatus {
	resp, err := control.Client(control.Path(cfg.DataDir), 2*time.Second).Get(control.BaseURL + "/status")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	var st liveStatus
	if json.NewDecoder(resp.Body).Decode(&st) != nil {
		return nil
	}
	return &st
}

// readFSView reads the profile's DB read-only and sizes its cache; false
// when it has no DB.
func readFSView(ctx context.Context, cfg *config.Config) (fsView, bool) {
	var v fsView
	fi, err := os.Stat(cfg.DBPath())
	if err != nil {
		return v, false
	}
	v.DBBytes = fi.Size()
	if total, n, err := dirStats(cfg.CachePath()); err == nil {
		v.CacheBytes, v.CacheFiles, v.CacheKnown = total, n, true
	}
	m, err := openMetaRead(cfg)
	if err != nil {
		return v, true
	}
	defer m.Close()
	v.UUID, _ = m.FSUUID(ctx)
	v.Format, _ = m.Format(ctx)
	v.ChunkSize, _ = m.ChunkSize(ctx)
	v.Codec, _ = m.CompressionCodec(ctx)
	switch {
	case v.Format >= 2:
		v.Encryption = "AES-256-GCM"
	default:
		if mode, err := m.GetKV(ctx, crypto.KVMode); err == nil {
			v.Encryption = string(mode)
		} else {
			v.Encryption = "plaintext"
		}
	}
	kv, err := m.GetKVs(ctx, snapshot.KVCurrentMsgID, snapshot.KVLastSnapUnix, snapshot.KVVersions)
	if err != nil {
		return v, true
	}
	v.SnapMsg, _ = strconv.ParseInt(strings.TrimSpace(string(kv[snapshot.KVCurrentMsgID])), 10, 64)
	v.SnapUnix, _ = strconv.ParseInt(strings.TrimSpace(string(kv[snapshot.KVLastSnapUnix])), 10, 64)
	var reg []wire.AnchorVersion
	if raw := kv[snapshot.KVVersions]; len(raw) > 0 && json.Unmarshal(raw, &reg) == nil && len(reg) > 0 {
		v.Versions = len(reg)
		v.NewestVersion = reg[len(reg)-1].Name // the registry is oldest first
	}
	return v, true
}

// ago renders a unix time as a terse age ("3 h ago"); "" for 0.
func ago(unix int64) string {
	if unix <= 0 {
		return ""
	}
	d := time.Since(time.Unix(unix, 0))
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%d min ago", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%d h ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%d d ago", int(d.Hours()/24))
	}
}

// lampProfiles are the profiles the masthead shows: those with a
// filesystem or a running daemon. With none, the selected profile alone
// (its lamps then say what is missing).
func lampProfiles(all []profileMonitor) []profileMonitor {
	var out []profileMonitor
	for _, p := range all {
		if p.HasDB || p.Online {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		for _, p := range all {
			if p.Selected {
				out = append(out, p)
			}
		}
	}
	return out
}
