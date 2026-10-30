package web

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"telfs/internal/secret"
	"telfs/internal/tg"
)

// ── /channel ──────────────────────────────────────────────────────

type channelShowData struct {
	pageBase
	HasAPI  bool
	HasAuth bool
	Bound   bool
	Channel channelView
}

func (s *Server) channelShow(w http.ResponseWriter, r *http.Request) {
	d := channelShowData{pageBase: s.basePage(w, r)}
	cfg, err := loadCfg(r)
	if err != nil {
		s.renderTemplate(w, "channel/show.html", d)
		return
	}
	d.HasAPI = cfg.RequireAPI() == nil
	// Authenticated = login completed ([identity] recorded), not just
	// session.json existing — gotd creates that file during the
	// handshake, before the user has entered their code.
	d.HasAuth = cfg.HasSession() && cfg.Identity != nil
	if cfg.Channel.ID != 0 {
		d.Bound = true
		d.Channel = channelView{Set: true, ID: cfg.Channel.ID, Title: cfg.Channel.Title}
	}
	s.renderTemplate(w, "channel/show.html", d)
}

// ── /channel/list (user account only) ─────────────────────────────

type channelListData struct {
	pageBase
	Channels   []tg.ChannelInfo
	Error      string
	NeedSecret bool
}

func (s *Server) channelList(w http.ResponseWriter, r *http.Request) {
	d := channelListData{pageBase: s.basePage(w, r)}
	cfg, err := loadCfg(r)
	if err != nil {
		d.Error = err.Error()
		s.renderTemplate(w, "channel/list.html", d)
		return
	}
	d.NeedSecret = secretIsPrompt(cfg) && r.Method == http.MethodGet
	if d.NeedSecret {
		s.renderTemplate(w, "channel/list.html", d)
		return
	}
	if r.Method == http.MethodPost {
		if err := s.state.CheckCSRF(r); err != nil {
			http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
			return
		}
	}
	client, err := actionClient(r, cfg, secret.Unlock)
	if err != nil {
		d.Error = err.Error()
		s.renderTemplate(w, "channel/list.html", d)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	chans, err := client.ListChannels(ctx)
	if err != nil {
		d.Error = err.Error()
		s.renderTemplate(w, "channel/list.html", d)
		return
	}
	d.Channels = chans
	s.renderTemplate(w, "channel/list.html", d)
}

// ── GET /setup/channels — wizard channel-picker partial ───────────
//
// HTMX-lazy-loaded by the setup wizard's channel step so the wizard
// page renders instantly while the dialog scan (a network RPC, up to
// ~15 s) fills in. Each row is a one-click bind: a mini form posting
// the channel's id + access_hash to /channel/set with from=setup.
type setupChannelsData struct {
	CSRF     string
	Channels []tg.ChannelInfo
	Error    string
	// SecretPrompt: each "use this channel" form carries a secret field.
	SecretPrompt bool
	// NeedSecret: render the "scan my channels" form first.
	NeedSecret bool
}

func (s *Server) setupChannels(w http.ResponseWriter, r *http.Request) {
	d := setupChannelsData{CSRF: s.state.EnsureCSRF(w, r)}
	cfg, err := loadCfg(r)
	if err != nil {
		d.Error = err.Error()
		s.renderPartial(w, "partials/setup_channels.html", d)
		return
	}
	d.SecretPrompt = secretIsPrompt(cfg)
	if d.SecretPrompt && r.Method == http.MethodGet {
		d.NeedSecret = true
		s.renderPartial(w, "partials/setup_channels.html", d)
		return
	}
	if r.Method == http.MethodPost {
		if err := s.state.CheckCSRF(r); err != nil {
			http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
			return
		}
	}
	client, err := actionClient(r, cfg, secret.Unlock)
	if err != nil {
		d.Error = err.Error()
		s.renderPartial(w, "partials/setup_channels.html", d)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	chans, err := client.ListChannels(ctx)
	if err != nil {
		d.Error = err.Error()
		s.renderPartial(w, "partials/setup_channels.html", d)
		return
	}
	d.Channels = chans
	s.renderPartial(w, "partials/setup_channels.html", d)
}

// ── /channel/set ──────────────────────────────────────────────────

type channelSetData struct {
	pageBase
}

func (s *Server) channelSetForm(w http.ResponseWriter, r *http.Request) {
	s.renderTemplate(w, "channel/set.html", channelSetData{pageBase: s.basePage(w, r)})
}

func (s *Server) channelSetSubmit(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	errBack := setupReturn(r, "/channel/set")
	idStr := strings.TrimSpace(r.FormValue("id"))
	if idStr == "" {
		SetFlash(w, "error", "channel id is required")
		http.Redirect(w, r, errBack, http.StatusSeeOther)
		return
	}
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		SetFlash(w, "error", fmt.Sprintf("bad id %q: %v", idStr, err))
		http.Redirect(w, r, errBack, http.StatusSeeOther)
		return
	}
	var accessHash int64
	if h := strings.TrimSpace(r.FormValue("access_hash")); h != "" {
		if v, err := strconv.ParseInt(h, 10, 64); err == nil {
			accessHash = v
		} else {
			SetFlash(w, "error", fmt.Sprintf("bad access_hash %q: %v", h, err))
			http.Redirect(w, r, errBack, http.StatusSeeOther)
			return
		}
	}
	cfg, err := loadCfg(r)
	if err != nil {
		SetFlash(w, "error", "config: "+err.Error())
		http.Redirect(w, r, errBack, http.StatusSeeOther)
		return
	}
	// Rebind guard (mirrors `telfs channel set`): once chunk_map
	// references messages they live in the CURRENTLY-bound channel;
	// rebinding orphans every read and misdirects writes, snapshots
	// and gc. The web form has no --force — use a fresh profile (or
	// the CLI) for deliberate rebinds.
	if cfg.Channel.ID != 0 && cfg.Channel.ID != id {
		if mstore, merr := openMetaExistingFor(cfg); merr == nil {
			has, rerr := mstore.HasAnyChunks(r.Context())
			mstore.Close()
			if rerr == nil && has {
				SetFlash(w, "error", fmt.Sprintf("this profile has data in the currently-bound channel (id=%d) — rebinding would orphan all of it. Create a new profile for a new channel, or use `telfs channel set --force` if you really mean it.", cfg.Channel.ID))
				http.Redirect(w, r, errBack, http.StatusSeeOther)
				return
			}
		}
	}
	client, err := actionClient(r, cfg, secret.Unlock)
	if err != nil {
		SetFlash(w, "error", err.Error())
		http.Redirect(w, r, errBack, http.StatusSeeOther)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	info, err := client.SetChannel(ctx, id, accessHash)
	if err != nil {
		SetFlash(w, "error", "set: "+err.Error())
		http.Redirect(w, r, errBack, http.StatusSeeOther)
		return
	}
	SetFlash(w, "ok", fmt.Sprintf("channel set: %s (id=%d)", info.Title, info.ID))
	http.Redirect(w, r, setupReturn(r, "/channel"), http.StatusSeeOther)
}
