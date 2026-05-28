package web

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"telfs/internal/config"
	"telfs/internal/tg"
)

// ── /channel ──────────────────────────────────────────────────────

type channelShowData struct {
	pageBase
	HasAPI   bool
	HasAuth  bool
	IsBot    bool
	Bound    bool
	Channel  channelView
}

func (s *Server) channelShow(w http.ResponseWriter, r *http.Request) {
	d := channelShowData{pageBase: s.basePage(w, r)}
	cfg, err := config.Load()
	if err != nil {
		s.renderTemplate(w, "channel/show.html", d)
		return
	}
	d.HasAPI = cfg.RequireAPI() == nil
	d.IsBot = cfg.EffectiveAuthMode() == config.AuthModeBot
	// HasAuth is approximated by "session.json exists" — gotd writes
	// it after a successful auth.
	if _, err := os.Stat(cfg.SessionPath()); err == nil {
		d.HasAuth = true
	}
	if cfg.Channel.ID != 0 {
		d.Bound = true
		d.Channel = channelView{Set: true, ID: cfg.Channel.ID, Title: cfg.Channel.Title}
	}
	s.renderTemplate(w, "channel/show.html", d)
}

// ── /channel/list (user account only) ─────────────────────────────

type channelListData struct {
	pageBase
	IsBot    bool
	Channels []tg.ChannelInfo
	Error    string
}

func (s *Server) channelList(w http.ResponseWriter, r *http.Request) {
	d := channelListData{pageBase: s.basePage(w, r)}
	cfg, err := config.Load()
	if err != nil {
		d.Error = err.Error()
		s.renderTemplate(w, "channel/list.html", d)
		return
	}
	d.IsBot = cfg.EffectiveAuthMode() == config.AuthModeBot
	if d.IsBot {
		s.renderTemplate(w, "channel/list.html", d)
		return
	}
	client, err := tg.New(cfg)
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

// ── /channel/set ──────────────────────────────────────────────────

type channelSetData struct {
	pageBase
	IsBot bool
}

func (s *Server) channelSetForm(w http.ResponseWriter, r *http.Request) {
	d := channelSetData{pageBase: s.basePage(w, r)}
	if cfg, err := config.Load(); err == nil {
		d.IsBot = cfg.EffectiveAuthMode() == config.AuthModeBot
	}
	s.renderTemplate(w, "channel/set.html", d)
}

func (s *Server) channelSetSubmit(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	idStr := strings.TrimSpace(r.FormValue("id"))
	if idStr == "" {
		SetFlash(w, "error", "channel id is required")
		http.Redirect(w, r, "/channel/set", http.StatusSeeOther)
		return
	}
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		SetFlash(w, "error", fmt.Sprintf("bad id %q: %v", idStr, err))
		http.Redirect(w, r, "/channel/set", http.StatusSeeOther)
		return
	}
	var accessHash int64
	if h := strings.TrimSpace(r.FormValue("access_hash")); h != "" {
		if v, err := strconv.ParseInt(h, 10, 64); err == nil {
			accessHash = v
		} else {
			SetFlash(w, "error", fmt.Sprintf("bad access_hash %q: %v", h, err))
			http.Redirect(w, r, "/channel/set", http.StatusSeeOther)
			return
		}
	}
	cfg, err := config.Load()
	if err != nil {
		SetFlash(w, "error", "config: "+err.Error())
		http.Redirect(w, r, "/channel/set", http.StatusSeeOther)
		return
	}
	if cfg.EffectiveAuthMode() == config.AuthModeBot && accessHash == 0 {
		SetFlash(w, "error", "bot mode: access_hash is required (bots can't auto-discover it)")
		http.Redirect(w, r, "/channel/set", http.StatusSeeOther)
		return
	}
	client, err := tg.New(cfg)
	if err != nil {
		SetFlash(w, "error", "tg.New: "+err.Error())
		http.Redirect(w, r, "/channel/set", http.StatusSeeOther)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	info, err := client.SetChannel(ctx, id, accessHash)
	if err != nil {
		SetFlash(w, "error", "set: "+err.Error())
		http.Redirect(w, r, "/channel/set", http.StatusSeeOther)
		return
	}
	SetFlash(w, "ok", fmt.Sprintf("channel set: %s (id=%d)", info.Title, info.ID))
	http.Redirect(w, r, "/channel", http.StatusSeeOther)
}
