package web

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"telfs/internal/config"
)

// Which profile a page is about. The console shows every profile at once
// (dashboard, masthead lamps), so there is no "selected" profile: a page
// about ONE profile says which in its address — ?profile=NAME — and shows
// a profile bar to switch (v1.1, Arash: "selecting a profile doesn't make
// sense when more than one can be displayed"). Forms carry the profile in
// a hidden field, redirects to profile pages get it appended (the
// response recorder), and without one a page shows the default profile:
// the CLI's default (~/.config/telfs/active) if it names a profile, else
// the first. Making a profile the CLI's default stays an explicit action
// (POST /profiles/{name}/default).

// errNoProfile is what a page says when there is no profile at all.
var errNoProfile = errors.New("no profile yet — create one (Setup → Profiles)")

// profilePages are the pages about one profile: they show the profile bar
// and their redirects carry the profile.
var profilePages = []string{"/setup", "/login", "/channel", "/encrypt", "/compression", "/telegram", "/state"}

// isProfilePage reports whether path is (under) one of profilePages.
func isProfilePage(path string) bool {
	for _, p := range profilePages {
		if path == p || strings.HasPrefix(path, p+"/") {
			return true
		}
	}
	return false
}

// defaultProfile is the profile a page shows without ?profile=.
func defaultProfile() string {
	if a := config.ActiveProfile(); a != "" && profileExists(a) {
		return a
	}
	if names := listProfileNames(); len(names) > 0 {
		return names[0]
	}
	return ""
}

// profileExists reports a valid name with a profile directory.
func profileExists(name string) bool {
	if config.ValidateProfileName(name) != nil {
		return false
	}
	dir, err := config.ProfileDir(name)
	if err != nil {
		return false
	}
	fi, err := os.Stat(dir)
	return err == nil && fi.IsDir()
}

// profileOf is the profile a request is about: its profile field (query or
// form) when it names one, else the default.
func profileOf(r *http.Request) string {
	if p := strings.TrimSpace(r.FormValue("profile")); profileExists(p) {
		return p
	}
	return defaultProfile()
}

// queryProfile is profileOf without reading a request body (the
// middleware: some handlers stream their own body). A form already parsed
// by the handler counts.
func queryProfile(r *http.Request) string {
	vals := r.URL.Query()
	if r.Form != nil {
		vals = r.Form
	}
	if p := strings.TrimSpace(vals.Get("profile")); profileExists(p) {
		return p
	}
	return defaultProfile()
}

// loadCfg loads the profile the request is about. It never creates a
// profile directory (config.LoadFromDir would).
func loadCfg(r *http.Request) (*config.Config, error) {
	return loadProfileCfg(profileOf(r))
}

func loadProfileCfg(name string) (*config.Config, error) {
	if name == "" {
		return nil, errNoProfile
	}
	if !profileExists(name) {
		return nil, fmt.Errorf("no profile %q", name)
	}
	dir, err := config.ProfileDir(name)
	if err != nil {
		return nil, err
	}
	return config.LoadFromDir(dir)
}

// withProfile adds ?profile=NAME to a local URL that has none.
func withProfile(u, name string) string {
	if name == "" || strings.Contains(u, "profile=") {
		return u
	}
	sep := "?"
	if strings.Contains(u, "?") {
		sep = "&"
	}
	return u + sep + "profile=" + url.QueryEscape(name)
}

// profileTab is one entry of the profile bar.
type profileTab struct {
	Name    string
	URL     string
	Current bool
}

// profileBar is the bar a profile page shows (nil elsewhere): one tab per
// profile, each the same page about that profile.
func profileBar(path, current string) []profileTab {
	if !isProfilePage(path) {
		return nil
	}
	var tabs []profileTab
	for _, n := range listProfileNames() {
		tabs = append(tabs, profileTab{Name: n, URL: withProfile(path, n), Current: n == current})
	}
	return tabs
}
