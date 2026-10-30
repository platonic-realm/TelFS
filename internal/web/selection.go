package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"

	"telfs/internal/config"
)

// The profile the console's pages act on is chosen per browser (v1.1 W1),
// not by the global active file: S9's services run two profiles at once
// and set no active profile, and every page used to say "no active
// profile". The selection is, in order:
//
//  1. the browser's choice (cookie telfs_profile, set by Select);
//  2. the CLI's default (~/.config/telfs/active), if it names a profile;
//  3. the first profile.
//
// Monitoring (dashboard, masthead lamps, the Telegram page) shows every
// profile whatever is selected. A page may still show another profile for
// one request with ?profile=NAME (requestProfile). Making a profile the
// CLI's default stays an explicit action (POST /profiles/{name}/default).

const profileCookie = "telfs_profile"

type selectedKey struct{}

// noProfileErr is what a page says when there is no profile at all.
var errNoProfile = errors.New("no profile yet — create one (Setup → Profiles)")

// selectedProfile resolves the request's profile ("" when there is none).
func selectedProfile(r *http.Request) string {
	if c, err := r.Cookie(profileCookie); err == nil && profileExists(c.Value) {
		return c.Value
	}
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

// withSelection puts the request's profile into its context (middleware).
func withSelection(r *http.Request) (*http.Request, string) {
	sel := selectedProfile(r)
	return r.WithContext(context.WithValue(r.Context(), selectedKey{}, sel)), sel
}

// profileOf is the profile this request acts on.
func profileOf(r *http.Request) string {
	if v, ok := r.Context().Value(selectedKey{}).(string); ok {
		return v
	}
	return selectedProfile(r)
}

// loadCfg loads the request's profile. It never creates a profile
// directory (config.LoadFromDir would).
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

// setSelected remembers the browser's choice of profile.
func setSelected(w http.ResponseWriter, name string) {
	http.SetCookie(w, &http.Cookie{
		Name:     profileCookie,
		Value:    name,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   365 * 24 * 60 * 60,
	})
}

// selectedFromWriter is the profile of the request a response belongs to
// (the middleware's recorder carries it): the masthead names it.
func selectedFromWriter(w http.ResponseWriter) string {
	if sr, ok := w.(*statusRecorder); ok {
		return sr.profile
	}
	return ""
}
