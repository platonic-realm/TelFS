package web

import (
	"bytes"
	"html/template"
	"strings"
	"testing"
)

// Templates are parsed at request time, so a typo only shows up live.
// Render every page P1 changed, in the states that matter.
func render(t *testing.T, name string, data any, extras ...string) string {
	t.Helper()
	files := append([]string{"templates/layout.html", "templates/" + name}, extras...)
	tm, err := template.New("").Funcs(tmplFuncs()).ParseFS(templatesFS, files...)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	var b bytes.Buffer
	if err := tm.ExecuteTemplate(&b, "layout", data); err != nil {
		t.Fatalf("exec %s: %v", name, err)
	}
	return b.String()
}

func TestP1TemplatesRender(t *testing.T) {
	prompt := pageBase{CSRF: "c", SecretPrompt: true, NewSecret: true}
	for _, key := range []string{"profile", "api", "secret", "auth", "channel", "init", "mount"} {
		d := setupWizardData{pageBase: prompt, Steps: []setupStep{{Key: key, Label: key}}, CurrentIdx: 0,
			Skipped: map[int]bool{}, Active: "p", SecretSource: "prompt"}
		out := render(t, "setup.html", d)
		if key == "auth" && !strings.Contains(out, `name="secret2"`) {
			t.Error("first login of a prompt profile must ask the new secret twice")
		}
		if key == "init" && !strings.Contains(out, `action="/setup/init"`) {
			t.Error("init step form missing")
		}
	}
	// Non-prompt profiles get no secret field.
	d := setupWizardData{pageBase: pageBase{CSRF: "c"}, Steps: []setupStep{{Key: "auth"}}, Skipped: map[int]bool{}}
	if out := render(t, "setup.html", d); strings.Contains(out, `name="secret"`) {
		t.Error("a file/keepassxc profile must not show a secret field")
	}
	render(t, "profile/show.html", profileShowData{CSRF: "c", Name: "p", HasMeta: true, Format: 1, SecretSource: "file", SecretFile: "/x"})
	out := render(t, "profile/show.html", profileShowData{CSRF: "c", Name: "p", HasMeta: true, Format: 1})
	if !strings.Contains(out, "protects only the Telegram session") {
		t.Error("old unencrypted filesystem: the session-only notice is missing")
	}
	render(t, "login/phone1.html", phone1Data{pageBase: prompt})
	render(t, "channel/set.html", channelSetData{pageBase: pageBase{CSRF: "c", SecretPrompt: true}})
	render(t, "channel/list.html", channelListData{pageBase: pageBase{CSRF: "c", SecretPrompt: true}, NeedSecret: true})
	for _, f := range []int{0, 1, 2} {
		render(t, "crypto/show.html", encryptShowData{pageBase: pageBase{CSRF: "c"}, HasMeta: true, Format: f})
	}
	render(t, "mount/new.html", struct {
		pageBase
		Profiles       []string
		DefaultProfile string
	}{pageBase: pageBase{CSRF: "c"}, Profiles: []string{"p"}, DefaultProfile: "p"})

	// The HTMX partial renders on its own.
	tm, err := template.New("").Funcs(tmplFuncs()).ParseFS(templatesFS, "templates/partials/setup_channels.html")
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []setupChannelsData{{CSRF: "c", NeedSecret: true}, {CSRF: "c", SecretPrompt: true}} {
		var b bytes.Buffer
		if err := tm.ExecuteTemplate(&b, "content", d); err != nil {
			t.Fatal(err)
		}
	}
}
