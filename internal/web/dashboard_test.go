package web

import (
	"strings"
	"testing"
)

// The dashboard's setup checklist is for getting started: it shows while a
// step is open and is gone once TelFS is configured (the Setup menu keeps
// every step reachable).
func TestDashboardSetupChecklistOnlyUntilConfigured(t *testing.T) {
	steps := []setupStep{{Key: "profile", Label: "Create a profile", Done: true, Href: "/profiles"},
		{Key: "mount", Label: "Mount it", Href: "/mount/new"}}
	open := render(t, "dashboard.html", dashboardData{Setup: setupChecklist{Steps: steps}})
	if !strings.Contains(open, "Setup &amp; configuration") || !strings.Contains(open, `id="setup-checklist"`) {
		t.Error("an open step must show the setup checklist")
	}
	steps[1].Done = true
	done := render(t, "dashboard.html", dashboardData{Setup: setupChecklist{Complete: true, Steps: steps}})
	if strings.Contains(done, "Setup &amp; configuration") || strings.Contains(done, `id="setup-checklist"`) {
		t.Error("a configured TelFS must not show the setup checklist")
	}
	if !strings.Contains(done, `href="/setup"`) {
		t.Error("the Setup menu must still link the guided wizard")
	}
}
