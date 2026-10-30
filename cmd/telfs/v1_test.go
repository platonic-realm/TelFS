package main

import (
	"context"
	"strings"
	"testing"
)

// Spec v1, addition 3: `telfs version` is the program version only; a
// missing or unknown subcommand fails (exit 1) instead of printing the
// program version and exiting 0. Usage errors exit 1 (2 = secret source).
func TestVersionCommandsAreStrict(t *testing.T) {
	ctx := context.Background()
	if err := dispatch(ctx, []string{"telfs", "version"}); err != nil {
		t.Fatalf("telfs version: %v", err)
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"telfs", "version", "create", "daily"}, "takes no arguments"},
		{[]string{"telfs", "--version", "list"}, "takes no arguments"},
		{[]string{"telfs", "versions"}, "missing subcommand"},
		{[]string{"telfs", "versions", "lst"}, "unknown subcommand"},
		{[]string{"telfs", "nonsense"}, "unknown command"},
		{[]string{"telfs"}, "missing command"},
	} {
		err := dispatch(ctx, tc.args)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%v: err %v, want %q", tc.args, err, tc.want)
			continue
		}
		if c := exitCode(err); c != 1 {
			t.Errorf("%v: exit %d, want 1", tc.args, c)
		}
	}
}

// Spec v1 §3: `telfs unmount` of something that is not mounted exits 5.
func TestUnmountNotMountedExits5(t *testing.T) {
	err := dispatch(context.Background(), []string{"telfs", "unmount", "/nonexistent/telfs-test-not-a-mount"})
	if err == nil || exitCode(err) != 5 {
		t.Fatalf("unmount of a path that is not mounted: err %v, exit %d, want 5", err, exitCode(err))
	}
}

// The usage text lists every command and every v1 flag, and the exit codes.
func TestUsageListsV1Interface(t *testing.T) {
	for _, want := range []string{
		"telfs login", "telfs channel list", "telfs channel set", "telfs channel info", "telfs channel ping",
		"telfs init", "telfs profile", "telfs mount", "telfs unmount [--timeout D]", "telfs status [--json [--version NAME]]",
		"telfs sync [--wait] [--timeout D]", "telfs upload limit [RATE]", "[read]", "telfs versions create NAME [--timeout D]", "telfs versions list [--json]",
		"telfs versions delete NAME", "telfs fsck --quick [--json]", "telfs restore [--version NAME] [--workers N] SRC DEST",
		"telfs recover --from-captions", "telfs snapshot", "telfs secret test", "telfs secret change", "telfs session encrypt",
		"telfs key export", "telfs encrypt status", "telfs fsck [--fix]", "telfs gc [--yes] [--pages N] [--min-age D]",
		"telfs doctor", "telfs trash", "telfs ratelimit", "telfs web", "telfs bench", "telfs version",
		"--service", "--readonly --version NAME", "--key-header FILE", "--metrics-addr ADDR", "--secret-from",
		"[staging]", "[snapshot]", "[upload]", "[secret]", "${CREDENTIALS_DIRECTORY}",
		"2 secret source unavailable", "3 wrong secret", "4 timeout / not synced yet", "5 not mounted",
		"6 Telegram", "7 integrity",
	} {
		if !strings.Contains(usage, want) {
			t.Errorf("usage does not mention %q", want)
		}
	}
	if strings.Contains(usage, "9090") {
		t.Error("usage still names the old fixed metrics address")
	}
}
