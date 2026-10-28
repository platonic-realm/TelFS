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
