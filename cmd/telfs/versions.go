package main

import (
	"context"
	"errors"
	"fmt"
)

const versionsUsage = "create NAME | list [--json] | delete NAME"

// cmdVersions is `telfs versions …` — pinned, read-only versions of a
// filesystem (spec v1 §2). A missing or unknown subcommand is an error
// (exit 1): a script must never mistake it for success.
func cmdVersions(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("telfs versions: missing subcommand (" + versionsUsage + ")")
	}
	switch args[0] {
	case "create", "list", "delete":
		return fmt.Errorf("telfs versions %s: not implemented in this build yet", args[0])
	default:
		return fmt.Errorf("telfs versions: unknown subcommand %q (%s)", args[0], versionsUsage)
	}
}
