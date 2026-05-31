package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"telfs/internal/config"
	"telfs/internal/meta"
)

const ratelimitUsage = `telfs ratelimit — view or change the TG requests/minute cap

The cap is enforced inside the daemon's gotd middleware. While the daemon is
running it polls meta_kv every 5 s, so set/clear take effect WITHOUT a
remount. Zero means unlimited (the historical default).

usage:
  telfs ratelimit show          print the current cap (0 = unlimited)
  telfs ratelimit set <N>       set cap to N requests/min (>0)
  telfs ratelimit clear         disable the cap (same as 'set 0')

Use this when bursty workloads (tar extracts, mass deletes) trigger
cascading FLOOD_WAITs. A conservative starting point is 60–120 req/min,
which leaves headroom under Telegram's per-user budget while still
making forward progress.
`

func cmdRateLimit(ctx context.Context, args []string) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, ratelimitUsage)
		os.Exit(2)
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	mstore, err := meta.Open(cfg.DBPath())
	if err != nil {
		return fmt.Errorf("open meta: %w", err)
	}
	defer mstore.Close()

	switch args[0] {
	case "show":
		n, err := mstore.TGRateLimitPerMin(ctx)
		if err != nil {
			return err
		}
		if n == 0 {
			fmt.Println("rate limit: unlimited (historical default)")
		} else {
			fmt.Printf("rate limit: %d requests/min\n", n)
		}
		return nil
	case "set":
		if len(args) != 2 {
			return fmt.Errorf("ratelimit set: expected <N>")
		}
		n, err := strconv.Atoi(args[1])
		if err != nil {
			return fmt.Errorf("ratelimit set: %w", err)
		}
		if err := mstore.SetTGRateLimitPerMin(ctx, n); err != nil {
			return err
		}
		if n == 0 {
			fmt.Println("rate limit cleared (unlimited)")
		} else {
			fmt.Printf("rate limit set to %d requests/min — takes effect on the running daemon within 5 s\n", n)
		}
		return nil
	case "clear":
		if err := mstore.SetTGRateLimitPerMin(ctx, 0); err != nil {
			return err
		}
		fmt.Println("rate limit cleared (unlimited)")
		return nil
	case "-h", "--help", "help":
		fmt.Print(ratelimitUsage)
		return nil
	default:
		fmt.Fprintf(os.Stderr, "unknown ratelimit subcommand %q\n\n%s", args[0], ratelimitUsage)
		os.Exit(2)
		return nil
	}
}
