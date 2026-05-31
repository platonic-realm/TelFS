package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"telfs/internal/config"
	"telfs/internal/meta"
)

const ratelimitUsage = `telfs ratelimit — view or change the TG requests/minute caps

The caps are enforced inside the daemon's gotd middleware. While the daemon
is running it polls meta_kv every 5 s, so set/clear take effect WITHOUT a
remount. Zero means unlimited (the safe default for downloads; the
historical default for uploads too).

Two independent lanes:
  upload    — every RPC that isn't a download (SaveFilePart, SaveBigFilePart,
              SendMedia, channel scans, journal posts, snapshot uploads).
  download  — upload.getFile only (cache misses, range reads, etc.).

usage:
  telfs ratelimit show                          print both caps
  telfs ratelimit set <N>                       set UPLOAD cap to N req/min
  telfs ratelimit set --download <N>            set DOWNLOAD cap to N req/min
  telfs ratelimit set --upload <U> --download <D>   set both at once
  telfs ratelimit clear                         disable BOTH caps
  telfs ratelimit clear --upload | --download   disable one lane

Use this when bursty workloads (tar extracts, mass deletes) trigger
cascading FLOOD_WAITs on the upload side. Throttle uploads first; leave
downloads unlimited unless you specifically need to slow reads.
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
		up, err := mstore.TGRateLimitPerMin(ctx)
		if err != nil {
			return err
		}
		down, err := mstore.TGDownloadRateLimitPerMin(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("upload   rate limit: %s\n", fmtRL(up))
		fmt.Printf("download rate limit: %s\n", fmtRL(down))
		return nil
	case "set":
		return setRateLimits(ctx, mstore, args[1:])
	case "clear":
		return clearRateLimits(ctx, mstore, args[1:])
	case "-h", "--help", "help":
		fmt.Print(ratelimitUsage)
		return nil
	default:
		fmt.Fprintf(os.Stderr, "unknown ratelimit subcommand %q\n\n%s", args[0], ratelimitUsage)
		os.Exit(2)
		return nil
	}
}

// fmtRL renders a per-minute value for the `show` output. Zero is
// rendered as "unlimited" rather than "0 requests/min" so operators
// don't have to remember the convention.
func fmtRL(n int) string {
	if n == 0 {
		return "unlimited"
	}
	return fmt.Sprintf("%d requests/min", n)
}

// setRateLimits accepts forms:
//
//	set N                          — upload cap only
//	set --upload N                  — upload cap only
//	set --download N                — download cap only
//	set --upload N --download M     — both
func setRateLimits(ctx context.Context, mstore *meta.Store, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("ratelimit set: expected <N> or --upload N / --download N")
	}
	// Bare-number form (back-compat).
	if len(args) == 1 {
		n, err := strconv.Atoi(args[0])
		if err == nil {
			if err := mstore.SetTGRateLimitPerMin(ctx, n); err != nil {
				return err
			}
			fmt.Printf("upload rate limit: %s — takes effect within 5 s\n", fmtRL(n))
			return nil
		}
	}
	var (
		upSet, downSet bool
		up, down       int
	)
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--upload", "-u":
			if i+1 >= len(args) {
				return fmt.Errorf("--upload: missing value")
			}
			i++
			n, err := strconv.Atoi(args[i])
			if err != nil {
				return fmt.Errorf("--upload %q: %w", args[i], err)
			}
			up, upSet = n, true
		case "--download", "-d":
			if i+1 >= len(args) {
				return fmt.Errorf("--download: missing value")
			}
			i++
			n, err := strconv.Atoi(args[i])
			if err != nil {
				return fmt.Errorf("--download %q: %w", args[i], err)
			}
			down, downSet = n, true
		default:
			return fmt.Errorf("unknown set flag %q", args[i])
		}
	}
	if !upSet && !downSet {
		return fmt.Errorf("ratelimit set: no lane specified")
	}
	if upSet {
		if err := mstore.SetTGRateLimitPerMin(ctx, up); err != nil {
			return err
		}
		fmt.Printf("upload rate limit: %s — takes effect within 5 s\n", fmtRL(up))
	}
	if downSet {
		if err := mstore.SetTGDownloadRateLimitPerMin(ctx, down); err != nil {
			return err
		}
		fmt.Printf("download rate limit: %s — takes effect within 5 s\n", fmtRL(down))
	}
	return nil
}

// clearRateLimits clears one or both lanes. Bare `clear` zeroes both
// (matching the legacy behavior, which only had one lane to clear).
func clearRateLimits(ctx context.Context, mstore *meta.Store, args []string) error {
	clearUp, clearDown := true, true
	if len(args) > 0 {
		clearUp, clearDown = false, false
		for _, a := range args {
			switch a {
			case "--upload", "-u":
				clearUp = true
			case "--download", "-d":
				clearDown = true
			default:
				return fmt.Errorf("unknown clear flag %q", a)
			}
		}
	}
	if clearUp {
		if err := mstore.SetTGRateLimitPerMin(ctx, 0); err != nil {
			return err
		}
		fmt.Println("upload rate limit cleared (unlimited)")
	}
	if clearDown {
		if err := mstore.SetTGDownloadRateLimitPerMin(ctx, 0); err != nil {
			return err
		}
		fmt.Println("download rate limit cleared (unlimited)")
	}
	return nil
}
