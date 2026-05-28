// Command telfs mounts a Telegram channel as a FUSE filesystem.
//
// Subcommands (planned):
//
//	telfs login                  one-time MTProto auth
//	telfs channel set <id>       pick the backing channel
//	telfs mount <mountpoint>     mount in the foreground
//
// See docs/architecture.md for the design.
package main

import (
	"fmt"
	"os"
)

const usage = `telfs — FUSE filesystem backed by a Telegram channel

Usage:
  telfs login                  Authenticate against Telegram (MTProto).
  telfs channel set <id>       Configure the backing channel.
  telfs mount <mountpoint>     Mount the filesystem (foreground).

Run 'telfs <subcommand> -h' for subcommand help.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	switch os.Args[1] {
	case "-h", "--help", "help":
		fmt.Fprint(os.Stdout, usage)
	case "login", "channel", "mount":
		fmt.Fprintf(os.Stderr, "telfs: %q is not implemented yet (M0 skeleton)\n", os.Args[1])
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "telfs: unknown subcommand %q\n\n%s", os.Args[1], usage)
		os.Exit(2)
	}
}
