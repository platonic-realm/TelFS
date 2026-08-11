package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"telfs/internal/config"
	"telfs/internal/web"
)

// cmdWeb is the `telfs web` subcommand entry point. Runs the management
// HTTP server; blocks until ctx is canceled.
func cmdWeb(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("web", flag.ContinueOnError)
	listen := fs.String("listen", "127.0.0.1:8080", "address to bind the HTTP server to (host:port)")
	token := fs.String("token", "", "if set, every request must carry Authorization: Bearer <token>. Required for non-loopback bind.")
	tlsCert := fs.String("tls-cert", "", "optional path to a TLS certificate (PEM)")
	tlsKey := fs.String("tls-key", "", "optional path to a TLS private key (PEM)")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: telfs web [--listen ADDR] [--token TOKEN] [--tls-cert PATH] [--tls-key PATH]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if (*tlsCert == "") != (*tlsKey == "") {
		return errors.New("--tls-cert and --tls-key must be set together")
	}
	// The web console manages MULTIPLE profiles, so its pages must
	// follow ~/.config/telfs/active for "Use <profile>" to take
	// effect. A TELFS_PROFILE inherited from the launching shell
	// would pin every config.Load() in this process to one profile
	// forever (env beats the active file) — which is exactly how the
	// UI's profile switch used to silently do nothing. Use the env
	// var only to seed the active file when none is set, then drop
	// it for this process. (Mount children are unaffected: the
	// supervisor sets the child's TELFS_PROFILE explicitly.)
	if envProf := strings.TrimSpace(os.Getenv(config.EnvProfile)); envProf != "" {
		os.Unsetenv(config.EnvProfile)
		if active := config.ActiveProfile(); active == "" {
			if err := config.SetActiveProfile(envProf); err != nil {
				return fmt.Errorf("seed active profile from %s: %w", config.EnvProfile, err)
			}
			fmt.Fprintf(os.Stderr, "telfs web: seeded active profile %q from %s\n", envProf, config.EnvProfile)
		} else {
			fmt.Fprintf(os.Stderr, "telfs web: ignoring %s=%q — the web console follows `telfs profile use` (currently %q)\n",
				config.EnvProfile, envProf, active)
		}
	}
	srv, err := web.New(web.Options{
		Listen:  *listen,
		Token:   *token,
		TLSCert: *tlsCert,
		TLSKey:  *tlsKey,
	})
	if err != nil {
		return err
	}
	return srv.Run(ctx)
}
