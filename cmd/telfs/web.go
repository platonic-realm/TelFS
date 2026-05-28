package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

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
