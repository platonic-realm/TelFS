// Package control is the daemon's control socket (spec v1, rows 5 and 10):
// an HTTP server on a Unix socket in the profile directory, through which
// `telfs status`, `sync`, `unmount`, `versions` and `fsck --quick`
// talk to the running mount — without the secret, which only the daemon
// holds. Owner-only: the socket is mode 0600 inside the 0700 profile
// directory, and every connection's peer uid (SO_PEERCRED) must be the
// daemon's own, so no other local user can unmount, sync or delete versions.
package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// SocketName is the control socket's file name in the profile directory.
const SocketName = "daemon.sock"

// BaseURL is the URL prefix for requests over a Client.
const BaseURL = "http://telfs"

// Path is the control socket of the profile whose data dir is dir.
func Path(dir string) string { return filepath.Join(dir, SocketName) }

// VersionPath is the control socket of the mount of version name of the
// profile whose data dir is dir: in its versions directory, named by a
// hash of the version name (a socket path is limited to 107 bytes; a
// version name may be 64 characters long).
func VersionPath(dir, name string) string {
	sum := sha256.Sum256([]byte(name))
	return filepath.Join(dir, "versions", "v-"+hex.EncodeToString(sum[:8])+".sock")
}

// ErrBusy: a live daemon already serves the socket.
var ErrBusy = errors.New("another telfs daemon serves this profile's control socket")

// Listen creates the control socket at path. A socket file left by a dead
// daemon is replaced; one a live daemon still answers on is never taken
// over (ErrBusy).
func Listen(path string) (net.Listener, error) {
	if c, err := net.DialTimeout("unix", path, time.Second); err == nil {
		c.Close()
		return nil, ErrBusy
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("control socket: remove stale %s: %w", path, err)
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("control socket: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		return nil, fmt.Errorf("control socket: chmod %s: %w", path, err)
	}
	return &peerListener{Listener: ln, uid: os.Getuid()}, nil
}

// peerListener drops every connection whose peer is not uid.
type peerListener struct {
	net.Listener
	uid int
}

func (l *peerListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if uid, ok := peerUID(c); ok && uid == l.uid {
			return c, nil
		}
		c.Close()
	}
}

func peerUID(c net.Conn) (int, bool) {
	uc, ok := c.(*net.UnixConn)
	if !ok {
		return 0, false
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return 0, false
	}
	var cred *syscall.Ucred
	var cerr error
	if err := raw.Control(func(fd uintptr) {
		cred, cerr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil || cerr != nil || cred == nil {
		return 0, false
	}
	return int(cred.Uid), true
}

// Client is an HTTP client whose every request goes to the socket at path
// (use BaseURL + "/endpoint" as the URL). timeout 0 = none.
func Client(path string, timeout time.Duration) *http.Client {
	var d net.Dialer
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return d.DialContext(ctx, "unix", path)
			},
		},
	}
}

// Serve runs h on ln until ctx ends, then closes the server and removes the
// socket file.
func Serve(ctx context.Context, ln net.Listener, path string, h http.Handler) {
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = srv.Shutdown(sctx)
		cancel()
		_ = os.Remove(path)
	}()
	_ = srv.Serve(ln)
}
