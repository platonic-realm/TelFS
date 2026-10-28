// Package sdnotify sends systemd service notifications (sd_notify) without
// cgo: one datagram to $NOTIFY_SOCKET. A no-op when the variable is unset
// (not running under a Type=notify unit).
package sdnotify

import (
	"net"
	"os"
)

// Enabled reports whether systemd listens for notifications.
func Enabled() bool { return os.Getenv("NOTIFY_SOCKET") != "" }

// Notify sends state lines such as "READY=1", "STATUS=…", "STOPPING=1",
// "EXTEND_TIMEOUT_USEC=…" (newline-separated for several).
func Notify(state string) error {
	sock := os.Getenv("NOTIFY_SOCKET")
	if sock == "" {
		return nil
	}
	if sock[0] == '@' { // abstract namespace
		sock = "\x00" + sock[1:]
	}
	conn, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: sock, Net: "unixgram"})
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Write([]byte(state))
	return err
}
