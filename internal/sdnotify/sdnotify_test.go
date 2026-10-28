package sdnotify

import (
	"net"
	"path/filepath"
	"testing"
)

func TestNotifySendsOneDatagram(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notify")
	ln, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	t.Setenv("NOTIFY_SOCKET", path)
	if !Enabled() {
		t.Fatal("not enabled")
	}
	if err := Notify("READY=1\nSTATUS=mounted"); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 128)
	n, _, err := ln.ReadFromUnix(buf)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:n]); got != "READY=1\nSTATUS=mounted" {
		t.Fatalf("got %q", got)
	}
	t.Setenv("NOTIFY_SOCKET", "")
	if Enabled() || Notify("READY=1") != nil {
		t.Fatal("without NOTIFY_SOCKET it must be a silent no-op")
	}
}
