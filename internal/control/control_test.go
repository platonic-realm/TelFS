package control

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSocketIsOwnerOnlyAndAnswersItsOwner(t *testing.T) {
	path := Path(t.TempDir())
	ln, err := Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 || fi.Mode()&os.ModeSocket == 0 {
		t.Fatalf("socket mode %v", fi.Mode())
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		Serve(ctx, ln, path, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "hi") }))
		close(done)
	}()
	resp, err := Client(path, 2*time.Second).Get(BaseURL + "/x")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(b) != "hi" {
		t.Fatalf("got %q", b)
	}
	// A second daemon must not take over a live socket.
	if _, err := Listen(path); !errors.Is(err, ErrBusy) {
		t.Fatalf("second Listen: %v, want ErrBusy", err)
	}
	cancel()
	<-done
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("socket file left after shutdown: %v", err)
	}
}

// A connection from any other uid is dropped (tested by pretending the
// daemon runs as uid+1).
func TestSocketDropsOtherUsers(t *testing.T) {
	path := Path(t.TempDir())
	ln, err := Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	ln.(*peerListener).uid = os.Getuid() + 1
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Serve(ctx, ln, path, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "secret things") }))
	if _, err := Client(path, 2*time.Second).Get(BaseURL + "/x"); err == nil {
		t.Fatal("a foreign uid got an answer")
	}
}

// A socket file left by a dead daemon is replaced.
func TestStaleSocketIsReplaced(t *testing.T) {
	dir := t.TempDir()
	path := Path(dir)
	old, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a crash: the file stays, nobody listens.
	old.(*net.UnixListener).SetUnlinkOnClose(false)
	old.Close()
	if _, err := os.Stat(filepath.Join(dir, SocketName)); err != nil {
		t.Fatal("stale socket file missing:", err)
	}
	ln, err := Listen(path)
	if err != nil {
		t.Fatalf("Listen over a stale socket: %v", err)
	}
	ln.Close()
}
