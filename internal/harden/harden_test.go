package harden

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// The helper runs as a child: it hardens itself and waits, with a
// marker in its environment.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("TELFS_HARDEN_HELPER") != "1" {
		return
	}
	if os.Getenv("TELFS_HARDEN_APPLY") == "1" {
		if err := NonDumpable(); err != nil {
			fmt.Println("ERR", err)
			os.Exit(1)
		}
	}
	fmt.Println("READY")
	time.Sleep(10 * time.Second)
	os.Exit(0)
}

func startHelper(t *testing.T, apply bool) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestHelperProcess")
	cmd.Env = append(os.Environ(), "TELFS_HARDEN_HELPER=1", "TELFS_MARKER=telfs-harden-marker")
	if apply {
		cmd.Env = append(cmd.Env, "TELFS_HARDEN_APPLY=1")
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	line, _ := bufio.NewReader(out).ReadString('\n')
	if !strings.HasPrefix(line, "READY") {
		cmd.Process.Kill()
		t.Fatalf("helper: %q", line)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	return cmd
}

func TestNonDumpableHidesProcFiles(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads every /proc file; run as a normal user")
	}
	// Control: an ordinary process's environ is readable by us.
	plain := startHelper(t, false)
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", plain.Process.Pid))
	if err != nil || !strings.Contains(string(b), "telfs-harden-marker") {
		t.Skipf("cannot read a normal process's environ here (%v) — environment doesn't allow the comparison", err)
	}

	hard := startHelper(t, true)
	pid := hard.Process.Pid
	for _, f := range []string{"environ", "maps"} {
		if _, err := os.ReadFile(fmt.Sprintf("/proc/%d/%s", pid, f)); err == nil {
			t.Errorf("/proc/%d/%s readable after PR_SET_DUMPABLE 0", pid, f)
		}
	}
	for _, l := range []string{"exe", "cwd", "fd/1"} {
		if _, err := os.Readlink(fmt.Sprintf("/proc/%d/%s", pid, l)); err == nil {
			t.Errorf("/proc/%d/%s resolvable after PR_SET_DUMPABLE 0", pid, l)
		}
	}
	// What mountctl relies on stays readable.
	for _, f := range []string{"cmdline", "stat", "status"} {
		if _, err := os.ReadFile(fmt.Sprintf("/proc/%d/%s", pid, f)); err != nil {
			t.Errorf("/proc/%d/%s must stay readable: %v", pid, f, err)
		}
	}
}
