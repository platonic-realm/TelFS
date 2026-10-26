// Package harden makes the running process non-dumpable (feature 6,
// decision D5: every telfs process, from the top of main).
//
// prctl(PR_SET_DUMPABLE, 0) — measured on S9 (kernel 7.2.11,
// 2026-10-25): other processes of the same user are DENIED
// /proc/<pid>/environ, exe, cwd, fd/* and maps (the files become
// root:root), no core dump is written, and same-user ptrace needs
// CAP_SYS_PTRACE. Still readable: cmdline, stat, status (so mountctl
// finds daemons through cmdline and /proc/mounts). Signals still work.
//
// What it does NOT cover: root; the few milliseconds before main's
// init runs; children (exec makes a new program dumpable again — which
// is why TELFS_PASSPHRASE is also stripped from the environment).
package harden

import (
	"fmt"
	"syscall"
)

const prSetDumpable = 4 // PR_SET_DUMPABLE

// NonDumpable applies PR_SET_DUMPABLE 0 to the whole process.
func NonDumpable() error {
	if _, _, e := syscall.RawSyscall(syscall.SYS_PRCTL, prSetDumpable, 0, 0); e != 0 {
		return fmt.Errorf("prctl(PR_SET_DUMPABLE, 0): %w", e)
	}
	return nil
}
