package fs

import (
	"context"
	"path/filepath"
	"slices"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"

	"telfs/internal/meta"
)

// Owner checks (v1.1 B2): default_permissions reaches the kernel's mount
// options exactly when asked for; ro and the source stay as they were.
func TestFuseOptionsDefaultPermissions(t *testing.T) {
	on := fuseOptions(MountOptions{AllowOther: true, DefaultPermissions: true, ReadOnly: true, Profile: "cloud", FSUUID: "u"})
	if !on.MountOptions.AllowOther || !slices.Contains(on.MountOptions.Options, "default_permissions") || !slices.Contains(on.MountOptions.Options, "ro") {
		t.Fatalf("options %+v", on.MountOptions.Options)
	}
	if on.MountOptions.FsName != "telfs:cloud:u" {
		t.Fatalf("source %q", on.MountOptions.FsName)
	}
	off := fuseOptions(MountOptions{AllowOther: true})
	if slices.Contains(off.MountOptions.Options, "default_permissions") {
		t.Fatal("default_permissions without being asked for")
	}
}

// df on a read-only mount (v1.1 S9): no fictional 128 PB — nothing can be
// written, so free is 0 and the total is what is used.
func TestStatfsReadOnly(t *testing.T) {
	m, err := meta.Open(filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	var out fuse.StatfsOut
	if errno := NewRoot(&Backend{Meta: m, ReadOnly: true}).Statfs(context.Background(), &out); errno != 0 {
		t.Fatal(errno)
	}
	if out.Bfree != 0 || out.Bavail != 0 || out.Blocks > 1<<20 || out.Bsize != 4096 {
		t.Fatalf("statfs %+v", out)
	}
}
