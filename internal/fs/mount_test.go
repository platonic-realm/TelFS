package fs

import (
	"slices"
	"testing"
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
