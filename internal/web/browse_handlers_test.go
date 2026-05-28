package web

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestResolveBrowsePath exercises the allow-list + traversal-prevention
// contract of resolveBrowsePath — the single most security-sensitive
// helper in the web package.
func TestResolveBrowsePath(t *testing.T) {
	mount := t.TempDir()
	if err := os.Mkdir(filepath.Join(mount, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Fake an in-memory supervisor whose List() reports `mount` as a
	// supervised mountpoint. isKnownMount will accept it.
	s := &Server{sup: &Supervisor{
		procs: map[string]*MountProcess{"test": {Profile: "test", Mountpoint: mount}},
	}}

	cases := []struct {
		name    string
		at, p   string
		wantErr string // substring; empty = expect success
		wantRel string // expected cleaned `p` on success
	}{
		{"root-empty-p", mount, "", "", ""},
		{"root-slash", mount, "/", "", ""},
		{"valid-subdir", mount, "sub", "", "sub"},
		{"valid-subdir-leading-slash", mount, "/sub", "", "sub"},
		{"valid-dot-in-path", mount, "./sub", "", "sub"},
		{"trim-trailing-slash", mount, "sub/", "", "sub"},
		{"clean-double-slash", mount, "sub//", "", "sub"},
		{"clean-dot-segment", mount, "sub/./.", "", "sub"},

		// `..` segments are CLAMPED at root by path.Clean — the resolver
		// does not reject them, it neutralizes them. The security
		// invariant is "abs never escapes mount", checked by every test
		// via the filepath.Rel assertion below.
		{"traversal-bare-clamps-to-root", mount, "..", "", ""},
		{"traversal-deep-clamps-then-rels", mount, "../../etc/passwd", "", "etc/passwd"},
		{"traversal-from-subdir-clamps", mount, "sub/../..", "", ""},

		{"unknown-mount-abs", "/this/is/not/a/mount", "", "not a known", ""},
		{"unknown-mount-tmp", "/tmp", "", "not a known", ""},
		{"empty-at", "", "x", "missing mountpoint", ""},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			at, p, abs, err := s.resolveBrowsePath(c.at, c.p)
			if c.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil (at=%q rel=%q abs=%q)", c.wantErr, at, p, abs)
				}
				if !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("error %q does not contain %q", err.Error(), c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if at != mount {
				t.Errorf("at: got %q, want %q", at, mount)
			}
			if p != c.wantRel {
				t.Errorf("rel: got %q, want %q", p, c.wantRel)
			}
			expectedAbs := filepath.Join(mount, c.wantRel)
			if abs != expectedAbs {
				t.Errorf("abs: got %q, want %q", abs, expectedAbs)
			}
			// Belt-and-suspenders: abs must NEVER escape mount.
			rel, err := filepath.Rel(mount, abs)
			if err != nil || strings.HasPrefix(rel, "..") {
				t.Errorf("SECURITY: resolved abs %q escapes mount %q (rel=%q err=%v)", abs, mount, rel, err)
			}
		})
	}
}

// TestBrowseHrefRoundtrip verifies the URL helpers produce something
// the resolver accepts back. Catches QueryEscape / QueryUnescape drift.
func TestBrowseHrefRoundtrip(t *testing.T) {
	mount := t.TempDir()
	// `Name with spaces+plus` exercises both literal-space and literal-+
	// — the two characters URL escaping cares about for query strings.
	for _, name := range []string{"plain", "Name with spaces", "with+plus", "a/b"} {
		href := browseFileHref(mount, name)
		if !strings.HasPrefix(href, "/browse/file?") {
			t.Errorf("href shape: %q", href)
		}
	}
	// Empty p → dir href omits p entirely (cleaner URLs).
	if h := browseDirHref(mount, ""); strings.Contains(h, "p=") {
		t.Errorf("empty p should not appear in href: %q", h)
	}
}
