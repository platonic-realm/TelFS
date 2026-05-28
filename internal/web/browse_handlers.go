package web

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Hard caps for uploads. Through FUSE → chunk pipeline → Telegram, so
// the limit is "one Telegram message" — TG-side it's 2 GiB. We cap at
// 2 GiB; oversized uploads should split into multiple files anyway.
const browseMaxUploadBytes = 2 * 1024 * 1024 * 1024

// ── /browse — pick a mountpoint ───────────────────────────────────

type browsePickData struct {
	pageBase
	Mounts []browseMountChoice
}

type browseMountChoice struct {
	Path  string
	Label string // "supervised: profile" or "external"
}

func (s *Server) browsePick(w http.ResponseWriter, r *http.Request) {
	d := browsePickData{pageBase: s.basePage(w, r)}
	for _, mp := range s.sup.List() {
		d.Mounts = append(d.Mounts, browseMountChoice{
			Path:  mp.Mountpoint,
			Label: "supervised: " + mp.Profile,
		})
	}
	for _, ext := range s.sup.ExternalMounts() {
		d.Mounts = append(d.Mounts, browseMountChoice{Path: ext, Label: "external"})
	}
	sort.Slice(d.Mounts, func(i, j int) bool { return d.Mounts[i].Path < d.Mounts[j].Path })
	s.renderTemplate(w, "browse/pick.html", d)
}

// ── /browse/dir?at=<mount>&p=<rel> ────────────────────────────────

type browseDirData struct {
	pageBase
	At       string
	P        string
	Entries  []browseEntry
	Parent   string // p value of the parent dir; empty if at root
	IsRoot   bool
	UpURL    string // /browse/dir?at=...&p=parent
	UploadOK bool   // false if the dir is read-only at the FS level (best-effort hint)
}

type browseEntry struct {
	Name     string
	IsDir    bool
	IsLink   bool
	Size     int64
	Mtime    string
	HrefDir  string // /browse/dir?at=...&p=...
	HrefFile string // /browse/file?at=...&p=...
}

func (s *Server) browseDir(w http.ResponseWriter, r *http.Request) {
	at, p, abs, err := s.resolveBrowsePath(r.URL.Query().Get("at"), r.URL.Query().Get("p"))
	if err != nil {
		SetFlash(w, "error", err.Error())
		http.Redirect(w, r, "/browse", http.StatusSeeOther)
		return
	}
	info, err := os.Stat(abs)
	if err != nil {
		SetFlash(w, "error", "stat: "+err.Error())
		http.Redirect(w, r, "/browse", http.StatusSeeOther)
		return
	}
	if !info.IsDir() {
		// Convenience: someone clicked /browse/dir on a file path — bounce to file handler.
		http.Redirect(w, r, browseFileHref(at, p), http.StatusSeeOther)
		return
	}
	entries, err := os.ReadDir(abs)
	if err != nil {
		SetFlash(w, "error", "readdir: "+err.Error())
		http.Redirect(w, r, "/browse", http.StatusSeeOther)
		return
	}
	d := browseDirData{
		pageBase: s.basePage(w, r),
		At:       at,
		P:        p,
		IsRoot:   p == "" || p == ".",
		UploadOK: true,
	}
	if !d.IsRoot {
		parent := path.Dir(p)
		if parent == "." {
			parent = ""
		}
		d.Parent = parent
		d.UpURL = browseDirHref(at, parent)
	}
	for _, e := range entries {
		fi, ferr := e.Info()
		if ferr != nil {
			continue
		}
		childRel := path.Join(p, e.Name())
		ent := browseEntry{
			Name:   e.Name(),
			IsDir:  e.IsDir(),
			IsLink: fi.Mode()&os.ModeSymlink != 0,
			Size:   fi.Size(),
			Mtime:  fi.ModTime().Format(time.RFC3339),
		}
		if ent.IsDir {
			ent.HrefDir = browseDirHref(at, childRel)
		} else {
			ent.HrefFile = browseFileHref(at, childRel)
		}
		d.Entries = append(d.Entries, ent)
	}
	sort.Slice(d.Entries, func(i, j int) bool {
		if d.Entries[i].IsDir != d.Entries[j].IsDir {
			return d.Entries[i].IsDir
		}
		return d.Entries[i].Name < d.Entries[j].Name
	})
	s.renderTemplate(w, "browse/dir.html", d)
}

// ── /browse/file?at=<mount>&p=<rel> — download ────────────────────

func (s *Server) browseFile(w http.ResponseWriter, r *http.Request) {
	_, _, abs, err := s.resolveBrowsePath(r.URL.Query().Get("at"), r.URL.Query().Get("p"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	info, err := os.Stat(abs)
	if err != nil {
		http.Error(w, "stat: "+err.Error(), http.StatusNotFound)
		return
	}
	if info.IsDir() {
		http.Error(w, "path is a directory", http.StatusBadRequest)
		return
	}
	// Set a sensible Content-Disposition so browsers default to download.
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename*=UTF-8''%s`, url.PathEscape(filepath.Base(abs))))
	http.ServeFile(w, r, abs)
}

// ── POST /browse/mkdir ────────────────────────────────────────────

func (s *Server) browseMkdir(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	at, p, abs, err := s.resolveBrowsePath(r.FormValue("at"), r.FormValue("p"))
	if err != nil {
		SetFlash(w, "error", err.Error())
		http.Redirect(w, r, "/browse", http.StatusSeeOther)
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" || strings.ContainsAny(name, "/\x00") {
		SetFlash(w, "error", "invalid directory name")
		http.Redirect(w, r, browseDirHref(at, p), http.StatusSeeOther)
		return
	}
	if err := os.Mkdir(filepath.Join(abs, name), 0o755); err != nil {
		SetFlash(w, "error", "mkdir: "+err.Error())
		http.Redirect(w, r, browseDirHref(at, p), http.StatusSeeOther)
		return
	}
	SetFlash(w, "ok", "created "+name)
	http.Redirect(w, r, browseDirHref(at, p), http.StatusSeeOther)
}

// ── POST /browse/upload (multipart) ───────────────────────────────

func (s *Server) browseUpload(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	// Cap the request body before any parsing.
	r.Body = http.MaxBytesReader(w, r.Body, browseMaxUploadBytes)
	at, p, abs, err := s.resolveBrowsePath(r.FormValue("at"), r.FormValue("p"))
	if err != nil {
		SetFlash(w, "error", err.Error())
		http.Redirect(w, r, "/browse", http.StatusSeeOther)
		return
	}
	file, hdr, err := r.FormFile("file")
	if err != nil {
		SetFlash(w, "error", "upload: "+err.Error())
		http.Redirect(w, r, browseDirHref(at, p), http.StatusSeeOther)
		return
	}
	defer file.Close()
	// Reject path-traversal in the original filename.
	name := filepath.Base(hdr.Filename)
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
		SetFlash(w, "error", "invalid filename")
		http.Redirect(w, r, browseDirHref(at, p), http.StatusSeeOther)
		return
	}
	dst := filepath.Join(abs, name)
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		SetFlash(w, "error", "create: "+err.Error())
		http.Redirect(w, r, browseDirHref(at, p), http.StatusSeeOther)
		return
	}
	n, err := io.Copy(out, file)
	closeErr := out.Close()
	if err != nil {
		_ = os.Remove(dst) // partial write — clean up
		SetFlash(w, "error", "write: "+err.Error())
		http.Redirect(w, r, browseDirHref(at, p), http.StatusSeeOther)
		return
	}
	if closeErr != nil {
		SetFlash(w, "error", "close: "+closeErr.Error())
		http.Redirect(w, r, browseDirHref(at, p), http.StatusSeeOther)
		return
	}
	SetFlash(w, "ok", fmt.Sprintf("uploaded %s (%d bytes)", name, n))
	http.Redirect(w, r, browseDirHref(at, p), http.StatusSeeOther)
}

// ── POST /browse/delete ───────────────────────────────────────────

func (s *Server) browseDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.state.CheckCSRF(r); err != nil {
		http.Error(w, "csrf: "+err.Error(), http.StatusForbidden)
		return
	}
	at, p, abs, err := s.resolveBrowsePath(r.FormValue("at"), r.FormValue("p"))
	if err != nil {
		SetFlash(w, "error", err.Error())
		http.Redirect(w, r, "/browse", http.StatusSeeOther)
		return
	}
	if p == "" || p == "." {
		SetFlash(w, "error", "refusing to delete the mountpoint root")
		http.Redirect(w, r, browseDirHref(at, p), http.StatusSeeOther)
		return
	}
	parent := path.Dir(p)
	if parent == "." {
		parent = ""
	}
	// os.Remove (NOT RemoveAll) — refuses non-empty directories. This is
	// a safety property we explicitly want: no accidental recursive
	// deletes from a single click in the web UI.
	if err := os.Remove(abs); err != nil {
		SetFlash(w, "error", "delete: "+err.Error())
		http.Redirect(w, r, browseDirHref(at, parent), http.StatusSeeOther)
		return
	}
	SetFlash(w, "ok", "deleted "+path.Base(p))
	http.Redirect(w, r, browseDirHref(at, parent), http.StatusSeeOther)
}

// resolveBrowsePath validates that `at` is a known TelFS mountpoint
// and that `p` resolves cleanly inside it. Returns the cleaned at, p
// and the resulting absolute path.
func (s *Server) resolveBrowsePath(at, p string) (string, string, string, error) {
	at = strings.TrimSpace(at)
	if at == "" {
		return "", "", "", errors.New("missing mountpoint (at)")
	}
	at = filepath.Clean(at)
	if !s.isKnownMount(at) {
		return "", "", "", errors.New("mountpoint is not a known TelFS mount")
	}
	// Path cleaning: treat empty/"/" as root, reject "..".
	if p == "/" {
		p = ""
	}
	clean := path.Clean("/" + p) // forces a leading slash and removes "."s
	if strings.HasPrefix(clean, "/..") || clean == "/.." {
		return "", "", "", errors.New("invalid path")
	}
	rel := strings.TrimPrefix(clean, "/")
	abs := filepath.Join(at, rel)
	// Belt-and-suspenders: confirm abs is still under at after Join.
	relCheck, err := filepath.Rel(at, abs)
	if err != nil || strings.HasPrefix(relCheck, "..") {
		return "", "", "", errors.New("path escapes mountpoint")
	}
	return at, rel, abs, nil
}

// isKnownMount returns true if `at` is one of our supervised mountpoints
// or appears in /proc/mounts as fuse.telfs. We re-scan on every call
// because mountpoints can come and go.
func (s *Server) isKnownMount(at string) bool {
	for _, mp := range s.sup.List() {
		if mp.Mountpoint == at {
			return true
		}
	}
	for _, ext := range scanProcMounts() {
		if ext == at {
			return true
		}
	}
	return false
}

func browseDirHref(at, p string) string {
	q := url.Values{}
	q.Set("at", at)
	if p != "" {
		q.Set("p", p)
	}
	return "/browse/dir?" + q.Encode()
}

func browseFileHref(at, p string) string {
	q := url.Values{}
	q.Set("at", at)
	q.Set("p", p)
	return "/browse/file?" + q.Encode()
}
