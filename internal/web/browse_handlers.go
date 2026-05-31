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
	AtURL    string // /browse/dir?at=<at> — the mountpoint root
	Crumbs   []browseCrumb
	UploadOK bool // false if the dir is read-only at the FS level (best-effort hint)
}

// browseCrumb is one path segment in the breadcrumb. Href is empty
// for the last (current) segment so the template renders it as plain
// text instead of a link.
type browseCrumb struct {
	Name string
	Href string
}

type browseEntry struct {
	Name       string
	IsDir      bool
	IsLink     bool
	Size       int64
	Mtime      string
	HrefDir    string // /browse/dir?at=...&p=...
	HrefFile   string // /browse/file?at=...&p=...
	LinkTarget string // populated when IsLink — best-effort os.Readlink
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
		AtURL:    browseDirHref(at, ""),
		UploadOK: true,
	}
	if !d.IsRoot {
		parent := path.Dir(p)
		if parent == "." {
			parent = ""
		}
		d.Parent = parent
		d.UpURL = browseDirHref(at, parent)
		// Build segment crumbs: each segment links to /browse/dir at
		// that prefix; the trailing segment renders without a link.
		segs := strings.Split(p, "/")
		acc := ""
		for i, seg := range segs {
			if seg == "" {
				continue
			}
			if acc == "" {
				acc = seg
			} else {
				acc = acc + "/" + seg
			}
			c := browseCrumb{Name: seg}
			if i < len(segs)-1 {
				c.Href = browseDirHref(at, acc)
			}
			d.Crumbs = append(d.Crumbs, c)
		}
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
		if ent.IsLink {
			if tgt, err := os.Readlink(filepath.Join(abs, e.Name())); err == nil {
				ent.LinkTarget = tgt
			}
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
	// Default to os.Remove — refuses non-empty directories. Recursive
	// delete requires the explicit `recursive=1` form value, which the
	// template asks for in a second confirm dialog. This keeps a single
	// click from wiping a populated tree.
	recursive := r.FormValue("recursive") == "1"
	info, statErr := os.Stat(abs)
	if statErr != nil {
		SetFlash(w, "error", "stat: "+statErr.Error())
		http.Redirect(w, r, browseDirHref(at, parent), http.StatusSeeOther)
		return
	}
	if recursive {
		if !info.IsDir() {
			SetFlash(w, "error", "recursive delete only applies to directories")
			http.Redirect(w, r, browseDirHref(at, parent), http.StatusSeeOther)
			return
		}
		if err := os.RemoveAll(abs); err != nil {
			SetFlash(w, "error", "delete (recursive): "+err.Error())
			http.Redirect(w, r, browseDirHref(at, parent), http.StatusSeeOther)
			return
		}
		SetFlash(w, "ok", "deleted directory "+path.Base(p)+" (recursive)")
		http.Redirect(w, r, browseDirHref(at, parent), http.StatusSeeOther)
		return
	}
	if err := os.Remove(abs); err != nil {
		// Hint at the recursive form when the dir is non-empty.
		if info.IsDir() {
			SetFlash(w, "error", "directory not empty — re-submit with the recursive option")
		} else {
			SetFlash(w, "error", "delete: "+err.Error())
		}
		http.Redirect(w, r, browseDirHref(at, parent), http.StatusSeeOther)
		return
	}
	SetFlash(w, "ok", "deleted "+path.Base(p))
	http.Redirect(w, r, browseDirHref(at, parent), http.StatusSeeOther)
}

// ── POST /browse/rename ───────────────────────────────────────────

func (s *Server) browseRename(w http.ResponseWriter, r *http.Request) {
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
	parent := path.Dir(p)
	if parent == "." {
		parent = ""
	}
	if p == "" || p == "." {
		SetFlash(w, "error", "refusing to rename the mountpoint root")
		http.Redirect(w, r, browseDirHref(at, p), http.StatusSeeOther)
		return
	}
	newName := strings.TrimSpace(r.FormValue("new_name"))
	if newName == "" || strings.ContainsAny(newName, "/\x00") {
		SetFlash(w, "error", "invalid new name")
		http.Redirect(w, r, browseDirHref(at, parent), http.StatusSeeOther)
		return
	}
	dst := filepath.Join(filepath.Dir(abs), newName)
	// Belt-and-suspenders: dst must stay under the same mountpoint root.
	if relCheck, err := filepath.Rel(at, dst); err != nil || strings.HasPrefix(relCheck, "..") {
		SetFlash(w, "error", "invalid target path")
		http.Redirect(w, r, browseDirHref(at, parent), http.StatusSeeOther)
		return
	}
	if _, err := os.Lstat(dst); err == nil {
		SetFlash(w, "error", "target already exists: "+newName)
		http.Redirect(w, r, browseDirHref(at, parent), http.StatusSeeOther)
		return
	}
	if err := os.Rename(abs, dst); err != nil {
		SetFlash(w, "error", "rename: "+err.Error())
		http.Redirect(w, r, browseDirHref(at, parent), http.StatusSeeOther)
		return
	}
	SetFlash(w, "ok", "renamed "+path.Base(p)+" → "+newName)
	http.Redirect(w, r, browseDirHref(at, parent), http.StatusSeeOther)
}

// ── POST /browse/newfile ──────────────────────────────────────────

func (s *Server) browseNewFile(w http.ResponseWriter, r *http.Request) {
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
		SetFlash(w, "error", "invalid file name")
		http.Redirect(w, r, browseDirHref(at, p), http.StatusSeeOther)
		return
	}
	dst := filepath.Join(abs, name)
	if _, err := os.Lstat(dst); err == nil {
		SetFlash(w, "error", "already exists: "+name)
		http.Redirect(w, r, browseDirHref(at, p), http.StatusSeeOther)
		return
	}
	// O_EXCL refuses to clobber an existing entry — second-line guard
	// alongside the Lstat above (covers a race between check and create).
	f, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		SetFlash(w, "error", "create: "+err.Error())
		http.Redirect(w, r, browseDirHref(at, p), http.StatusSeeOther)
		return
	}
	f.Close()
	SetFlash(w, "ok", "created "+name)
	http.Redirect(w, r, browseDirHref(at, p), http.StatusSeeOther)
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
