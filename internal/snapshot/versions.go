package snapshot

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"telfs/internal/meta"
	"telfs/internal/wire"
)

// Versions (spec v1 §2, "Versions", decision 13 row 3 A): a version is a
// whole-DB snapshot posted under a name and kept until it is deleted —
// immutable by construction (a sealed message nothing edits). The
// filesystem's versions are listed in the local registry (meta_kv
// KVVersions) and in the pinned anchor, which is re-posted after every
// version change, so a recovery restores the registry from the anchor.
// v1 has no reclamation: nothing a version references is ever deleted
// (telfs gc refuses on a filesystem with versions). Format 2 only.

// KVVersions is the version registry: a JSON list of wire.AnchorVersion,
// oldest first.
const KVVersions = "versions"

// KVAnchorDue, when set, says the pinned anchor's version list is out of
// date (a version change whose anchor post failed): the policy loop
// re-posts the anchor. PostAnchor clears it.
const KVAnchorDue = "anchor_due"

// VersionMaxName is the longest version name.
const VersionMaxName = 64

var versionNameRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// ValidateVersionName checks a version name: 1–64 characters of
// [A-Za-z0-9._-], not "." or "..". (A name becomes a file name in the
// profile's versions directory and a field of the mount source.)
func ValidateVersionName(name string) error {
	if !versionNameRE.MatchString(name) || name == "." || name == ".." {
		return fmt.Errorf("invalid version name %q: use 1-%d characters of A-Z a-z 0-9 . _ - (not \".\" or \"..\")", name, VersionMaxName)
	}
	return nil
}

// readErr remembers the error its reader failed with (io.EOF aside).
type readErr struct {
	r   io.Reader
	err error
}

func (r *readErr) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if err != nil && err != io.EOF {
		r.err = err
	}
	return n, err
}

// Errors of the version operations.
var (
	// ErrVersionIntegrity: a downloaded version does not decrypt, or is
	// not the DB the registry describes (size, SHA-256, no filesystem id)
	// — an integrity problem (exit 7), not a reason to retry.
	ErrVersionIntegrity = errors.New("the version's data is damaged or not what the registry says")
	ErrVersionExists    = errors.New("a version with this name already exists")
	ErrNoVersion        = errors.New("no version with this name")
	ErrVersionFormat1   = errors.New("versions need a format-2 filesystem (format 1 keeps working as in v0.22, without versions; move the data to a new filesystem made by `telfs init`)")
	// ErrChannel wraps a failure of Telegram or the channel (exit 6 in
	// the v1 interface).
	ErrChannel = errors.New("Telegram or the channel failed")
)

// LoadVersions reads the registry (nil when there is none).
func LoadVersions(ctx context.Context, m *meta.Store) ([]wire.AnchorVersion, error) {
	b, err := m.GetKV(ctx, KVVersions)
	if errors.Is(err, meta.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return DecodeVersions(b)
}

// DecodeVersions parses a registry value.
func DecodeVersions(b []byte) ([]wire.AnchorVersion, error) {
	var vs []wire.AnchorVersion
	if len(bytes.TrimSpace(b)) == 0 {
		return nil, nil
	}
	if err := json.Unmarshal(b, &vs); err != nil {
		return nil, fmt.Errorf("version registry: %w", err)
	}
	return vs, nil
}

// EncodeVersions is the registry value for vs ("[]" when empty).
func EncodeVersions(vs []wire.AnchorVersion) ([]byte, error) {
	if vs == nil {
		vs = []wire.AnchorVersion{}
	}
	return json.Marshal(vs)
}

// StoreVersions replaces the registry.
func StoreVersions(ctx context.Context, m *meta.Store, vs []wire.AnchorVersion) error {
	b, err := EncodeVersions(vs)
	if err != nil {
		return err
	}
	return m.PutKV(ctx, KVVersions, b)
}

// FindVersion returns the entry named name.
func FindVersion(vs []wire.AnchorVersion, name string) (wire.AnchorVersion, bool) {
	for _, v := range vs {
		if v.Name == name {
			return v, true
		}
	}
	return wire.AnchorVersion{}, false
}

// versionsMu serializes registry changes in this process.
var versionsMu sync.Mutex

// UploadGate is the part of the daemon's upload pool a version looks at:
// registrations are numbered (chunk.UploadPool), and a file's size is
// committed only AFTER its chunks' uploads were registered.
type UploadGate interface {
	LastRegistered() uint64
	OpenUpTo(seq uint64) int
}

// ErrVersionIncomplete: the version could not be taken complete — uploads
// were in flight, or files have a size without all their chunks. Nothing
// was posted; the caller syncs again and retries (spec v1 §3: exit 4 when
// it never comes out complete).
var ErrVersionIncomplete = errors.New("the version would not be complete")

// IncompleteError says why a version was not taken.
type IncompleteError struct {
	Open  int      // uploads in flight when (or while) it was taken
	Files []string // files whose chunks do not cover their size (the first ones)
	Count int      // how many such files
}

func (e *IncompleteError) Error() string {
	if e.Count > 0 {
		more := ""
		if e.Count > len(e.Files) {
			more = fmt.Sprintf(" and %d more", e.Count-len(e.Files))
		}
		return fmt.Sprintf("%v: %d file(s) have a size without all their chunks (%s%s)", ErrVersionIncomplete, e.Count, strings.Join(e.Files, ", "), more)
	}
	return fmt.Sprintf("%v: %d upload(s) in flight while it was taken", ErrVersionIncomplete, e.Open)
}

func (e *IncompleteError) Unwrap() error { return ErrVersionIncomplete }

// incompleteListed is how many incomplete files an IncompleteError names.
const incompleteListed = 20

// takeVersion takes the version's snapshot only while nothing is in
// flight, and checks the copy: a version is immutable, and no journal
// follows it to fill in a chunk that lands later — a file caught with
// its new size and without (all) its new chunks would read as lost data,
// or as old blocks under a new size, forever. gate nil: no upload pool
// (the check of the copy still runs).
func takeVersion(ctx context.Context, m *meta.Store, gate UploadGate) (*Snapshot, error) {
	var r0 uint64
	if gate != nil {
		r0 = gate.LastRegistered()
		if open := gate.OpenUpTo(r0); open > 0 {
			return nil, &IncompleteError{Open: open}
		}
	}
	var inc *IncompleteError
	snap, err := takeChecked(ctx, m, func(ctx context.Context, path string) error {
		c, err := meta.OpenReadOnly(path, true)
		if err != nil {
			return err
		}
		defer c.Close()
		reps, err := c.ScanPartialFiles(ctx, nil)
		if err != nil {
			return err
		}
		dirs := map[int64]string{}
		for _, r := range reps {
			if r.NewSize >= r.OldSize {
				continue // stray rows past the end: nothing is missing
			}
			if inc == nil {
				inc = &IncompleteError{}
			}
			inc.Count++
			if len(inc.Files) < incompleteListed {
				p, ok := c.PathOfCached(ctx, r.Ino, dirs)
				if !ok {
					p = fmt.Sprintf("(ino %d)", r.Ino)
				}
				inc.Files = append(inc.Files, p)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// A registration made while the copy was taken: a file's new size
	// may be in it without the chunk that upload places.
	if gate != nil {
		if now := gate.LastRegistered(); now != r0 {
			return nil, &IncompleteError{Open: gate.OpenUpTo(now)}
		}
	}
	if inc != nil {
		return nil, inc
	}
	return snap, nil
}

// CreateVersion takes a snapshot of m and posts it as version name: the
// sealed body, a caption of kind "v", then the registry entry and an
// anchor listing it. The caller has synced first (everything written
// before the request is on the channel), so the version and the channel
// agree. The snapshot is taken only while no upload is in flight (gate)
// and no file of it lacks chunks; otherwise nothing is posted and the
// error is an *IncompleteError (ErrVersionIncomplete). The version is on
// the channel when this returns without error; warn is non-empty when the
// anchor could not be re-posted (it is marked due and the daemon's policy
// loop retries it).
func CreateVersion(ctx context.Context, ch Channel, m *meta.Store, keys *wire.Keys, name string, gate UploadGate, logf func(string, ...any)) (v wire.AnchorVersion, warn string, err error) {
	if err := ValidateVersionName(name); err != nil {
		return v, "", err
	}
	versionsMu.Lock()
	defer versionsMu.Unlock()
	if f, err := m.Format(ctx); err != nil {
		return v, "", err
	} else if f != 2 {
		return v, "", ErrVersionFormat1
	}
	vs, err := LoadVersions(ctx, m)
	if err != nil {
		return v, "", err
	}
	if _, dup := FindVersion(vs, name); dup {
		return v, "", fmt.Errorf("%w: %q (telfs versions list)", ErrVersionExists, name)
	}
	snap, err := takeVersion(ctx, m, gate)
	if err != nil {
		if errors.Is(err, ErrVersionIncomplete) {
			return v, "", err
		}
		return v, "", fmt.Errorf("take: %w", err)
	}
	v = wire.AnchorVersion{Name: name, Seq: snap.JournalSeq, Created: time.Now().Unix(),
		Size: snap.RawSize, SHA256: hex.EncodeToString(snap.RawSHA256)}
	body, capt, err := keys.SealVersion(v, snap.Bytes)
	if err != nil {
		return v, "", err
	}
	id, err := ch.UploadDocument(ctx, bytes.NewReader(body), wire.DocName, capt)
	if err != nil {
		return v, "", fmt.Errorf("%w: upload version %q: %w", ErrChannel, name, err)
	}
	v.Msg = id
	// The version is on the channel: record it whatever ctx does now
	// (a stop must not leave an unlisted version message, which would
	// block gc for good), with the mark that the anchor does not list it
	// yet IN THE SAME transaction — a stop or crash before the anchor is
	// posted leaves the mark, and the policy loop posts it.
	bg := context.WithoutCancel(ctx)
	if err := storeVersionsDue(bg, m, append(vs, v)); err != nil {
		// Unrecorded, the message would be a version nobody lists.
		_ = ch.DeleteChannelMessages(bg, id)
		return v, "", fmt.Errorf("record version %q: %w", name, err)
	}
	return v, repostAnchor(bg, ch, m, keys, logf), nil
}

// storeVersionsDue replaces the registry and marks the anchor due, in one
// transaction (PostAnchor clears the mark once an anchor lists it).
func storeVersionsDue(ctx context.Context, m *meta.Store, vs []wire.AnchorVersion) error {
	b, err := EncodeVersions(vs)
	if err != nil {
		return err
	}
	return m.UpdateKV(ctx, func(k *meta.KVTx) error {
		if err := k.Put(KVVersions, b); err != nil {
			return err
		}
		return k.Put(KVAnchorDue, []byte("the version list changed"))
	})
}

// DeleteVersion deletes version name: its message, then its registry
// entry, then a new anchor without it. warn as for CreateVersion.
func DeleteVersion(ctx context.Context, ch Channel, m *meta.Store, keys *wire.Keys, name string, logf func(string, ...any)) (v wire.AnchorVersion, warn string, err error) {
	versionsMu.Lock()
	defer versionsMu.Unlock()
	vs, err := LoadVersions(ctx, m)
	if err != nil {
		return v, "", err
	}
	v, ok := FindVersion(vs, name)
	if !ok {
		return v, "", fmt.Errorf("%w: %q (telfs versions list)", ErrNoVersion, name)
	}
	if err := ch.DeleteChannelMessages(ctx, v.Msg); err != nil {
		return v, "", fmt.Errorf("%w: delete version %q (message %d): %w", ErrChannel, name, v.Msg, err)
	}
	kept := make([]wire.AnchorVersion, 0, len(vs))
	for _, x := range vs {
		if x.Name != name {
			kept = append(kept, x)
		}
	}
	bg := context.WithoutCancel(ctx)
	if err := storeVersionsDue(bg, m, kept); err != nil {
		return v, "", fmt.Errorf("version %q: its message is deleted, the registry is not updated: %w", name, err)
	}
	return v, repostAnchor(bg, ch, m, keys, logf), nil
}

// repostAnchor posts an anchor carrying the current registry (the caller
// has marked it due; a successful post clears the mark) and returns the
// warning to show when it fails (the policy loop re-posts it).
func repostAnchor(ctx context.Context, ch Channel, m *meta.Store, keys *wire.Keys, logf func(string, ...any)) string {
	if _, err := PostAnchor(ctx, ch, m, keys, logf); err != nil {
		return fmt.Sprintf("the pinned anchor does not list the change yet (the mount re-posts it): %v", err)
	}
	return ""
}

// VersionFileMatches reports whether path holds version v's DB: same
// size and, when v records it, the same SHA-256.
func VersionFileMatches(path string, v wire.AnchorVersion) bool {
	fi, err := os.Stat(path)
	if err != nil || !fi.Mode().IsRegular() || v.Size <= 0 || fi.Size() != v.Size {
		return false
	}
	if v.SHA256 == "" {
		return true
	}
	sum, err := fileSHA256(path)
	return err == nil && sum == v.SHA256
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// DownloadVersion fetches version v from the channel into dbPath (an
// atomic replace), byte for byte the DB the version was taken from:
// checked against the registry's size and SHA-256 and opened read-only
// before it replaces anything (unlike Restore, nothing rewrites it — a
// version mount opens it immutable, so the copy keeps matching).
func DownloadVersion(ctx context.Context, ch Channel, keys *wire.Keys, v wire.AnchorVersion, dbPath string) error {
	var buf bytes.Buffer
	if _, err := ch.DownloadDocument(ctx, v.Msg, &buf); err != nil {
		return fmt.Errorf("%w: download version %q (message %d): %w", ErrChannel, v.Name, v.Msg, err)
	}
	gz, err := keys.OpenVersion(buf.Bytes())
	if err != nil {
		return fmt.Errorf("%w: version %q (message %d): %w", ErrVersionIntegrity, v.Name, v.Msg, err)
	}
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		return err
	}
	tmp := dbPath + ".download"
	defer os.Remove(tmp)
	if err := writeGunzipped(gz, tmp, v); err != nil {
		return fmt.Errorf("version %q: %w", v.Name, err)
	}
	vm, err := meta.OpenReadOnly(tmp, true)
	if err != nil {
		return fmt.Errorf("%w: version %q: %w", ErrVersionIntegrity, v.Name, err)
	}
	_, err = vm.FSUUID(ctx)
	vm.Close()
	if err != nil {
		return fmt.Errorf("%w: version %q: no filesystem id in the DB: %w", ErrVersionIntegrity, v.Name, err)
	}
	for _, sidecar := range []string{dbPath + "-wal", dbPath + "-shm"} {
		if err := os.Remove(sidecar); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return os.Rename(tmp, dbPath)
}

// writeGunzipped writes the gunzipped gz to path and checks it against
// v's size and SHA-256 (when recorded).
func writeGunzipped(gz []byte, path string, v wire.AnchorVersion) error {
	gr, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		return fmt.Errorf("%w: gunzip: %w", ErrVersionIntegrity, err)
	}
	defer gr.Close()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	h := sha256.New()
	rr := &readErr{r: gr}
	n, err := io.Copy(io.MultiWriter(f, h), rr)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		if rr.err != nil {
			// The gzip stream itself is damaged, not the local disk.
			return fmt.Errorf("%w: gunzip: %w", ErrVersionIntegrity, rr.err)
		}
		return fmt.Errorf("write: %w", err)
	}
	if v.Size > 0 && n != v.Size {
		return fmt.Errorf("%w: the DB has %d bytes, the registry says %d", ErrVersionIntegrity, n, v.Size)
	}
	if v.SHA256 != "" && hex.EncodeToString(h.Sum(nil)) != v.SHA256 {
		return fmt.Errorf("%w: the DB's SHA-256 differs from the registry's", ErrVersionIntegrity)
	}
	return nil
}
