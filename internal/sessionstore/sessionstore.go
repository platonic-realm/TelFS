// Package sessionstore keeps the gotd MTProto session encrypted at rest
// (feature 6, decision D5). The session's auth key is a full login to
// the Telegram account — and so to the channel — so session.json in the
// clear would hand the account to anyone who copies the profile folder
// (backups, snapshots).
//
// File (session.enc):
//
//	"TFS2S" ‖ version(1) ‖ uint16 header length ‖ header JSON ‖ nonce ‖ ciphertext ‖ tag
//
// header = {kdf, t, m, p, salt, n}; n counts the writes (the nonce
// limit, P1 §1). The ciphertext is AES-256-GCM under
// HKDF(Argon2id(secret, salt, params), "telfs2 session"), with
// everything before the nonce as AAD.
//
// The session key does NOT come from the filesystem's DEK: the DEK is
// only reachable through the DB or the channel, and reading the channel
// needs the session. So the file carries its own salt and opens with the
// secret alone.
//
// Writes are atomic (temp file, fsync, rename, fsync dir). gotd's own
// FileStorage rewrites session.json in place on every reconnect, so a
// crash mid-write left a torn file and a forced re-login.
package sessionstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/gotd/td/session"

	"telfs/internal/crypto"
)

const (
	magic   = "TFS2S"
	version = 1
	aadLbl  = "telfs2 session file"
)

// ErrLegacy: a plaintext session.json from TelFS v0.22 or older sits in
// the profile and session.enc doesn't exist.
var ErrLegacy = errors.New("this profile has a plaintext session.json from an older TelFS — run `telfs session encrypt` to convert it (or log in again)")

type header struct {
	KDF  string `json:"kdf"`
	T    uint32 `json:"t"`
	M    uint32 `json:"m"`
	P    uint8  `json:"p"`
	Salt []byte `json:"salt"`
	N    uint64 `json:"n"`
}

// Store is a gotd session.Storage over session.enc. It holds only the
// derived session key, never the secret.
type Store struct {
	path string
	mu   sync.Mutex
	key  *crypto.Box
	hdr  header
}

var _ session.Storage = (*Store)(nil)

// Open prepares the store at path. If the file exists, the secret is
// checked against it now (wrong secret → crypto.ErrWrongSecret before
// anything talks to Telegram). If it doesn't, a fresh salt is made and
// the first StoreSession creates the file with params. Costs one
// Argon2id derivation either way.
func Open(path string, secret []byte, params crypto.ArgonParams) (*Store, error) {
	s := &Store{path: path}
	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		hdr, _, _, perr := parse(raw)
		if perr != nil {
			return nil, perr
		}
		s.hdr = hdr
		if err := s.derive(secret); err != nil {
			return nil, err
		}
		if _, err := s.decrypt(raw); err != nil {
			return nil, err
		}
		return s, nil
	case errors.Is(err, os.ErrNotExist):
		salt, serr := crypto.NewSalt()
		if serr != nil {
			return nil, serr
		}
		s.hdr = header{KDF: crypto.KDFName, T: params.Time, M: params.Memory, P: params.Threads, Salt: salt}
		if err := s.derive(secret); err != nil {
			return nil, err
		}
		return s, nil
	default:
		return nil, fmt.Errorf("session.enc: %w", err)
	}
}

func (s *Store) derive(secret []byte) error {
	if s.hdr.KDF != crypto.KDFName {
		return fmt.Errorf("session.enc: unknown kdf %q", s.hdr.KDF)
	}
	root := crypto.DeriveKey(secret, s.hdr.Salt, crypto.ArgonParams{Time: s.hdr.T, Memory: s.hdr.M, Threads: s.hdr.P})
	defer zero(root)
	k, err := crypto.DeriveSubkey(root, crypto.LabelSession)
	if err != nil {
		return err
	}
	defer zero(k)
	box, err := crypto.NewBox(k)
	if err != nil {
		return err
	}
	s.key = box
	return nil
}

func parse(raw []byte) (hdr header, aad, sealed []byte, err error) {
	if len(raw) < len(magic)+3 || string(raw[:len(magic)]) != magic {
		return hdr, nil, nil, errors.New("session.enc: not a TelFS session file")
	}
	if raw[len(magic)] != version {
		return hdr, nil, nil, fmt.Errorf("session.enc: unsupported version %d", raw[len(magic)])
	}
	hl := int(binary.BigEndian.Uint16(raw[len(magic)+1:]))
	end := len(magic) + 3 + hl
	if end > len(raw) {
		return hdr, nil, nil, errors.New("session.enc: truncated header")
	}
	if err := json.Unmarshal(raw[len(magic)+3:end], &hdr); err != nil {
		return hdr, nil, nil, fmt.Errorf("session.enc: header: %w", err)
	}
	return hdr, raw[:end], raw[end:], nil
}

func (s *Store) decrypt(raw []byte) ([]byte, error) {
	_, aad, sealed, err := parse(raw)
	if err != nil {
		return nil, err
	}
	pt, err := s.key.Open(sealed, append([]byte(aadLbl), aad...))
	if err != nil {
		return nil, fmt.Errorf("session.enc: %w", crypto.ErrWrongSecret)
	}
	return pt, nil
}

// LoadSession implements session.Storage.
func (s *Store) LoadSession(_ context.Context) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, session.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read session.enc: %w", err)
	}
	return s.decrypt(raw)
}

// StoreSession implements session.Storage: seal under a fresh nonce,
// write atomically.
func (s *Store) StoreSession(_ context.Context, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hdr.N >= crypto.NonceLimit {
		return crypto.ErrNonceLimit
	}
	s.hdr.N++
	hj, err := json.Marshal(s.hdr)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	buf.WriteString(magic)
	buf.WriteByte(version)
	_ = binary.Write(&buf, binary.BigEndian, uint16(len(hj)))
	buf.Write(hj)
	aad := append([]byte(aadLbl), buf.Bytes()...)
	sealed, err := s.key.Seal(data, aad)
	if err != nil {
		return err
	}
	buf.Write(sealed)
	return WriteAtomic(s.path, buf.Bytes(), 0o600)
}

// Path returns the file this store writes.
func (s *Store) Path() string { return s.path }

// WriteAtomic writes data to path through a temp file in the same
// directory: write, fsync, rename, fsync the directory. A crash leaves
// either the old file or the new one, never a torn one.
func WriteAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	ok := false
	defer func() {
		if !ok {
			os.Remove(tmp)
		}
	}()
	if err := f.Chmod(perm); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	ok = true
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}

// Convert encrypts a v0.22 plaintext session.json into encPath with the
// secret and removes the plaintext file. The plaintext is checked to be
// gotd's session JSON first, so a random file isn't wrapped by mistake.
func Convert(legacyPath, encPath string, secret []byte, params crypto.ArgonParams) error {
	if _, err := os.Stat(encPath); err == nil {
		return fmt.Errorf("%s already exists — nothing to convert", encPath)
	}
	raw, err := os.ReadFile(legacyPath)
	if err != nil {
		return err
	}
	defer zero(raw)
	var probe struct {
		Version int
		Data    json.RawMessage
	}
	if err := json.Unmarshal(raw, &probe); err != nil || probe.Version == 0 || len(probe.Data) == 0 {
		return fmt.Errorf("%s does not look like a gotd session file", legacyPath)
	}
	st, err := Open(encPath, secret, params)
	if err != nil {
		return err
	}
	if err := st.StoreSession(context.Background(), raw); err != nil {
		return err
	}
	// Prove the new file opens before the old one goes.
	back, err := st.LoadSession(context.Background())
	if err != nil || !bytes.Equal(back, raw) {
		os.Remove(encPath)
		return fmt.Errorf("verify %s after writing: %v", encPath, err)
	}
	zero(back)
	return os.Remove(legacyPath)
}

func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
