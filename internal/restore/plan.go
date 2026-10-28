// Package restore is `telfs restore` (spec v1 row 6 + addition 4): a
// bulk copy of a tree of a format-2 filesystem — the live one or a
// version — onto a local directory. It reads chunk placements from one
// consistent read of the DB, downloads every needed channel message ONCE
// (a pack as one ranged read over the extents it needs, a direct chunk
// as its whole document) over a small worker pool, writes files,
// directories, symlinks, hard links, modes, mtimes and user.* xattrs,
// then re-reads every restored file and checks each chunk against its
// keyed content hash.
package restore

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"telfs/internal/meta"
	"telfs/internal/trash"
)

// Entry is one name of the restored tree.
type Entry struct {
	// Path is the entry's path inside the filesystem ("/a/b").
	Path string
	// Rel is its slash path under DEST ("" = DEST itself).
	Rel   string
	Inode meta.Inode
	// Xattrs are the inode's extended attributes.
	Xattrs []meta.Xattr
	// LinkOf >= 0 makes this name a hard link of Entries[LinkOf] (the
	// inode's first name in the tree, which holds the data).
	LinkOf int
	// Chunks are a data-holding regular file's chunk rows that start
	// inside the file (idx order); rows past the end are not restored.
	Chunks []meta.Chunk
	// Missing are chunk slots inside the file without a row, and Short
	// rows that hold fewer bytes than their slot — both only for a
	// non-sparse file, where the mount reports them as lost data.
	Missing, Short []int32
}

// Plan is the restore of SRC as read from the DB.
type Plan struct {
	Src     string
	Entries []Entry
	// Stray counts chunk rows past the end of their file (ignored).
	Stray int
	// Problems found while planning (names that cannot be restored).
	Problems []Problem
}

// Totals counts a plan's entries by kind.
func (p *Plan) Totals() (dirs, files, symlinks, links int, bytes int64) {
	for _, e := range p.Entries {
		switch {
		case e.LinkOf >= 0:
			links++
		case e.Inode.Kind == meta.KindDir:
			dirs++
		case e.Inode.Kind == meta.KindSymlink:
			symlinks++
		default:
			files++
			bytes += e.Inode.Size
		}
	}
	return
}

// ErrNotFound: SRC names nothing in the filesystem.
var ErrNotFound = errors.New("no such path in the filesystem")

// BuildPlan reads what restoring src needs from m, in one consistent
// read (it may run beside the live mount). src is an absolute path in
// the filesystem: a directory's CONTENTS become DEST's, any other entry
// is restored as DEST/<its name>. /.trash (the rm safety net) is not
// restored with "/"; name it to restore it.
func BuildPlan(ctx context.Context, m *meta.Store, src string) (*Plan, error) {
	if !strings.HasPrefix(src, "/") {
		return nil, fmt.Errorf("restore: SRC %q must be an absolute path inside the filesystem (e.g. / or /home)", src)
	}
	src = path.Clean(src)
	p := &Plan{Src: src}
	err := m.Read(ctx, func(v *meta.ReadView) error {
		root, err := v.GetInode(ctx, meta.RootIno)
		if err != nil {
			return fmt.Errorf("root inode: %w", err)
		}
		in := root
		if src != "/" {
			for _, name := range strings.Split(strings.TrimPrefix(src, "/"), "/") {
				if in.Kind != meta.KindDir {
					return fmt.Errorf("%w: %s", ErrNotFound, src)
				}
				next, err := v.Lookup(ctx, in.Ino, name)
				if errors.Is(err, meta.ErrNotFound) {
					return fmt.Errorf("%w: %s", ErrNotFound, src)
				}
				if err != nil {
					return err
				}
				in = next
			}
		}
		xattrs, err := v.AllXattrs(ctx)
		if err != nil {
			return err
		}
		b := &planner{ctx: ctx, v: v, p: p, xattrs: xattrs, first: map[int64]int{}}
		if in.Kind != meta.KindDir {
			return b.add(src, path.Base(src), in)
		}
		if err := b.add(src, "", in); err != nil {
			return err
		}
		return b.walk(src, "", in.Ino)
	})
	if err != nil {
		return nil, err
	}
	return p, nil
}

type planner struct {
	ctx    context.Context
	v      *meta.ReadView
	p      *Plan
	xattrs map[int64][]meta.Xattr
	first  map[int64]int // inode → its first entry (hard links)
}

// walk adds a directory's subtree, depth first, by name: every parent
// comes before its children.
func (b *planner) walk(fsPath, rel string, dir int64) error {
	kids, err := b.v.Children(b.ctx, dir)
	if err != nil {
		return err
	}
	for _, k := range kids {
		if dir == meta.RootIno && k.Name == trash.DirName {
			continue
		}
		childPath := fsPath + "/" + k.Name // not path.Join: a bad name is reported as it is
		if fsPath == "/" {
			childPath = "/" + k.Name
		}
		if !safeName(k.Name) {
			b.p.Problems = append(b.p.Problems, Problem{Kind: KindIntegrity, Path: childPath,
				Detail: fmt.Sprintf("the DB names an entry %q of directory %s: not a file name, not restored", k.Name, fsPath)})
			continue
		}
		childRel := k.Name
		if rel != "" {
			childRel = rel + "/" + k.Name
		}
		if err := b.add(childPath, childRel, k.Inode); err != nil {
			return err
		}
		if k.Kind == meta.KindDir {
			if err := b.walk(childPath, childRel, k.Ino); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *planner) add(fsPath, rel string, in meta.Inode) error {
	e := Entry{Path: fsPath, Rel: rel, Inode: in, LinkOf: -1}
	switch in.Kind {
	case meta.KindDir, meta.KindSymlink:
	case meta.KindFile:
		if i, ok := b.first[in.Ino]; ok {
			e.LinkOf = i
			b.p.Entries = append(b.p.Entries, e)
			return nil
		}
		b.first[in.Ino] = len(b.p.Entries)
		rows, err := b.v.Chunks(b.ctx, in.Ino)
		if err != nil {
			return err
		}
		if err := b.chunks(&e, rows); err != nil {
			return err
		}
	default:
		b.p.Problems = append(b.p.Problems, Problem{Kind: KindIntegrity, Path: fsPath,
			Detail: fmt.Sprintf("unknown inode kind %q, not restored", in.Kind)})
		return nil
	}
	e.Xattrs = b.xattrs[in.Ino]
	b.p.Entries = append(b.p.Entries, e)
	return nil
}

// chunks keeps the rows inside the file and finds the slots the mount
// would report as lost (non-sparse files only: a sparse file's missing
// slot is a hole of zeros, a short row is zero-padded).
func (b *planner) chunks(e *Entry, rows []meta.Chunk) error {
	in := e.Inode
	if in.Size > 0 && in.ChunkSize <= 0 {
		return fmt.Errorf("%s: inode %d has chunk size %d", e.Path, in.Ino, in.ChunkSize)
	}
	have := map[int32]bool{}
	for _, c := range rows {
		off := int64(c.Idx) * in.ChunkSize
		if c.Idx < 0 || off >= in.Size {
			b.p.Stray++
			continue
		}
		e.Chunks = append(e.Chunks, c)
		have[c.Idx] = true
		if !in.Sparse && int64(c.Size) < slotLen(in, c.Idx) {
			e.Short = append(e.Short, c.Idx)
		}
	}
	if in.Sparse || in.Size == 0 {
		return nil
	}
	n := (in.Size + in.ChunkSize - 1) / in.ChunkSize
	for i := int64(0); i < n; i++ {
		if !have[int32(i)] {
			e.Missing = append(e.Missing, int32(i))
		}
	}
	return nil
}

// slotLen is how many bytes of the file chunk idx covers.
func slotLen(in meta.Inode, idx int32) int64 {
	off := int64(idx) * in.ChunkSize
	n := in.Size - off
	if n > in.ChunkSize {
		n = in.ChunkSize
	}
	if n < 0 {
		n = 0
	}
	return n
}

// safeName: a single path component that stays inside its directory.
func safeName(n string) bool {
	return n != "" && n != "." && n != ".." && !strings.ContainsAny(n, "/\x00")
}
