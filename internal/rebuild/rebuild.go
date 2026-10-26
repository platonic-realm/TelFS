// Package rebuild reconstructs a format-2 filesystem's metadata from the
// channel alone — no snapshot (feature 10, spec P1 §8). It is the last
// resort for "every snapshot is gone or unreadable":
//
//  1. Every chunk message's encrypted caption and every pack's encrypted
//     index say which inode/index a chunk belongs to, its hash, length,
//     the inode's chunk size, the file's size, mode, mtime and path at
//     upload (the name hint).
//  2. Journal batches still on the channel (unless ignored) are replayed
//     on the ORIGINAL inode numbers: they restore directories, symlinks,
//     empty files, hard links, renames, deletions and dedup `ref`s.
//  3. An inode with content but no name after the replay is placed at its
//     newest name hint, with rsync's temporary names mapped back.
//
// The result is a fresh local DB; the next mount posts a new snapshot.
package rebuild

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"

	"telfs/internal/meta"
	"telfs/internal/tg"
	"telfs/internal/wire"
)

// Message is one channel message as the rebuild sees it.
type Message struct {
	ID      int
	HasDoc  bool
	Caption string
}

// Source is the channel.
type Source interface {
	// Walk visits EVERY message of the channel (any order); it must not
	// stop early.
	Walk(ctx context.Context, fn func(Message) error) error
	ReadRange(ctx context.Context, msgID int, off, n int64) ([]byte, error)
	ReadAll(ctx context.Context, msgID int) ([]byte, error)
}

// Options control a rebuild.
type Options struct {
	// UseJournal replays the journal batches found (default true in the
	// command; false = captions only).
	UseJournal bool
	// Logf receives progress lines (may be nil).
	Logf func(format string, args ...any)
}

// Name sources in the report.
const (
	SrcJournal     = "journal"
	SrcHint        = "hint"
	SrcHintRsync   = "hint+rsync-mapped"
	SrcLostFound   = "lost+found"
	SrcLostFoundDu = "lost+found (name taken)"
)

// FileReport is one line of the report.
type FileReport struct {
	Path       string
	Ino        int64
	Size       int64
	Found      int // chunks found
	Expected   int // chunks the size implies
	NameSource string
	Notes      []string
}

// Stats summarizes a rebuild.
type Stats struct {
	Messages, ChunkCaptions, Packs, PackEntries, JournalBatches, Snapshots int
	JournalOps, JournalOpsSkipped                                          int
	Files, Incomplete, DeletedByJournal                                    int
}

// Result of a rebuild.
type Result struct {
	Files []FileReport
	Stats Stats
	// ChunkSizes / Codecs tally what the captions say, so the command can
	// set the rebuilt filesystem's defaults (most common chunk size; zstd
	// if any chunk used it).
	ChunkSizes map[int64]int
	Codecs     map[string]int
}

type chunkObs struct {
	msgID       int
	idx         int32
	hash        []byte
	length      int64
	packOff     int64
	packSize    int32
	cs, fsz     int64
	mode        uint32
	mtime, nl   int64
	name        string
	fromCaption bool
}

type inodeObs struct {
	ino    int64
	chunks map[int32]chunkObs // newest message per slot
	newest chunkObs           // facts from the newest message of this inode
}

type jbatch struct {
	msgID int
	cap   wire.Caption
}

// Run rebuilds into m, which must be a FRESH store; keys must be the
// filesystem's.
func Run(ctx context.Context, src Source, keys *wire.Keys, m *meta.Store, opt Options) (*Result, error) {
	logf := opt.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	res := &Result{ChunkSizes: map[int64]int{}, Codecs: map[string]int{}}
	inodes := map[int64]*inodeObs{}
	var batches []jbatch
	var packs []jbatch

	note := func(ino int64, o chunkObs) {
		in := inodes[ino]
		if in == nil {
			in = &inodeObs{ino: ino, chunks: map[int32]chunkObs{}}
			inodes[ino] = in
		}
		if prev, ok := in.chunks[o.idx]; !ok || o.msgID > prev.msgID {
			in.chunks[o.idx] = o
		}
		if o.msgID >= in.newest.msgID {
			in.newest = o
		}
	}

	logf("Reading every message's caption…")
	err := src.Walk(ctx, func(msg Message) error {
		res.Stats.Messages++
		if !msg.HasDoc || !wire.LooksLikeCaption(msg.Caption) {
			return nil
		}
		c, err := keys.OpenCaption(msg.Caption)
		if err != nil {
			return nil // not ours
		}
		switch c.K {
		case wire.KindChunk:
			res.Stats.ChunkCaptions++
			res.ChunkSizes[c.CS]++
			res.Codecs[c.Codec]++
			note(c.Ino, chunkObs{msgID: msg.ID, idx: c.Idx, hash: c.H, length: c.Len, cs: c.CS, fsz: c.FSz,
				mode: c.Mode, mtime: c.Mtime, nl: c.NL, name: c.Name, fromCaption: true})
		case wire.KindPack:
			res.Stats.Packs++
			packs = append(packs, jbatch{msgID: msg.ID, cap: c})
		case wire.KindJournal:
			res.Stats.JournalBatches++
			batches = append(batches, jbatch{msgID: msg.ID, cap: c})
		case wire.KindSnapshot:
			res.Stats.Snapshots++
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	logf("Reading %d pack index(es)…", len(packs))
	for _, p := range packs {
		raw, err := src.ReadRange(ctx, p.msgID, p.cap.IO, p.cap.IL)
		if err != nil {
			return nil, fmt.Errorf("pack %d index: %w", p.msgID, err)
		}
		entries, err := keys.OpenPackIndex(raw)
		if err != nil {
			return nil, fmt.Errorf("pack %d index: %w", p.msgID, err)
		}
		for _, e := range entries {
			res.Stats.PackEntries++
			res.Codecs[e.Codec]++
			note(e.Ino, chunkObs{msgID: p.msgID, idx: e.Idx, hash: e.H, length: e.Len, packOff: e.Off, packSize: e.WLen,
				cs: e.CS, fsz: e.FSz, mode: e.Mode, mtime: e.Mtime, nl: e.NL, name: e.Name})
		}
	}

	// The rebuilt journal continues after every sequence number already on
	// the channel — replayed or ignored — so its new batches never clash
	// with old ones (meta.AdvanceJournalSeq).
	var maxSeq int64
	for _, b := range batches {
		if b.cap.Last > maxSeq {
			maxSeq = b.cap.Last
		}
	}
	defer func() { _ = m.AdvanceJournalSeq(ctx, maxSeq) }()

	// New inode numbers (lost+found, folders made from hints) start
	// above every original one, so originals can keep their numbers.
	maxIno := meta.RootIno
	for ino := range inodes {
		if ino > maxIno {
			maxIno = ino
		}
	}

	// 2. Journal replay on the original inode numbers.
	refAt := map[slot]int{} // slot → journal batch message id of its newest ref
	created := map[int64]bool{}
	if opt.UseJournal && len(batches) > 0 {
		type seqOp struct {
			seq   int64
			msgID int
			op    meta.JournalOp
		}
		var ops []seqOp
		for _, b := range batches {
			raw, err := src.ReadAll(ctx, b.msgID)
			if err != nil {
				return nil, fmt.Errorf("journal batch %d: %w", b.msgID, err)
			}
			sealed, err := keys.OpenMeta("journal", raw)
			if err != nil {
				return nil, fmt.Errorf("journal batch %d: %w", b.msgID, err)
			}
			plain, err := wire.UnpackJournal(sealed)
			if err != nil {
				return nil, fmt.Errorf("journal batch %d: %w", b.msgID, err)
			}
			var body struct {
				Ops []tg.JournalBatchEntry `json:"ops"`
			}
			if err := json.Unmarshal(plain, &body); err != nil {
				return nil, fmt.Errorf("journal batch %d: %w", b.msgID, err)
			}
			for _, e := range body.Ops {
				op, err := meta.DecodeOp(e.Op)
				if err != nil {
					continue
				}
				ops = append(ops, seqOp{e.Seq, b.msgID, op})
			}
		}
		sort.SliceStable(ops, func(i, j int) bool { return ops[i].seq < ops[j].seq })
		seen := map[int64]bool{}
		dedup := ops[:0]
		for _, o := range ops {
			if !seen[o.seq] {
				seen[o.seq] = true
				dedup = append(dedup, o)
			}
		}
		ops = dedup
		if len(ops) > 0 {
			logf("Replaying %d journal op(s) (seq %d…%d)…", len(ops), ops[0].seq, ops[len(ops)-1].seq)
		}
		for _, o := range ops {
			if o.op.Ino > maxIno {
				maxIno = o.op.Ino
			}
			if err := m.ReplayOp(ctx, o.op); err != nil {
				res.Stats.JournalOpsSkipped++
				continue
			}
			res.Stats.JournalOps++
			switch o.op.Op {
			case meta.OpCreateChild:
				created[o.op.Ino] = true
			case meta.OpRef:
				refAt[slot{o.op.Ino, o.op.Idx}] = o.msgID
			}
		}
	}
	// Inodes the journal created and that are gone now were deleted:
	// their captions must not bring them back.
	deleted := map[int64]bool{}
	for ino := range created {
		if _, err := m.GetInode(ctx, ino); err != nil {
			deleted[ino] = true
		}
	}

	b := &builder{ctx: ctx, m: m, next: maxIno + 1, claims: map[string]claim{}}

	// 3. Content and names for every inode the captions describe.
	order := make([]int64, 0, len(inodes))
	for ino := range inodes {
		order = append(order, ino)
	}
	// Newer uploads first, so they win contested names.
	sort.Slice(order, func(i, j int) bool {
		return inodes[order[i]].newest.msgID > inodes[order[j]].newest.msgID
	})
	for _, ino := range order {
		in := inodes[ino]
		if deleted[ino] {
			res.Stats.DeletedByJournal++
			continue
		}
		rep := FileReport{Ino: ino}
		facts := in.newest
		cs := facts.cs
		if cs <= 0 {
			cs = meta.DefaultChunkSize
		}
		size := int64(0)
		for _, o := range in.chunks {
			if o.fsz > size {
				size = o.fsz
			}
			if end := int64(o.idx)*cs + o.length; end > size {
				size = end
			}
		}
		existing, gerr := m.GetInode(ctx, ino)
		if gerr == nil && facts.cs > 0 && existing.ChunkSize != facts.cs {
			// The chunks were written at the size their captions name.
			if err := m.ReplayOp(ctx, meta.JournalOp{Op: meta.OpSetChunkSize, Ino: ino, ChunkSize: facts.cs}); err == nil {
				existing.ChunkSize = facts.cs
			}
		}
		if gerr == nil {
			rep.NameSource = SrcJournal
			if existing.Size > 0 || size == 0 {
				size = existing.Size // the journal's setsize is newer than any caption
			} else if err := setSizeKeepMtime(ctx, m, ino, size, existing.Mtime); err != nil {
				rep.Notes = append(rep.Notes, "size not set: "+err.Error())
			}
			if p, ok := m.PathOf(ctx, ino); ok {
				rep.Path = p
			}
		} else {
			p, src, notes := b.place(ino, facts.name)
			rep.Path, rep.NameSource, rep.Notes = p, src, append(rep.Notes, notes...)
			if err := b.createFile(ino, p, facts, cs, size); err != nil {
				return nil, fmt.Errorf("create ino %d at %s: %w", ino, p, err)
			}
		}
		for idx, o := range in.chunks {
			if refMsg, ok := refAt[slot{ino, idx}]; ok && refMsg > o.msgID {
				continue // a newer dedup ref already filed this slot
			}
			if err := m.PutChunk(ctx, meta.Chunk{Ino: ino, Idx: idx, TGMessageID: int64(o.msgID), Size: int32(o.length),
				PackOffset: o.packOff, PackSize: o.packSize, Hash: o.hash}); err != nil {
				return nil, fmt.Errorf("chunk %d/%d: %w", ino, idx, err)
			}
		}
		rep.Size = size
		rows, _ := m.ListChunks(ctx, ino)
		rep.Found = len(rows)
		rep.Expected = int((size + cs - 1) / cs)
		if rep.Found < rep.Expected {
			rep.Notes = append(rep.Notes, fmt.Sprintf("incomplete: %d of %d chunks", rep.Found, rep.Expected))
			res.Stats.Incomplete++
		}
		if facts.nl > 1 && rep.NameSource != SrcJournal {
			rep.Notes = append(rep.Notes, fmt.Sprintf("hard link: %d names, %d restored", facts.nl, 1))
		}
		res.Files = append(res.Files, rep)
	}

	// Files that exist only through the journal (empty files, fully
	// deduplicated content, symlinks): report them too.
	reported := map[int64]bool{}
	for _, f := range res.Files {
		reported[f.Ino] = true
	}
	for ino := range created {
		if reported[ino] || deleted[ino] {
			continue
		}
		in, err := m.GetInode(ctx, ino)
		if err != nil || in.Kind == meta.KindDir {
			continue
		}
		p, _ := m.PathOf(ctx, ino)
		rows, _ := m.ListChunks(ctx, ino)
		cs := in.ChunkSize
		if cs <= 0 {
			cs = meta.DefaultChunkSize
		}
		rep := FileReport{Path: p, Ino: ino, Size: in.Size, Found: len(rows), NameSource: SrcJournal}
		if in.Kind == meta.KindFile {
			rep.Expected = int((in.Size + cs - 1) / cs)
			if rep.Found < rep.Expected && !in.Sparse {
				rep.Notes = append(rep.Notes, fmt.Sprintf("incomplete: %d of %d chunks", rep.Found, rep.Expected))
				res.Stats.Incomplete++
			}
		} else {
			rep.Notes = append(rep.Notes, string(in.Kind))
		}
		res.Files = append(res.Files, rep)
	}
	sort.Slice(res.Files, func(i, j int) bool { return res.Files[i].Path < res.Files[j].Path })
	res.Stats.Files = len(res.Files)
	return res, nil
}

type slot struct {
	ino int64
	idx int32
}

// Rebuilt files and folders belong to the user running the rebuild
// (captions don't carry owners; journal-created ones keep theirs).
func uid() uint32 { return uint32(os.Getuid()) }
func gid() uint32 { return uint32(os.Getgid()) }

// ---- placing files by name hint ----

// rsyncTemp matches rsync's default temporary name: ".NAME.XXXXXX"
// (6 alphanumerics; measured on rsync 3.5.1, 2026-10-25).
var rsyncTemp = regexp.MustCompile(`^\.(.+)\.[A-Za-z0-9]{6}$`)

// MapRsync maps a hinted path to the name rsync was going to give it.
// --inplace writes the final name, so it passes through unchanged.
func MapRsync(p string) (string, bool) {
	dir, base := path.Split(p)
	mapped := false
	// --delay-updates stages files in ".~tmp~/" under the target folder.
	if strings.HasSuffix(strings.TrimSuffix(dir, "/"), "/.~tmp~") || strings.TrimSuffix(dir, "/") == ".~tmp~" {
		dir = path.Dir(strings.TrimSuffix(dir, "/")) + "/"
		mapped = true
	}
	if m := rsyncTemp.FindStringSubmatch(base); m != nil {
		base = m[1]
		mapped = true
	}
	return path.Clean(dir + base), mapped
}

type claim struct {
	ino   int64
	msgID int
}

type builder struct {
	ctx    context.Context
	m      *meta.Store
	next   int64
	claims map[string]claim
	lf     int64 // /lost+found ino
}

// place picks the path for an inode without a journal name.
func (b *builder) place(ino int64, hint string) (string, string, []string) {
	var notes []string
	if hint == "" || strings.HasPrefix(hint, "…") || !strings.HasPrefix(hint, "/") {
		if strings.HasPrefix(hint, "…") {
			notes = append(notes, "name hint truncated: "+hint)
			return b.lostFound(strings.TrimLeft(strings.TrimPrefix(hint, "…"), "/"), ino), SrcLostFound, notes
		}
		return b.lostFound(fmt.Sprintf("ino%d", ino), ino), SrcLostFound, notes
	}
	target, mapped := MapRsync(hint)
	src := SrcHint
	if mapped {
		src = SrcHintRsync
		notes = append(notes, "uploaded as "+hint)
	}
	if _, taken := b.claims[target]; taken {
		notes = append(notes, "name already restored for a newer upload")
		return b.lostFound(strings.TrimPrefix(target, "/")+fmt.Sprintf("~ino%d", ino), ino), SrcLostFoundDu, notes
	}
	if in, err := b.lookupPath(target); err == nil && in.Ino != ino {
		notes = append(notes, "name taken by a journal-restored file")
		return b.lostFound(strings.TrimPrefix(target, "/")+fmt.Sprintf("~ino%d", ino), ino), SrcLostFoundDu, notes
	}
	b.claims[target] = claim{ino: ino}
	return target, src, notes
}

func (b *builder) lostFound(rel string, ino int64) string {
	p := path.Clean("/lost+found/" + rel)
	if _, taken := b.claims[p]; taken {
		p = fmt.Sprintf("%s~ino%d", p, ino)
	}
	b.claims[p] = claim{ino: ino}
	return p
}

func (b *builder) lookupPath(p string) (meta.Inode, error) {
	cur := meta.RootIno
	var in meta.Inode
	for _, part := range strings.Split(strings.Trim(p, "/"), "/") {
		if part == "" {
			continue
		}
		x, err := b.m.Lookup(b.ctx, cur, part)
		if err != nil {
			return meta.Inode{}, err
		}
		in, cur = x, x.Ino
	}
	return in, nil
}

// mkdirAll makes (or finds) the folders of dir, with new inode numbers.
func (b *builder) mkdirAll(dir string) (int64, error) {
	cur := meta.RootIno
	for _, part := range strings.Split(strings.Trim(dir, "/"), "/") {
		if part == "" {
			continue
		}
		x, err := b.m.Lookup(b.ctx, cur, part)
		if err == nil {
			if x.Kind != meta.KindDir {
				return 0, fmt.Errorf("%s: a file is in the way", part)
			}
			cur = x.Ino
			continue
		}
		ino := b.next
		b.next++
		if err := b.m.ReplayOp(b.ctx, meta.JournalOp{Op: meta.OpCreateChild, Parent: cur, Name: part, Ino: ino,
			Kind: meta.KindDir, Mode: 0o40755, UID: uid(), GID: gid()}); err != nil {
			return 0, err
		}
		cur = ino
	}
	return cur, nil
}

func (b *builder) createFile(ino int64, p string, facts chunkObs, cs, size int64) error {
	dir, base := path.Split(p)
	parent, err := b.mkdirAll(dir)
	if err != nil {
		return err
	}
	mode := facts.mode
	if mode == 0 {
		mode = 0o100644
	}
	if err := b.m.ReplayOp(b.ctx, meta.JournalOp{Op: meta.OpCreateChild, Parent: parent, Name: base, Ino: ino,
		Kind: meta.KindFile, Mode: mode, Mtime: facts.mtime, ChunkSize: cs, UID: uid(), GID: gid()}); err != nil {
		return err
	}
	return setSizeKeepMtime(b.ctx, b.m, ino, size, facts.mtime)
}

// setSizeKeepMtime sets a rebuilt file's size without letting SetSize's
// "mtime = now" overwrite the mtime the rebuild just restored.
func setSizeKeepMtime(ctx context.Context, m *meta.Store, ino, size, mtime int64) error {
	if err := m.ReplayOp(ctx, meta.JournalOp{Op: meta.OpSetSize, Ino: ino, Size: size}); err != nil {
		return err
	}
	if mtime == 0 {
		return nil
	}
	return m.ReplayOp(ctx, meta.JournalOp{Op: meta.OpSetAttr, Ino: ino, Set: []string{"mtime"}, Mtime: mtime})
}
