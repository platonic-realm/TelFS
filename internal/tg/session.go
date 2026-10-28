package tg

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"golang.org/x/sync/errgroup"

	"telfs/internal/config"
)

// DownloadRangeParallelism caps the number of concurrent
// upload.getFile RPCs issued for a single DownloadDocumentRange
// call. The download rate-limit lane (default unlimited) is the
// outer budget; this just bounds how many gotd round trips one
// chunk-read can have in flight at once. Matches the upload
// pool's default concurrency so the two sides see symmetric
// budgets at the gotd layer.
const DownloadRangeParallelism = 4

// ErrNotConnected is returned by Session methods called while no
// gotd client is bound (during a reconnect window, or before the
// SessionManager has performed its first connect). Callers should
// treat this as transient and retry — the next call after the
// reconnect lands will succeed.
var ErrNotConnected = errors.New("tg: session not connected")

// Session is a stable handle to an MTProto context. Used by the
// short-lived `RunSession` (one-shot CLI ops) AND by the long-lived
// daemon's SessionManager, which atomically swaps the underlying
// *tg.Client across reconnects so callers don't need to re-create
// their references.
//
// Concurrency: reads of the api pointer use apiMu (RWMutex). Reads
// are common (every Session method); swaps happen on reconnect only.
type Session struct {
	cfg     *config.Config
	tracker *Tracker

	apiMu sync.RWMutex
	api   *tg.Client
	// upAPI uploads parts through the current connection cycle's upload
	// pool ([upload] connections; nil = no pool now, parts go over api).
	// Guarded by apiMu like api.
	upAPI *tg.Client
	// uploadThreads is the parts in flight per upload on the pool
	// ([upload] parts_per_upload).
	uploadThreads int

	// docCache memoizes channels.getMessages → *tg.Document lookups
	// so a chunk download doesn't pay an extra round trip just to
	// recover the FileReference. Telegram-documented stable
	// FileReference lifetime is ~30 min; we use 20 min for headroom.
	// On FILE_REFERENCE_EXPIRED we invalidate the entry; the
	// existing single-retry path in DownloadDocument{,Range} refetches.
	docCache sync.Map // map[int64]*docCacheEntry, keyed by msgID
	// docSweepAt is when the last docCache expiry sweep ran; gates
	// maybeSweepDocCache to once per TTL window.
	docSweepAt atomic.Pointer[time.Time]

	// readOnly makes every channel-WRITING call fail with ErrReadOnly
	// (`telfs mount --readonly`, e.g. a recovery drill on a second
	// machine): uploads, posts, deletions, pins. Structural, so no code
	// path can post a snapshot or journal batch by accident.
	readOnly atomic.Bool
}

// ErrReadOnly: a channel write was attempted on a read-only session.
var ErrReadOnly = errors.New("read-only mount: TelFS does not write to the channel")

// SetReadOnly turns the channel-write guard on or off.
func (s *Session) SetReadOnly(on bool) { s.readOnly.Store(on) }

// ReadOnly reports whether channel writes are refused.
func (s *Session) ReadOnly() bool { return s.readOnly.Load() }

// Connected reports whether the session currently has a live connection.
func (s *Session) Connected() bool {
	s.apiMu.RLock()
	defer s.apiMu.RUnlock()
	return s.api != nil
}

type docCacheEntry struct {
	doc       *tg.Document
	fetchedAt time.Time
}

// docCacheTTL is how long a cached *tg.Document (and its
// FileReference) stays usable. Conservative under TG's ~30-min
// guarantee — if a download fails with FILE_REFERENCE_EXPIRED
// despite this, the entry gets invalidated and refreshed.
const docCacheTTL = 20 * time.Minute

// Tracker returns the request log this session writes to (shared
// with the parent Client). Used by the web /telegram handler.
func (s *Session) Tracker() *Tracker { return s.tracker }

// API returns the current live gotd *tg.Client, or nil if no client
// is bound (reconnect window).
func (s *Session) API() *tg.Client {
	s.apiMu.RLock()
	defer s.apiMu.RUnlock()
	return s.api
}

// currentAPI is the internal helper for session methods: returns the
// live *tg.Client OR an error if no client is bound. Use this instead
// of touching s.api directly so reconnect swaps are race-free.
func (s *Session) currentAPI() (*tg.Client, error) {
	s.apiMu.RLock()
	defer s.apiMu.RUnlock()
	if s.api == nil {
		return nil, ErrNotConnected
	}
	return s.api, nil
}

// swapAPI installs a new live gotd client (or nil to mark the session
// as disconnected). Called by the SessionManager's run loop on each
// reconnect. In-flight callers reading via currentAPI will get the
// pre-swap value and may complete or fail naturally; calls that start
// AFTER the swap pick up the new client.
func (s *Session) swapAPI(api *tg.Client) {
	s.apiMu.Lock()
	s.api = api
	s.apiMu.Unlock()
}

// swapUploadAPI installs the API of a connection cycle's upload pool, or
// nil when the cycle ends (uploads then use the main connection).
func (s *Session) swapUploadAPI(api *tg.Client) {
	s.apiMu.Lock()
	s.upAPI = api
	s.apiMu.Unlock()
}

// partsAPI is where an upload's parts go, and how many at once: the
// upload pool with [upload] parts_per_upload in flight when a pool is
// bound; otherwise main, one part at a time (as before v1 — the main
// connection also carries downloads, journal posts and everything else).
func (s *Session) partsAPI(main *tg.Client) (*tg.Client, int) {
	s.apiMu.RLock()
	up := s.upAPI
	s.apiMu.RUnlock()
	if up == nil {
		return main, 1
	}
	if s.uploadThreads < 1 {
		return up, 1
	}
	return up, s.uploadThreads
}

// Config returns the configuration the session was opened with.
func (s *Session) Config() *config.Config { return s.cfg }

// channelPeer returns MTProto peer references for the configured backing
// channel.
func (s *Session) channelPeer() (*tg.InputPeerChannel, *tg.InputChannel, error) {
	if err := s.cfg.RequireChannel(); err != nil {
		return nil, nil, err
	}
	return &tg.InputPeerChannel{
		ChannelID:  s.cfg.Channel.ID,
		AccessHash: s.cfg.Channel.AccessHash,
	}, &tg.InputChannel{
		ChannelID:  s.cfg.Channel.ID,
		AccessHash: s.cfg.Channel.AccessHash,
	}, nil
}

// VerifyAccess confirms with two cheap RPCs that (1) the session
// holds a supported (user, non-bot) identity, and (2) that identity
// can actually access the bound channel. Called by mount preflight so
// a broken profile fails in seconds with a message naming the actor
// and the fix — instead of coming up "fine" and burying an endless
// CHANNEL_INVALID retry storm in the daemon log (channel access
// hashes are identity-scoped: a hash resolved by one account is
// garbage under another).
func (s *Session) VerifyAccess(ctx context.Context) error {
	api, err := s.currentAPI()
	if err != nil {
		return err
	}
	users, err := api.UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUserSelf{}})
	if err != nil {
		return fmt.Errorf("verify identity: %w", err)
	}
	var self *tg.User
	for _, u := range users {
		if full, ok := u.(*tg.User); ok {
			self = full
			break
		}
	}
	if self == nil {
		return errors.New("verify identity: users.getUsers returned no usable self user")
	}
	who := "@" + self.Username
	if self.Username == "" {
		who = fmt.Sprintf("id=%d", self.ID)
	}
	// A leftover bot session from the removed bot-auth feature must be
	// rejected, not silently used as the wrong actor (its channel
	// access would be broken anyway — access hashes are identity-
	// scoped).
	if self.Bot {
		return fmt.Errorf("session identity is BOT %s — bot auth is no longer supported; run `telfs profile reset-auth` and log in with your phone", who)
	}
	// Legacy profiles predate identity tracking — record it once so
	// status/web can display who this profile acts as.
	if s.cfg.Identity == nil {
		s.cfg.Identity = &config.Identity{UserID: self.ID, Username: self.Username, IsBot: self.Bot}
		_ = s.cfg.Save()
	}
	_, input, err := s.channelPeer()
	if err != nil {
		return err
	}
	if _, err := api.ChannelsGetChannels(ctx, []tg.InputChannelClass{input}); err != nil {
		return fmt.Errorf("the authenticated account %s cannot access the bound channel (id=%d, %q): %w\n"+
			"the account may have left the channel, or the stored access_hash is stale — re-run `telfs channel list` then `telfs channel set %d`",
			who, s.cfg.Channel.ID, s.cfg.Channel.Title, err, s.cfg.Channel.ID)
	}
	return nil
}

// ListChannels enumerates the user's channels from their dialogs. Includes
// both channels they own and channels they're a member of.
func (s *Session) ListChannels(ctx context.Context) ([]ChannelInfo, error) {
	api, err := s.currentAPI()
	if err != nil {
		return nil, err
	}
	res, err := api.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
		OffsetPeer: &tg.InputPeerEmpty{},
		Limit:      200,
	})
	if err != nil {
		return nil, fmt.Errorf("get dialogs: %w", err)
	}
	var chats []tg.ChatClass
	switch r := res.(type) {
	case *tg.MessagesDialogs:
		chats = r.Chats
	case *tg.MessagesDialogsSlice:
		chats = r.Chats
	default:
		return nil, fmt.Errorf("unexpected dialogs response: %T", res)
	}
	var out []ChannelInfo
	for _, chat := range chats {
		ch, ok := chat.(*tg.Channel)
		if !ok {
			continue
		}
		if ch.Left {
			continue
		}
		out = append(out, ChannelInfo{
			ID:         ch.ID,
			AccessHash: ch.AccessHash,
			Title:      ch.Title,
			Username:   ch.Username,
			IsCreator:  ch.Creator,
			CanPost:    ch.Creator || ch.AdminRights.PostMessages,
		})
	}
	return out, nil
}

// ResolveChannel looks up the given channel id in the user's dialogs and
// returns its full ChannelInfo (including AccessHash). The id is normalized
// via NormalizeChannelID first, so both raw and -100... forms are accepted.
func (s *Session) ResolveChannel(ctx context.Context, id int64) (ChannelInfo, error) {
	id, err := NormalizeChannelID(id)
	if err != nil {
		return ChannelInfo{}, err
	}
	chans, err := s.ListChannels(ctx)
	if err != nil {
		return ChannelInfo{}, err
	}
	for _, ch := range chans {
		if ch.ID == id {
			return ch, nil
		}
	}
	return ChannelInfo{}, fmt.Errorf("channel %d not found in your dialogs (run 'telfs channel list' to see ids)", id)
}

// SetChannel resolves the channel id, validates it can be posted to, and
// persists it to the config. Mutates s.cfg in-place.
//
// The normal path looks the channel up by id and harvests its
// access_hash from the dialog list. A non-zero accessHash skips the
// scan and trusts the caller (`channel set --access-hash` — a manual
// override for channels the dialog scan can't see).
func (s *Session) SetChannel(ctx context.Context, id int64, accessHash int64) (ChannelInfo, error) {
	if accessHash != 0 {
		// Trust-the-caller path (`channel set --access-hash`).
		nid, err := NormalizeChannelID(id)
		if err != nil {
			return ChannelInfo{}, err
		}
		info := ChannelInfo{
			ID:         nid,
			AccessHash: accessHash,
			Title:      fmt.Sprintf("channel-%d", nid),
			CanPost:    true,
		}
		s.cfg.Channel = config.ChannelConfig{
			ID:         info.ID,
			AccessHash: info.AccessHash,
			Title:      info.Title,
			Username:   info.Username,
		}
		if err := s.cfg.Save(); err != nil {
			return ChannelInfo{}, fmt.Errorf("save config: %w", err)
		}
		return info, nil
	}

	info, err := s.ResolveChannel(ctx, id)
	if err != nil {
		return ChannelInfo{}, err
	}
	if !info.CanPost {
		return ChannelInfo{}, fmt.Errorf("you don't have post permission on channel %q (%d)", info.Title, info.ID)
	}
	s.cfg.Channel = config.ChannelConfig{
		ID:         info.ID,
		AccessHash: info.AccessHash,
		Title:      info.Title,
		Username:   info.Username,
	}
	if err := s.cfg.Save(); err != nil {
		return ChannelInfo{}, fmt.Errorf("save config: %w", err)
	}
	return info, nil
}

// PostMessage sends a plain text message to the configured channel and
// returns the new message id.
func (s *Session) PostMessage(ctx context.Context, text string) (int, error) {
	if s.readOnly.Load() {
		return 0, ErrReadOnly
	}
	peer, _, err := s.channelPeer()
	if err != nil {
		return 0, err
	}
	rid, err := randomID()
	if err != nil {
		return 0, err
	}
	api, err := s.currentAPI()
	if err != nil {
		return 0, err
	}
	upd, err := api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
		Peer:     peer,
		Message:  text,
		RandomID: rid,
	})
	if err != nil {
		return 0, fmt.Errorf("send message: %w", err)
	}
	return extractNewMessageID(upd, rid)
}

// GetMessageText fetches a message by id and returns its text body.
func (s *Session) GetMessageText(ctx context.Context, msgID int) (string, error) {
	msg, err := s.getMessage(ctx, msgID)
	if err != nil {
		return "", err
	}
	return msg.Message, nil
}

// uploadMaxAttempts caps how many times UploadDocument will retry a
// transient failure. Each attempt is a fresh uploader.NewUploader +
// SaveFilePart chain, so retrying isn't free — but it's much cheaper
// than propagating EIO to the FUSE write path and forcing the user
// to retry the entire cp.
const uploadMaxAttempts = 4

// UploadDocument uploads the contents of r as a document attached to a
// message in the configured channel. The document is sent with the given
// filename (used for the DocumentAttributeFilename) and optional caption.
// Returns the new message id.
//
// The reader is buffered up-front so the upload can be retried on
// transient gotd errors ("engine forcibly closed", connection reset,
// i/o timeout) without the caller having to re-stage the source. We
// don't retry caller-canceled contexts (Close racing a Flush) — that's
// honest cancellation. FLOOD_WAIT is handled before reaching us by
// the gotd middleware.
func (s *Session) UploadDocument(ctx context.Context, r io.Reader, filename, caption string) (int, error) {
	if s.readOnly.Load() {
		return 0, ErrReadOnly
	}
	peer, _, err := s.channelPeer()
	if err != nil {
		return 0, err
	}
	// Buffer once — the reader is a chunk wire (≤ 64 MiB tier + GCM
	// overhead) or a packed blob (≤ 32 MiB) or a snapshot body, so the
	// memory cost is bounded by the largest caller.
	body, err := io.ReadAll(r)
	if err != nil {
		return 0, fmt.Errorf("read upload bytes: %w", err)
	}
	// Telegram hard-caps documents at 2 GiB (4 GiB premium). Chunk
	// tiers are far below this; the realistic way to reach it is a
	// whole-DB snapshot that outgrew the cap. Fail with a diagnosis
	// instead of letting the server reject every part-upload late —
	// silently-failing snapshots recede the recovery anchor for weeks
	// before anyone notices.
	const tgDocumentCap = 2 << 30
	if len(body) >= tgDocumentCap {
		return 0, fmt.Errorf("upload %q is %d bytes — at/over Telegram's 2 GiB document cap; if this is a metadata snapshot, prune the journal / shrink the DB (this failure repeats every cycle until addressed)", filename, len(body))
	}

	// ONE RandomID for every attempt of this logical upload. Telegram
	// dedups sendMedia server-side by random_id, so a retry after a
	// lost ack (engine torn down between the server committing the
	// message and us reading the response) resolves to the SAME
	// message instead of posting a duplicate copy of the blob.
	rid, err := randomID()
	if err != nil {
		return 0, err
	}

	var lastErr error
	for attempt := 1; attempt <= uploadMaxAttempts; attempt++ {
		if attempt > 1 {
			// 1s, 2s, 4s exponential backoff between retries.
			delay := time.Duration(1<<(attempt-2)) * time.Second
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(delay):
			}
		}
		msgID, err := s.tryUploadOnce(ctx, peer, body, filename, caption, rid)
		if err == nil {
			return msgID, nil
		}
		lastErr = err
		if !isRetriableUploadErr(ctx, err) {
			return 0, err
		}
	}
	return 0, fmt.Errorf("upload failed after %d attempts: %w", uploadMaxAttempts, lastErr)
}

func (s *Session) tryUploadOnce(ctx context.Context, peer *tg.InputPeerChannel, body []byte, filename, caption string, rid int64) (int, error) {
	api, err := s.currentAPI()
	if err != nil {
		return 0, err
	}
	// The parts go through the upload pool when there is one (spec v1
	// row 2: one TCP connection to the account's far-away data centre
	// carries ~0.2 MB/s, so throughput comes from more connections),
	// several in flight; the message itself is sent on the main
	// connection — the parts belong to the account, not to a connection.
	partsAPI, threads := s.partsAPI(api)
	uf, err := uploadParts(ctx, partsAPI, filename, body, threads)
	if err != nil {
		return 0, fmt.Errorf("upload bytes: %w", err)
	}
	media := &tg.InputMediaUploadedDocument{
		File:     uf,
		MimeType: "application/octet-stream",
		Attributes: []tg.DocumentAttributeClass{
			&tg.DocumentAttributeFilename{FileName: filename},
		},
	}
	upd, err := api.MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
		Peer:     peer,
		Media:    media,
		RandomID: rid,
		Message:  caption,
	})
	if err != nil {
		return 0, fmt.Errorf("send media: %w", err)
	}
	return extractNewMessageID(upd, rid)
}

// uploadPartTries is how often a part is sent again when Telegram answers
// it with false ("not saved"); gotd's uploader retried that forever.
const uploadPartTries = 3

// uploadParts uploads body as one file through api, threads parts in
// flight, and returns the file for sendMedia.
//
// The wire format is Telegram's streamed upload, which TelFS has always used
// (gotd's uploader.FromReader): upload.saveBigFilePart for every size,
// 512 KiB parts (Telegram's largest — a 32 MiB pack is 64 RPCs, not 256),
// file_total_parts = -1 on every part but the last, which carries the total
// and goes out once all the others have been saved — as in a one-at-a-time
// upload. TelFS sends the parts itself because gotd can't do this in
// parallel: its streamed upload with more than one thread is a data race
// (the reader goroutine sets the total while the senders read it — found by
// the race detector), and its known-size path sends files up to 10 MiB one
// part at a time.
func uploadParts(ctx context.Context, api *tg.Client, name string, body []byte, threads int) (tg.InputFileClass, error) {
	if len(body) == 0 {
		return nil, errors.New("nothing to upload (empty body)")
	}
	fileID, err := randomID()
	if err != nil {
		return nil, err
	}
	n := (len(body) + uploadPartMax - 1) / uploadPartMax
	send := func(ctx context.Context, i, total int) error {
		req := &tg.UploadSaveBigFilePartRequest{
			FileID:         fileID,
			FilePart:       i,
			FileTotalParts: total,
			Bytes:          body[i*uploadPartMax : min((i+1)*uploadPartMax, len(body))],
		}
		for try := 1; ; try++ {
			ok, err := api.UploadSaveBigFilePart(ctx, req)
			if err != nil {
				return fmt.Errorf("part %d of %d: %w", i+1, n, err)
			}
			if ok {
				return nil
			}
			if try == uploadPartTries {
				return fmt.Errorf("part %d of %d: Telegram did not save it (%d tries)", i+1, n, try)
			}
		}
	}
	if threads < 1 {
		threads = 1
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(threads)
	for i := 0; i < n-1 && gctx.Err() == nil; i++ {
		g.Go(func() error { return send(gctx, i, -1) })
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := send(ctx, n-1, n); err != nil {
		return nil, err
	}
	return &tg.InputFileBig{ID: fileID, Parts: n, Name: name}, nil
}

// isRetriableUploadErr decides whether an UploadDocument failure looks
// transient enough to be worth retrying. We never retry once the
// caller's ctx is done (honest shutdown). Otherwise we look for the
// few gotd error stringings we've actually observed in the wild —
// keeping the allow-list narrow so a real misconfiguration (bad
// channel id, expired session) doesn't get masked by a retry loop.
func isRetriableUploadErr(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	// A reconnect window (stall-watchdog swap, network blip) surfaces
	// as ErrNotConnected while the manager builds the next session.
	// That's the MOST retriable condition there is — failing fast here
	// used to burn the entire retry ladder inside one 5-10s reconnect
	// and lose the chunk when the writer had already been Released.
	if errors.Is(err, ErrNotConnected) {
		return true
	}
	s := err.Error()
	for _, marker := range []string{
		"engine forcibly closed",
		"connection reset",
		"i/o timeout",
		"broken pipe",
		"unexpected EOF",
	} {
		if strings.Contains(s, marker) {
			return true
		}
	}
	return false
}

// DownloadDocument fetches the document attached to msgID in the
// configured channel and streams its bytes to w. Returns the byte count.
//
// FILE_REFERENCE_EXPIRED is transparently handled: gotd returns this
// error when the file_reference token cached in the InputDocumentFileLocation
// is too old (typically hours). On that error we re-fetch the message to
// pick up a fresh reference and retry once.
func (s *Session) DownloadDocument(ctx context.Context, msgID int, w io.Writer) (int64, error) {
	doc, err := s.fetchDocument(ctx, msgID)
	if err != nil {
		return 0, err
	}
	n, err := s.streamDocument(ctx, doc, w)
	if err == nil {
		return n, nil
	}
	if !tgerr.Is(err, "FILE_REFERENCE_EXPIRED") {
		return n, err
	}
	// Cached FileReference is stale — drop it so we don't keep
	// returning the same expired token from docCache, then refresh
	// from the wire and retry once.
	s.invalidateDocument(msgID)
	doc, err = s.refreshDocument(ctx, msgID)
	if err != nil {
		return 0, fmt.Errorf("refresh file_reference: %w", err)
	}
	return s.streamDocument(ctx, doc, w)
}

// rangePartSize is the size of each upload.getFile request when
// fetching a byte range. Telegram caps a single getFile response at
// 1 MiB, and offset+limit must each be a multiple of 4 KiB. Using
// 1 MiB minimizes the round-trip count for cross-MiB ranges.
const rangePartSize = 1 << 20

// rangeAlignment is the byte-alignment Telegram requires for offset
// and limit fields in upload.getFile. Reads not aligned on this
// boundary must round outward and slice the surplus client-side.
const rangeAlignment = 4096

// DownloadDocumentRange fetches a byte range [offset, offset+length)
// of the document attached to msgID, streaming it to w. Uses the
// Precise flag on upload.getFile so non-keyframe-aligned offsets are
// accepted (otherwise Telegram is restrictive about where you can
// seek for media files).
//
// The caller can ask for any (offset, length); this method handles
// the 4 KiB alignment + 1 MiB-per-request constraints by rounding
// the request outward and slicing the surplus before writing to w.
//
// Sequential reads are issued serially (no parallelism inside one
// range request). For random reads of a small slice this is the
// expected pattern; large sequential reads should still use the
// whole-document Download* APIs.
func (s *Session) DownloadDocumentRange(ctx context.Context, msgID int, w io.Writer, offset, length int64) (int64, error) {
	if length <= 0 {
		return 0, nil
	}
	doc, err := s.fetchDocument(ctx, msgID)
	if err != nil {
		return 0, err
	}
	n, err := s.streamDocumentRange(ctx, doc, w, offset, length)
	if err == nil {
		return n, nil
	}
	if !tgerr.Is(err, "FILE_REFERENCE_EXPIRED") {
		return n, err
	}
	s.invalidateDocument(msgID)
	doc, err = s.refreshDocument(ctx, msgID)
	if err != nil {
		return 0, fmt.Errorf("refresh file_reference: %w", err)
	}
	return s.streamDocumentRange(ctx, doc, w, offset, length)
}

// rangePart is one upload.getFile request: a 4 KiB-aligned
// [offset, offset+limit) sub-range that never crosses a 1 MiB
// boundary. Pre-computed up front so the parallel workers can chew
// through the list without coordinating on cursor math.
type rangePart struct {
	offset int64
	limit  int64
}

// planRangeParts splits the aligned range [alignedStart, alignedEnd)
// into 1 MiB-bounded chunks. Each chunk is at most rangePartSize and
// never spans a 1 MiB boundary (Telegram returns LIMIT_INVALID for
// such requests). Returned in offset order; an empty range produces
// an empty slice.
func planRangeParts(alignedStart, alignedEnd int64) []rangePart {
	if alignedEnd <= alignedStart {
		return nil
	}
	// Worst-case cap = aligned span / (smallest possible request),
	// which can't go below 4 KiB; in practice it's close to
	// (span / 1 MiB) + 1. Slightly over-allocate to skip a regrowth.
	parts := make([]rangePart, 0, (alignedEnd-alignedStart)/rangePartSize+2)
	cur := alignedStart
	for cur < alignedEnd {
		limit := int64(rangePartSize)
		if cur+limit > alignedEnd {
			limit = alignedEnd - cur
		}
		mibBoundary := ((cur / (1 << 20)) + 1) * (1 << 20)
		if cur+limit > mibBoundary {
			limit = mibBoundary - cur
		}
		parts = append(parts, rangePart{offset: cur, limit: limit})
		cur += limit
	}
	return parts
}

func (s *Session) streamDocumentRange(ctx context.Context, doc *tg.Document, w io.Writer, offset, length int64) (int64, error) {
	api, err := s.currentAPI()
	if err != nil {
		return 0, err
	}
	loc := &tg.InputDocumentFileLocation{
		ID:            doc.ID,
		AccessHash:    doc.AccessHash,
		FileReference: doc.FileReference,
	}
	// Round the request outward so offset+limit are 4 KiB-aligned;
	// slice the surplus before returning. End = first multiple of
	// rangeAlignment >= offset+length.
	alignedStart := (offset / rangeAlignment) * rangeAlignment
	end := offset + length
	alignedEnd := ((end + rangeAlignment - 1) / rangeAlignment) * rangeAlignment
	startOffsetInResult := offset - alignedStart

	parts := planRangeParts(alignedStart, alignedEnd)
	if len(parts) == 0 {
		return 0, nil
	}
	// Fan out: up to DownloadRangeParallelism workers fetch parts
	// concurrently into a slice indexed by part position. The
	// in-order writer below stitches them together. The single-part
	// case (a small slice that fits in one MiB block) short-circuits
	// to the serial fast path so we don't pay errgroup overhead for
	// a one-RPC read.
	results := make([][]byte, len(parts))
	if len(parts) == 1 {
		b, err := fetchRangePart(ctx, api, loc, parts[0])
		if err != nil {
			return 0, err
		}
		results[0] = b
	} else {
		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(DownloadRangeParallelism)
		for i := range parts {
			i := i
			g.Go(func() error {
				b, err := fetchRangePart(gCtx, api, loc, parts[i])
				if err != nil {
					return err
				}
				results[i] = b
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return 0, err
		}
	}

	// In-order stitch + write. Apply the leading-surplus trim on
	// the first part, the trailing-want trim across the tail, and
	// stop early on a short response (TG's EOF signal).
	var written int64
	wanted := length
	for i, partBytes := range results {
		if i == 0 && startOffsetInResult > 0 {
			if int64(len(partBytes)) <= startOffsetInResult {
				// First part was entirely surplus — caller's offset
				// landed past the end of the first response. Drop and
				// fall through; the loop terminates when wanted hits 0
				// or when a later part comes up short.
				continue
			}
			partBytes = partBytes[startOffsetInResult:]
		}
		if int64(len(partBytes)) > wanted {
			partBytes = partBytes[:wanted]
		}
		if len(partBytes) == 0 {
			break
		}
		n, werr := w.Write(partBytes)
		written += int64(n)
		wanted -= int64(n)
		if werr != nil {
			return written, werr
		}
		if wanted <= 0 {
			break
		}
		// Short response = EOF. No need to read subsequent parts.
		if int64(len(results[i])) < parts[i].limit {
			break
		}
	}
	return written, nil
}

// fetchRangePart issues one upload.getFile RPC for a planned part
// and returns its bytes. Called from N parallel workers in
// streamDocumentRange.
func fetchRangePart(ctx context.Context, api *tg.Client, loc *tg.InputDocumentFileLocation, p rangePart) ([]byte, error) {
	req := &tg.UploadGetFileRequest{
		Precise:  true,
		Location: loc,
		Offset:   p.offset,
		Limit:    int(p.limit),
	}
	res, err := api.UploadGetFile(ctx, req)
	if err != nil {
		return nil, err
	}
	f, ok := res.(*tg.UploadFile)
	if !ok {
		return nil, fmt.Errorf("unexpected getFile response %T", res)
	}
	return f.Bytes, nil
}

// downloadThreads is the parallelism inside ONE document download.
// gotd's Builder.Parallel splits the file into 512 KiB parts (the
// downloader's default part size) and fetches `threads` of them at
// once via concurrent UploadGetFile RPCs. For a 4 MiB chunk that's
// 8 parts; 4 threads gets us 2 round-trip passes instead of 8 serial
// ones, which is the difference between "slow chunk reads" and
// "reads at par with writes".
const downloadThreads = 4

func (s *Session) streamDocument(ctx context.Context, doc *tg.Document, w io.Writer) (int64, error) {
	loc := &tg.InputDocumentFileLocation{
		ID:            doc.ID,
		AccessHash:    doc.AccessHash,
		FileReference: doc.FileReference,
	}
	// We need an io.WriterAt for Builder.Parallel. We don't always know
	// the document size up front (.Size on tg.Document is set but the
	// API doesn't guarantee it matches the bytes actually streamed),
	// so we use a growable bufferWriterAt that allocates on first
	// non-contiguous write. After download completes, we copy the
	// resulting bytes into the caller's io.Writer.
	api, err := s.currentAPI()
	if err != nil {
		return 0, err
	}
	buf := &bufferWriterAt{}
	d := downloader.NewDownloader()
	if _, err := d.Download(api, loc).WithThreads(downloadThreads).Parallel(ctx, buf); err != nil {
		return 0, err
	}
	n, err := w.Write(buf.bytes())
	return int64(n), err
}

// bufferWriterAt is a minimal io.WriterAt that grows its backing slice
// to fit out-of-order writes. gotd's Parallel downloader writes the
// file in fixed-size parts to known offsets; the size field tracks the
// high-water mark so bytes() returns exactly the downloaded length.
type bufferWriterAt struct {
	mu   sync.Mutex
	data []byte
	size int64
}

func (b *bufferWriterAt) WriteAt(p []byte, off int64) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	end := off + int64(len(p))
	if end > int64(len(b.data)) {
		// Grow with some headroom to avoid a tiny re-alloc on every part.
		newCap := int64(len(b.data)) * 2
		if newCap < end {
			newCap = end
		}
		grown := make([]byte, end, newCap)
		copy(grown, b.data)
		b.data = grown
	}
	copy(b.data[off:], p)
	if end > b.size {
		b.size = end
	}
	return len(p), nil
}

func (b *bufferWriterAt) bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.data[:b.size]
}

// getMessage fetches a single channel message by id, returning it as the
// concrete *tg.Message (rejecting MessageEmpty / MessageService).
func (s *Session) getMessage(ctx context.Context, msgID int) (*tg.Message, error) {
	_, inCh, err := s.channelPeer()
	if err != nil {
		return nil, err
	}
	api, err := s.currentAPI()
	if err != nil {
		return nil, err
	}
	res, err := api.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
		Channel: inCh,
		ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: msgID}},
	})
	if err != nil {
		return nil, fmt.Errorf("get messages: %w", err)
	}
	var msgs []tg.MessageClass
	switch r := res.(type) {
	case *tg.MessagesChannelMessages:
		msgs = r.Messages
	case *tg.MessagesMessages:
		msgs = r.Messages
	case *tg.MessagesMessagesSlice:
		msgs = r.Messages
	default:
		return nil, fmt.Errorf("unexpected messages response: %T", res)
	}
	for _, m := range msgs {
		switch mm := m.(type) {
		case *tg.Message:
			if mm.ID == msgID {
				return mm, nil
			}
		case *tg.MessageEmpty:
			return nil, fmt.Errorf("message %d is empty (deleted?)", msgID)
		}
	}
	return nil, fmt.Errorf("message %d not found in channel", msgID)
}

// fetchDocument retrieves the document attached to a channel message.
// Honors the docCache on the Session — a previously-fetched document
// whose FileReference hasn't yet expired comes back directly without
// a round trip. Callers that need a guaranteed-fresh reference
// (e.g. after FILE_REFERENCE_EXPIRED) should call refreshDocument
// instead.
func (s *Session) fetchDocument(ctx context.Context, msgID int) (*tg.Document, error) {
	key := int64(msgID)
	if v, ok := s.docCache.Load(key); ok {
		if e, ok := v.(*docCacheEntry); ok && time.Since(e.fetchedAt) < docCacheTTL {
			return e.doc, nil
		}
	}
	return s.refreshDocument(ctx, msgID)
}

// refreshDocument unconditionally re-fetches and re-caches the
// document. Used both by the cache-miss path inside fetchDocument
// and by the FILE_REFERENCE_EXPIRED retry path in DownloadDocument /
// DownloadDocumentRange.
func (s *Session) refreshDocument(ctx context.Context, msgID int) (*tg.Document, error) {
	msg, err := s.getMessage(ctx, msgID)
	if err != nil {
		return nil, err
	}
	media, ok := msg.Media.(*tg.MessageMediaDocument)
	if !ok {
		return nil, fmt.Errorf("message %d has no document (media=%T)", msgID, msg.Media)
	}
	doc, ok := media.Document.(*tg.Document)
	if !ok {
		return nil, fmt.Errorf("message %d document is %T (expected *tg.Document)", msgID, media.Document)
	}
	s.docCache.Store(int64(msgID), &docCacheEntry{doc: doc, fetchedAt: time.Now()})
	// Amortized expiry sweep: entries were TTL-checked on read but
	// never removed, so a long-lived daemon reading millions of
	// distinct chunks accumulated one dead *tg.Document per message
	// forever. Sweep at most once per TTL window.
	s.maybeSweepDocCache()
	return doc, nil
}

// maybeSweepDocCache drops expired docCache entries, at most once per
// docCacheTTL. Cheap enough to run inline on a cache store: the sweep
// itself is O(live entries), and the once-per-TTL gate makes the
// amortized cost per store negligible.
func (s *Session) maybeSweepDocCache() {
	now := time.Now()
	last := s.docSweepAt.Load()
	if last != nil && now.Sub(*last) < docCacheTTL {
		return
	}
	stamp := now
	if !s.docSweepAt.CompareAndSwap(last, &stamp) {
		return // another goroutine took the sweep
	}
	s.docCache.Range(func(k, v any) bool {
		if e, ok := v.(*docCacheEntry); ok && now.Sub(e.fetchedAt) >= docCacheTTL {
			s.docCache.Delete(k)
		}
		return true
	})
}

// invalidateDocument drops a cached entry for msgID. Called by the
// FILE_REFERENCE_EXPIRED handlers so the next fetchDocument call
// goes back to the wire even if the entry hasn't aged out.
func (s *Session) invalidateDocument(msgID int) {
	s.docCache.Delete(int64(msgID))
}

// extractNewMessageID walks the UpdatesClass returned by SendMessage /
// SendMedia and finds the id of the message corresponding to the
// caller's RandomID `rid`.
//
// MTProto's SendMessage / SendMedia responses can contain MULTIPLE
// updates — including UpdateNewChannelMessage entries for messages
// that other concurrent calls created. (We have at least three
// background producers on one Session: the chunk write path, the
// snapshot manager every 5 min, and the trash GC. Snapshot-while-cp
// is the easy reproducer.) Picking the first UpdateNewChannelMessage
// in the response — what we used to do — silently returns the wrong
// message id whenever there's contention, which records the wrong
// (ino, idx, tg_message_id) tuple in chunk_map and causes the
// "preHash == readHash, file md5 differs" corruption pattern that
// dogged v0.5..v0.7 on the live mount.
//
// The canonical Telegram primitive for "match my SendMedia call to
// the server-assigned message id" is UpdateMessageID, which carries
// our original RandomID alongside the new id. We use it
// preferentially. If the server didn't include UpdateMessageID (some
// API methods that return UpdateShortSentMessage don't), we fall back
// to the first new-message update — that case is the no-contention
// path and works correctly.
func extractNewMessageID(upd tg.UpdatesClass, rid int64) (int, error) {
	switch u := upd.(type) {
	case *tg.UpdateShortSentMessage:
		return u.ID, nil
	case *tg.Updates:
		return findIDByRandomID(u.Updates, rid)
	case *tg.UpdatesCombined:
		return findIDByRandomID(u.Updates, rid)
	}
	return 0, fmt.Errorf("no new-message update in response (%T)", upd)
}

// findIDByRandomID first looks for an UpdateMessageID whose RandomID
// matches `rid` — that's the unambiguous answer. Without that, falls
// back to the first new-message update.
func findIDByRandomID(updates []tg.UpdateClass, rid int64) (int, error) {
	for _, up := range updates {
		if mid, ok := up.(*tg.UpdateMessageID); ok && mid.RandomID == rid {
			return mid.ID, nil
		}
	}
	// No UpdateMessageID in the response — typical for "stand-alone"
	// sends with no other concurrent traffic. Use the first new-message
	// update as before; safe because there's at most one such update in
	// this case.
	for _, up := range updates {
		if id, ok := messageIDFromUpdate(up); ok {
			return id, nil
		}
	}
	return 0, fmt.Errorf("no UpdateMessageID for RandomID %d and no fallback new-message update", rid)
}

func messageIDFromUpdate(u tg.UpdateClass) (int, bool) {
	switch up := u.(type) {
	case *tg.UpdateNewChannelMessage:
		if m, ok := up.Message.(*tg.Message); ok {
			return m.ID, true
		}
	case *tg.UpdateNewMessage:
		if m, ok := up.Message.(*tg.Message); ok {
			return m.ID, true
		}
	}
	return 0, false
}

// randomID returns the 64-bit random message id required by MTProto for
// idempotent retries.
func randomID() (int64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(b[:])), nil
}

// Static check that ErrNotAuthorized round-trips errors.Is — Run wraps it
// via fmt.Errorf but inner ops here may return it bare.
var _ = errors.Is
