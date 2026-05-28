package tg

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/gotd/td/tg"
)

// ChannelInfo is the subset of Telegram channel metadata TelFS cares about.
type ChannelInfo struct {
	ID         int64
	AccessHash int64
	Title      string
	Username   string // may be empty for private channels
	IsCreator  bool
	CanPost    bool
}

// InputPeer builds an InputPeerChannel suitable for MTProto calls.
func (ci ChannelInfo) InputPeer() *tg.InputPeerChannel {
	return &tg.InputPeerChannel{ChannelID: ci.ID, AccessHash: ci.AccessHash}
}

// InputChannel builds an InputChannel suitable for channels.* MTProto calls.
func (ci ChannelInfo) InputChannel() *tg.InputChannel {
	return &tg.InputChannel{ChannelID: ci.ID, AccessHash: ci.AccessHash}
}

// NormalizeChannelID converts a user-supplied channel id into the raw
// positive form MTProto expects. Accepts:
//
//   - Raw positive id (e.g. 1234567890) — returned unchanged.
//   - Bot-API "marked" form -100<id> (e.g. -1001234567890) — strips the -100
//     prefix. The marking is decimal-string concatenation, not arithmetic,
//     so we decode it as a string.
//
// Returns an error for ids that look like basic-chat (group) ids, since
// TelFS only supports channels.
func NormalizeChannelID(id int64) (int64, error) {
	if id > 0 {
		return id, nil
	}
	if id == 0 {
		return 0, fmt.Errorf("id 0 is not a channel id")
	}
	s := strconv.FormatInt(-id, 10)
	if !strings.HasPrefix(s, "100") || len(s) <= 3 {
		return 0, fmt.Errorf("id %d is not a channel id (looks like a basic-chat / group id)", id)
	}
	v, err := strconv.ParseInt(s[3:], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("id %d invalid: %w", id, err)
	}
	return v, nil
}
