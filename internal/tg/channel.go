package tg

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/gotd/td/tg"

	"telfs/internal/config"
)

// ChannelInfo is the subset of Telegram channel metadata TelFS cares about.
type ChannelInfo struct {
	ID         int64
	AccessHash int64
	Title      string
	Username   string  // may be empty for private channels
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

// ListChannels enumerates the user's channels from their dialogs. Includes
// both channels they own and channels they're a member of.
func (c *Client) ListChannels(ctx context.Context) ([]ChannelInfo, error) {
	var out []ChannelInfo
	err := c.Run(ctx, func(ctx context.Context, api *tg.Client) error {
		res, err := api.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
			OffsetPeer: &tg.InputPeerEmpty{},
			Limit:      200,
		})
		if err != nil {
			return fmt.Errorf("get dialogs: %w", err)
		}

		var chats []tg.ChatClass
		switch r := res.(type) {
		case *tg.MessagesDialogs:
			chats = r.Chats
		case *tg.MessagesDialogsSlice:
			chats = r.Chats
		default:
			return fmt.Errorf("unexpected dialogs response: %T", res)
		}

		for _, chat := range chats {
			ch, ok := chat.(*tg.Channel)
			if !ok {
				continue
			}
			if ch.Left {
				continue
			}
			info := ChannelInfo{
				ID:         ch.ID,
				AccessHash: ch.AccessHash,
				Title:      ch.Title,
				Username:   ch.Username,
				IsCreator:  ch.Creator,
				CanPost:    ch.Creator || ch.AdminRights.PostMessages,
			}
			out = append(out, info)
		}
		return nil
	})
	return out, err
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

// ResolveChannel looks up the given channel id in the user's dialogs and
// returns its full ChannelInfo (including AccessHash). The id is normalized
// via NormalizeChannelID first, so both raw and -100... forms are accepted.
// Returns an error if the user isn't a member or doesn't have it in their
// dialogs.
func (c *Client) ResolveChannel(ctx context.Context, id int64) (ChannelInfo, error) {
	id, err := NormalizeChannelID(id)
	if err != nil {
		return ChannelInfo{}, err
	}
	chans, err := c.ListChannels(ctx)
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

// SetChannel resolves the channel id, validates it, and persists it to the
// config. Requires the user to already have the channel in their dialogs.
func (c *Client) SetChannel(ctx context.Context, id int64) (ChannelInfo, error) {
	info, err := c.ResolveChannel(ctx, id)
	if err != nil {
		return ChannelInfo{}, err
	}
	if !info.CanPost {
		return ChannelInfo{}, fmt.Errorf("you don't have post permission on channel %q (%d)", info.Title, info.ID)
	}
	c.cfg.Channel = config.ChannelConfig{
		ID:         info.ID,
		AccessHash: info.AccessHash,
		Title:      info.Title,
		Username:   info.Username,
	}
	if err := c.cfg.Save(); err != nil {
		return ChannelInfo{}, fmt.Errorf("save config: %w", err)
	}
	return info, nil
}

// configuredPeer returns the InputPeerChannel for the configured backing
// channel, or an error if none is configured.
func (c *Client) configuredPeer() (*tg.InputPeerChannel, *tg.InputChannel, error) {
	if err := c.cfg.RequireChannel(); err != nil {
		return nil, nil, err
	}
	peer := &tg.InputPeerChannel{
		ChannelID:  c.cfg.Channel.ID,
		AccessHash: c.cfg.Channel.AccessHash,
	}
	in := &tg.InputChannel{
		ChannelID:  c.cfg.Channel.ID,
		AccessHash: c.cfg.Channel.AccessHash,
	}
	return peer, in, nil
}
