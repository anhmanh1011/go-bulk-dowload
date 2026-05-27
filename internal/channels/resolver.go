// Package channels resolves Telegram channel access hashes at pipeline
// startup so per-job RPCs can pass a fully-qualified InputPeerChannel
// without re-querying messages.getDialogs.
package channels

import (
	"context"
	"errors"
	"fmt"

	"github.com/gotd/td/tg"
)

// Invoker is the subset of tg.Invoker the resolver needs. session.Pool
// satisfies it; tests can supply a fake.
type Invoker interface {
	tg.Invoker
}

// Resolve walks the user's dialog list and returns the AccessHash for the
// channel whose ChannelID == chatID. Errors if the channel is not in the
// user's dialog list (the user must join/subscribe first).
func Resolve(ctx context.Context, inv Invoker, chatID int64) (int64, error) {
	api := tg.NewClient(inv)
	req := &tg.MessagesGetDialogsRequest{
		OffsetPeer: &tg.InputPeerEmpty{},
		Limit:      500,
	}
	res, err := api.MessagesGetDialogs(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("getDialogs: %w", err)
	}
	var chats []tg.ChatClass
	switch v := res.(type) {
	case *tg.MessagesDialogs:
		chats = v.Chats
	case *tg.MessagesDialogsSlice:
		chats = v.Chats
	default:
		return 0, fmt.Errorf("unexpected dialogs response %T", res)
	}
	for _, c := range chats {
		ch, ok := c.(*tg.Channel)
		if !ok {
			continue
		}
		if ch.ID == chatID {
			return ch.AccessHash, nil
		}
	}
	return 0, errors.New("channel not found in dialogs — make sure the account has joined it")
}

// VerifyPostRights calls messages.getFullChannel and rejects when the account
// lacks send-media privileges on the channel. Used at pipeline startup as a
// fail-fast precheck for the upload target — surfaces a misconfigured target
// in seconds rather than after the first batch reaches the uploader.
//
// Per Telegram MTProto, posting to a broadcast channel requires either:
//   - admin rights with PostMessages permission, OR
//   - the channel is a megagroup the account can send to (no broadcast flag)
//
// Banned rights with SendMedia=true also blocks publishing. This function
// errors when posting would fail; nil means the precheck passed.
func VerifyPostRights(ctx context.Context, inv Invoker, chatID, accessHash int64) error {
	api := tg.NewClient(inv)
	res, err := api.ChannelsGetFullChannel(ctx, &tg.InputChannel{
		ChannelID:  chatID,
		AccessHash: accessHash,
	})
	if err != nil {
		return fmt.Errorf("getFullChannel: %w", err)
	}
	for _, c := range res.Chats {
		ch, ok := c.(*tg.Channel)
		if !ok || ch.ID != chatID {
			continue
		}
		if br, ok := ch.GetBannedRights(); ok && br.SendMedia {
			return errors.New("verify post rights: account is banned from sending media on target channel")
		}
		// Broadcast channels require admin rights with PostMessages. Megagroups
		// (Megagroup=true) allow regular member posts unless banned.
		if ch.Broadcast {
			ar, ok := ch.GetAdminRights()
			if !ok || !ar.PostMessages {
				return errors.New("verify post rights: target is a broadcast channel and account lacks PostMessages admin right")
			}
		}
		return nil
	}
	return fmt.Errorf("verify post rights: channel %d not found in getFullChannel response", chatID)
}
