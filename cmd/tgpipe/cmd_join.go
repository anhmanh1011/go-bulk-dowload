package main

import (
	"fmt"
	"strings"

	"github.com/gotd/td/tg"
	"github.com/spf13/cobra"

	"github.com/manh/tgpipe/internal/config"
	"github.com/manh/tgpipe/internal/logging"
	"github.com/manh/tgpipe/internal/session"
)

// joinCmd joins a Telegram channel via an invite link or raw hash.
// After joining, run `tgpipe dialogs` to get the channel's numeric ID for
// use in config source_channel / target_channel.
//
// Example:
//
//	tgpipe join 'https://t.me/+NJ9r4tz8jsg5ZGI6'
//	tgpipe join NJ9r4tz8jsg5ZGI6
var joinCmd = &cobra.Command{
	Use:   "join <invite-link-or-hash>",
	Short: "Join a Telegram channel via invite link, then run 'dialogs' to get its ID",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		cfg, err := config.Load(cfgPath)
		if err != nil {
			return err
		}
		if _, err := logging.Setup(logging.Config{
			Level:  resolveLogLevel(cfg.Logging.Level),
			Format: cfg.Logging.Format,
		}); err != nil {
			return err
		}

		// Accept full URL (https://t.me/+HASH or https://t.me/joinchat/HASH)
		// or bare hash.
		hash := args[0]
		if i := strings.LastIndex(hash, "+"); i >= 0 {
			hash = hash[i+1:]
		} else if i := strings.LastIndex(hash, "/"); i >= 0 {
			hash = hash[i+1:]
		}
		// Strip trailing query string or fragment if present.
		if i := strings.IndexAny(hash, "?#"); i >= 0 {
			hash = hash[:i]
		}
		if hash == "" {
			return fmt.Errorf("could not extract invite hash from %q", args[0])
		}

		gate := &session.FloodGate{}
		pool, err := session.NewFetchPool(ctx, session.Config{
			APIID:       cfg.Account.APIID,
			APIHash:     cfg.Account.APIHash,
			SessionFile: cfg.Account.SessionFile,
			Size:        1,
		}, gate)
		if err != nil {
			return err
		}
		defer pool.Close()

		api := tg.NewClient(pool)

		// Check the invite first — works regardless of membership status and
		// returns the channel title + ID so we can print it even when already a member.
		info, checkErr := api.MessagesCheckChatInvite(ctx, hash)
		if checkErr == nil {
			switch v := info.(type) {
			case *tg.ChatInviteAlready:
				if ch, ok := v.Chat.(*tg.Channel); ok {
					fmt.Printf("Already a member of: %s\n", ch.Title)
					fmt.Printf("  ID (for config): %d\n", ch.ID)
					fmt.Printf("  Bot-API ID:      %d\n", -(1_000_000_000_000 + ch.ID))
					fmt.Printf("\nSet godaddy_filter.source_channel: %d in config.yaml\n", ch.ID)
					return nil
				}
			case *tg.ChatInvitePeek:
				if ch, ok := v.Chat.(*tg.Channel); ok {
					fmt.Printf("Peek (already joined): %s\n", ch.Title)
					fmt.Printf("  ID (for config): %d\n", ch.ID)
					fmt.Printf("  Bot-API ID:      %d\n", -(1_000_000_000_000 + ch.ID))
					fmt.Printf("\nSet godaddy_filter.source_channel: %d in config.yaml\n", ch.ID)
					return nil
				}
			}
		}

		res, err := api.MessagesImportChatInvite(ctx, hash)
		if err != nil {
			return fmt.Errorf("join channel (hash=%s): %w", hash, err)
		}

		switch v := res.(type) {
		case *tg.Updates:
			for _, chat := range v.Chats {
				if ch, ok := chat.(*tg.Channel); ok {
					fmt.Printf("Joined: %s\n", ch.Title)
					fmt.Printf("  ID (for config): %d\n", ch.ID)
					fmt.Printf("  Bot-API ID:      %d\n", -(1_000_000_000_000 + ch.ID))
					fmt.Printf("\nSet godaddy_filter.source_channel: %d in config.yaml\n", ch.ID)
				}
			}
		}
		return nil
	},
}
