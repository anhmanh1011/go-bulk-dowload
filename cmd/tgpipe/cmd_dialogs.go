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

// dialogsCmd lists the channels/groups the account belongs to so the user can
// discover the raw channel ID to put in config.source_channel /
// config.target_channel. The resolver (internal/channels) matches the *raw*
// channel ID (tg.Channel.ID, positive), so that is the value printed under "ID".
var dialogsCmd = &cobra.Command{
	Use:   "dialogs",
	Short: "List joined channels/groups with their IDs (to fill source_channel/target_channel)",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()
		cfg, err := config.Load(cfgPath)
		if err != nil {
			return err
		}
		// Keep RPC noise down so the table is readable; warn is enough.
		lvl := resolveLogLevel(cfg.Logging.Level)
		if lvl == "info" {
			lvl = "warn"
		}
		if _, err := logging.Setup(logging.Config{Level: lvl, Format: cfg.Logging.Format}); err != nil {
			return err
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
		res, err := api.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
			OffsetPeer: &tg.InputPeerEmpty{},
			Limit:      500,
		})
		if err != nil {
			return fmt.Errorf("getDialogs: %w", err)
		}
		var chats []tg.ChatClass
		switch v := res.(type) {
		case *tg.MessagesDialogs:
			chats = v.Chats
		case *tg.MessagesDialogsSlice:
			chats = v.Chats
		default:
			return fmt.Errorf("unexpected dialogs response %T", res)
		}

		fmt.Printf("%-14s  %-8s  %-18s  %s\n", "ID (config)", "TYPE", "BOT-API ID", "TITLE")
		fmt.Println(strings.Repeat("-", 72))
		n := 0
		for _, c := range chats {
			ch, ok := c.(*tg.Channel)
			if !ok {
				continue
			}
			kind := "channel"
			if ch.Megagroup {
				kind = "group"
			}
			// Bot-API form for reference: -(1000000000000 + id).
			botAPI := -(1_000_000_000_000 + ch.ID)
			fmt.Printf("%-14d  %-8s  %-18d  %s\n", ch.ID, kind, botAPI, ch.Title)
			n++
		}
		if n == 0 {
			fmt.Println("(no channels found — make sure the account has joined the channel)")
		}
		fmt.Printf("\nPut the value under \"ID (config)\" into source_channel/target_channel in %s\n", cfgPath)
		return nil
	},
}
