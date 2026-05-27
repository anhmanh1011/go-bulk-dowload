package main

import (
	"context"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/manh/tgpipe/internal/config"
	"github.com/manh/tgpipe/internal/state"
)

var resetMsgIDs string

var resetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Reset specific msg_ids back to pending",
	RunE: func(cmd *cobra.Command, _ []string) error {
		cfg, err := config.Load(cfgPath)
		if err != nil {
			return err
		}
		store, err := state.Open(cfg.State.DBPath)
		if err != nil {
			return err
		}
		defer store.Close()
		ctx := context.Background()
		for _, s := range strings.Split(resetMsgIDs, ",") {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			id, perr := strconv.ParseInt(s, 10, 64)
			if perr != nil {
				return perr
			}
			if err := store.ResetMsgID(ctx, id); err != nil {
				return err
			}
		}
		return nil
	},
}

func init() {
	resetCmd.Flags().StringVar(&resetMsgIDs, "msg-ids", "", "comma-separated msg_ids to reset")
}
