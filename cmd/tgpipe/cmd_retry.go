package main

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/manh/tgpipe/internal/config"
	"github.com/manh/tgpipe/internal/state"
)

var retryStatusFlag string

var retryCmd = &cobra.Command{
	Use:   "retry",
	Short: "Reset jobs with the given status back to pending",
	RunE: func(cmd *cobra.Command, _ []string) error {
		if retryStatusFlag == "" {
			return fmt.Errorf("--status is required (e.g. failed)")
		}
		cfg, err := config.Load(cfgPath)
		if err != nil {
			return err
		}
		store, err := state.Open(cfg.State.DBPath)
		if err != nil {
			return err
		}
		defer store.Close()
		return store.ResetStatus(context.Background(), state.JobStatus(retryStatusFlag), state.StatusPending)
	},
}

func init() {
	retryCmd.Flags().StringVar(&retryStatusFlag, "status", "", "current status to flip back to pending (e.g. failed)")
}
