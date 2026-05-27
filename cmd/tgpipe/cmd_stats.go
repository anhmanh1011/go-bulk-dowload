package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/manh/tgpipe/internal/config"
	"github.com/manh/tgpipe/internal/state"
)

var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Show DB summary",
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
		s, err := store.Stats(cmd.Context())
		if err != nil {
			return err
		}
		fmt.Printf("pending:      %d\n", s.Pending)
		fmt.Printf("in_progress:  %d\n", s.InProgress)
		fmt.Printf("done:         %d\n", s.Done)
		fmt.Printf("failed:       %d\n", s.Failed)
		fmt.Printf("total size:   %d bytes\n", s.TotalSize)
		if s.TotalSize > 0 {
			fmt.Printf("completed:    %.1f%% (%d / %d bytes)\n",
				100*float64(s.DoneSize)/float64(s.TotalSize), s.DoneSize, s.TotalSize)
		}
		return nil
	},
}
