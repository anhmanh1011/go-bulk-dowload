package main

import (
	"github.com/spf13/cobra"

	"github.com/manh/tgpipe/internal/channels"
	"github.com/manh/tgpipe/internal/config"
	"github.com/manh/tgpipe/internal/crawler"
	"github.com/manh/tgpipe/internal/logging"
	"github.com/manh/tgpipe/internal/session"
	"github.com/manh/tgpipe/internal/state"
)

var crawlCmd = &cobra.Command{
	Use:   "crawl",
	Short: "Walk source channel and seed jobs table",
	RunE: func(cmd *cobra.Command, _ []string) error {
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
		store, err := state.Open(cfg.State.DBPath)
		if err != nil {
			return err
		}
		defer store.Close()
		// Init runs migrations and the resume SQL
		// (UPDATE jobs SET status='pending' WHERE status='in_progress').
		if err := store.Init(ctx); err != nil {
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
		srcHash, err := channels.Resolve(ctx, pool, cfg.SourceChannel)
		if err != nil {
			return err
		}
		c := crawler.New(pool, store, crawler.Config{
			SourceChannel:    cfg.SourceChannel,
			SourceAccessHash: srcHash,
			BatchSize:        100,
		})
		return c.Run(ctx)
	},
}
