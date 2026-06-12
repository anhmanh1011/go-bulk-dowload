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

// godaddyCrawlCmd walks godaddy_filter.source_channel and seeds its jobs DB.
// Mirror of `ms-crawl` pointed at the godaddy_filter config block.
var godaddyCrawlCmd = &cobra.Command{
	Use:   "godaddy-crawl",
	Short: "Walk godaddy_filter.source_channel and seed the GoDaddy jobs DB",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()
		cfg, err := config.Load(cfgPath)
		if err != nil {
			return err
		}
		if err := cfg.GodaddyFilter.Validate(); err != nil {
			return err
		}
		if _, err := logging.Setup(logging.Config{
			Level:  resolveLogLevel(cfg.Logging.Level),
			Format: cfg.Logging.Format,
		}); err != nil {
			return err
		}
		store, err := state.Open(cfg.GodaddyFilter.DBPath)
		if err != nil {
			return err
		}
		defer store.Close()
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
		srcHash, err := channels.Resolve(ctx, pool, cfg.GodaddyFilter.SourceChannel)
		if err != nil {
			return err
		}
		c := crawler.New(pool, store, crawler.Config{
			SourceChannel:    cfg.GodaddyFilter.SourceChannel,
			SourceAccessHash: srcHash,
			BatchSize:        100,
		})
		return c.Run(ctx)
	},
}
