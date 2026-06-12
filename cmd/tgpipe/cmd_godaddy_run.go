package main

import (
	"net/http"
	_ "net/http/pprof"

	"github.com/spf13/cobra"

	"github.com/manh/tgpipe/internal/config"
	"github.com/manh/tgpipe/internal/logging"
	"github.com/manh/tgpipe/internal/pipeline"
	"github.com/manh/tgpipe/internal/processor"
	"github.com/manh/tgpipe/internal/session"
	"github.com/manh/tgpipe/internal/state"
)

// godaddyRunCmd runs an isolated pipeline that keeps only lines containing
// "godaddy.com" (including raw e-mail content where From is
// donotreply@godaddy.com) from godaddy_filter.source_channel and uploads
// the results to godaddy_filter.target_channel. Uses a separate state DB
// (godaddy_filter.db_path) so it never conflicts with the main pipeline.
var godaddyRunCmd = &cobra.Command{
	Use:   "godaddy-run",
	Short: "Run the GoDaddy filter pipeline → godaddy_filter.target_channel",
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
		if debugPprof {
			go func() { _ = http.ListenAndServe("127.0.0.1:6060", nil) }()
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
		fetchPool, err := session.NewFetchPool(ctx, session.Config{
			APIID:       cfg.Account.APIID,
			APIHash:     cfg.Account.APIHash,
			SessionFile: cfg.Account.SessionFile,
			Size:        cfg.Fetcher.Sessions,
		}, gate)
		if err != nil {
			return err
		}
		defer fetchPool.Close()
		uploadPool, err := session.NewUploadPool(ctx, session.Config{
			APIID:       cfg.Account.APIID,
			APIHash:     cfg.Account.APIHash,
			SessionFile: cfg.Account.SessionFile,
			Size:        cfg.Uploader.Sessions,
		}, gate)
		if err != nil {
			return err
		}
		defer uploadPool.Close()
		p := pipeline.New(cfg, store, fetchPool, uploadPool, gate, pipeline.Options{
			SourceChannel:  cfg.GodaddyFilter.SourceChannel,
			TargetChannel:  cfg.GodaddyFilter.TargetChannel,
			Processor:      &processor.GodaddyFilter{},
			BatchSizeMB:    0,
			BatchSizeLines: cfg.GodaddyFilter.BatchLines,
			NewestFirst:    true,
		})
		return p.Run(ctx)
	},
}
