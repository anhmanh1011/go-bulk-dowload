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

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Start the 5-stage pipeline",
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
		if debugPprof {
			go func() { _ = http.ListenAndServe("127.0.0.1:6060", nil) }()
		}
		store, err := state.Open(cfg.State.DBPath)
		if err != nil {
			return err
		}
		defer store.Close()
		// Init runs migrations + the resume SQL
		// (UPDATE jobs SET status='pending' WHERE status='in_progress').
		// Any rows half-processed by a crashed run get picked up by
		// PickPending inside Pipeline.Run.
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
			SourceChannel: cfg.SourceChannel,
			TargetChannel: cfg.TargetChannel,
			Processor:     &processor.UrlUserPassExtractor{},
			BatchSizeMB:   cfg.Writer.BatchSizeMB,
			// BatchSizeLines left 0 — run flushes by MB, unchanged.
		})
		return p.Run(ctx)
	},
}
