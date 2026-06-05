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

// msRunCmd runs the isolated pipeline that keeps only Microsoft consumer
// emails from ms_filter.source_channel and uploads email:pass files
// (ms_filter.batch_lines per file) to ms_filter.target_channel (HOTMAIL_COMBO).
// Uses a separate state DB (ms_filter.db_path) and the MicrosoftOnlyExtractor;
// the writer flushes by line count (BatchSizeMB=0 → size trigger off).
var msRunCmd = &cobra.Command{
	Use:   "ms-run",
	Short: "Run the Microsoft-consumer filter pipeline → HOTMAIL_COMBO",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()
		cfg, err := config.Load(cfgPath)
		if err != nil {
			return err
		}
		if err := cfg.MSFilter.Validate(); err != nil {
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
		store, err := state.Open(cfg.MSFilter.DBPath)
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
			SourceChannel:  cfg.MSFilter.SourceChannel,
			TargetChannel:  cfg.MSFilter.TargetChannel,
			Processor:      &processor.MicrosoftOnlyExtractor{},
			BatchSizeMB:    0, // size trigger off — flush by line count
			BatchSizeLines: cfg.MSFilter.BatchLines,
		})
		return p.Run(ctx)
	},
}
