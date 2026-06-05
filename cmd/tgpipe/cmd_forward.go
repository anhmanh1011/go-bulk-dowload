package main

import (
	"errors"

	"github.com/spf13/cobra"

	"github.com/manh/tgpipe/internal/channels"
	"github.com/manh/tgpipe/internal/config"
	"github.com/manh/tgpipe/internal/forwarder"
	"github.com/manh/tgpipe/internal/logging"
	"github.com/manh/tgpipe/internal/session"
	"github.com/manh/tgpipe/internal/state"
)

// forwardCmd mirrors .txt document messages from source_channel to
// target_channel using messages.forwardMessages (copy, no source header). It
// never downloads file bytes and applies no line-level processing — it is a
// pure server-side mirror, separate from the download pipeline. Resume is
// idempotent via the `forwarded` table.
var forwardCmd = &cobra.Command{
	Use:   "forward",
	Short: "Mirror all .txt files from source channel to target channel (no download, no filter)",
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
		if cfg.SourceChannel == 0 || cfg.TargetChannel == 0 {
			return errors.New("forward requires both source_channel and target_channel in config")
		}

		store, err := state.Open(cfg.State.DBPath)
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

		srcHash, err := channels.Resolve(ctx, pool, cfg.SourceChannel)
		if err != nil {
			return err
		}
		dstHash, err := channels.Resolve(ctx, pool, cfg.TargetChannel)
		if err != nil {
			return err
		}
		// Fail fast if the account can't post to the target.
		if err := channels.VerifyPostRights(ctx, pool, cfg.TargetChannel, dstHash); err != nil {
			return err
		}

		f := forwarder.New(pool, store, gate, forwarder.Config{
			SourceChannel:    cfg.SourceChannel,
			SourceAccessHash: srcHash,
			TargetChannel:    cfg.TargetChannel,
			TargetAccessHash: dstHash,
		})
		return f.Run(ctx)
	},
}
