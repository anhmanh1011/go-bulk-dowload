// Package pipeline wires all 5 stages (fetcher → splitter → processor →
// writer → uploader) under a single errgroup so the first fatal error
// cancels every stage. The job feeder pulls pending rows from SQLite in
// batches and closes jobsCh when no more remain — that close propagates
// through the chain and shuts the pipeline down cleanly.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/manh/tgpipe/internal/channels"
	"github.com/manh/tgpipe/internal/config"
	"github.com/manh/tgpipe/internal/fetcher"
	"github.com/manh/tgpipe/internal/processor"
	"github.com/manh/tgpipe/internal/session"
	"github.com/manh/tgpipe/internal/splitter"
	"github.com/manh/tgpipe/internal/state"
	"github.com/manh/tgpipe/internal/telemetry"
	"github.com/manh/tgpipe/internal/tracker"
	"github.com/manh/tgpipe/internal/types"
	"github.com/manh/tgpipe/internal/uploader"
	"github.com/manh/tgpipe/internal/writer"
)

// Pipeline owns the long-lived dependencies (config, store, session pools,
// flood gate, tracker, counters) and constructs/runs the per-run dataflow.
type Pipeline struct {
	cfg        *config.Config
	store      *state.Store
	fetchPool  session.Pool
	uploadPool session.Pool
	gate       *session.FloodGate
	tracker    *tracker.SourceTracker
	counters   *telemetry.Counters
}

// New constructs a Pipeline. Caller retains ownership of cfg, store, pools,
// and gate — Run does not close any of them.
func New(cfg *config.Config, store *state.Store, fetchPool, uploadPool session.Pool, gate *session.FloodGate) *Pipeline {
	return &Pipeline{
		cfg:        cfg,
		store:      store,
		fetchPool:  fetchPool,
		uploadPool: uploadPool,
		gate:       gate,
		tracker:    tracker.New(store),
		counters:   &telemetry.Counters{},
	}
}

// Run starts the orchestrator. Assumes store.Init(ctx) has already been
// called by the caller (cmd/tgpipe/cmd_run.go). store.Init runs the resume
// SQL:
//
//	UPDATE jobs SET status = 'pending' WHERE status = 'in_progress'
//
// so any rows half-processed by a crashed run are picked up again here.
func (p *Pipeline) Run(ctx context.Context) error {
	// Resolve channel access hashes once at startup (via messages.getDialogs).
	// New jobs crawled later need a hash too; the cmd_run.go startup also
	// passes srcHash into the crawler, but the pipeline itself only needs
	// dstHash to upload (jobs already carry chat_access_hash per-row for the
	// fetch side, populated by the crawler).
	srcHash, err := channels.Resolve(ctx, p.fetchPool, p.cfg.SourceChannel)
	if err != nil {
		return fmt.Errorf("resolve source channel access hash: %w", err)
	}
	dstHash, err := channels.Resolve(ctx, p.uploadPool, p.cfg.TargetChannel)
	if err != nil {
		return fmt.Errorf("resolve target channel access hash: %w", err)
	}
	slog.Info("channels resolved", "stage", "pipeline",
		"source", p.cfg.SourceChannel, "src_hash", srcHash,
		"target", p.cfg.TargetChannel)

	jobsCh := make(chan state.Job, p.cfg.Fetcher.JobChannelCap)
	chunkCh := make(chan types.Chunk, p.cfg.Fetcher.ChunkChannelCap)
	lineCh := make(chan types.Line, p.cfg.Splitter.LineChannelCap)
	recordCh := make(chan types.Record, p.cfg.Processor.RecordChannelCap)
	outputCh := make(chan types.OutputFile, p.cfg.Writer.OutputChannelCap)

	gauges := telemetry.Gauges{
		ChunkChan:  func() (int, int) { return len(chunkCh), cap(chunkCh) },
		LineChan:   func() (int, int) { return len(lineCh), cap(lineCh) },
		RecordChan: func() (int, int) { return len(recordCh), cap(recordCh) },
		OutputChan: func() (int, int) { return len(outputCh), cap(outputCh) },
	}
	statsFn := func(ctx context.Context) (int64, int64, int64, int64, error) {
		s, err := p.store.Stats(ctx)
		return s.Pending, s.InProgress, s.Done, s.Failed, err
	}
	progress := telemetry.NewLogger(p.counters, gauges, statsFn,
		time.Duration(p.cfg.Logging.ProgressIntervalSec)*time.Second)

	splWorkers := p.cfg.Splitter.Workers
	if splWorkers <= 0 {
		splWorkers = runtime.NumCPU()
	}
	procWorkers := p.cfg.Processor.Workers
	if procWorkers <= 0 {
		procWorkers = runtime.NumCPU() * 2
	}

	fetch := fetcher.New(p.fetchPool, p.store, p.tracker, p.gate, p.counters, fetcher.Config{
		Sessions:           p.cfg.Fetcher.Sessions,
		ChunkSizeBytes:     p.cfg.Fetcher.ChunkSizeBytes,
		MaxRetriesPerChunk: p.cfg.Fetcher.MaxRetriesPerChunk,
	})
	spl := splitter.New(splWorkers, p.tracker)
	proc := processor.New(procWorkers, &processor.UrlUserPassExtractor{}, p.counters)
	bp := writer.NewBackpressureGate(p.cfg.Writer.OutputDir, p.cfg.Backpressure.MaxPendingOutputFiles)
	w := writer.New(writer.Config{
		OutputDir:        p.cfg.Writer.OutputDir,
		BatchSizeMB:      p.cfg.Writer.BatchSizeMB,
		FlushIntervalSec: p.cfg.Writer.FlushIntervalSec,
		OutputChannelCap: p.cfg.Writer.OutputChannelCap,
		BatchSeqStart:    1,
	}, bp, p.tracker)
	up := uploader.New(p.uploadPool, p.tracker, p.gate, p.counters, uploader.Config{
		Sessions:         p.cfg.Uploader.Sessions,
		ParallelParts:    p.cfg.Uploader.ParallelParts,
		TargetChannel:    p.cfg.TargetChannel,
		TargetAccessHash: dstHash,
	})

	g, gctx := errgroup.WithContext(ctx)

	// Job feeder: read pending jobs from DB → jobsCh, close when no more.
	g.Go(func() error {
		defer close(jobsCh)
		const batch = 16
		for {
			select {
			case <-gctx.Done():
				return gctx.Err()
			default:
			}
			jobs, err := p.store.PickPending(gctx, batch)
			if err != nil {
				return err
			}
			if len(jobs) == 0 {
				return nil
			}
			for _, j := range jobs {
				select {
				case <-gctx.Done():
					return gctx.Err()
				case jobsCh <- j:
				}
			}
		}
	})
	g.Go(func() error { defer close(chunkCh); return fetch.Run(gctx, jobsCh, chunkCh) })
	g.Go(func() error { defer close(lineCh); return spl.Run(gctx, chunkCh, lineCh) })
	g.Go(func() error { defer close(recordCh); return proc.Run(gctx, lineCh, recordCh) })
	g.Go(func() error { defer close(outputCh); return w.Run(gctx, recordCh, outputCh) })
	g.Go(func() error { return up.Run(gctx, outputCh) })
	g.Go(func() error { return progress.Run(gctx) })

	err = g.Wait()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
