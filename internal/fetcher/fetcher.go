// Package fetcher implements Stage 1 of the pipeline: parallel chunk
// download of source documents from Telegram via gotd/td.
//
// Each worker pops one Job from the input channel and fetches its document
// sequentially as 1MB chunks, emitting *types.Chunk to the output channel in
// Seq order. Per-account FLOOD_WAIT signals propagate via session.FloodGate;
// FILE_REFERENCE_EXPIRED is recovered inline by re-fetching the source
// message and updating the Store before retrying the same chunk.
package fetcher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/manh/tgpipe/internal/retry"
	"github.com/manh/tgpipe/internal/session"
	"github.com/manh/tgpipe/internal/state"
	"github.com/manh/tgpipe/internal/types"
)

// Store is the subset of state.Store the fetcher needs.
type Store interface {
	UpdateFileReference(ctx context.Context, msgID int64, ref []byte) error
}

// Tracker is the subset of tracker.SourceTracker the fetcher needs.
// (ChunkConsumed lives downstream in the Splitter, not here.)
type Tracker interface {
	Register(msgID int64, totalChunks int)
}

// Recorder receives telemetry counters from the fetcher. All methods must be
// safe for concurrent use.
type Recorder interface {
	AddDownloadBytes(int64)
	IncFloodWait()
	IncRetry()
	IncFileRefExpired()
}

// Compile-time assertion: state.Store satisfies the fetcher.Store interface.
var _ Store = (*state.Store)(nil)

// Config carries tunables for the fetcher.
type Config struct {
	Sessions           int
	ChunkSizeBytes     int
	MaxRetriesPerChunk int
}

// Fetcher pulls Jobs and produces Chunks.
type Fetcher struct {
	pool     session.Pool
	api      *tg.Client
	store    Store
	tracker  Tracker
	gate     *session.FloodGate
	recorder Recorder
	cfg      Config
}

// New constructs a Fetcher. Callers are responsible for the lifecycle of
// pool, store, tracker, gate, and recorder.
func New(pool session.Pool, store Store, tracker Tracker, gate *session.FloodGate, rec Recorder, cfg Config) *Fetcher {
	return &Fetcher{
		pool:     pool,
		api:      tg.NewClient(pool),
		store:    store,
		tracker:  tracker,
		gate:     gate,
		recorder: rec,
		cfg:      cfg,
	}
}

// Run consumes Jobs from `jobs` and emits Chunks to `out`. Returns when
// `jobs` is closed and all in-flight fetches are done, or when ctx is
// canceled. Per-job errors are logged and the worker continues — the only
// way Run returns non-nil is via ctx cancellation propagated by callers.
func (f *Fetcher) Run(ctx context.Context, jobs <-chan state.Job, out chan<- types.Chunk) error {
	var wg sync.WaitGroup
	for i := range f.cfg.Sessions {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-jobs:
					if !ok {
						return
					}
					if err := f.fetchJob(ctx, job, out); err != nil {
						if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
							return
						}
						slog.Error("fetch job",
							"stage", "fetcher",
							"worker", workerID,
							"msg_id", job.MsgID,
							"err", err,
						)
					}
				}
			}
		}(i)
	}
	wg.Wait()
	return nil
}

// fetchJob walks the document chunk-by-chunk in Seq order. Chunks for one
// MsgID are emitted sequentially so the downstream Splitter sees them in
// order — this is load-bearing for the edge-line drop logic in Stage 2.
func (f *Fetcher) fetchJob(ctx context.Context, job state.Job, out chan<- types.Chunk) error {
	totalChunks := int((job.Size + int64(f.cfg.ChunkSizeBytes) - 1) / int64(f.cfg.ChunkSizeBytes))
	f.tracker.Register(job.MsgID, totalChunks)

	loc := &tg.InputDocumentFileLocation{
		ID:            job.FileID,
		AccessHash:    job.AccessHash,
		FileReference: job.FileReference,
	}
	req := &tg.UploadGetFileRequest{
		Location: loc,
		Limit:    f.cfg.ChunkSizeBytes,
	}

	for seq := range totalChunks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		req.Offset = int64(seq) * int64(f.cfg.ChunkSizeBytes)
		var data []byte
		err := retry.WithBackoff(ctx, f.cfg.MaxRetriesPerChunk, func() error {
			res, invokeErr := f.api.UploadGetFile(ctx, req)
			if invokeErr != nil {
				if tgerr.Is(invokeErr, "FILE_REFERENCE_EXPIRED") {
					f.recorder.IncFileRefExpired()
					if refreshErr := f.refreshFileReference(ctx, &job, loc); refreshErr != nil {
						return retry.Retryable(refreshErr)
					}
					return retry.Retryable(invokeErr)
				}
				if fw, sec := session.IsFloodWait(invokeErr); fw {
					f.recorder.IncFloodWait()
					f.gate.Trigger(time.Duration(sec+1) * time.Second)
					return retry.Retryable(invokeErr)
				}
				f.recorder.IncRetry()
				return retry.Retryable(invokeErr)
			}
			file, ok := res.(*tg.UploadFile)
			if !ok {
				// CDN redirect or unexpected variant — non-retryable so we
				// fail fast instead of looping; caller logs and skips.
				return fmt.Errorf("unexpected upload response type %T", res)
			}
			data = file.Bytes
			return nil
		})
		if err != nil {
			return fmt.Errorf("fetch chunk seq=%d msg=%d: %w", seq, job.MsgID, err)
		}

		f.recorder.AddDownloadBytes(int64(len(data)))
		chunk := types.Chunk{
			MsgID:  job.MsgID,
			Seq:    seq,
			Data:   data,
			IsLast: seq == totalChunks-1,
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- chunk:
		}
	}
	return nil
}

// refreshFileReference re-fetches the source message to obtain a fresh
// FILE_REFERENCE via channels.getMessages, mutating both `loc` (so the
// caller's next retry uses it) and `job.FileReference` (for any further
// chunks in this job), and persists the new reference to the Store.
func (f *Fetcher) refreshFileReference(ctx context.Context, job *state.Job, loc *tg.InputDocumentFileLocation) error {
	res, err := f.api.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
		Channel: &tg.InputChannel{
			ChannelID:  job.ChatID,
			AccessHash: job.ChatAccessHash,
		},
		ID: []tg.InputMessageClass{&tg.InputMessageID{ID: int(job.MsgID)}},
	})
	if err != nil {
		return fmt.Errorf("refresh ref: %w", err)
	}
	modified, ok := res.AsModified()
	if !ok {
		return errors.New("refresh ref: messages response not modified-shape")
	}
	for _, m := range modified.GetMessages() {
		msg, ok := m.(*tg.Message)
		if !ok {
			continue
		}
		media, ok := msg.Media.(*tg.MessageMediaDocument)
		if !ok {
			continue
		}
		doc, ok := media.Document.(*tg.Document)
		if !ok {
			continue
		}
		loc.FileReference = doc.FileReference
		job.FileReference = doc.FileReference
		return f.store.UpdateFileReference(ctx, job.MsgID, doc.FileReference)
	}
	return errors.New("refresh ref: document not found in response")
}
