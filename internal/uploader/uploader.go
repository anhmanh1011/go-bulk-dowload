// Package uploader implements Stage 5 of the pipeline: parallel-part upload
// of finalized batch files to a Telegram target channel.
//
// Each worker pops one OutputFile from the input channel, streams the file
// to Telegram via upload.saveFilePart in parallel parts (bounded by
// ParallelParts), publishes the document via messages.sendMedia, then
// notifies the tracker and removes the local file. Per-account FLOOD_WAIT
// signals propagate via session.FloodGate.
package uploader

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"golang.org/x/sync/errgroup"

	"github.com/manh/tgpipe/internal/retry"
	"github.com/manh/tgpipe/internal/session"
	"github.com/manh/tgpipe/internal/types"
)

// Tracker is the subset of tracker.SourceTracker the uploader needs.
type Tracker interface {
	OutputUploaded(ctx context.Context, srcIDs []int64, path string) error
}

// Recorder receives telemetry counters from the uploader. All methods must
// be safe for concurrent use.
type Recorder interface {
	AddUploadBytes(int64)
	IncFloodWait()
	IncRetry()
}

// Config carries tunables for the uploader.
type Config struct {
	Sessions         int
	ParallelParts    int
	TargetChannel    int64
	TargetAccessHash int64 // required — resolved at startup by internal/channels
}

// Uploader consumes OutputFile items and publishes them to a target channel.
type Uploader struct {
	pool     session.Pool
	api      *tg.Client // typed wrapper
	tracker  Tracker
	gate     *session.FloodGate
	recorder Recorder
	cfg      Config
}

// New constructs an Uploader. Callers are responsible for the lifecycle of
// pool, tracker, gate, and recorder.
func New(pool session.Pool, tracker Tracker, gate *session.FloodGate, rec Recorder, cfg Config) *Uploader {
	return &Uploader{
		pool:     pool,
		api:      tg.NewClient(pool),
		tracker:  tracker,
		gate:     gate,
		recorder: rec,
		cfg:      cfg,
	}
}

// Run consumes OutputFiles from `in` and uploads each to the target channel.
// Returns when `in` is closed and all in-flight uploads are done, or when
// ctx is canceled. Per-file errors are logged and the worker continues —
// the file remains on disk so the next run can retry it.
func (u *Uploader) Run(ctx context.Context, in <-chan types.OutputFile) error {
	var wg sync.WaitGroup
	for range u.cfg.Sessions {
		wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case of, ok := <-in:
					if !ok {
						return
					}
					if err := u.uploadOne(ctx, of); err != nil {
						if errors.Is(err, context.Canceled) {
							return
						}
						slog.Error("upload file", "stage", "uploader", "path", of.Path, "err", err)
						// File remains on disk; next run can retry.
						continue
					}
					if err := u.tracker.OutputUploaded(ctx, of.SourceMsgIDs, of.Path); err != nil {
						slog.Error("tracker notify", "stage", "uploader", "path", of.Path, "err", err)
						continue
					}
					if err := os.Remove(of.Path); err != nil {
						slog.Warn("remove uploaded file", "stage", "uploader", "path", of.Path, "err", err)
					}
				}
			}
		})
	}
	wg.Wait()
	return nil
}

const partSize = 512 * 1024 // 512KB — accepted by Telegram and a multiple of 1KB

// uploadOne streams the file via upload.saveFilePart in chunks, then publishes
// via messages.sendMedia. Parts are read from disk INSIDE each goroutine —
// only `ParallelParts` part-sized buffers exist in memory at any moment.
func (u *Uploader) uploadOne(ctx context.Context, of types.OutputFile) error {
	f, err := os.Open(of.Path)
	if err != nil {
		return fmt.Errorf("open output: %w", err)
	}
	defer f.Close()

	fileID := mustRandomInt64()
	totalParts := int((of.SizeBytes + int64(partSize) - 1) / int64(partSize))

	// Bounded buffer pool: ParallelParts buffers, each partSize bytes.
	// Acquiring a buffer also acts as the parallelism semaphore.
	bufCh := make(chan []byte, u.cfg.ParallelParts)
	for range u.cfg.ParallelParts {
		bufCh <- make([]byte, partSize)
	}

	g, gctx := errgroup.WithContext(ctx)
	for partIdx := range totalParts {
		// Acquire a buffer (blocks if all ParallelParts are in-flight).
		var buf []byte
		select {
		case buf = <-bufCh:
		case <-gctx.Done():
			return gctx.Err()
		}
		g.Go(func() error {
			defer func() { bufCh <- buf[:cap(buf)] }()
			// Read THIS part from disk inside the goroutine — no whole-file load.
			n, rerr := f.ReadAt(buf, int64(partIdx)*int64(partSize))
			if rerr != nil && !errors.Is(rerr, io.EOF) {
				return fmt.Errorf("read part %d: %w", partIdx, rerr)
			}
			part := buf[:n]
			if len(part) == 0 {
				return fmt.Errorf("part %d: zero bytes read", partIdx)
			}
			return retry.WithBackoff(gctx, 5, func() error {
				_, err := u.api.UploadSaveFilePart(gctx, &tg.UploadSaveFilePartRequest{
					FileID:   fileID,
					FilePart: partIdx,
					Bytes:    part,
				})
				if err != nil {
					if fw, sec := session.IsFloodWait(err); fw {
						u.recorder.IncFloodWait()
						u.gate.Trigger(time.Duration(sec+1) * time.Second)
						return retry.Retryable(err)
					}
					if tgerr.Is(err, "FILE_PARTS_INVALID", "FILE_PART_SIZE_INVALID") {
						// Permanent — caller should not retry this file.
						return err
					}
					u.recorder.IncRetry()
					return retry.Retryable(err)
				}
				return nil
			})
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	u.recorder.AddUploadBytes(of.SizeBytes)

	// Send media with caption.
	caption := fmt.Sprintf("Batch %d · %d records · %s",
		of.BatchSeq, of.LineCount, humanize(of.SizeBytes))
	media := &tg.InputMediaUploadedDocument{
		File: &tg.InputFile{
			ID:    fileID,
			Parts: totalParts,
			Name:  filepath.Base(of.Path),
		},
		MimeType:   "text/plain",
		Attributes: []tg.DocumentAttributeClass{&tg.DocumentAttributeFilename{FileName: filepath.Base(of.Path)}},
	}
	sendReq := &tg.MessagesSendMediaRequest{
		Peer: &tg.InputPeerChannel{
			ChannelID:  u.cfg.TargetChannel,
			AccessHash: u.cfg.TargetAccessHash,
		},
		Media:    media,
		Message:  caption,
		RandomID: mustRandomInt64(),
	}
	return retry.WithBackoff(ctx, 5, func() error {
		_, err := u.api.MessagesSendMedia(ctx, sendReq)
		if err != nil {
			if fw, sec := session.IsFloodWait(err); fw {
				u.recorder.IncFloodWait()
				u.gate.Trigger(time.Duration(sec+1) * time.Second)
				return retry.Retryable(err)
			}
			u.recorder.IncRetry()
			return retry.Retryable(err)
		}
		return nil
	})
}

// helpers ---------------------------------------------------------------------

func randomInt64() (int64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(b[:])), nil
}

// mustRandomInt64 produces a cryptographically random int64 for the
// messages.sendMedia RandomID field. Telegram uses RandomID to deduplicate
// retried sends — collisions would silently drop messages. If rand fails we
// MUST NOT fall back to a timestamp (predictable, low entropy). The pipeline
// will surface this as a fatal error at the call site.
func mustRandomInt64() int64 {
	n, err := randomInt64()
	if err != nil {
		// In the rare event the OS RNG is unavailable, terminate loudly —
		// continuing would silently corrupt the output channel with duplicate
		// or dropped messages.
		panic(fmt.Sprintf("uploader: crypto/rand failed: %v", err))
	}
	return n
}

func humanize(n int64) string {
	const KB, MB, GB = 1024, 1024 * 1024, 1024 * 1024 * 1024
	switch {
	case n >= GB:
		return fmt.Sprintf("%.2f GB", float64(n)/GB)
	case n >= MB:
		return fmt.Sprintf("%.2f MB", float64(n)/MB)
	case n >= KB:
		return fmt.Sprintf("%.2f KB", float64(n)/KB)
	}
	return fmt.Sprintf("%d B", n)
}
