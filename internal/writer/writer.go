package writer

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/manh/tgpipe/internal/types"
)

type Tracker interface {
	OutputFlushed(srcIDs []int64, path string)
}

type Config struct {
	OutputDir        string
	BatchSizeMB      int
	FlushIntervalSec int
	OutputChannelCap int
	BatchSeqStart    int // for tests; production = 1
}

type Writer struct {
	cfg     Config
	gate    *BackpressureGate
	tracker Tracker
	seq     atomic.Int32
}

func New(cfg Config, gate *BackpressureGate, tr Tracker) *Writer {
	if cfg.BatchSeqStart < 1 {
		cfg.BatchSeqStart = 1
	}
	w := &Writer{cfg: cfg, gate: gate, tracker: tr}
	w.seq.Store(int32(cfg.BatchSeqStart - 1))
	return w
}

// Run consumes Records from `in`, batches them in-memory up to BatchSizeMB or
// FlushIntervalSec, and emits finalized OutputFile metadata on `out`. The
// writer is single-goroutine (the batch buffer is not shared across workers).
//
// On `in` close, flushes any remaining records (graceful shutdown preserves data).
func (w *Writer) Run(ctx context.Context, in <-chan types.Record, out chan<- types.OutputFile) error {
	if err := os.MkdirAll(w.cfg.OutputDir, 0o755); err != nil {
		return fmt.Errorf("mkdir output: %w", err)
	}
	var buf bytes.Buffer
	srcSet := make(map[int64]struct{})
	lineCount := 0
	threshold := w.cfg.BatchSizeMB * 1024 * 1024
	flushInterval := time.Duration(w.cfg.FlushIntervalSec) * time.Second
	timer := time.NewTimer(flushInterval)
	defer timer.Stop()
	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(flushInterval)
	}
	flush := func() error {
		if buf.Len() == 0 {
			return nil
		}
		// Acquire backpressure BEFORE writing to disk — the gate exists to
		// prevent the output directory from growing unbounded. Acquiring after
		// the write would defeat that purpose (the disk has already grown).
		if w.gate != nil {
			if err := w.gate.Acquire(ctx); err != nil {
				return err
			}
		}
		seq := int(w.seq.Add(1))
		path := filepath.Join(w.cfg.OutputDir,
			fmt.Sprintf("out_%d_%04d.txt", time.Now().Unix(), seq))
		f, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("create output: %w", err)
		}
		size, err := f.Write(buf.Bytes())
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
		if err != nil {
			_ = os.Remove(path)
			return fmt.Errorf("write output: %w", err)
		}
		ids := make([]int64, 0, len(srcSet))
		for id := range srcSet {
			ids = append(ids, id)
		}
		of := types.OutputFile{
			Path: path, LineCount: lineCount, SizeBytes: int64(size),
			BatchSeq: seq, SourceMsgIDs: ids,
		}
		w.tracker.OutputFlushed(ids, path)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- of:
		}
		buf.Reset()
		srcSet = make(map[int64]struct{})
		lineCount = 0
		resetTimer()
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			// Best-effort flush before exit. Log a shutdown-flush failure so
			// the operator notices data loss rather than silently discarding it.
			if ferr := flush(); ferr != nil {
				slog.Error("writer: shutdown flush failed", "err", ferr,
					"pending_lines", lineCount, "pending_bytes", buf.Len())
			}
			return ctx.Err()
		case <-timer.C:
			if err := flush(); err != nil {
				return err
			}
		case rec, ok := <-in:
			if !ok {
				return flush()
			}
			buf.Write(rec.Email)
			buf.WriteByte(':')
			buf.Write(rec.Pass)
			buf.WriteByte('\n')
			srcSet[rec.MsgID] = struct{}{}
			lineCount++
			if buf.Len() >= threshold {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}
