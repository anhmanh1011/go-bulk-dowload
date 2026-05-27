package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Gauges holds optional fn-pointers that return current (used, cap) of each
// inter-stage channel. Any field may be nil — fmtQ renders "-/-" then.
type Gauges struct {
	ChunkChan  func() (used, cap int)
	LineChan   func() (used, cap int)
	RecordChan func() (used, cap int)
	OutputChan func() (used, cap int)
}

// StatsFetcher pulls job-status counts from the state store. Returning an
// error means "stats unavailable this tick" — the logger logs zeros rather
// than failing.
type StatsFetcher func(ctx context.Context) (pending, inprog, done, failed int64, err error)

// Logger emits a single structured "progress" line every interval. Run
// blocks until ctx is canceled.
type Logger struct {
	counters     *Counters
	gauges       Gauges
	statsFetcher StatsFetcher
	interval     time.Duration
}

// NewLogger constructs a Logger. interval defaults to 30s when ≤ 0.
func NewLogger(c *Counters, g Gauges, sf StatsFetcher, interval time.Duration) *Logger {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &Logger{counters: c, gauges: g, statsFetcher: sf, interval: interval}
}

// Run ticks every interval, computes throughput deltas, and emits one
// "progress" slog.Info line. Returns nil when ctx is canceled.
func (l *Logger) Run(ctx context.Context) error {
	prev := l.counters.Snapshot()
	prevTime := time.Now()
	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			cur := l.counters.Snapshot()
			dt := now.Sub(prevTime).Seconds()
			if dt <= 0 {
				continue
			}
			dl := float64(cur.DownloadBytes-prev.DownloadBytes) / dt / (1024 * 1024)
			up := float64(cur.UploadBytes-prev.UploadBytes) / dt / (1024 * 1024)

			pending, inprog, done, failed := int64(0), int64(0), int64(0), int64(0)
			if l.statsFetcher != nil {
				p, ip, d, f, _ := l.statsFetcher(ctx)
				pending, inprog, done, failed = p, ip, d, f
			}

			slog.Info("progress",
				"stage", "telemetry",
				"download_mbps", fmt.Sprintf("%.1f", dl),
				"upload_mbps", fmt.Sprintf("%.1f", up),
				"chunk_q", fmtQ(l.gauges.ChunkChan),
				"line_q", fmtQ(l.gauges.LineChan),
				"record_q", fmtQ(l.gauges.RecordChan),
				"output_q", fmtQ(l.gauges.OutputChan),
				"jobs_done", done,
				"jobs_inprog", inprog,
				"jobs_pending", pending,
				"jobs_failed", failed,
				"floods_delta", cur.FloodWaits-prev.FloodWaits,
				"retries_delta", cur.Retries-prev.Retries,
				"dropped_lines_delta", cur.DroppedInvalidLines-prev.DroppedInvalidLines,
			)
			prev = cur
			prevTime = now
		}
	}
}

func fmtQ(fn func() (int, int)) string {
	if fn == nil {
		return "-/-"
	}
	used, capacity := fn()
	return fmt.Sprintf("%d/%d", used, capacity)
}
