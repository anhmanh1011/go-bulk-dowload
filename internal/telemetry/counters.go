// Package telemetry provides atomic counters and a periodic progress logger
// for the pipeline. Counters is a wide struct that satisfies every stage's
// narrow Recorder interface via method promotion.
package telemetry

import "sync/atomic"

// Counters aggregates pipeline-wide metrics. All fields are safe for
// concurrent use; readers should go through Snapshot for a coherent view.
type Counters struct {
	DownloadBytes       atomic.Int64
	UploadBytes         atomic.Int64
	LinesEmitted        atomic.Int64
	DroppedInvalidLines atomic.Int64
	DroppedEdgeBytes    atomic.Int64
	FloodWaits          atomic.Int64
	Retries             atomic.Int64
	FileRefExpiredHits  atomic.Int64
	JobsDone            atomic.Int64
	JobsFailed          atomic.Int64
}

// Snapshot is an immutable point-in-time view of all counters. Used by the
// progress logger to compute deltas between ticks.
type Snapshot struct {
	DownloadBytes, UploadBytes              int64
	LinesEmitted, DroppedInvalidLines       int64
	DroppedEdgeBytes                        int64
	FloodWaits, Retries, FileRefExpiredHits int64
	JobsDone, JobsFailed                    int64
}

// Snapshot returns a coherent point-in-time view. Each Load is atomic but
// the snapshot is not transactional — slight skew across fields is OK for
// progress logging.
func (c *Counters) Snapshot() Snapshot {
	return Snapshot{
		DownloadBytes:       c.DownloadBytes.Load(),
		UploadBytes:         c.UploadBytes.Load(),
		LinesEmitted:        c.LinesEmitted.Load(),
		DroppedInvalidLines: c.DroppedInvalidLines.Load(),
		DroppedEdgeBytes:    c.DroppedEdgeBytes.Load(),
		FloodWaits:          c.FloodWaits.Load(),
		Retries:             c.Retries.Load(),
		FileRefExpiredHits:  c.FileRefExpiredHits.Load(),
		JobsDone:            c.JobsDone.Load(),
		JobsFailed:          c.JobsFailed.Load(),
	}
}
