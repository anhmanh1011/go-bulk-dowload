package telemetry

// Recorder adapter methods. Each stage depends on a narrow subset of these
// (e.g. uploader.Recorder needs AddUploadBytes/IncFloodWait/IncRetry).
// *Counters satisfies all of them via method promotion — no separate
// adapter type is needed.

func (c *Counters) AddDownloadBytes(n int64)    { c.DownloadBytes.Add(n) }
func (c *Counters) AddUploadBytes(n int64)      { c.UploadBytes.Add(n) }
func (c *Counters) IncLinesEmitted()            { c.LinesEmitted.Add(1) }
func (c *Counters) IncDroppedInvalidLine()      { c.DroppedInvalidLines.Add(1) }
func (c *Counters) AddDroppedEdgeBytes(n int64) { c.DroppedEdgeBytes.Add(n) }
func (c *Counters) IncFloodWait()               { c.FloodWaits.Add(1) }
func (c *Counters) IncRetry()                   { c.Retries.Add(1) }
func (c *Counters) IncFileRefExpired()          { c.FileRefExpiredHits.Add(1) }
func (c *Counters) IncJobDone()                 { c.JobsDone.Add(1) }
func (c *Counters) IncJobFailed()               { c.JobsFailed.Add(1) }
