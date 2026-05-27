package processor

import "github.com/manh/tgpipe/internal/types"

// LineProcessor transforms a raw line into a Record. The bool return
// indicates whether the record should be kept (false → drop, e.g.
// malformed input). A non-nil error is fatal for the pipeline.
type LineProcessor interface {
	Process(line []byte) (types.Record, bool, error)
}
