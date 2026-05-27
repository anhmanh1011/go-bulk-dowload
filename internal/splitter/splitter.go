package splitter

import (
	"bytes"
	"context"
	"log/slog"

	"github.com/manh/tgpipe/internal/types"
)

// Tracker is the subset of SourceTracker the splitter needs.
type Tracker interface {
	ChunkConsumed(msgID int64)
}

// Splitter is the Stage-2 line splitter. Single goroutine; maintains a per-MsgID
// remainder buffer to stitch lines across 1 MB chunk boundaries. Edge bytes
// (head of Seq==0, tail of IsLast) are dropped exactly once per source.
type Splitter struct {
	tracker Tracker
}

func New(workers int, tr Tracker) *Splitter {
	if workers > 1 {
		slog.Warn("splitter: workers>1 ignored — splitter is single-goroutine by design",
			"requested", workers)
	}
	return &Splitter{tracker: tr}
}

// Run consumes chunks until `in` is closed or ctx is cancelled. Never returns
// an error — failures upstream propagate via ctx.
func (s *Splitter) Run(ctx context.Context, in <-chan types.Chunk, out chan<- types.Line) error {
	remainders := make(map[int64][]byte)

	emit := func(msgID int64, line []byte) bool {
		buf := make([]byte, len(line))
		copy(buf, line)
		select {
		case <-ctx.Done():
			slog.Debug("splitter: emit cancelled mid-line", "msg_id", msgID)
			return false
		case out <- types.Line{MsgID: msgID, Data: buf}:
			return true
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case chunk, ok := <-in:
			if !ok {
				return nil
			}
			data := chunk.Data

			// data here aliases chunk.Data; safe because emit() and remainder code both copy.
			if chunk.Seq == 0 {
				if i := bytes.IndexByte(data, '\n'); i >= 0 {
					data = data[i+1:]
				} else {
					data = nil
				}
			} else if r, has := remainders[chunk.MsgID]; has && len(r) > 0 {
				stitched := make([]byte, 0, len(r)+len(data))
				stitched = append(stitched, r...)
				stitched = append(stitched, data...)
				data = stitched
				delete(remainders, chunk.MsgID)
			}

			lastNL := bytes.LastIndexByte(data, '\n')
			if lastNL >= 0 {
				body := data[:lastNL]
				if !chunk.IsLast && lastNL+1 < len(data) {
					tail := make([]byte, len(data)-(lastNL+1))
					copy(tail, data[lastNL+1:])
					remainders[chunk.MsgID] = tail
				}
				for len(body) > 0 {
					idx := bytes.IndexByte(body, '\n')
					if idx < 0 {
						if !emit(chunk.MsgID, body) {
							return nil
						}
						break
					}
					if !emit(chunk.MsgID, body[:idx]) {
						return nil
					}
					body = body[idx+1:]
				}
			} else if !chunk.IsLast {
				buf := make([]byte, len(data))
				copy(buf, data)
				remainders[chunk.MsgID] = buf
			}

			if chunk.IsLast {
				delete(remainders, chunk.MsgID)
			}

			s.tracker.ChunkConsumed(chunk.MsgID)
		}
	}
}
