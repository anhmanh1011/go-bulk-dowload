package processor

import (
	"context"
	"runtime"
	"sync"

	"github.com/manh/tgpipe/internal/types"
)

// Recorder is the subset of telemetry.Counters the processor needs.
type Recorder interface {
	IncDroppedInvalidLine()
	IncLinesEmitted()
}

// Processor is the Stage-3 line processor. It fans Line in over a worker
// pool, applies impl.Process, and emits surviving Records downstream.
type Processor struct {
	workers  int
	impl     LineProcessor
	recorder Recorder
}

func New(workers int, impl LineProcessor, rec Recorder) *Processor {
	if workers <= 0 {
		workers = runtime.NumCPU() * 2
	}
	return &Processor{workers: workers, impl: impl, recorder: rec}
}

// Run consumes lines from `in` until it's closed or ctx is cancelled.
// Never returns an error — failures from impl.Process are fatal and
// propagated via ctx by upstream coordination, but here we just stop
// the worker on first error to avoid masking it with downstream sends.
func (p *Processor) Run(ctx context.Context, in <-chan types.Line, out chan<- types.Record) error {
	var wg sync.WaitGroup
	for range p.workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case ln, ok := <-in:
					if !ok {
						return
					}
					rec, keep, err := p.impl.Process(ln.Data)
					if err != nil {
						return
					}
					if !keep {
						p.recorder.IncDroppedInvalidLine()
						continue
					}
					rec.MsgID = ln.MsgID
					p.recorder.IncLinesEmitted()
					select {
					case <-ctx.Done():
						return
					case out <- rec:
					}
				}
			}
		}()
	}
	wg.Wait()
	return nil
}
