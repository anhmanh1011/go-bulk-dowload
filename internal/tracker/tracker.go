package tracker

import (
	"context"
	"sync"

	"github.com/manh/tgpipe/internal/state"
)

// Store is the subset of state.Store that the tracker needs.
type Store interface {
	MarkDone(ctx context.Context, msgID int64, outputPath string) error
	MarkFailed(ctx context.Context, msgID int64, errMsg string) error
}

// Verify state.Store satisfies the interface at compile time.
var _ Store = (*state.Store)(nil)

type srcState struct {
	totalChunks        int
	chunksConsumed     int
	outputFilesPending int
	lastOutputPath     string
}

// SourceTracker accounts for per-source completion across the pipeline.
// All methods are safe for concurrent use.
type SourceTracker struct {
	mu      sync.Mutex
	sources map[int64]*srcState
	store   Store
}

func New(store Store) *SourceTracker {
	return &SourceTracker{
		sources: make(map[int64]*srcState),
		store:   store,
	}
}

func (t *SourceTracker) Register(msgID int64, totalChunks int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.sources[msgID]; exists {
		return // idempotent
	}
	t.sources[msgID] = &srcState{totalChunks: totalChunks}
}

func (t *SourceTracker) ChunkConsumed(msgID int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if s, ok := t.sources[msgID]; ok {
		s.chunksConsumed++
	}
}

func (t *SourceTracker) OutputFlushed(srcIDs []int64, path string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, id := range srcIDs {
		if s, ok := t.sources[id]; ok {
			s.outputFilesPending++
			s.lastOutputPath = path
		}
	}
}

// OutputUploaded notifies the tracker that path has been uploaded successfully.
// For each contributing source, decrement the pending count and — if all chunks
// have been consumed and no pending uploads remain — persist done state.
func (t *SourceTracker) OutputUploaded(ctx context.Context, srcIDs []int64, path string) error {
	t.mu.Lock()
	var toMarkDone []struct {
		msgID int64
		path  string
	}
	for _, id := range srcIDs {
		s, ok := t.sources[id]
		if !ok {
			continue
		}
		s.outputFilesPending--
		if s.chunksConsumed >= s.totalChunks && s.outputFilesPending <= 0 {
			toMarkDone = append(toMarkDone, struct {
				msgID int64
				path  string
			}{id, s.lastOutputPath})
			delete(t.sources, id)
		}
	}
	t.mu.Unlock()
	for _, m := range toMarkDone {
		if err := t.store.MarkDone(ctx, m.msgID, m.path); err != nil {
			return err
		}
	}
	return nil
}

// Fail removes the source from tracking and marks it failed in the store.
func (t *SourceTracker) Fail(ctx context.Context, msgID int64, errMsg string) error {
	t.mu.Lock()
	delete(t.sources, msgID)
	t.mu.Unlock()
	return t.store.MarkFailed(ctx, msgID, errMsg)
}
