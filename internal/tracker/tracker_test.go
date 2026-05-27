package tracker_test

import (
	"context"
	"sync"
	"testing"

	"github.com/manh/tgpipe/internal/state"
	"github.com/manh/tgpipe/internal/tracker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeStore struct {
	mu     sync.Mutex
	done   map[int64]string
	failed map[int64]string
}

func newFakeStore() *fakeStore {
	return &fakeStore{done: map[int64]string{}, failed: map[int64]string{}}
}
func (f *fakeStore) MarkDone(_ context.Context, id int64, path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.done[id] = path
	return nil
}
func (f *fakeStore) MarkFailed(_ context.Context, id int64, msg string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed[id] = msg
	return nil
}

// unused state methods to satisfy interface — provide noop stubs in test
func (f *fakeStore) Init(context.Context) error                               { return nil }
func (f *fakeStore) InsertJob(context.Context, state.Job) error               { return nil }
func (f *fakeStore) PickPending(context.Context, int) ([]state.Job, error)    { return nil, nil }
func (f *fakeStore) UpdateFileReference(context.Context, int64, []byte) error { return nil }
func (f *fakeStore) Stats(context.Context) (state.Stats, error)               { return state.Stats{}, nil }
func (f *fakeStore) Close() error                                             { return nil }

func TestTracker_MarksDoneWhenAllUploaded(t *testing.T) {
	store := newFakeStore()
	tr := tracker.New(store)
	ctx := context.Background()
	tr.Register(100, 3) // 3 chunks for msg_id 100
	// Stage 2 consumes all 3 chunks
	tr.ChunkConsumed(100)
	tr.ChunkConsumed(100)
	tr.ChunkConsumed(100)
	// Writer flushes 2 output files containing msg_id=100
	tr.OutputFlushed([]int64{100}, "/out/0001.txt")
	tr.OutputFlushed([]int64{100}, "/out/0002.txt")
	// First upload — not done yet (still 1 file pending)
	require.NoError(t, tr.OutputUploaded(ctx, []int64{100}, "/out/0001.txt"))
	store.mu.Lock()
	_, doneAfter1 := store.done[100]
	store.mu.Unlock()
	assert.False(t, doneAfter1)
	// Second upload — now should mark done
	require.NoError(t, tr.OutputUploaded(ctx, []int64{100}, "/out/0002.txt"))
	store.mu.Lock()
	path, doneAfter2 := store.done[100]
	store.mu.Unlock()
	assert.True(t, doneAfter2)
	assert.Equal(t, "/out/0002.txt", path)
}

func TestTracker_DoesNotMarkDoneIfChunksMissing(t *testing.T) {
	store := newFakeStore()
	tr := tracker.New(store)
	ctx := context.Background()
	tr.Register(200, 5)
	tr.ChunkConsumed(200) // only 1 of 5
	tr.OutputFlushed([]int64{200}, "/out/x.txt")
	require.NoError(t, tr.OutputUploaded(ctx, []int64{200}, "/out/x.txt"))
	store.mu.Lock()
	_, done := store.done[200]
	store.mu.Unlock()
	assert.False(t, done)
}

func TestTracker_MultipleSources_Mixed(t *testing.T) {
	store := newFakeStore()
	tr := tracker.New(store)
	ctx := context.Background()
	tr.Register(1, 1)
	tr.Register(2, 1)
	tr.ChunkConsumed(1)
	tr.ChunkConsumed(2)
	tr.OutputFlushed([]int64{1, 2}, "/out/mixed.txt")
	require.NoError(t, tr.OutputUploaded(ctx, []int64{1, 2}, "/out/mixed.txt"))
	store.mu.Lock()
	_, d1 := store.done[1]
	_, d2 := store.done[2]
	store.mu.Unlock()
	assert.True(t, d1)
	assert.True(t, d2)
}

func TestTracker_ConcurrentSafe(t *testing.T) {
	store := newFakeStore()
	tr := tracker.New(store)
	ctx := context.Background()
	const N = 100
	for i := int64(1); i <= N; i++ {
		tr.Register(i, 2)
	}
	var wg sync.WaitGroup
	for i := int64(1); i <= N; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			tr.ChunkConsumed(id)
			tr.ChunkConsumed(id)
			tr.OutputFlushed([]int64{id}, "/out/x.txt")
			_ = tr.OutputUploaded(ctx, []int64{id}, "/out/x.txt")
		}(i)
	}
	wg.Wait()
	store.mu.Lock()
	defer store.mu.Unlock()
	assert.Len(t, store.done, N)
}
