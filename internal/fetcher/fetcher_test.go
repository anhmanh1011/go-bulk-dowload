package fetcher_test

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manh/tgpipe/internal/fetcher"
	"github.com/manh/tgpipe/internal/session"
	"github.com/manh/tgpipe/internal/state"
	"github.com/manh/tgpipe/internal/types"
)

// fakePool implements session.Pool for tests. It satisfies tg.Invoker by
// encoding a synthetic *tg.UploadFile into a bin.Buffer and decoding it back
// into the caller-supplied bin.Decoder — gotd's own canonical test pattern.
type fakePool struct {
	mu             sync.Mutex
	calls          int
	respondGet     func(offset int64) []byte
	failFirst      atomic.Int32
	invokeOverride func(input bin.Encoder, output bin.Decoder) error
}

func roundTrip(src bin.Encoder, dst bin.Decoder) error {
	var b bin.Buffer
	if err := src.Encode(&b); err != nil {
		return err
	}
	return dst.Decode(&b)
}

func (p *fakePool) Invoke(_ context.Context, input bin.Encoder, output bin.Decoder) error {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	if p.invokeOverride != nil {
		return p.invokeOverride(input, output)
	}
	if getReq, ok := input.(*tg.UploadGetFileRequest); ok {
		if p.failFirst.Load() > 0 {
			p.failFirst.Add(-1)
			return errors.New("transient")
		}
		resp := &tg.UploadFile{
			Type:  &tg.StorageFileUnknown{},
			Bytes: p.respondGet(getReq.Offset),
		}
		return roundTrip(resp, output)
	}
	return errors.New("unhandled rpc")
}

func (p *fakePool) Size() int    { return 1 }
func (p *fakePool) Close() error { return nil }

type fakeStore struct{}

func (fakeStore) UpdateFileReference(context.Context, int64, []byte) error { return nil }

type capturingStore struct {
	onUpdate func(ctx context.Context, msgID int64, ref []byte) error
}

func (c *capturingStore) UpdateFileReference(ctx context.Context, msgID int64, ref []byte) error {
	return c.onUpdate(ctx, msgID, ref)
}

type fakeTracker struct {
	mu         sync.Mutex
	registered map[int64]int
}

func (f *fakeTracker) Register(id int64, total int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.registered == nil {
		f.registered = map[int64]int{}
	}
	f.registered[id] = total
}

type fakeRecorder struct {
	dl      atomic.Int64
	flood   atomic.Int64
	retry   atomic.Int64
	expired atomic.Int64
}

func (f *fakeRecorder) AddDownloadBytes(n int64) { f.dl.Add(n) }
func (f *fakeRecorder) IncFloodWait()            { f.flood.Add(1) }
func (f *fakeRecorder) IncRetry()                { f.retry.Add(1) }
func (f *fakeRecorder) IncFileRefExpired()       { f.expired.Add(1) }

func TestFetcher_DispatchesParallelChunks(t *testing.T) {
	t.Parallel()
	pool := &fakePool{respondGet: func(offset int64) []byte {
		return []byte{byte(offset / 1024)}
	}}
	gate := &session.FloodGate{}
	tr := &fakeTracker{}
	rec := &fakeRecorder{}
	f := fetcher.New(pool, fakeStore{}, tr, gate, rec, fetcher.Config{
		Sessions: 2, ChunkSizeBytes: 1024, MaxRetriesPerChunk: 3,
	})
	jobs := make(chan state.Job, 1)
	out := make(chan types.Chunk, 16)
	jobs <- state.Job{MsgID: 42, FileID: 1, AccessHash: 1, FileReference: []byte{1}, Size: 3 * 1024}
	close(jobs)
	go func() {
		require.NoError(t, f.Run(context.Background(), jobs, out))
		close(out)
	}()
	var chunks []types.Chunk
	for c := range out {
		chunks = append(chunks, c)
	}
	assert.Len(t, chunks, 3)
	// Order may interleave across workers/sessions, so sort by Seq before asserting.
	sort.Slice(chunks, func(i, j int) bool { return chunks[i].Seq < chunks[j].Seq })
	for i, c := range chunks {
		assert.Equal(t, int64(42), c.MsgID)
		assert.Equal(t, i, c.Seq)
		assert.Equal(t, i == 2, c.IsLast, "IsLast set only on final chunk")
	}
	assert.Equal(t, 3, tr.registered[42])
}

func TestFetcher_RetryOnTransient(t *testing.T) {
	t.Parallel()
	pool := &fakePool{respondGet: func(offset int64) []byte { return []byte("ok") }}
	pool.failFirst.Store(2)
	gate := &session.FloodGate{}
	rec := &fakeRecorder{}
	f := fetcher.New(pool, fakeStore{}, &fakeTracker{}, gate, rec, fetcher.Config{
		Sessions: 1, ChunkSizeBytes: 1024, MaxRetriesPerChunk: 5,
	})
	jobs := make(chan state.Job, 1)
	out := make(chan types.Chunk, 4)
	jobs <- state.Job{MsgID: 1, FileID: 1, AccessHash: 1, FileReference: []byte{1}, Size: 1024}
	close(jobs)
	go func() {
		require.NoError(t, f.Run(context.Background(), jobs, out))
		close(out)
	}()
	got := 0
	for range out {
		got++
	}
	assert.Equal(t, 1, got)
	assert.GreaterOrEqual(t, rec.retry.Load(), int64(1))
}

func TestFetcher_RefreshesFileReferenceOnExpired(t *testing.T) {
	t.Parallel()

	var (
		getFileCalls atomic.Int32
		getMsgCalls  atomic.Int32
	)

	pool := &fakePool{}
	// Override Invoke to handle both upload.getFile and channels.getMessages.
	// First upload.getFile call returns FILE_REFERENCE_EXPIRED; subsequent calls succeed.
	pool.invokeOverride = func(input bin.Encoder, output bin.Decoder) error {
		switch req := input.(type) {
		case *tg.UploadGetFileRequest:
			n := getFileCalls.Add(1)
			if n == 1 {
				return tgerr.New(0, "FILE_REFERENCE_EXPIRED")
			}
			resp := &tg.UploadFile{Type: &tg.StorageFileUnknown{}, Bytes: []byte("ok")}
			return roundTrip(resp, output)
		case *tg.ChannelsGetMessagesRequest:
			_ = req
			getMsgCalls.Add(1)
			media := &tg.MessageMediaDocument{}
			media.SetDocument(&tg.Document{
				ID:            1,
				AccessHash:    1,
				FileReference: []byte{9, 9, 9},
			})
			msg := &tg.Message{
				ID:     1,
				PeerID: &tg.PeerChannel{ChannelID: 100},
			}
			msg.SetMedia(media)
			resp := &tg.MessagesMessages{
				Messages: []tg.MessageClass{msg},
			}
			return roundTrip(resp, output)
		}
		return errors.New("unhandled rpc")
	}

	var (
		storeMu        sync.Mutex
		storeUpdatedTo []byte
	)
	store := &capturingStore{
		onUpdate: func(_ context.Context, _ int64, ref []byte) error {
			storeMu.Lock()
			defer storeMu.Unlock()
			storeUpdatedTo = append([]byte(nil), ref...)
			return nil
		},
	}

	gate := &session.FloodGate{}
	rec := &fakeRecorder{}
	f := fetcher.New(pool, store, &fakeTracker{}, gate, rec, fetcher.Config{
		Sessions: 1, ChunkSizeBytes: 1024, MaxRetriesPerChunk: 3,
	})

	jobs := make(chan state.Job, 1)
	out := make(chan types.Chunk, 1)
	jobs <- state.Job{
		MsgID:          1,
		ChatID:         100,
		ChatAccessHash: 200,
		FileID:         1,
		AccessHash:     1,
		FileReference:  []byte{1},
		Size:           1024,
	}
	close(jobs)
	go func() { require.NoError(t, f.Run(context.Background(), jobs, out)); close(out) }()

	var got []types.Chunk
	for c := range out {
		got = append(got, c)
	}
	assert.Len(t, got, 1)
	assert.GreaterOrEqual(t, getFileCalls.Load(), int32(2))
	assert.Equal(t, int32(1), getMsgCalls.Load())
	assert.Equal(t, int64(1), rec.expired.Load())

	storeMu.Lock()
	defer storeMu.Unlock()
	assert.Equal(t, []byte{9, 9, 9}, storeUpdatedTo)
}
