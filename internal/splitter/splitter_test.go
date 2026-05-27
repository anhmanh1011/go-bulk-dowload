package splitter_test

import (
	"context"
	"sync"
	"testing"

	"github.com/manh/tgpipe/internal/splitter"
	"github.com/manh/tgpipe/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeTracker struct {
	mu             sync.Mutex
	chunksConsumed map[int64]int
}

func (f *fakeTracker) ChunkConsumed(id int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.chunksConsumed == nil {
		f.chunksConsumed = map[int64]int{}
	}
	f.chunksConsumed[id]++
}

func TestSplitter_SingleChunkFile(t *testing.T) {
	t.Parallel()
	tr := &fakeTracker{}
	s := splitter.New(1, tr)
	in := make(chan types.Chunk, 1)
	out := make(chan types.Line, 16)
	in <- types.Chunk{MsgID: 7, Seq: 0, IsLast: true,
		Data: []byte("HEAD\nfoo\nbar\nbaz\nTAIL")}
	close(in)
	require.NoError(t, s.Run(context.Background(), in, out))
	close(out)
	var got []string
	for ln := range out {
		got = append(got, string(ln.Data))
	}
	assert.Equal(t, []string{"foo", "bar", "baz"}, got)
}

func TestSplitter_MultiChunkFile_StitchesAcrossBoundary(t *testing.T) {
	t.Parallel()
	tr := &fakeTracker{}
	s := splitter.New(1, tr)
	in := make(chan types.Chunk, 3)
	out := make(chan types.Line, 16)
	in <- types.Chunk{MsgID: 9, Seq: 0, IsLast: false, Data: []byte("HEAD\nfoo\nba")}
	in <- types.Chunk{MsgID: 9, Seq: 1, IsLast: false, Data: []byte("r\nbaz\nqu")}
	in <- types.Chunk{MsgID: 9, Seq: 2, IsLast: true, Data: []byte("x\nTAIL")}
	close(in)
	require.NoError(t, s.Run(context.Background(), in, out))
	close(out)
	var got []string
	for ln := range out {
		got = append(got, string(ln.Data))
	}
	assert.Equal(t, []string{"foo", "bar", "baz", "qux"}, got)
	assert.Equal(t, 3, tr.chunksConsumed[9])
}

func TestSplitter_InterleavedSources(t *testing.T) {
	t.Parallel()
	tr := &fakeTracker{}
	s := splitter.New(1, tr)
	in := make(chan types.Chunk, 4)
	out := make(chan types.Line, 16)
	in <- types.Chunk{MsgID: 7, Seq: 0, IsLast: false, Data: []byte("H7\nA\nB\n")}
	in <- types.Chunk{MsgID: 8, Seq: 0, IsLast: false, Data: []byte("H8\nX\nY\n")}
	in <- types.Chunk{MsgID: 7, Seq: 1, IsLast: true, Data: []byte("C\nT7")}
	in <- types.Chunk{MsgID: 8, Seq: 1, IsLast: true, Data: []byte("Z\nT8")}
	close(in)
	require.NoError(t, s.Run(context.Background(), in, out))
	close(out)
	got := map[int64][]string{}
	for ln := range out {
		got[ln.MsgID] = append(got[ln.MsgID], string(ln.Data))
	}
	assert.Equal(t, []string{"A", "B", "C"}, got[7])
	assert.Equal(t, []string{"X", "Y", "Z"}, got[8])
}

func TestSplitter_ContextCancel(t *testing.T) {
	t.Parallel()
	s := splitter.New(1, &fakeTracker{})
	in := make(chan types.Chunk)
	out := make(chan types.Line, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := s.Run(ctx, in, out)
	assert.NoError(t, err)
	close(in)
	close(out)
}

func TestSplitter_SingleChunkNoNewline(t *testing.T) {
	t.Parallel()
	s := splitter.New(1, &fakeTracker{})
	in := make(chan types.Chunk, 1)
	out := make(chan types.Line, 4)
	in <- types.Chunk{MsgID: 1, Seq: 0, IsLast: true, Data: []byte("nonewline")}
	close(in)
	require.NoError(t, s.Run(context.Background(), in, out))
	close(out)
	var got []string
	for ln := range out {
		got = append(got, string(ln.Data))
	}
	assert.Empty(t, got, "single-chunk file with no newline should emit nothing")
}
