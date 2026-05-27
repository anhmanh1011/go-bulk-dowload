package writer_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/manh/tgpipe/internal/types"
	"github.com/manh/tgpipe/internal/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeTracker struct {
	mu      sync.Mutex
	flushed []struct {
		ids  []int64
		path string
	}
}

func (f *fakeTracker) OutputFlushed(ids []int64, path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.flushed = append(f.flushed, struct {
		ids  []int64
		path string
	}{append([]int64{}, ids...), path})
}

func TestWriter_FlushOnSize(t *testing.T) {
	dir := t.TempDir()
	tr := &fakeTracker{}
	w := writer.New(writer.Config{
		OutputDir:        dir,
		BatchSizeMB:      1, // small for test
		FlushIntervalSec: 60,
		BatchSeqStart:    1,
	}, nil, tr)
	in := make(chan types.Record, 1024)
	out := make(chan types.OutputFile, 4)
	// Produce ~1MB of records
	go func() {
		for i := range 20000 {
			in <- types.Record{MsgID: int64(i % 3), Email: []byte("u@x.com"), Pass: []byte("password1234567")}
		}
		close(in)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { require.NoError(t, w.Run(ctx, in, out)); close(out) }()
	count := 0
	var totalLines int
	for f := range out {
		count++
		totalLines += f.LineCount
		assert.FileExists(t, f.Path)
	}
	assert.Greater(t, count, 0)
	assert.Equal(t, 20000, totalLines)
}

func TestWriter_FlushOnInterval(t *testing.T) {
	dir := t.TempDir()
	tr := &fakeTracker{}
	w := writer.New(writer.Config{
		OutputDir:        dir,
		BatchSizeMB:      1000,
		FlushIntervalSec: 1, // 1s
		BatchSeqStart:    1,
	}, nil, tr)
	in := make(chan types.Record, 4)
	out := make(chan types.OutputFile, 4)
	in <- types.Record{MsgID: 1, Email: []byte("a@b.com"), Pass: []byte("x")}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); _ = w.Run(ctx, in, out); close(out) }()
	select {
	case f := <-out:
		assert.Equal(t, 1, f.LineCount)
	case <-time.After(2500 * time.Millisecond):
		t.Fatal("expected flush by interval")
	}
	close(in)
	<-done
}

func TestWriter_TracksSourceMsgIDs(t *testing.T) {
	dir := t.TempDir()
	tr := &fakeTracker{}
	w := writer.New(writer.Config{
		OutputDir:        dir,
		BatchSizeMB:      1,
		FlushIntervalSec: 60,
		BatchSeqStart:    1,
	}, nil, tr)
	in := make(chan types.Record, 100)
	out := make(chan types.OutputFile, 4)
	go func() {
		for i := range 30000 {
			in <- types.Record{MsgID: int64(i % 4), Email: []byte("u@x.com"), Pass: []byte("paddingpaddingpadding")}
		}
		close(in)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = w.Run(ctx, in, out); close(out) }()
	for f := range out {
		assert.NotEmpty(t, f.SourceMsgIDs)
		for _, id := range f.SourceMsgIDs {
			assert.True(t, id >= 0 && id < 4)
		}
	}
}

func TestWriter_FileNamingConvention(t *testing.T) {
	dir := t.TempDir()
	tr := &fakeTracker{}
	w := writer.New(writer.Config{
		OutputDir:        dir,
		BatchSizeMB:      1,
		FlushIntervalSec: 60,
		BatchSeqStart:    1,
	}, nil, tr)
	in := make(chan types.Record, 1024)
	out := make(chan types.OutputFile, 4)
	go func() {
		for range 30000 {
			in <- types.Record{MsgID: 1, Email: []byte("a@b.com"), Pass: []byte("pad12345678901234567")}
		}
		close(in)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = w.Run(ctx, in, out); close(out) }()
	for f := range out {
		base := filepath.Base(f.Path)
		assert.True(t, strings.HasPrefix(base, "out_"))
		assert.True(t, strings.HasSuffix(base, ".txt"))
	}
}

func TestWriter_FlushOnShutdownPreservesData(t *testing.T) {
	dir := t.TempDir()
	tr := &fakeTracker{}
	w := writer.New(writer.Config{
		OutputDir:        dir,
		BatchSizeMB:      1000, // never triggers by size
		FlushIntervalSec: 60,
		BatchSeqStart:    1,
	}, nil, tr)
	in := make(chan types.Record, 4)
	out := make(chan types.OutputFile, 4)
	in <- types.Record{MsgID: 1, Email: []byte("a@b.com"), Pass: []byte("x")}
	close(in)
	require.NoError(t, w.Run(context.Background(), in, out))
	close(out)
	files, _ := os.ReadDir(dir)
	assert.Len(t, files, 1, "expected one flushed file on graceful shutdown")
}
