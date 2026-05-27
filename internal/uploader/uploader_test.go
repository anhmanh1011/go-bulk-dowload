package uploader_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manh/tgpipe/internal/session"
	"github.com/manh/tgpipe/internal/types"
	"github.com/manh/tgpipe/internal/uploader"
)

// roundTrip encodes src and decodes it into dst — mirrors a real Telegram round-trip
// so the typed client wrappers (tg.NewClient(pool).UploadSaveFilePart, …) decode
// the synthetic response correctly.
func roundTrip(src bin.Encoder, dst bin.Decoder) error {
	var b bin.Buffer
	if err := src.Encode(&b); err != nil {
		return err
	}
	return dst.Decode(&b)
}

type fakePool struct {
	mu        sync.Mutex
	saveParts int
	sendMedia int
}

func (p *fakePool) Invoke(_ context.Context, input bin.Encoder, output bin.Decoder) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch input.(type) {
	case *tg.UploadSaveFilePartRequest:
		p.saveParts++
		// upload.saveFilePart returns boolTrue.
		return roundTrip(&tg.BoolBox{Bool: &tg.BoolTrue{}}, output)
	case *tg.MessagesSendMediaRequest:
		p.sendMedia++
		// messages.sendMedia returns Updates — minimum non-nil to satisfy
		// the typed client's response shape.
		return roundTrip(&tg.UpdatesBox{Updates: &tg.Updates{}}, output)
	default:
		return errors.New("unhandled rpc")
	}
}
func (p *fakePool) Size() int    { return 1 }
func (p *fakePool) Close() error { return nil }

type fakeTracker struct {
	mu       sync.Mutex
	uploaded map[string][]int64
}

func (f *fakeTracker) OutputUploaded(_ context.Context, ids []int64, path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.uploaded == nil {
		f.uploaded = map[string][]int64{}
	}
	f.uploaded[path] = append([]int64{}, ids...)
	return nil
}

type fakeRecorder struct{ up, flood, retry atomic.Int64 }

func (f *fakeRecorder) AddUploadBytes(n int64) { f.up.Add(n) }
func (f *fakeRecorder) IncFloodWait()          { f.flood.Add(1) }
func (f *fakeRecorder) IncRetry()              { f.retry.Add(1) }

func TestUploader_HappyPath(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "out_1234_0001.txt")
	require.NoError(t, os.WriteFile(p, []byte("a@b.com:p1\nc@d.com:p2\n"), 0o644))

	pool := &fakePool{}
	tr := &fakeTracker{}
	rec := &fakeRecorder{}
	u := uploader.New(pool, tr, &session.FloodGate{}, rec, uploader.Config{
		Sessions: 1, ParallelParts: 2, TargetChannel: -100, TargetAccessHash: 999,
	})
	in := make(chan types.OutputFile, 1)
	in <- types.OutputFile{Path: p, LineCount: 2, SizeBytes: 22, BatchSeq: 1, SourceMsgIDs: []int64{42}}
	close(in)
	require.NoError(t, u.Run(context.Background(), in))

	assert.Equal(t, 1, pool.sendMedia)
	assert.GreaterOrEqual(t, pool.saveParts, 1)
	assert.Equal(t, int64(22), rec.up.Load())
	tr.mu.Lock()
	defer tr.mu.Unlock()
	assert.Equal(t, []int64{42}, tr.uploaded[p])
	_, statErr := os.Stat(p)
	assert.True(t, os.IsNotExist(statErr), "file should be removed after upload")
}
