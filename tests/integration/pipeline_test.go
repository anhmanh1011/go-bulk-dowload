package integration_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manh/tgpipe/internal/config"
	"github.com/manh/tgpipe/internal/pipeline"
	"github.com/manh/tgpipe/internal/session"
	"github.com/manh/tgpipe/internal/state"
)

// roundTrip encodes src to a bin.Buffer and decodes it into dst, mimicking
// what an MTProto transport would do over the wire. Used by fakePool to
// satisfy the contract of tg.Invoker (which the typed tg.NewClient wrapper
// requires).
func roundTrip(t *testing.T, src bin.Encoder, dst bin.Decoder) error {
	t.Helper()
	var buf bin.Buffer
	if err := src.Encode(&buf); err != nil {
		return err
	}
	return dst.Decode(&buf)
}

// fakePool implements session.Pool (which embeds tg.Invoker).
// It serves synthetic .txt content for upload.getFile, accepts
// upload.saveFilePart and messages.sendMedia, and emulates messages.getDialogs
// so channels.Resolve can find the source/target peers.
type fakePool struct {
	t       *testing.T
	mu      sync.Mutex
	content map[int64][]byte // fileID → bytes
	srcID   int64
	srcHash int64
	dstID   int64
	dstHash int64
}

func (p *fakePool) Invoke(_ context.Context, in bin.Encoder, out bin.Decoder) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch r := in.(type) {
	case *tg.UploadGetFileRequest:
		loc, ok := r.Location.(*tg.InputDocumentFileLocation)
		if !ok {
			return fmt.Errorf("unknown loc %T", r.Location)
		}
		data := p.content[loc.ID]
		start := int(r.Offset)
		end := start + r.Limit
		if start > len(data) {
			start = len(data)
		}
		if end > len(data) {
			end = len(data)
		}
		resp := &tg.UploadFile{
			Type:  &tg.StorageFileUnknown{},
			Bytes: data[start:end],
		}
		return roundTrip(p.t, resp, out)

	case *tg.UploadSaveFilePartRequest:
		return roundTrip(p.t, &tg.BoolBox{Bool: &tg.BoolTrue{}}, out)

	case *tg.MessagesSendMediaRequest:
		return roundTrip(p.t, &tg.UpdatesBox{Updates: &tg.Updates{}}, out)

	case *tg.MessagesGetDialogsRequest:
		dlg := &tg.MessagesDialogs{
			Chats: []tg.ChatClass{
				&tg.Channel{ID: p.srcID, AccessHash: p.srcHash, Photo: &tg.ChatPhotoEmpty{}},
				&tg.Channel{ID: p.dstID, AccessHash: p.dstHash, Photo: &tg.ChatPhotoEmpty{}},
			},
		}
		return roundTrip(p.t, &tg.MessagesDialogsBox{Dialogs: dlg}, out)

	case *tg.ChannelsGetFullChannelRequest:
		// VerifyPostRights precheck — return a megagroup-style channel with
		// no banned rights so the precheck passes.
		ic, ok := r.Channel.(*tg.InputChannel)
		if !ok {
			return fmt.Errorf("unexpected input channel type %T", r.Channel)
		}
		full := &tg.MessagesChatFull{
			FullChat: &tg.ChannelFull{
				ID:        ic.ChannelID,
				ChatPhoto: &tg.PhotoEmpty{},
			},
			Chats: []tg.ChatClass{
				&tg.Channel{
					ID:         ic.ChannelID,
					AccessHash: ic.AccessHash,
					Photo:      &tg.ChatPhotoEmpty{},
					Megagroup:  true,
				},
			},
		}
		return roundTrip(p.t, full, out)

	default:
		return fmt.Errorf("unhandled %T", r)
	}
}

func (p *fakePool) Size() int    { return 1 }
func (p *fakePool) Close() error { return nil }

// Compile-time assertion that fakePool satisfies session.Pool.
var _ session.Pool = (*fakePool)(nil)

func makeContent(lines int) []byte {
	var buf []byte
	for i := range lines {
		buf = append(buf, fmt.Appendf(nil, "https://example.com:user%d@x.com:pass%d\n", i, i)...)
	}
	return buf
}

func TestPipeline_EndToEnd(t *testing.T) {
	tmp := t.TempDir()
	dbPath := tmp + "/state.db"
	outDir := tmp + "/out"

	store, err := state.Open(dbPath)
	require.NoError(t, err)
	require.NoError(t, store.Init(context.Background()))
	defer store.Close()

	const srcID, srcHash = int64(-100), int64(42)
	const dstID, dstHash = int64(-200), int64(99)

	pool := &fakePool{
		t: t,
		content: map[int64][]byte{
			1001: makeContent(5000),
			1002: makeContent(4000),
			1003: makeContent(3000),
		},
		srcID: srcID, srcHash: srcHash,
		dstID: dstID, dstHash: dstHash,
	}
	for i, fid := range []int64{1001, 1002, 1003} {
		require.NoError(t, store.InsertJobIfAbsent(context.Background(), state.Job{
			MsgID:          int64(i + 1),
			ChatID:         srcID,
			ChatAccessHash: srcHash,
			FileID:         fid,
			AccessHash:     1,
			FileReference:  []byte{1},
			DCID:           2,
			Size:           int64(len(pool.content[fid])),
			Status:         state.StatusPending,
			CreatedAt:      time.Now(),
			UpdatedAt:      time.Now(),
		}))
	}

	cfg := &config.Config{
		SourceChannel: srcID, TargetChannel: dstID,
		Fetcher: config.FetcherConfig{
			Sessions: 2, ChunkSizeBytes: 1024, ChunkChannelCap: 16, JobChannelCap: 4,
			MaxRetriesPerChunk: 3, MaxRetriesPerJob: 3,
		},
		Splitter:  config.SplitterConfig{Workers: 1, LineChannelCap: 1024},
		Processor: config.ProcessorConfig{Workers: 2, RecordChannelCap: 1024},
		Writer: config.WriterConfig{
			OutputDir: outDir, BatchSizeMB: 1, FlushIntervalSec: 1, OutputChannelCap: 8,
		},
		Uploader:     config.UploaderConfig{Sessions: 1, ParallelParts: 2, UploadChannelCap: 4},
		Backpressure: config.BackpressureConfig{MaxPendingOutputFiles: 32},
		Logging:      config.LoggingConfig{Level: "info", Format: "text", ProgressIntervalSec: 60},
	}

	p := pipeline.New(cfg, store, pool, pool, &session.FloodGate{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	require.NoError(t, p.Run(ctx))

	stats, err := store.Stats(context.Background())
	require.NoError(t, err)
	assert.EqualValues(t, 3, stats.Done, "all 3 sources should be done")
	assert.EqualValues(t, 0, stats.Pending+stats.InProgress+stats.Failed)
}
