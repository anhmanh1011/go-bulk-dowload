package state_test

import (
	"context"
	"testing"
	"time"

	"github.com/manh/tgpipe/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustOpen(t *testing.T) *state.Store {
	t.Helper()
	s, err := state.Open(":memory:")
	require.NoError(t, err)
	require.NoError(t, s.Init(context.Background()))
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func sampleJob(msgID int64) state.Job {
	return state.Job{
		MsgID:          msgID,
		ChatID:         -100,
		ChatAccessHash: 5678,
		FileID:         1000 + msgID,
		AccessHash:     1234,
		FileReference:  []byte{1, 2, 3},
		DCID:           2,
		Size:           1024,
		FileName:       "x.txt",
		MimeType:       "text/plain",
		Status:         state.StatusPending,
		CreatedAt:      time.Unix(1000, 0),
		UpdatedAt:      time.Unix(1000, 0),
	}
}

func TestStore_InsertAndPick(t *testing.T) {
	t.Parallel()
	s := mustOpen(t)
	ctx := context.Background()
	for i := int64(1); i <= 5; i++ {
		require.NoError(t, s.InsertJobIfAbsent(ctx, sampleJob(i)))
	}
	jobs, err := s.PickPending(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, jobs, 5)
	for _, j := range jobs {
		assert.Equal(t, state.StatusInProgress, j.Status)
		assert.Equal(t, int64(5678), j.ChatAccessHash)
	}
	more, err := s.PickPending(ctx, 10)
	require.NoError(t, err)
	assert.Empty(t, more, "no more pending")
}

func TestStore_MarkDone(t *testing.T) {
	t.Parallel()
	s := mustOpen(t)
	ctx := context.Background()
	require.NoError(t, s.InsertJobIfAbsent(ctx, sampleJob(1)))
	_, err := s.PickPending(ctx, 1)
	require.NoError(t, err)
	require.NoError(t, s.MarkDone(ctx, 1, "/out/x.txt"))
	stats, err := s.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats.Done)
}

func TestStore_ResumeResetsInProgress(t *testing.T) {
	t.Parallel()
	s, err := state.Open(":memory:")
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, s.Init(ctx))
	require.NoError(t, s.InsertJobIfAbsent(ctx, sampleJob(1)))
	_, err = s.PickPending(ctx, 1)
	require.NoError(t, err)
	// Re-Init simulates restart — should flip in_progress → pending
	require.NoError(t, s.Init(ctx))
	jobs, err := s.PickPending(ctx, 10)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	_ = s.Close()
}

func TestStore_UpdateFileReference(t *testing.T) {
	t.Parallel()
	s := mustOpen(t)
	ctx := context.Background()
	require.NoError(t, s.InsertJobIfAbsent(ctx, sampleJob(1)))
	newRef := []byte{9, 9, 9}
	require.NoError(t, s.UpdateFileReference(ctx, 1, newRef))
}

func TestStore_MarkFailed(t *testing.T) {
	t.Parallel()
	s := mustOpen(t)
	ctx := context.Background()
	require.NoError(t, s.InsertJobIfAbsent(ctx, sampleJob(1)))
	_, err := s.PickPending(ctx, 1)
	require.NoError(t, err)
	require.NoError(t, s.MarkFailed(ctx, 1, "auth invalid"))
	stats, err := s.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats.Failed)
}

func TestStore_InsertJobIfAbsent_Idempotent(t *testing.T) {
	t.Parallel()
	s := mustOpen(t)
	ctx := context.Background()
	require.NoError(t, s.InsertJobIfAbsent(ctx, sampleJob(1)))
	// Inserting same msg_id should not error and should not duplicate.
	require.NoError(t, s.InsertJobIfAbsent(ctx, sampleJob(1)))
	jobs, err := s.PickPending(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, jobs, 1)
}

func TestStore_IncRetries(t *testing.T) {
	t.Parallel()
	s := mustOpen(t)
	ctx := context.Background()
	require.NoError(t, s.InsertJobIfAbsent(ctx, sampleJob(1)))
	n, err := s.IncRetries(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	n, err = s.IncRetries(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
	n, err = s.IncRetries(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, int64(3), n)
}

func TestStore_PickPendingConcurrent(t *testing.T) {
	t.Parallel()
	s := mustOpen(t)
	ctx := context.Background()
	for i := int64(1); i <= 30; i++ {
		require.NoError(t, s.InsertJobIfAbsent(ctx, sampleJob(i)))
	}
	type res struct {
		jobs []state.Job
		err  error
	}
	ch := make(chan res, 3)
	for g := 0; g < 3; g++ {
		go func() {
			j, e := s.PickPending(ctx, 10)
			ch <- res{j, e}
		}()
	}
	seen := map[int64]bool{}
	total := 0
	for i := 0; i < 3; i++ {
		r := <-ch
		require.NoError(t, r.err)
		for _, j := range r.jobs {
			assert.False(t, seen[j.MsgID], "duplicate MsgID %d", j.MsgID)
			seen[j.MsgID] = true
			total++
		}
	}
	assert.LessOrEqual(t, total, 30)
}
