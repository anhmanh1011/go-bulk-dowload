package state_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_FilterUnforwarded(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()

	// Empty input → empty output, no DB error.
	got, err := s.FilterUnforwarded(ctx, nil)
	require.NoError(t, err)
	assert.Empty(t, got)

	// Nothing marked yet → all ids returned, order preserved.
	got, err = s.FilterUnforwarded(ctx, []int64{30, 10, 20})
	require.NoError(t, err)
	assert.Equal(t, []int64{30, 10, 20}, got)

	// Mark a subset, then filter again.
	require.NoError(t, s.MarkForwarded(ctx, []int64{10, 30}, 1700))
	got, err = s.FilterUnforwarded(ctx, []int64{30, 10, 20, 40})
	require.NoError(t, err)
	assert.Equal(t, []int64{20, 40}, got)
}

func TestStore_MarkForwarded_Idempotent(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()

	require.NoError(t, s.MarkForwarded(ctx, []int64{1, 2, 3}, 1700))
	// Re-marking overlapping ids must not error (INSERT OR IGNORE).
	require.NoError(t, s.MarkForwarded(ctx, []int64{2, 3, 4}, 1800))

	got, err := s.FilterUnforwarded(ctx, []int64{1, 2, 3, 4, 5})
	require.NoError(t, err)
	assert.Equal(t, []int64{5}, got)
}

func TestStore_MarkForwarded_Empty(t *testing.T) {
	s := mustOpen(t)
	require.NoError(t, s.MarkForwarded(context.Background(), nil, 1700))
}
