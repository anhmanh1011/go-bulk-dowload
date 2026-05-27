package retry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/manh/tgpipe/internal/retry"
	"github.com/stretchr/testify/assert"
)

func TestWithBackoff_SucceedsFirstTry(t *testing.T) {
	calls := 0
	err := retry.WithBackoff(context.Background(), 3, func() error {
		calls++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, calls)
}

func TestWithBackoff_RetriesThenSucceeds(t *testing.T) {
	calls := 0
	err := retry.WithBackoff(context.Background(), 5, func() error {
		calls++
		if calls < 3 {
			return retry.Retryable(errors.New("transient"))
		}
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 3, calls)
}

func TestWithBackoff_NonRetryableStopsImmediately(t *testing.T) {
	calls := 0
	terminal := errors.New("permanent failure")
	err := retry.WithBackoff(context.Background(), 5, func() error {
		calls++
		return terminal
	})
	assert.ErrorIs(t, err, terminal)
	assert.Equal(t, 1, calls)
}

func TestWithBackoff_ExhaustsAttempts(t *testing.T) {
	calls := 0
	err := retry.WithBackoff(context.Background(), 3, func() error {
		calls++
		return retry.Retryable(errors.New("flaky"))
	})
	assert.Error(t, err)
	assert.Equal(t, 3, calls)
}

func TestWithBackoff_RespectsCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	err := retry.WithBackoff(ctx, 10, func() error {
		calls++
		return retry.Retryable(errors.New("flaky"))
	})
	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, calls, 10)
}
