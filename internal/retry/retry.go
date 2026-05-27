package retry

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// retryableErr marks an error as eligible for retry. Use Retryable() to wrap.
type retryableErr struct{ err error }

func (e *retryableErr) Error() string { return e.err.Error() }
func (e *retryableErr) Unwrap() error { return e.err }

// Retryable wraps err so WithBackoff will retry. Without this wrapper,
// any error is treated as terminal.
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return &retryableErr{err: err}
}

// IsRetryable returns true if err (or any wrapped error) was marked Retryable.
func IsRetryable(err error) bool {
	var r *retryableErr
	return errors.As(err, &r)
}

// WithBackoff invokes op up to maxAttempts times, doubling the delay each time
// (capped at 8s). Returns nil on success, the last error on exhaustion, or
// ctx.Err() if canceled. Non-retryable errors stop immediately.
func WithBackoff(ctx context.Context, maxAttempts int, op func() error) error {
	if maxAttempts < 1 {
		return errors.New("retry: maxAttempts must be >= 1")
	}
	backoff := 500 * time.Millisecond
	const maxBackoff = 8 * time.Second
	var lastErr error
	for attempt := range maxAttempts {
		err := op()
		if err == nil {
			return nil
		}
		lastErr = err
		if !IsRetryable(err) {
			return err
		}
		if attempt == maxAttempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
	return fmt.Errorf("retry: exhausted %d attempts: %w", maxAttempts, lastErr)
}
