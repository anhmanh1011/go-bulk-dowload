package session

import (
	"context"
	"sync/atomic"
	"time"
)

// FloodGate coordinates account-wide FLOOD_WAIT pauses across both
// fetch and upload sub-pools. When the Telegram API returns FLOOD_WAIT_X
// on any session, every other session on the same account should also
// back off to avoid amplifying the throttle.
type FloodGate struct {
	until atomic.Int64 // unix nanos; 0 = no active flood
}

// Trigger records a flood wait of duration d from now. Repeated calls
// extend the wait monotonically — a later, shorter trigger never shrinks
// an existing longer wait.
func (g *FloodGate) Trigger(d time.Duration) {
	if d <= 0 {
		return
	}
	newUntil := time.Now().Add(d).UnixNano()
	for {
		cur := g.until.Load()
		if newUntil <= cur {
			return
		}
		if g.until.CompareAndSwap(cur, newUntil) {
			return
		}
	}
}

// Wait blocks until any active flood-wait period has elapsed, or ctx is canceled.
// Returns nil if the gate was already open or the wait completed normally.
//
// If Trigger() extends the deadline while Wait() is already sleeping, the
// extension MUST be honoured — otherwise a quick second flood could leak
// through. The loop re-reads `until` after each timer firing.
func (g *FloodGate) Wait(ctx context.Context) error {
	for {
		until := g.until.Load()
		if until == 0 {
			return nil
		}
		diff := time.Until(time.Unix(0, until))
		if diff <= 0 {
			return nil
		}
		timer := time.NewTimer(diff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			// Re-check: an extending Trigger() may have raised `until` while we slept.
		}
	}
}
