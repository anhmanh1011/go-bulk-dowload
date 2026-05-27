package session_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/manh/tgpipe/internal/session"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFloodGate_NoTriggerPassesImmediately(t *testing.T) {
	g := &session.FloodGate{}
	start := time.Now()
	require.NoError(t, g.Wait(context.Background()))
	assert.Less(t, time.Since(start), 5*time.Millisecond)
}

func TestFloodGate_TriggerBlocks(t *testing.T) {
	g := &session.FloodGate{}
	g.Trigger(80 * time.Millisecond)
	start := time.Now()
	require.NoError(t, g.Wait(context.Background()))
	assert.GreaterOrEqual(t, time.Since(start), 70*time.Millisecond)
}

func TestFloodGate_TriggerExtendsButDoesntShrink(t *testing.T) {
	g := &session.FloodGate{}
	g.Trigger(200 * time.Millisecond)
	g.Trigger(50 * time.Millisecond) // shorter — should NOT shrink
	start := time.Now()
	require.NoError(t, g.Wait(context.Background()))
	assert.GreaterOrEqual(t, time.Since(start), 180*time.Millisecond)
}

func TestFloodGate_CancelInterrupts(t *testing.T) {
	g := &session.FloodGate{}
	g.Trigger(5 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	err := g.Wait(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestFloodGate_ConcurrentTriggers(t *testing.T) {
	g := &session.FloodGate{}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); g.Trigger(20 * time.Millisecond) }()
	}
	wg.Wait()
	require.NoError(t, g.Wait(context.Background()))
}

// Regression: Trigger() extension while Wait() is asleep must be honoured.
func TestFloodGate_ExtensionDuringWait(t *testing.T) {
	g := &session.FloodGate{}
	g.Trigger(50 * time.Millisecond)
	go func() {
		time.Sleep(30 * time.Millisecond)
		g.Trigger(150 * time.Millisecond) // extend mid-wait
	}()
	start := time.Now()
	require.NoError(t, g.Wait(context.Background()))
	// Original would have fired at ~50ms; extension pushes total to ~180ms.
	assert.GreaterOrEqual(t, time.Since(start), 150*time.Millisecond)
}
