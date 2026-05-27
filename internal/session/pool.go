package session

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// Pool is a round-robin RPC dispatcher across N MTProto clients sharing one
// auth_key. Implementations also honour a shared FloodGate so any single
// client hitting FLOOD_WAIT will pause the whole pool.
//
// Pool implements tg.Invoker, so callers can wrap it via tg.NewClient(pool)
// to use the typed API surface (UploadGetFile, UploadSaveFilePart, ...).
type Pool interface {
	tg.Invoker
	Size() int
	Close() error
}

// Config for constructing a Pool. APIHash is treated as a secret and must
// never be logged.
type Config struct {
	APIID       int
	APIHash     string
	SessionFile string
	Size        int
}

type clientPool struct {
	gate      *FloodGate
	clients   []*telegram.Client
	next      atomic.Uint64 // monotonic round-robin counter
	closeFn   func()
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// Compile-time assertions.
var (
	_ Pool       = (*clientPool)(nil)
	_ tg.Invoker = (*clientPool)(nil)
)

// NewFetchPool builds a Pool intended for downloading documents from a
// source channel. It currently shares construction with NewUploadPool;
// the two stay distinct so the caller's intent (and future divergence —
// e.g. different DC affinity) remains explicit at the call site.
func NewFetchPool(ctx context.Context, cfg Config, gate *FloodGate) (Pool, error) {
	return newPool(ctx, cfg, gate)
}

// NewUploadPool builds a Pool intended for uploading documents to a
// destination channel. See NewFetchPool for the rationale of the split.
func NewUploadPool(ctx context.Context, cfg Config, gate *FloodGate) (Pool, error) {
	return newPool(ctx, cfg, gate)
}

func newPool(ctx context.Context, cfg Config, gate *FloodGate) (*clientPool, error) {
	if cfg.Size < 1 {
		return nil, errors.New("pool size must be >= 1")
	}
	if cfg.APIID == 0 || cfg.APIHash == "" {
		return nil, errors.New("pool requires APIID and APIHash")
	}
	if gate == nil {
		return nil, errors.New("pool requires non-nil FloodGate")
	}

	p := &clientPool{gate: gate}
	poolCtx, cancel := context.WithCancel(ctx)
	p.closeFn = cancel

	for range cfg.Size {
		client := telegram.NewClient(cfg.APIID, cfg.APIHash, telegram.Options{
			SessionStorage: newSessionStorage(cfg.SessionFile),
		})
		p.clients = append(p.clients, client)

		ready := make(chan struct{})
		errCh := make(chan error, 1)
		p.wg.Add(1)
		go func(c *telegram.Client) {
			defer p.wg.Done()
			// telegram.Client.Run blocks until the inner callback returns
			// or ctx is cancelled. We signal readiness, then park on ctx
			// so the client stays alive for the pool's lifetime. If Run
			// returns an error *before* the callback fires (e.g. auth
			// failure), `ready` is never closed; we surface the error
			// via errCh so newPool's select doesn't block forever.
			// context.Canceled is filtered out so the normal Close path
			// — which cancels poolCtx — isn't misread as a startup error
			// when an earlier client has already failed.
			err := c.Run(poolCtx, func(ctx context.Context) error {
				close(ready)
				<-ctx.Done()
				return nil
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}(client)

		select {
		case <-ready:
			// client initialised; proceed to next.
		case err := <-errCh:
			// Run returned before the callback ever fired — auth failure,
			// bad session, etc. Tear down and surface the original error.
			_ = p.Close()
			return nil, fmt.Errorf("client init: %w", err)
		case <-poolCtx.Done():
			// ctx cancelled before the client signalled ready — tear
			// down cleanly and surface the cancellation.
			_ = p.Close()
			return nil, poolCtx.Err()
		}
	}
	return p, nil
}

// Invoke implements tg.Invoker. Round-robin via an atomic counter — no
// channel, no lock, no race between "pick" and "use".
func (p *clientPool) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	if err := p.gate.Wait(ctx); err != nil {
		return err
	}
	idx := p.next.Add(1) - 1
	client := p.clients[int(idx%uint64(len(p.clients)))]
	err := client.Invoke(ctx, input, output)
	if ok, wait := IsFloodWait(err); ok {
		// One client hit FLOOD_WAIT → pause the whole pool. The throttle
		// is account-wide, not per-session. The caller observes the
		// original error and decides whether to retry; the gate ensures
		// subsequent Invoke calls block until the wait elapses.
		p.gate.Trigger(secondsToDuration(wait))
	}
	return err
}

// Size returns the number of underlying clients in the pool.
func (p *clientPool) Size() int { return len(p.clients) }

// Close cancels the pool context and blocks until every client goroutine
// has returned. Safe to call multiple times — the cancel func runs at
// most once via sync.Once, and wg.Wait is itself idempotent.
func (p *clientPool) Close() error {
	p.closeOnce.Do(func() {
		if p.closeFn != nil {
			p.closeFn()
		}
	})
	p.wg.Wait()
	return nil
}

// IsFloodWait inspects err for a Telegram FLOOD_WAIT_X (or
// FLOOD_PREMIUM_WAIT_X) RPC error and returns (true, X) where X is the
// wait duration in whole seconds. Returns (false, 0) for any other error
// (including nil).
func IsFloodWait(err error) (bool, int) {
	if err == nil {
		return false, 0
	}
	if d, ok := tgerr.AsFloodWait(err); ok {
		// Round up: a sub-second wait truncates to 0 otherwise, which
		// would make the caller skip the back-off entirely. CLAUDE.md §9
		// specifies "time.Sleep(X+1)" for FLOOD_WAIT_X.
		return true, int(d/time.Second) + 1
	}
	return false, 0
}
