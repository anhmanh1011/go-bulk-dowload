package writer

import (
	"context"
	"os"
	"path/filepath"
	"time"
)

// BackpressureGate blocks the writer when too many output files are pending
// upload (i.e., still present on disk in OutputDir). Polls dir count at
// short intervals — coarse-grained but simple.
type BackpressureGate struct {
	Dir       string
	MaxFiles  int
	pollEvery time.Duration
}

func NewBackpressureGate(dir string, maxFiles int) *BackpressureGate {
	return &BackpressureGate{Dir: dir, MaxFiles: maxFiles, pollEvery: 200 * time.Millisecond}
}

func (g *BackpressureGate) Acquire(ctx context.Context) error {
	if g.MaxFiles <= 0 {
		return nil
	}
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		n, err := countFiles(g.Dir)
		if err != nil {
			return err
		}
		if n < g.MaxFiles {
			return nil
		}
		if timer == nil {
			timer = time.NewTimer(g.pollEvery)
		} else {
			timer.Reset(g.pollEvery)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func countFiles(dir string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".txt" {
			n++
		}
	}
	return n, nil
}
