package processor_test

import (
	"context"
	"errors"
	"testing"

	"github.com/manh/tgpipe/internal/processor"
	"github.com/manh/tgpipe/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeRecorder is a no-op Recorder that counts invocations for tests.
type fakeRecorder struct {
	dropped int
	emitted int
}

func (f *fakeRecorder) IncDroppedInvalidLine() { f.dropped++ }
func (f *fakeRecorder) IncLinesEmitted()       { f.emitted++ }

// errImpl is a LineProcessor that always returns a fatal error.
type errImpl struct{}

func (errImpl) Process(_ []byte) (types.Record, bool, error) {
	return types.Record{}, false, errors.New("synthetic fatal")
}

// TestProcessor_PropagatesFatalError verifies that a non-nil error from
// LineProcessor.Process is captured and returned by Run so the pipeline
// orchestrator (errgroup) can tear down all stages.
func TestProcessor_PropagatesFatalError(t *testing.T) {
	t.Parallel()
	p := processor.New(1, errImpl{}, &fakeRecorder{})
	in := make(chan types.Line, 1)
	out := make(chan types.Record, 1)
	in <- types.Line{MsgID: 1, Data: []byte("anything")}
	close(in)
	err := p.Run(context.Background(), in, out)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "synthetic fatal")
}
