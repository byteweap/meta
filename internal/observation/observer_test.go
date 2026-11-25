package observation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta/component/observe"
)

type recordingObserver struct {
	stage   observe.Stage
	elapsed time.Duration
	reason  observe.Reason
	panics  bool
}

func (o *recordingObserver) Observe(stage observe.Stage, elapsed time.Duration) {
	o.stage, o.elapsed = stage, elapsed
	if o.panics {
		panic("observe")
	}
}

func (o *recordingObserver) Reject(reason observe.Reason) {
	o.reason = reason
	if o.panics {
		panic("reject")
	}
}

func TestProtect(t *testing.T) {
	require.Nil(t, Protect(nil))

	original := &recordingObserver{panics: true}
	protected := Protect(original)
	require.NotPanics(t, func() { protected.Observe(observe.Queue, time.Second) })
	require.NotPanics(t, func() { protected.Reject(observe.QueueFull) })
	require.Equal(t, observe.Queue, original.stage)
	require.Equal(t, time.Second, original.elapsed)
	require.Equal(t, observe.QueueFull, original.reason)
}
