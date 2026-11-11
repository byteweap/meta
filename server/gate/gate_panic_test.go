package gate

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta/component/broker"
)

type panicBroker struct {
	testBroker
	handler  broker.Handler
	attempts chan *broker.Message
	closed   chan struct{}
}

func (b *panicBroker) Sub(_ context.Context, _ string, handler broker.Handler, _ ...broker.SubscribeOption) (broker.Subscription, error) {
	b.handler = handler
	return &panicSubscription{closed: b.closed}, nil
}

func (b *panicBroker) Reply(ctx context.Context, msg *broker.Message, data []byte, opts ...broker.ReplyOption) error {
	b.attempts <- msg
	if msg.Reply == "panic" {
		panic("reply transport failed")
	}
	return b.testBroker.Reply(ctx, msg, data, opts...)
}

type panicSubscription struct {
	testSubscription
	closed chan struct{}
}

func (s *panicSubscription) Close() error {
	close(s.closed)
	return nil
}

func TestGatePanicKeepsMessageLoopRunning(t *testing.T) {
	bro := &panicBroker{attempts: make(chan *broker.Message, 4), closed: make(chan struct{})}
	g := New(Broker(bro))
	ctx, cancel := context.WithCancel(context.Background())
	g.ctx = ctx
	stop := func() {
		cancel()
		select {
		case <-bro.closed:
		case <-time.After(time.Second):
			t.Error("message loop did not close its subscription")
		}
	}
	t.Cleanup(stop)
	require.NoError(t, g.loop())

	first := &broker.Message{Reply: "panic"}
	second := &broker.Message{Reply: "success"}
	bro.handler(first)
	bro.handler(second)
	for _, expected := range []*broker.Message{first, second} {
		select {
		case actual := <-bro.attempts:
			require.Same(t, expected, actual)
		case <-time.After(time.Second):
			t.Fatal("message loop did not process the next message after a panic")
		}
	}
	stop()
	require.Empty(t, bro.attempts, "each message must attempt only one reply")
	bro.mu.Lock()
	defer bro.mu.Unlock()
	require.Equal(t, 1, bro.replyCalls)
	require.Equal(t, "501", bro.replyHeader.Get("code"))
}
