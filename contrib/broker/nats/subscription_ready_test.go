package nats

import (
	"context"
	"testing"
	"time"

	natssrv "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta/component/broker"
)

func TestSubscribeReadyForAnotherConnection(t *testing.T) {
	for _, queue := range []string{"", "workers"} {
		t.Run("queue="+queue, func(t *testing.T) {
			s := runNatsServer(t)
			b, err := New(URLs(s.ClientURL()))
			require.NoError(t, err)
			t.Cleanup(b.nc.Close)
			publisher, err := nats.Connect(s.ClientURL())
			require.NoError(t, err)
			t.Cleanup(publisher.Close)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			got := make(chan *broker.Message, 1)
			sub, err := b.Sub(ctx, "t.ready", func(msg *broker.Message) {
				got <- msg
			}, broker.SubQueue(queue))
			require.NoError(t, err)
			require.True(t, sub.(*subscription).sub.IsValid())
			state, err := s.Subsz(&natssrv.SubszOptions{Subscriptions: true, Test: "t.ready"})
			require.NoError(t, err)
			require.Len(t, state.Subs, 1)
			require.Equal(t, queue, state.Subs[0].Queue)

			require.NoError(t, publisher.Publish("t.ready", []byte("first")))
			select {
			case msg := <-got:
				require.Equal(t, []byte("first"), msg.Data)
			case <-time.After(time.Second):
				t.Fatal("subscription missed the first message from another connection")
			}
		})
	}
}

func TestSubscribeFlushTimeoutUnsubscribes(t *testing.T) {
	for _, callerDeadline := range []bool{false, true} {
		name := "broker timeout"
		if callerDeadline {
			name = "caller deadline"
		}
		t.Run(name, func(t *testing.T) {
			b := reconnectingBroker(t)
			ctx := context.Background()
			if callerDeadline {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 30*time.Millisecond)
				defer cancel()
			} else {
				b.opts.connectTimeout = 30 * time.Millisecond
			}

			sub, err := b.Sub(ctx, "t.timeout", func(*broker.Message) {})
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Nil(t, sub)
			require.Zero(t, b.nc.NumSubscriptions())
		})
	}
}

func TestSubscribeCancelDuringFlushUnsubscribes(t *testing.T) {
	b := reconnectingBroker(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		sub broker.Subscription
		err error
	}
	finished := make(chan result, 1)
	go func() {
		sub, err := b.Sub(ctx, "t.cancel-flush", func(*broker.Message) {})
		finished <- result{sub, err}
	}()
	require.Eventually(t, func() bool { return b.nc.NumSubscriptions() == 1 }, time.Second, time.Millisecond)
	cancel()
	select {
	case got := <-finished:
		require.ErrorIs(t, got.err, context.Canceled)
		require.Nil(t, got.sub)
		require.Zero(t, b.nc.NumSubscriptions())
	case <-time.After(time.Second):
		t.Fatal("subscription confirmation did not stop after cancellation")
	}
}

// A reconnecting client accepts local subscriptions but cannot confirm them.
func reconnectingBroker(t *testing.T) *Broker {
	t.Helper()
	s := runNatsServer(t)
	b, err := New(URLs(s.ClientURL()))
	require.NoError(t, err)
	t.Cleanup(b.nc.Close)
	s.Shutdown()
	require.Eventually(t, b.nc.IsReconnecting, time.Second, time.Millisecond)
	return b
}
