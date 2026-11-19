package mesh

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta"
	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/observe"
	"github.com/byteweap/meta/envelope"
	"github.com/byteweap/meta/internal/cluster"
)

type requestObservation struct {
	mu      sync.Mutex
	timings map[observe.Stage][]time.Duration
	reasons []observe.Reason
}

func (o *requestObservation) Observe(stage observe.Stage, elapsed time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.timings == nil {
		o.timings = make(map[observe.Stage][]time.Duration)
	}
	o.timings[stage] = append(o.timings[stage], elapsed)
}

func (o *requestObservation) Reject(reason observe.Reason) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.reasons = append(o.reasons, reason)
}

func TestMeshRequestObservation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bro := &drainBroker{
			panicBroker: panicBroker{replies: make(chan panicReply, 1)},
			published:   make(chan []byte, 1),
			beforeReply: func(context.Context) { time.Sleep(5 * time.Millisecond) },
		}
		obs := &requestObservation{}
		m := New(Broker(bro), Locator(lifecycleLocator{}), Observer(obs))
		m.RPCRoute("work", "1", func(context.Context, *Mesh, *broker.Message) ([]byte, string, int) {
			time.Sleep(2 * time.Millisecond)
			return nil, "business timeout", http.StatusGatewayTimeout
		})
		m.Route(1, 1, func(m *Mesh, msg *broker.Message, frame *envelope.ClientEnvelope) {
			time.Sleep(3 * time.Millisecond)
			ctx := newContext(m, msg, frame)
			defer ctx.release()
			ctx.OkResp()
		})
		runTestMesh(t, m)
		bro.handler(poolRequest("rpc-reply"))
		synctest.Wait()
		bro.handler(&broker.Message{
			Header: cluster.BuildHeader(42, cluster.EventBusiness, "client-reply", "gate", "game"),
			Data:   mustBusinessMessage(t, 1, 1, "game", nil),
		})
		bro.handler(&broker.Message{Header: sessionEventHeader(42, cluster.EventOnline, "s1")})
		require.Equal(t, "504", (<-bro.replies).code)
		<-bro.published
		require.NoError(t, m.Stop(t.Context()))

		require.Equal(t, []time.Duration{0, 7 * time.Millisecond}, obs.timings[observe.Queue])
		require.Equal(t, []time.Duration{2 * time.Millisecond, 3 * time.Millisecond}, obs.timings[observe.Handler])
		require.Equal(t, []time.Duration{5 * time.Millisecond, 0}, obs.timings[observe.Reply])
		require.Empty(t, obs.reasons, "业务状态码不计为框架拒绝")
	})
}

func TestMeshRequestObservationDropsQueuedRequestsOnStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bro := &panicBroker{replies: make(chan panicReply, 3)}
		obs := &requestObservation{}
		m := New(Broker(bro), Locator(lifecycleLocator{}), Observer(obs))
		m.RPCRoute("work", "1", func(ctx context.Context, _ *Mesh, msg *broker.Message) ([]byte, string, int) {
			if msg.Reply != "running" {
				t.Errorf("stopped Mesh executed %q", msg.Reply)
			}
			<-ctx.Done()
			return nil, "", http.StatusOK
		})
		runTestMesh(t, m)
		bro.handler(poolRequest("running"))
		synctest.Wait()
		bro.handler(poolRequest("queued-1"))
		bro.handler(&broker.Message{Header: sessionEventHeader(42, cluster.EventOnline, "s1")})
		bro.handler(poolRequest("queued-2"))
		stopCtx, cancel := context.WithCancel(t.Context())
		cancel()
		m.BeginDrain(stopCtx)
		require.NoError(t, m.Stop(t.Context()))

		require.Equal(t, []observe.Reason{observe.Stopping, observe.Stopping}, obs.reasons)
		require.Len(t, obs.timings[observe.Queue], 1)
		require.Len(t, obs.timings[observe.Handler], 1)
		require.Empty(t, obs.timings[observe.Reply])
		require.Empty(t, bro.replies)
	})
}

type panicObserver struct{}

func (panicObserver) Observe(observe.Stage, time.Duration) { panic("test observation failure") }
func (panicObserver) Reject(observe.Reason)                { panic("test observation failure") }

func TestMeshObserverPanicPreservesRPCReplies(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bro := &panicBroker{replies: make(chan panicReply, 1)}
		m := New(Broker(bro), Locator(lifecycleLocator{}), RPCConcurrency(1, 0), Observer(panicObserver{}))
		m.RPCRoute("work", "1", func(context.Context, *Mesh, *broker.Message) ([]byte, string, int) {
			return nil, "", http.StatusOK
		})
		runTestMesh(t, m)

		bro.handler(poolRequest("ok"))
		require.Equal(t, "200", (<-bro.replies).code)
		synctest.Wait()

		expired := poolRequest("expired")
		expired.Header.Set(rpcDeadlineHeader, "1")
		bro.handler(expired)
		require.Equal(t, "504", (<-bro.replies).code)
	})
}

type delayedQueueObserver struct {
	requestObservation
	entered chan struct{}
}

func (o *delayedQueueObserver) Observe(stage observe.Stage, elapsed time.Duration) {
	if stage == observe.Queue {
		close(o.entered)
		time.Sleep(2 * time.Millisecond)
	}
	o.requestObservation.Observe(stage, elapsed)
}

func TestMeshQueueObservationRespectsDrainDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bro := &panicBroker{}
		obs := &delayedQueueObserver{entered: make(chan struct{})}
		m := New(Broker(bro), Locator(lifecycleLocator{}), Observer(obs))
		m.Route(1, 1, func(*Mesh, *broker.Message, *envelope.ClientEnvelope) {
			t.Error("client handler started after the drain deadline")
		})
		runTestMesh(t, m)

		bro.handler(&broker.Message{Data: mustBusinessMessage(t, 1, 1, "game", nil)})
		<-obs.entered
		ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
		defer cancel()
		m.BeginDrain(ctx)
		require.NoError(t, m.Stop(t.Context()))
		require.Equal(t, []observe.Reason{observe.Stopping}, obs.reasons)
	})
}

type startupObservationBroker struct {
	mockBroker
	delivered chan struct{}
}

func (b *startupObservationBroker) Sub(ctx context.Context, _ string, fn broker.Handler, _ ...broker.SubscribeOption) (broker.Subscription, error) {
	fn(poolRequest("before-ready"))
	close(b.delivered)
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestMeshObservationCountsDroppedStartupRequests(t *testing.T) {
	t.Run("stop during subscription", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			bro := &startupObservationBroker{delivered: make(chan struct{})}
			obs := &requestObservation{}
			m := New(Broker(bro), Locator(lifecycleLocator{}), Observer(obs))
			done := startLifecycleMesh(t, m)
			<-bro.delivered
			require.NoError(t, m.Stop(t.Context()))
			require.ErrorIs(t, <-done, context.Canceled)
			require.Equal(t, []observe.Reason{observe.Stopping}, obs.reasons)
			require.Empty(t, obs.timings)
		})
	})
	t.Run("subscription fails", func(t *testing.T) {
		obs := &requestObservation{}
		m := New(Broker(&failedRPCSubscription{}), Locator(lifecycleLocator{}), Observer(obs))
		require.ErrorContains(t, m.Start(meta.NewContext(t.Context(), lifecycleAppInfo{})), "subscribe failed")
		require.Equal(t, []observe.Reason{observe.Stopping}, obs.reasons)
		require.Empty(t, obs.timings)
	})
}
