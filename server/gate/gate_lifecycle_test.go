package gate

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta"
	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/selector"
)

type lifecycleBroker struct {
	testBroker
	subscribe func(context.Context) error
}

func (b *lifecycleBroker) Sub(ctx context.Context, _ string, _ broker.Handler, _ ...broker.SubscribeOption) (broker.Subscription, error) {
	if err := b.subscribe(ctx); err != nil {
		return nil, err
	}
	return &testSubscription{}, nil
}

func newLifecycleGate(bro broker.Broker) *Gate {
	return New(
		Addr("127.0.0.1:0"),
		UserIDExtractor(func(*http.Request) int64 { return 42 }),
		Locator(&testLocator{}), Broker(bro), Discovery(&testRegistry{}),
		SelectorFunc(func() selector.Selector { return &testSelector{} }),
	)
}

func startLifecycleGate(t *testing.T, g *Gate) <-chan error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- g.Start(meta.NewContext(ctx, testAppInfo{id: "gate-1", name: "gate"}))
	}()
	t.Cleanup(func() {
		cancel()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
		defer stopCancel()
		_ = g.Stop(stopCtx)
	})
	return done
}

func TestGateLifecycleReadyWaitsForSubscription(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	g := newLifecycleGate(&lifecycleBroker{subscribe: func(ctx context.Context) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}})
	ready := g.Ready()
	done := startLifecycleGate(t, g)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("subscription was not started")
	}
	select {
	case <-ready:
		t.Fatal("gate became ready before subscription succeeded")
	default:
	}
	close(release)
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("gate did not become ready")
	}
	require.Equal(t, ready, g.Ready())
	require.NoError(t, g.Stop(context.Background()))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("gate did not stop")
	}
}

func TestGateLifecycleSubscriptionFailureDoesNotSignalReady(t *testing.T) {
	want := errors.New("subscription failed")
	g := newLifecycleGate(&lifecycleBroker{subscribe: func(context.Context) error { return want }})
	done := startLifecycleGate(t, g)
	select {
	case err := <-done:
		require.ErrorIs(t, err, want)
	case <-time.After(time.Second):
		t.Fatal("gate did not return its startup failure")
	}
	select {
	case <-g.Ready():
		t.Fatal("failed gate must not signal readiness")
	default:
	}
}

func TestGateLifecycleStopClosesEndpointBeforeStart(t *testing.T) {
	g := newLifecycleGate(&testBroker{})
	endpoint, err := g.Endpoint(context.Background())
	require.NoError(t, err)
	require.NoError(t, g.Stop(context.Background()))
	listener, err := net.Listen("tcp", endpoint.Host)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	ctx := meta.NewContext(context.Background(), testAppInfo{id: "gate-1", name: "gate"})
	require.ErrorIs(t, g.Start(ctx), context.Canceled)
	_, err = g.Endpoint(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestGateLifecycleStopDuringSubscription(t *testing.T) {
	entered := make(chan struct{})
	g := newLifecycleGate(&lifecycleBroker{subscribe: func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}})
	done := startLifecycleGate(t, g)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("subscription was not started")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, g.Stop(ctx))
	select {
	case err := <-done:
		require.Equal(t, context.Canceled, err)
	case <-ctx.Done():
		t.Fatal("startup did not observe stop")
	}
	select {
	case <-g.Ready():
		t.Fatal("stopped gate must not signal readiness")
	default:
	}
}

func TestGateLifecycleCanceledStartDoesNotListen(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	g := newLifecycleGate(&testBroker{})
	err := g.Start(meta.NewContext(ctx, testAppInfo{id: "gate-1", name: "gate"}))
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, g.ln)
}
