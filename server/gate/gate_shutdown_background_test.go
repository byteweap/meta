package gate

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/registry"
	"github.com/byteweap/meta/component/selector"
)

type backgroundSubscription struct {
	testSubscription
	close func() error
}

func (s *backgroundSubscription) Close() error { return s.close() }

type backgroundBroker struct {
	testBroker
	sub broker.Subscription
}

func (b *backgroundBroker) Sub(context.Context, string, broker.Handler, ...broker.SubscribeOption) (broker.Subscription, error) {
	return b.sub, nil
}

type backgroundRegistry struct {
	testRegistry
	watch func(context.Context, string) (registry.Watcher, error)
}

func (r *backgroundRegistry) Watch(ctx context.Context, service string) (registry.Watcher, error) {
	return r.watch(ctx, service)
}

type backgroundWatcher struct {
	next func() ([]*registry.ServiceInstance, error)
	stop func() error
}

func (w *backgroundWatcher) Next() ([]*registry.ServiceInstance, error) { return w.next() }
func (w *backgroundWatcher) Stop() error                                { return w.stop() }

func awaitBackground[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for background cleanup")
		var zero T
		return zero
	}
}

func stopBackground(g *Gate, ctx context.Context) <-chan error {
	result := make(chan error, 1)
	go func() { result <- g.Stop(ctx) }()
	return result
}

func TestGateShutdownBackgroundWaitsForSubscriptionClose(t *testing.T) {
	entered, release := make(chan struct{}, 1), make(chan struct{})
	releaseClose := sync.OnceFunc(func() { close(release) })
	closeErr := errors.New("subscription close failed")
	var calls atomic.Int32
	g := New(Broker(&backgroundBroker{sub: &backgroundSubscription{close: func() error {
		calls.Add(1)
		entered <- struct{}{}
		<-release
		return closeErr
	}}}))
	t.Cleanup(func() {
		releaseClose()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = g.Stop(ctx)
	})
	require.NoError(t, g.loop())

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	first := stopBackground(g, ctx)
	awaitBackground(t, entered)
	require.ErrorIs(t, awaitBackground(t, first), context.DeadlineExceeded)

	second := stopBackground(g, context.Background())
	select {
	case err := <-second:
		t.Fatalf("repeated Stop returned before subscription close: %v", err)
	default:
	}
	releaseClose()
	require.ErrorIs(t, awaitBackground(t, second), closeErr)
	require.ErrorIs(t, g.Stop(context.Background()), closeErr)
	require.Equal(t, int32(1), calls.Load())
}

func TestGateShutdownBackgroundClosesLateWatcher(t *testing.T) {
	watchEntered, releaseWatch := make(chan struct{}, 1), make(chan struct{})
	closeEntered, releaseClose := make(chan struct{}, 1), make(chan struct{})
	unblockWatch := sync.OnceFunc(func() { close(releaseWatch) })
	unblockClose := sync.OnceFunc(func() { close(releaseClose) })
	closeErr := errors.New("late watcher close failed")
	var watchCalls, closeCalls, nextCalls atomic.Int32
	w := &backgroundWatcher{
		next: func() ([]*registry.ServiceInstance, error) {
			nextCalls.Add(1)
			return nil, context.Canceled
		},
		stop: func() error {
			closeCalls.Add(1)
			closeEntered <- struct{}{}
			<-releaseClose
			return closeErr
		},
	}
	reg := &backgroundRegistry{watch: func(context.Context, string) (registry.Watcher, error) {
		watchCalls.Add(1)
		watchEntered <- struct{}{}
		<-releaseWatch // Simulate a Watch implementation that returns after cancellation.
		return w, nil
	}}
	g := New(Discovery(reg), SelectorFunc(func() selector.Selector { return &testSelector{} }))
	t.Cleanup(func() {
		unblockWatch()
		unblockClose()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = g.Stop(ctx)
	})
	type result struct {
		sel selector.Selector
		err error
	}
	ensured := make(chan result, 1)
	go func() {
		sel, err := g.ensure("game")
		ensured <- result{sel, err}
	}()
	awaitBackground(t, watchEntered)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, g.Stop(ctx), context.DeadlineExceeded)

	unblockWatch()
	awaitBackground(t, closeEntered)
	stopped := stopBackground(g, context.Background())
	select {
	case err := <-stopped:
		t.Fatalf("Stop returned before the late watcher closed: %v", err)
	default:
	}
	unblockClose()
	got := awaitBackground(t, ensured)
	require.Nil(t, got.sel)
	require.ErrorIs(t, got.err, context.Canceled)
	require.ErrorIs(t, awaitBackground(t, stopped), closeErr)
	_, err := g.ensure("another-service")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int32(1), watchCalls.Load())
	require.Equal(t, int32(1), closeCalls.Load())
	require.Zero(t, nextCalls.Load())
	require.Nil(t, g.resolver().Cached("game"))
}

func TestGateShutdownBackgroundStopsWatchersIndependently(t *testing.T) {
	nextA, nextB := make(chan struct{}), make(chan struct{})
	stopA, stopB := make(chan struct{}, 1), make(chan struct{}, 1)
	releaseNextA, releaseStopA := make(chan struct{}), make(chan struct{})
	releaseNextB := make(chan struct{})
	unblockNextA := sync.OnceFunc(func() { close(releaseNextA) })
	unblockNextB := sync.OnceFunc(func() { close(releaseNextB) })
	unblockStopA := sync.OnceFunc(func() { close(releaseStopA) })
	closeA, closeB := errors.New("watcher A close failed"), errors.New("watcher B close failed")
	var callsA, callsB atomic.Int32
	var g *Gate
	wA := &backgroundWatcher{
		next: func() ([]*registry.ServiceInstance, error) {
			close(nextA)
			<-releaseNextA
			return nil, context.Canceled
		},
		stop: func() error {
			callsA.Add(1)
			_ = g.resolver().Cached("a") // 关闭回调也可以读取缓存
			stopA <- struct{}{}
			<-releaseStopA
			return closeA
		},
	}
	wB := &backgroundWatcher{
		next: func() ([]*registry.ServiceInstance, error) {
			close(nextB)
			<-releaseNextB
			return nil, context.Canceled
		},
		stop: func() error {
			callsB.Add(1)
			_ = g.resolver().Cached("b")
			unblockNextB()
			stopB <- struct{}{}
			return closeB
		},
	}
	reg := &backgroundRegistry{watch: func(_ context.Context, service string) (registry.Watcher, error) {
		if service == "a" {
			return wA, nil
		}
		return wB, nil
	}}
	g = New(Discovery(reg), SelectorFunc(func() selector.Selector { return &testSelector{} }))
	t.Cleanup(func() {
		unblockStopA()
		unblockNextA()
		unblockNextB()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = g.Stop(ctx)
	})
	_, err := g.ensure("a")
	require.NoError(t, err)
	_, err = g.ensure("b")
	require.NoError(t, err)
	awaitBackground(t, nextA)
	awaitBackground(t, nextB)
	stopped := stopBackground(g, context.Background())
	awaitBackground(t, stopA)
	awaitBackground(t, stopB) // B must close while A's close operation is still blocked.
	unblockStopA()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, g.Stop(ctx), context.DeadlineExceeded, "Next must exit before cleanup completes")
	unblockNextA()
	err = awaitBackground(t, stopped)
	require.ErrorIs(t, err, closeA)
	require.ErrorIs(t, err, closeB)
	require.Equal(t, int32(1), callsA.Load())
	require.Equal(t, int32(1), callsB.Load())
}
