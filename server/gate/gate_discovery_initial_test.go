package gate

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta/component/registry"
	"github.com/byteweap/meta/component/selector"
	"github.com/byteweap/meta/envelope"
)

type initialDiscoveryRegistry struct {
	testRegistry
	watch      func(context.Context, string) (registry.Watcher, error)
	get        func(context.Context, string) ([]*registry.ServiceInstance, error)
	watchCount atomic.Int32
	getCount   atomic.Int32
}

func (r *initialDiscoveryRegistry) Watch(ctx context.Context, service string) (registry.Watcher, error) {
	r.watchCount.Add(1)
	return r.watch(ctx, service)
}

func (r *initialDiscoveryRegistry) GetService(ctx context.Context, service string) ([]*registry.ServiceInstance, error) {
	r.getCount.Add(1)
	return r.get(ctx, service)
}

type initialDiscoveryWatcher struct {
	updates chan []*registry.ServiceInstance
	done    chan struct{}
	next    chan struct{}
	entered sync.Once
	closed  sync.Once
	stops   atomic.Int32
	stopErr error
}

func newInitialDiscoveryWatcher() *initialDiscoveryWatcher {
	return &initialDiscoveryWatcher{
		updates: make(chan []*registry.ServiceInstance),
		done:    make(chan struct{}),
		next:    make(chan struct{}),
	}
}

func (w *initialDiscoveryWatcher) Next() ([]*registry.ServiceInstance, error) {
	w.entered.Do(func() { close(w.next) })
	select {
	case snapshot := <-w.updates:
		return snapshot, nil
	case <-w.done:
		return nil, context.Canceled
	}
}

func (w *initialDiscoveryWatcher) Stop() error {
	w.stops.Add(1)
	w.closed.Do(func() { close(w.done) })
	return w.stopErr
}

func newInitialDiscoveryGate(t *testing.T, reg registry.Registry, bro *testBroker) *Gate {
	t.Helper()
	g := New(
		Discovery(reg), Locator(&testLocator{}), Broker(bro),
		SelectorFunc(func() selector.Selector { return &testSelector{} }),
	)
	g.appName, g.appID = "gate", "gate-1"
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, g.Stop(ctx))
	})
	return g
}

func initialDiscoverySnapshot(id string) []*registry.ServiceInstance {
	return []*registry.ServiceInstance{{ID: id, Name: "game", Version: "v1", Metadata: map[string]string{"weight": "2"}}}
}

func TestGateDiscoveryInitialDispatchWaitsForSnapshot(t *testing.T) {
	w := newInitialDiscoveryWatcher()
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	reg := &initialDiscoveryRegistry{
		watch: func(context.Context, string) (registry.Watcher, error) { return w, nil },
		get: func(ctx context.Context, _ string) ([]*registry.ServiceInstance, error) {
			close(entered)
			select {
			case <-release:
				return initialDiscoverySnapshot("game-first"), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	bro := &testBroker{}
	g := newInitialDiscoveryGate(t, reg, bro)
	t.Cleanup(unblock)
	finished := make(chan struct{})
	go func() {
		g.dispatch(42, &envelope.IMessage{Service: "game", Header: &envelope.Header{Cmd: 1}})
		close(finished)
	}()
	awaitBackground(t, entered)
	require.Equal(t, int32(1), reg.watchCount.Load(), "watch must be established before the initial snapshot")
	select {
	case <-finished:
		t.Fatal("first dispatch returned before the initial snapshot was loaded")
	default:
	}
	bro.mu.Lock()
	publications := len(bro.pubSubjects)
	bro.mu.Unlock()
	require.Zero(t, publications)
	unblock()
	awaitBackground(t, finished) // Next has never supplied a snapshot.
	bro.mu.Lock()
	subjects := append([]string(nil), bro.pubSubjects...)
	bro.mu.Unlock()
	require.Equal(t, []string{"meta.gate.game.game-first"}, subjects)
	require.Equal(t, int32(1), reg.getCount.Load())
}

func TestGateDiscoveryInitialEmptySnapshotCanReceiveUpdates(t *testing.T) {
	w := newInitialDiscoveryWatcher()
	reg := &initialDiscoveryRegistry{
		watch: func(context.Context, string) (registry.Watcher, error) { return w, nil },
		get:   func(context.Context, string) ([]*registry.ServiceInstance, error) { return nil, nil },
	}
	g := newInitialDiscoveryGate(t, reg, &testBroker{})
	type result struct {
		sel selector.Selector
		err error
	}
	ensured := make(chan result, 1)
	go func() {
		sel, err := g.ensure("game")
		ensured <- result{sel, err}
	}()
	got := awaitBackground(t, ensured)
	require.NoError(t, got.err)
	require.NotNil(t, got.sel)
	_, err := got.sel.Select("42")
	require.ErrorIs(t, err, selector.ErrNoAvailableNode)
	awaitBackground(t, w.next)
	select {
	case w.updates <- initialDiscoverySnapshot("game-later"):
	case <-time.After(time.Second):
		t.Fatal("watcher did not receive the service update")
	}
	require.Eventually(t, func() bool {
		node, selectErr := got.sel.Select("42")
		return selectErr == nil && node.ID() == "game-later"
	}, time.Second, time.Millisecond)
	current, err := g.ensure("game")
	require.NoError(t, err)
	require.Same(t, got.sel, current)
	require.Equal(t, int32(1), reg.watchCount.Load())
	require.Equal(t, int32(1), reg.getCount.Load())
}

func TestGateDiscoveryInitialFailureClosesWatcherAndRetries(t *testing.T) {
	first, second := newInitialDiscoveryWatcher(), newInitialDiscoveryWatcher()
	failure := errors.New("initial snapshot failed")
	reg := &initialDiscoveryRegistry{}
	reg.watch = func(context.Context, string) (registry.Watcher, error) {
		if reg.watchCount.Load() == 1 {
			return first, nil
		}
		return second, nil
	}
	reg.get = func(context.Context, string) ([]*registry.ServiceInstance, error) {
		if reg.getCount.Load() == 1 {
			return nil, failure
		}
		return initialDiscoverySnapshot("game-retry"), nil
	}
	g := newInitialDiscoveryGate(t, reg, &testBroker{})
	sel, err := g.ensure("game")
	require.ErrorIs(t, err, failure)
	require.Nil(t, sel)
	require.Equal(t, int32(1), first.stops.Load())
	select {
	case <-first.next:
		t.Fatal("failed initialization must not start the watch loop")
	default:
	}
	sel, err = g.ensure("game")
	require.NoError(t, err)
	node, err := sel.Select("42")
	require.NoError(t, err)
	require.Equal(t, "game-retry", node.ID())
	require.Equal(t, int32(2), reg.watchCount.Load())
	require.Equal(t, int32(2), reg.getCount.Load())
	require.NoError(t, g.Stop(context.Background()))
	require.Equal(t, int32(1), first.stops.Load())
	require.Equal(t, int32(1), second.stops.Load())
}

func TestGateDiscoveryInitialWatchErrorClosesReturnedWatcher(t *testing.T) {
	first, second := newInitialDiscoveryWatcher(), newInitialDiscoveryWatcher()
	failure := errors.New("watch failed after allocating a watcher")
	reg := &initialDiscoveryRegistry{}
	reg.watch = func(context.Context, string) (registry.Watcher, error) {
		if reg.watchCount.Load() == 1 {
			return first, failure
		}
		return second, nil
	}
	reg.get = func(context.Context, string) ([]*registry.ServiceInstance, error) {
		return initialDiscoverySnapshot("game-retry"), nil
	}
	g := newInitialDiscoveryGate(t, reg, &testBroker{})
	sel, err := g.ensure("game")
	require.ErrorIs(t, err, failure)
	require.Nil(t, sel)
	require.Equal(t, int32(1), first.stops.Load())
	require.Zero(t, reg.getCount.Load(), "a failed Watch must not start snapshot loading")
	select {
	case <-first.next:
		t.Fatal("a failed Watch must not start its update loop")
	default:
	}
	sel, err = g.ensure("game")
	require.NoError(t, err)
	node, err := sel.Select("42")
	require.NoError(t, err)
	require.Equal(t, "game-retry", node.ID())
	require.Equal(t, int32(2), reg.watchCount.Load(), "the failed attempt must not remain cached")
	require.Equal(t, int32(1), reg.getCount.Load())
	require.NoError(t, g.Stop(context.Background()))
	require.Equal(t, int32(1), first.stops.Load())
	require.Equal(t, int32(1), second.stops.Load())
}

func TestGateDiscoveryInitialFailurePreservesCleanupError(t *testing.T) {
	w := newInitialDiscoveryWatcher()
	w.stopErr = errors.New("watcher cleanup failed")
	failure := errors.New("initial snapshot failed")
	reg := &initialDiscoveryRegistry{
		watch: func(context.Context, string) (registry.Watcher, error) { return w, nil },
		get:   func(context.Context, string) ([]*registry.ServiceInstance, error) { return nil, failure },
	}
	g := New(Discovery(reg), SelectorFunc(func() selector.Selector { return &testSelector{} }))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.ErrorIs(t, g.Stop(ctx), w.stopErr)
		require.Equal(t, int32(1), w.stops.Load())
	})
	sel, err := g.ensure("game")
	require.Nil(t, sel)
	require.ErrorIs(t, err, failure)
	require.ErrorIs(t, err, w.stopErr)
	require.Equal(t, int32(1), w.stops.Load())
}

func TestGateDiscoveryInitialConcurrentCallsShareReadySelector(t *testing.T) {
	w := newInitialDiscoveryWatcher()
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	reg := &initialDiscoveryRegistry{
		watch: func(context.Context, string) (registry.Watcher, error) { return w, nil },
		get: func(ctx context.Context, _ string) ([]*registry.ServiceInstance, error) {
			close(entered)
			select {
			case <-release:
				return initialDiscoverySnapshot("game-shared"), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	g := newInitialDiscoveryGate(t, reg, &testBroker{})
	t.Cleanup(unblock)
	const callers = 8
	type result struct {
		sel  selector.Selector
		node selector.Node
		err  error
	}
	results := make(chan result, callers)
	started := make(chan struct{}, callers)
	for range callers {
		go func() {
			started <- struct{}{}
			sel, err := g.ensure("game")
			got := result{sel: sel, err: err}
			if err == nil {
				got.node, got.err = sel.Select("42")
			}
			results <- got
		}()
	}
	for range callers {
		awaitBackground(t, started)
	}
	awaitBackground(t, entered)
	select {
	case <-results:
		t.Fatal("caller received a selector before the initial snapshot was ready")
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	var shared selector.Selector
	for range callers {
		got := awaitBackground(t, results)
		require.NoError(t, got.err)
		require.Equal(t, "game-shared", got.node.ID())
		if shared == nil {
			shared = got.sel
		}
		require.Same(t, shared, got.sel)
	}
	require.Equal(t, int32(1), reg.watchCount.Load())
	require.Equal(t, int32(1), reg.getCount.Load())
}
