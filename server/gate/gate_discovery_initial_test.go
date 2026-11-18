package gate

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olahol/melody"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/byteweap/meta/component/registry"
	"github.com/byteweap/meta/component/selector"
	"github.com/byteweap/meta/envelope"
	"github.com/byteweap/meta/internal/cluster"
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

func (w *initialDiscoveryWatcher) send(t *testing.T, instances []*registry.ServiceInstance) {
	t.Helper()
	select {
	case w.updates <- instances:
	case <-time.After(time.Second):
		t.Fatal("watcher did not receive the snapshot")
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
	return []*registry.ServiceInstance{discoveryTestInstance("game", id)}
}

func discoveryTestInstance(service, id string) *registry.ServiceInstance {
	return &registry.ServiceInstance{ID: id, Name: service, Version: "v1", Metadata: map[string]string{"weight": "2"}}
}

func TestGateDiscoveryInitialDispatchWaitsForSnapshot(t *testing.T) {
	w := newInitialDiscoveryWatcher()
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	signal := sync.OnceFunc(func() { close(entered) })
	reg := &initialDiscoveryRegistry{
		watch: func(context.Context, string) (registry.Watcher, error) { return w, nil },
		get: func(ctx context.Context, _ string) ([]*registry.ServiceInstance, error) {
			signal()
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
	session := &melody.Session{Keys: map[string]any{"uid": int64(42), cluster.FieldNameSessionID: "session-42"}}
	data, err := proto.Marshal(&envelope.ClientEnvelope{Service: "game", Header: &envelope.Header{Cmd: 1}})
	require.NoError(t, err)
	finished := make(chan struct{})
	go func() {
		g.handleBinaryMessage(session, data)
		close(finished)
	}()
	awaitBackground(t, entered)
	require.Zero(t, reg.watchCount.Load(), "a pending service lookup must not create a watcher")
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
	require.Equal(t, int32(2), reg.getCount.Load())
}

func TestGateDispatchTracksServiceAvailability(t *testing.T) {
	var instances []*registry.ServiceInstance
	var watcher *initialDiscoveryWatcher
	reg := &initialDiscoveryRegistry{
		watch: func(context.Context, string) (registry.Watcher, error) {
			watcher = newInitialDiscoveryWatcher()
			return watcher, nil
		},
		get: func(context.Context, string) ([]*registry.ServiceInstance, error) { return instances, nil },
	}
	bro := &testBroker{}
	var bound string
	g := New(Discovery(reg), testGateSelector(), Broker(bro),
		Locator(&dispatchTestLocator{node: func() (string, error) { return bound, nil }}))
	g.appName, g.appID = "gate", "gate-1"
	t.Cleanup(func() { require.NoError(t, g.Stop(context.Background())) })
	dispatch := func() error { return g.dispatch(42, "session-42", "match", nil) }
	require.ErrorIs(t, dispatch(), errServiceNotFound)
	require.Zero(t, reg.watchCount.Load())

	// 实例只提供服务名和 ID，Gate 即可转发
	instances = []*registry.ServiceInstance{{Name: "match", ID: "match-1"}}
	require.NoError(t, dispatch())
	require.NoError(t, dispatch())
	require.EqualValues(t, 3, reg.getCount.Load(), "cached dispatch must not query the registry")
	require.EqualValues(t, 1, reg.watchCount.Load())

	instances = []*registry.ServiceInstance{{Name: "match", ID: "match-2"}}
	watcher.send(t, instances)
	current := g.resolver().Cached("match")
	require.Eventually(t, func() bool { return current.Subject("match-2") != "" }, time.Second, time.Millisecond)
	bound = "match-1"
	require.ErrorIs(t, dispatch(), selector.ErrNoAvailableNode)
	bound = "match-2"
	require.NoError(t, dispatch())

	instances = nil
	watcher.send(t, instances)
	awaitBackground(t, watcher.done)
	require.Nil(t, g.resolver().Cached("match"))
	require.ErrorIs(t, dispatch(), errServiceNotFound)

	instances = []*registry.ServiceInstance{{Name: "match", ID: "match-3"}}
	bound = ""
	require.NoError(t, dispatch())
	require.EqualValues(t, 2, reg.watchCount.Load())
	require.Equal(t, []string{"meta.gate.match.match-1", "meta.gate.match.match-1", "meta.gate.match.match-2", "meta.gate.match.match-3"}, bro.pubSubjects)
}

func TestGateServiceDisappearsWhileSubscribing(t *testing.T) {
	w := newInitialDiscoveryWatcher()
	reg := &initialDiscoveryRegistry{}
	reg.watch = func(context.Context, string) (registry.Watcher, error) { return w, nil }
	reg.get = func(context.Context, string) ([]*registry.ServiceInstance, error) {
		if reg.watchCount.Load() == 0 {
			return initialDiscoverySnapshot("game-1"), nil
		}
		return nil, nil
	}
	g := newInitialDiscoveryGate(t, reg, &testBroker{})
	_, err := g.ensure("game")
	require.ErrorIs(t, err, errServiceNotFound)
	require.Nil(t, g.resolver().Cached("game"))
	require.EqualValues(t, 1, w.stops.Load())
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
		if reg.getCount.Load() == 2 {
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
	require.Equal(t, int32(4), reg.getCount.Load())
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
	require.EqualValues(t, 1, reg.getCount.Load(), "a failed Watch must not start the second lookup")
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
	require.Equal(t, int32(3), reg.getCount.Load())
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
	}
	reg.get = func(context.Context, string) ([]*registry.ServiceInstance, error) {
		if reg.getCount.Load() == 2 {
			return nil, failure
		}
		return initialDiscoverySnapshot("game-1"), nil
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
	signal := sync.OnceFunc(func() { close(entered) })
	reg := &initialDiscoveryRegistry{
		watch: func(context.Context, string) (registry.Watcher, error) { return w, nil },
		get: func(ctx context.Context, _ string) ([]*registry.ServiceInstance, error) {
			signal()
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
	require.Equal(t, int32(2), reg.getCount.Load())
}
