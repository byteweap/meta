package gate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta/component/registry"
	"github.com/byteweap/meta/component/selector"
)

type discoveryRecoveryWatcher struct {
	nextEntered chan struct{}
	nextOnce    sync.Once
	nextError   chan error
	stopEntered chan struct{}
	stopRelease <-chan struct{}
	stopError   error
	stopCalls   atomic.Int32
	stopped     chan struct{}
	stopOnce    sync.Once
}

func newDiscoveryRecoveryWatcher() *discoveryRecoveryWatcher {
	return &discoveryRecoveryWatcher{
		nextEntered: make(chan struct{}), nextError: make(chan error, 1),
		stopEntered: make(chan struct{}, 4), stopped: make(chan struct{}),
	}
}

func (w *discoveryRecoveryWatcher) Next() ([]*registry.ServiceInstance, error) {
	w.nextOnce.Do(func() { close(w.nextEntered) })
	select {
	case err := <-w.nextError:
		return nil, err
	case <-w.stopped:
		return nil, context.Canceled
	}
}

func (w *discoveryRecoveryWatcher) Stop() error {
	w.stopCalls.Add(1)
	w.stopEntered <- struct{}{}
	if w.stopRelease != nil {
		<-w.stopRelease
	}
	w.stopOnce.Do(func() { close(w.stopped) })
	return w.stopError
}

type discoveryRecoveryRegistry struct {
	testRegistry
	watchers   []*discoveryRecoveryWatcher
	watchCalls atomic.Int32
	getCalls   atomic.Int32
	get        func(context.Context, string, int32) ([]*registry.ServiceInstance, error)
}

func (r *discoveryRecoveryRegistry) Watch(context.Context, string) (registry.Watcher, error) {
	call := r.watchCalls.Add(1)
	if int(call) > len(r.watchers) {
		return nil, fmt.Errorf("unexpected Watch call %d", call)
	}
	return r.watchers[call-1], nil
}

func (r *discoveryRecoveryRegistry) GetService(ctx context.Context, service string) ([]*registry.ServiceInstance, error) {
	call := r.getCalls.Add(1)
	if r.get != nil {
		return r.get(ctx, service, call)
	}
	return []*registry.ServiceInstance{discoveryTestInstance(service, fmt.Sprintf("node-%d", (call+1)/2))}, nil
}

func newDiscoveryRecoveryGate(t *testing.T, r registry.Registry) *Gate {
	t.Helper()
	g := New(Discovery(r), SelectorFunc(func() selector.Selector { return &testSelector{} }))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = g.Stop(ctx) // Some cases deliberately retain the watcher's close error.
	})
	return g
}

type discoveryRecoveryResult struct {
	selector selector.Selector
	err      error
}

func ensureDiscoveryRecovery(g *Gate) <-chan discoveryRecoveryResult {
	result := make(chan discoveryRecoveryResult, 1)
	go func() {
		sel, err := g.ensure("game")
		result <- discoveryRecoveryResult{sel, err}
	}()
	return result
}

func requireDiscoveryRecoveryNode(t *testing.T, sel selector.Selector, id string) {
	t.Helper()
	node, err := sel.Select("42")
	require.NoError(t, err)
	require.Equal(t, id, node.ID())
}

func TestDiscoveryRecoveryInvalidatesFailedWatcher(t *testing.T) {
	first, second := newDiscoveryRecoveryWatcher(), newDiscoveryRecoveryWatcher()
	r := &discoveryRecoveryRegistry{watchers: []*discoveryRecoveryWatcher{first, second}}
	g := newDiscoveryRecoveryGate(t, r)
	old, err := g.ensure("game")
	require.NoError(t, err)
	requireDiscoveryRecoveryNode(t, old, "node-1")
	awaitBackground(t, first.nextEntered)
	first.nextError <- errors.New("watch stream failed")
	awaitBackground(t, first.stopEntered)
	require.Empty(t, old.Nodes(), "failed watcher must not leave stale nodes usable")
	_, err = old.Select("42")
	require.ErrorIs(t, err, selector.ErrNoAvailableNode)

	current, err := g.ensure("game")
	require.NoError(t, err)
	require.NotSame(t, old, current)
	requireDiscoveryRecoveryNode(t, current, "node-2")
	require.Equal(t, int32(2), r.watchCalls.Load())
	require.Equal(t, int32(4), r.getCalls.Load())
	require.Equal(t, int32(1), first.stopCalls.Load())
}

func TestDiscoveryRecoveryRebuildsWhileOldCloseIsBlocked(t *testing.T) {
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	first, second := newDiscoveryRecoveryWatcher(), newDiscoveryRecoveryWatcher()
	first.stopRelease = release
	first.stopError = errors.New("old watcher close failed")
	r := &discoveryRecoveryRegistry{watchers: []*discoveryRecoveryWatcher{first, second}}
	g := newDiscoveryRecoveryGate(t, r)
	t.Cleanup(unblock)
	old, err := g.ensure("game")
	require.NoError(t, err)
	awaitBackground(t, first.nextEntered)
	first.nextError <- errors.New("watch stream failed")
	awaitBackground(t, first.stopEntered)
	require.Empty(t, old.Nodes())

	rebuilt := awaitBackground(t, ensureDiscoveryRecovery(g))
	require.NoError(t, rebuilt.err)
	require.NotSame(t, old, rebuilt.selector)
	requireDiscoveryRecoveryNode(t, rebuilt.selector, "node-2")
	awaitBackground(t, second.nextEntered)
	unblock()
	awaitBackground(t, first.stopped)
	cached, err := g.ensure("game")
	require.NoError(t, err)
	require.Same(t, rebuilt.selector, cached, "finishing the old close must preserve the new cache entry")
	requireDiscoveryRecoveryNode(t, cached, "node-2")
	require.Equal(t, int32(2), r.watchCalls.Load())
	require.Equal(t, int32(1), first.stopCalls.Load())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = g.Stop(ctx)
	require.ErrorIs(t, err, first.stopError, "recovery must not hide the old watcher's cleanup failure")
	require.Equal(t, 1, strings.Count(err.Error(), first.stopError.Error()), "close error must be recorded only once")
}

func TestDiscoveryRecoveryWaitsForLateInitialSnapshot(t *testing.T) {
	snapshotEntered, releaseSnapshot := make(chan struct{}), make(chan struct{})
	releaseClose := make(chan struct{})
	unblockSnapshot := sync.OnceFunc(func() { close(releaseSnapshot) })
	unblockClose := sync.OnceFunc(func() { close(releaseClose) })
	w := newDiscoveryRecoveryWatcher()
	w.stopRelease = releaseClose
	w.stopError = errors.New("late watcher close failed")
	r := &discoveryRecoveryRegistry{
		watchers: []*discoveryRecoveryWatcher{w},
		get: func(_ context.Context, _ string, call int32) ([]*registry.ServiceInstance, error) {
			if call == 1 {
				return initialDiscoverySnapshot("early"), nil
			}
			close(snapshotEntered)
			<-releaseSnapshot // Simulate a registry returning despite cancellation.
			return initialDiscoverySnapshot("late"), nil
		},
	}
	g := newDiscoveryRecoveryGate(t, r)
	t.Cleanup(unblockSnapshot)
	t.Cleanup(unblockClose)
	ensured := ensureDiscoveryRecovery(g)
	awaitBackground(t, snapshotEntered)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, g.Stop(ctx), context.DeadlineExceeded)
	unblockSnapshot()
	awaitBackground(t, w.stopEntered)
	stopped := stopBackground(g, context.Background())
	select {
	case err := <-stopped:
		t.Fatalf("Stop completed while late watcher cleanup was blocked: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	unblockClose()
	got := awaitBackground(t, ensured)
	require.Nil(t, got.selector)
	require.ErrorIs(t, got.err, context.Canceled)
	require.ErrorIs(t, awaitBackground(t, stopped), w.stopError)
	require.Nil(t, g.cachedDiscovery("game"), "late initialization must not reinsert a stopped discovery")
	require.Equal(t, int32(1), w.stopCalls.Load())
	select {
	case <-w.nextEntered:
		t.Fatal("late initial snapshot started a watch loop")
	default:
	}
	_, err := g.ensure("game")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int32(1), r.watchCalls.Load())
	require.Equal(t, int32(2), r.getCalls.Load())
}

func TestDiscoveryRecoverySharesCloseWithStop(t *testing.T) {
	for _, failureFirst := range []bool{true, false} {
		name := "stop first"
		if failureFirst {
			name = "watch failure first"
		}
		t.Run(name, func(t *testing.T) {
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			w := newDiscoveryRecoveryWatcher()
			w.stopRelease = release
			w.stopError = errors.New("watcher close failed")
			g := newDiscoveryRecoveryGate(t, &discoveryRecoveryRegistry{watchers: []*discoveryRecoveryWatcher{w}})
			t.Cleanup(unblock)
			sel, err := g.ensure("game")
			require.NoError(t, err)
			awaitBackground(t, w.nextEntered)
			if failureFirst {
				w.nextError <- errors.New("watch stream failed")
				awaitBackground(t, w.stopEntered)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			first := stopBackground(g, ctx)
			if !failureFirst {
				awaitBackground(t, w.stopEntered)
				w.nextError <- errors.New("watch stream failed")
			}
			require.ErrorIs(t, awaitBackground(t, first), context.DeadlineExceeded)
			require.Equal(t, int32(1), w.stopCalls.Load())
			second := stopBackground(g, context.Background())
			unblock()
			err = awaitBackground(t, second)
			require.ErrorIs(t, err, w.stopError)
			require.Equal(t, 1, strings.Count(err.Error(), w.stopError.Error()), "competing cleanup paths must share one recorded error")
			require.ErrorIs(t, g.Stop(context.Background()), w.stopError)
			require.Equal(t, int32(1), w.stopCalls.Load())
			require.Empty(t, sel.Nodes())
			require.Nil(t, g.cachedDiscovery("game"))
		})
	}
}
