package discovery

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta/component/registry"
	"github.com/byteweap/meta/component/selector"
)

type resolverTestRegistry struct {
	registry.Registry
	get     func(context.Context) ([]*registry.ServiceInstance, error)
	watcher registry.Watcher
}

func (r *resolverTestRegistry) GetService(ctx context.Context, _ string) ([]*registry.ServiceInstance, error) {
	return r.get(ctx)
}

func (r *resolverTestRegistry) Watch(context.Context, string) (registry.Watcher, error) {
	return r.watcher, nil
}

type resolverTestWatcher struct {
	next func() ([]*registry.ServiceInstance, error)
	stop func() error
}

func (w resolverTestWatcher) Next() ([]*registry.ServiceInstance, error) { return w.next() }
func (w resolverTestWatcher) Stop() error                                { return w.stop() }

type resolverTestSelector struct{ selector.Selector }

func (resolverTestSelector) Update([]selector.Node) {}

type statelessTestSelector struct {
	selector.Selector
	update func()
}

func (statelessTestSelector) Select(string, ...selector.Filter) (selector.Node, error) {
	return selector.NewNode("live", "game", "", nil), nil
}

func (s statelessTestSelector) Update([]selector.Node) {
	if s.update != nil {
		s.update()
	}
}

func TestStatelessDeclarationCoversAllRoutableInstances(t *testing.T) {
	live := &registry.ServiceInstance{Name: "game", ID: "live", Metadata: map[string]string{registry.MetadataStateless: "true"}}
	r := &Resolver{subject: func(_, id string) string {
		if id == "invalid" {
			return ""
		}
		return id
	}}
	s := &Service{Selector: statelessTestSelector{}}
	for _, tc := range []struct {
		name      string
		instances []*registry.ServiceInstance
		want      string
	}{
		{name: "unanimous", instances: []*registry.ServiceInstance{live}, want: "live"},
		{
			name: "ignore records without a routable address",
			instances: []*registry.ServiceInstance{
				live, nil, {Name: "game"}, {Name: "game", ID: "invalid"}, {Name: "other", ID: "live"},
			},
			want: "live",
		},
		{
			name: "declaration must equal true",
			instances: []*registry.ServiceInstance{
				live, {Name: "game", ID: "other", Metadata: map[string]string{registry.MetadataStateless: "TRUE"}},
			},
		},
		{name: "duplicate id disagrees", instances: []*registry.ServiceInstance{live, {Name: "game", ID: "live"}}},
		{name: "empty snapshot"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r.update(s, "game", tc.instances)
			require.Equal(t, tc.want != "", s.Stateless())
			subject, err := s.SelectStatelessSubject("42")
			require.NoError(t, err)
			require.Equal(t, tc.want, subject)
		})
	}
}

func TestStatelessDeclarationChangesWithSelectorUpdate(t *testing.T) {
	r := &Resolver{subject: func(_, id string) string { return id }}
	sel := &statelessTestSelector{}
	s := &Service{Selector: sel}
	instances := []*registry.ServiceInstance{
		{Name: "game", ID: "live", Metadata: map[string]string{registry.MetadataStateless: "true"}},
	}
	require.True(t, r.update(s, "game", instances))
	sel.update = func() {
		require.True(t, s.Stateless(), "替换前保留上一份快照的声明")
		panic("selector update failed")
	}
	require.Panics(t, func() { r.update(s, "game", instances) })
	require.Empty(t, s.Subject("live"))
	subject, err := s.SelectStatelessSubject("42")
	require.NoError(t, err)
	require.Empty(t, subject)
}

func TestLastWaiterCancelsPendingLookup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release, stopped := make(chan struct{}), make(chan struct{})
		var calls atomic.Int32
		var lookup context.Context
		reg := &resolverTestRegistry{
			get: func(ctx context.Context) ([]*registry.ServiceInstance, error) {
				if calls.Add(1) == 1 {
					lookup = ctx
					<-release // 模拟旧查询在下一代缓存就绪后返回
					return nil, nil
				}
				return []*registry.ServiceInstance{{Name: "room", ID: "new"}}, nil
			},
			watcher: resolverTestWatcher{
				next: func() ([]*registry.ServiceInstance, error) {
					<-stopped
					return nil, context.Canceled
				},
				stop: func() error { close(stopped); return nil },
			},
		}
		r := New(t.Context(), reg, func() selector.Selector { return resolverTestSelector{} }, func(_, id string) string { return id })
		ctx, cancel := context.WithCancel(t.Context())
		first := make(chan error, 1)
		go func() {
			_, err := r.Resolve(ctx, "room")
			first <- err
		}()
		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-first, context.Canceled)
		require.ErrorIs(t, lookup.Err(), context.Canceled)

		current, err := r.Resolve(t.Context(), "room")
		require.NoError(t, err)
		close(release)
		synctest.Wait()
		require.Same(t, current, r.Cached("room"))
		require.Equal(t, "new", current.Subject("new"))
		require.NoError(t, r.Close())
	})
}

func TestWatcherNextPanicInvalidatesPublishedSnapshot(t *testing.T) {
	trigger, stopped := make(chan struct{}), make(chan struct{})
	reg := &resolverTestRegistry{
		get: func(context.Context) ([]*registry.ServiceInstance, error) {
			return []*registry.ServiceInstance{{Name: "room", ID: "a"}}, nil
		},
		watcher: resolverTestWatcher{
			next: func() ([]*registry.ServiceInstance, error) {
				<-trigger
				panic("watch failed")
			},
			stop: func() error { close(stopped); return nil },
		},
	}
	r := New(t.Context(), reg, func() selector.Selector { return resolverTestSelector{} }, func(_, id string) string { return id })
	current, err := r.Resolve(t.Context(), "room")
	require.NoError(t, err)
	close(trigger)
	<-stopped
	require.Nil(t, r.Cached("room"))
	require.Empty(t, current.Subject("a"))
	require.NoError(t, r.Close())
}

func TestSelectorPanicReleasesWatcherAndAllowsRetry(t *testing.T) {
	var opens, stops atomic.Int32
	reg := &resolverTestRegistry{get: func(context.Context) ([]*registry.ServiceInstance, error) {
		if opens.Add(1) > 2 {
			return nil, nil
		}
		return []*registry.ServiceInstance{{Name: "room", ID: "a"}}, nil
	}, watcher: resolverTestWatcher{stop: func() error { stops.Add(1); return nil }}}
	r := New(t.Context(), reg, func() selector.Selector { panic("selector failed") }, func(_, id string) string { return id })
	defer func() { require.NoError(t, r.Close()) }()
	_, err := r.Resolve(t.Context(), "room")
	require.ErrorContains(t, err, "selector failed")
	require.EqualValues(t, 1, stops.Load())
	_, err = r.Resolve(t.Context(), "room")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestWatcherStopPanicBecomesCleanupError(t *testing.T) {
	reg := &resolverTestRegistry{get: func(context.Context) ([]*registry.ServiceInstance, error) {
		return []*registry.ServiceInstance{{Name: "room", ID: "a"}}, nil
	}, watcher: resolverTestWatcher{stop: func() error { panic("watch stop failed") }}}
	r := New(t.Context(), reg, func() selector.Selector { return nil }, func(_, id string) string { return id })
	_, err := r.Resolve(t.Context(), "room")
	require.ErrorContains(t, err, "watch stop failed")
	require.ErrorContains(t, r.Close(), "watch stop failed")
}

type fixedSelector struct {
	selector.Selector
	node selector.Node
	err  error
}

func (s fixedSelector) Select(string, ...selector.Filter) (selector.Node, error) {
	return s.node, s.err
}

func TestServiceSelectionRejectsStaleAndUnavailableNodes(t *testing.T) {
	failure := errors.New("selection failed")
	for _, test := range []struct {
		sel  selector.Selector
		want error
	}{
		{fixedSelector{err: failure}, failure},
		{fixedSelector{node: selector.NewNode("stale", "room", "v1", nil)}, selector.ErrNoAvailableNode},
		{fixedSelector{}, selector.ErrNoAvailableNode},
	} {
		s := &Service{Selector: test.sel, subjects: map[string]string{"live": "room.live"}}
		subject, err := s.SelectSubject("42")
		require.Empty(t, subject)
		require.ErrorIs(t, err, test.want)
	}
}

type openingRegistry struct {
	registry.Registry
	get   func(context.Context) ([]*registry.ServiceInstance, error)
	watch func(context.Context) (registry.Watcher, error)
}

func (r openingRegistry) GetService(ctx context.Context, _ string) ([]*registry.ServiceInstance, error) {
	return r.get(ctx)
}

func (r openingRegistry) Watch(ctx context.Context, _ string) (registry.Watcher, error) {
	return r.watch(ctx)
}

func TestResolverRejectsCanceledAndInvalidLookups(t *testing.T) {
	r := New(t.Context(), nil, nil, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := r.Resolve(ctx, "room")
	require.ErrorIs(t, err, context.Canceled)

	_, err = r.Resolve(t.Context(), "bad topic")
	require.ErrorIs(t, err, ErrNotFound)
	require.NoError(t, r.Close())
	_, err = r.Resolve(t.Context(), "room")
	require.ErrorIs(t, err, context.Canceled)
}

func TestResolverReusesCacheAndClearsItWhenWatchEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		unblock := make(chan struct{})
		stopped := make(chan struct{})
		instance := &registry.ServiceInstance{Name: "room", ID: "live"}
		var watchContext context.Context
		reg := openingRegistry{
			get: func(context.Context) ([]*registry.ServiceInstance, error) {
				return []*registry.ServiceInstance{instance}, nil
			},
			watch: func(ctx context.Context) (registry.Watcher, error) {
				watchContext = ctx
				return resolverTestWatcher{
					next: func() ([]*registry.ServiceInstance, error) {
						<-unblock
						return nil, nil
					},
					stop: func() error { close(stopped); return nil },
				}, nil
			},
		}
		r := New(t.Context(), reg, func() selector.Selector { return resolverTestSelector{} }, func(_, id string) string { return id })
		service, err := r.Resolve(t.Context(), "room")
		require.NoError(t, err)
		cached, err := r.Resolve(t.Context(), "room")
		require.NoError(t, err)
		require.Same(t, service, cached)
		require.Equal(t, "live", service.Subject("live"))
		require.NoError(t, watchContext.Err())

		closeDone := make(chan error, 1)
		go func() { closeDone <- r.Close() }()
		<-stopped
		close(unblock)
		require.NoError(t, <-closeDone)
		require.Nil(t, r.Cached("room"))
		require.Empty(t, service.Subject("live"))
	})
}

func TestResolverOpensWatchTransactionally(t *testing.T) {
	failure, stopFailure := errors.New("discovery failed"), context.DeadlineExceeded
	instance := &registry.ServiceInstance{Name: "room", ID: "live"}
	for _, tc := range []struct {
		name string
		mode string
		want error
	}{
		{"first query fails", "first-error", failure},
		{"first query cancels", "first-cancel", context.Canceled},
		{"watch error", "watch-error", failure},
		{"watch and cleanup error", "watch-close-error", failure},
		{"watch cancels", "watch-cancel", context.Canceled},
		{"second query fails", "second-error", failure},
		{"second query cancels", "second-cancel", context.Canceled},
		{"initial snapshot became empty", "empty", ErrNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			queryCount := 0
			var stops atomic.Int32
			watcher := resolverTestWatcher{stop: func() error {
				stops.Add(1)
				if tc.mode == "watch-close-error" {
					return stopFailure
				}
				return nil
			}}
			reg := openingRegistry{
				get: func(context.Context) ([]*registry.ServiceInstance, error) {
					queryCount++
					prefix := "first"
					if queryCount > 1 {
						prefix = "second"
					}
					switch tc.mode {
					case prefix + "-error":
						return nil, failure
					case prefix + "-cancel":
						cancel()
					case "empty":
						if queryCount > 1 {
							return nil, nil
						}
					}
					return []*registry.ServiceInstance{instance}, nil
				},
				watch: func(context.Context) (registry.Watcher, error) {
					switch tc.mode {
					case "watch-error":
						return nil, failure
					case "watch-close-error":
						return watcher, failure
					case "watch-cancel":
						cancel()
					}
					return watcher, nil
				},
			}
			r := New(ctx, reg, func() selector.Selector { return resolverTestSelector{} }, func(_, id string) string { return id })
			service, err := r.open(ctx, "room", cancel)
			require.ErrorIs(t, err, tc.want)
			if service != nil {
				require.NoError(t, service.stop())
			}
			if tc.mode == "watch-close-error" {
				require.ErrorIs(t, err, stopFailure)
				require.ErrorIs(t, r.Close(), stopFailure)
				require.EqualValues(t, 1, stops.Load())
			} else {
				require.NoError(t, r.Close())
			}
			require.Nil(t, r.Cached("room"))
		})
	}
}
