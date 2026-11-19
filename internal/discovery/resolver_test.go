package discovery

import (
	"context"
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
