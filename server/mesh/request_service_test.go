package mesh

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta"
	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/registry"
	"github.com/byteweap/meta/component/selector"
	es "github.com/byteweap/meta/errors"
)

type serviceRegistry struct {
	registry.Registry
	initial                 []*registry.ServiceInstance
	updates                 chan []*registry.ServiceInstance
	wait                    chan struct{}
	stopErr                 error
	queries, watches, stops atomic.Int32
}

func (r *serviceRegistry) GetService(ctx context.Context, name string) ([]*registry.ServiceInstance, error) {
	r.queries.Add(1)
	if r.wait != nil {
		select {
		case <-r.wait:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if name != "room" {
		return nil, nil
	}
	return r.initial, nil
}

func (r *serviceRegistry) Watch(ctx context.Context, _ string) (registry.Watcher, error) {
	r.watches.Add(1)
	return &serviceWatcher{ctx, r}, nil
}

type serviceWatcher struct {
	ctx context.Context
	reg *serviceRegistry
}

func (w *serviceWatcher) Next() ([]*registry.ServiceInstance, error) {
	select {
	case snapshot := <-w.reg.updates:
		return snapshot, nil
	case <-w.ctx.Done():
		return nil, w.ctx.Err()
	}
}

func (w *serviceWatcher) Stop() error {
	w.reg.stops.Add(1)
	return w.reg.stopErr
}

type serviceSelector struct {
	mu    sync.RWMutex
	nodes []selector.Node
}

func newServiceSelector() selector.Selector { return &serviceSelector{} }

func (s *serviceSelector) Update(nodes []selector.Node) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes = nodes
}

func (s *serviceSelector) Select(_ string, _ ...selector.Filter) (selector.Node, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.nodes) == 0 {
		return nil, selector.ErrNoAvailableNode
	}
	return s.nodes[0], nil
}

func (s *serviceSelector) Nodes() []selector.Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Clone(s.nodes)
}

func newServiceMesh(tb testing.TB, reg *serviceRegistry, bro broker.Broker) *Mesh {
	tb.Helper()
	m := New(Broker(bro), Locator(lifecycleLocator{}), Discovery(reg), SelectorFunc(newServiceSelector))
	finished := make(chan error, 1)
	go func() { finished <- m.Start(meta.NewContext(tb.Context(), lifecycleAppInfo{})) }()
	select {
	case <-m.Ready():
	case err := <-finished:
		tb.Fatalf("start: %v", err)
	}
	tb.Cleanup(func() {
		require.NoError(tb, m.Stop(context.Background()))
		require.NoError(tb, <-finished)
	})
	return m
}

func TestRequestServiceSendsToOneLiveInstance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reg := &serviceRegistry{
			initial: []*registry.ServiceInstance{{Name: "room", ID: "*"}, {Name: "room", ID: "a"}, {Name: "room", ID: "b"}},
			updates: make(chan []*registry.ServiceInstance),
		}
		var subjects []string
		var sendErr error
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		deadline, _ := ctx.Deadline()
		bro := &rpcRequestBroker{request: func(_ context.Context, subject string, data []byte, h broker.Header) (*broker.Message, error) {
			subjects = append(subjects, subject)
			require.Equal(t, []byte("input"), data)
			require.Equal(t, "create", h.Get("cmd"))
			require.Equal(t, "1", h.Get("version"))
			require.Equal(t, strconv.FormatInt(deadline.UnixMilli(), 10), h.Get(rpcDeadlineHeader))
			return &broker.Message{Header: broker.Header{"code": {"201"}, "tip": {"created"}}, Data: []byte("output")}, sendErr
		}}
		m := newServiceMesh(t, reg, bro)
		call := func(service string) error {
			data, tip, code, err := m.RequestService(ctx, service, "create", "1", []byte("input"))
			if err == nil {
				require.Equal(t, []byte("output"), data)
				require.Equal(t, "created", tip)
				require.Equal(t, 201, code)
			}
			return err
		}
		require.NoError(t, call("room"))
		require.NoError(t, call("room"))
		require.EqualValues(t, 2, reg.queries.Load())
		require.EqualValues(t, 1, reg.watches.Load())
		sendErr = errors.New("transport failed")
		require.ErrorIs(t, call("room"), sendErr)
		sendErr = nil
		reg.updates <- []*registry.ServiceInstance{{Name: "room", ID: "b"}}
		synctest.Wait()
		require.NoError(t, call("room"))
		reg.initial = nil
		reg.updates <- nil
		synctest.Wait()
		require.EqualValues(t, 1, reg.stops.Load())
		require.ErrorIs(t, call("room"), selector.ErrNoAvailableNode)
		reg.initial = []*registry.ServiceInstance{{Name: "room", ID: "c"}}
		require.NoError(t, call("room"))
		require.Equal(t, []string{"meta.game.room.a", "meta.game.room.a", "meta.game.room.a", "meta.game.room.b", "meta.game.room.c"}, subjects)
		require.NoError(t, m.Stop(t.Context()))
		require.EqualValues(t, 2, reg.stops.Load())
		require.ErrorIs(t, call("room"), context.Canceled)
	})
}

func requestRoom(ctx context.Context, m *Mesh) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, _, _, err := m.RequestService(ctx, "room", "create", "1", nil)
		done <- err
	}()
	return done
}

func TestRequestServiceWaitersHaveIndependentDeadlines(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reg := &serviceRegistry{initial: []*registry.ServiceInstance{{Name: "room", ID: "a"}}, wait: make(chan struct{})}
		var sent atomic.Int32
		bro := &rpcRequestBroker{request: func(context.Context, string, []byte, broker.Header) (*broker.Message, error) {
			sent.Add(1)
			return &broker.Message{}, nil
		}}
		m := newServiceMesh(t, reg, bro)
		short, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		expired := requestRoom(short, m)
		synctest.Wait()
		second := requestRoom(t.Context(), m)
		require.ErrorIs(t, <-expired, context.DeadlineExceeded)
		require.EqualValues(t, 1, reg.queries.Load())
		require.Zero(t, sent.Load())
		close(reg.wait)
		require.NoError(t, <-second)
		require.EqualValues(t, 1, sent.Load())
		require.EqualValues(t, 1, reg.watches.Load())
		require.EqualValues(t, 2, reg.queries.Load())
	})
}

func TestRequestServiceStopDuringLookup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reg := &serviceRegistry{wait: make(chan struct{})}
		m := newServiceMesh(t, reg, &rpcRequestBroker{})
		done := requestRoom(t.Context(), m)
		synctest.Wait()
		require.EqualValues(t, 1, reg.queries.Load())
		require.NoError(t, m.Stop(t.Context()))
		require.ErrorIs(t, <-done, context.Canceled)
		require.Zero(t, reg.watches.Load())
	})
}

func TestMeshStopReportsDiscoveryCleanupError(t *testing.T) {
	reg := &serviceRegistry{
		initial: []*registry.ServiceInstance{{Name: "room", ID: "a"}},
		stopErr: errors.New("watcher close failed"),
	}
	m := New(Broker(&mockBroker{}), Locator(lifecycleLocator{}), Discovery(reg), SelectorFunc(newServiceSelector))
	finished := startLifecycleMesh(t, m)
	<-m.Ready()
	_, _, code, err := m.RequestService(t.Context(), "room", "create", "1", nil)
	require.NoError(t, err)
	require.Zero(t, code)
	require.ErrorIs(t, m.Stop(t.Context()), reg.stopErr)
	require.NoError(t, <-finished)
	require.ErrorIs(t, m.Stop(t.Context()), reg.stopErr)
	require.EqualValues(t, 1, reg.stops.Load())
}

type cancelOnSelect struct {
	selector.Selector
	cancel context.CancelFunc
}

func (s cancelOnSelect) Select(key string, filters ...selector.Filter) (selector.Node, error) {
	s.cancel()
	return s.Selector.Select(key, filters...)
}

func TestRequestServiceCancellationDuringSelectionSkipsSend(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reg := &serviceRegistry{initial: []*registry.ServiceInstance{{Name: "room", ID: "a"}}}
	bro := &rpcRequestBroker{request: func(context.Context, string, []byte, broker.Header) (*broker.Message, error) {
		t.Fatal("canceled request reached broker")
		return nil, nil
	}}
	m := New(Broker(bro), Locator(lifecycleLocator{}), Discovery(reg), SelectorFunc(func() selector.Selector {
		return cancelOnSelect{newServiceSelector(), cancel}
	}))
	finished := startLifecycleMesh(t, m)
	<-m.Ready()
	_, _, code, err := m.RequestService(ctx, "room", "create", "1", nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, code)
	require.NoError(t, m.Stop(t.Context()))
	require.NoError(t, <-finished)
}

func TestRequestServiceConfiguration(t *testing.T) {
	for _, tc := range []struct {
		m    *Mesh
		want error
	}{
		{New(), es.ErrDiscoveryRequired},
		{New(Discovery(&serviceRegistry{})), es.ErrSelectorRequired},
		{New(Discovery(&serviceRegistry{}), SelectorFunc(newServiceSelector)), es.ErrAppNotFound},
	} {
		_, _, _, err := tc.m.RequestService(t.Context(), "room", "create", "1", nil)
		require.ErrorIs(t, err, tc.want)
	}
}

func TestRequestServiceRejectsWildcardTarget(t *testing.T) {
	reg := &serviceRegistry{initial: []*registry.ServiceInstance{{Name: "room", ID: "*"}}}
	bro := &rpcRequestBroker{request: func(context.Context, string, []byte, broker.Header) (*broker.Message, error) {
		t.Fatal("invalid target reached the broker")
		return nil, nil
	}}
	m := newServiceMesh(t, reg, bro)
	_, _, code, err := m.RequestService(t.Context(), "room.*", "create", "1", nil)
	require.Error(t, err)
	require.Zero(t, code)
	require.Zero(t, reg.queries.Load())
	_, _, code, err = m.RequestService(t.Context(), "room", "create", "1", nil)
	require.ErrorIs(t, err, selector.ErrNoAvailableNode)
	require.Zero(t, code)
	require.Zero(t, reg.watches.Load())
}

func TestMeshSubscriptionRejectsConfiguredWildcards(t *testing.T) {
	for _, tc := range []struct{ prefix, service, id string }{
		{"meta.*", "room", "a"},
		{"meta", "*", "a"},
		{"meta", "room", ">"},
	} {
		t.Run(tc.prefix+"-"+tc.service+"-"+tc.id, func(t *testing.T) {
			called := false
			bro := &lifecycleBroker{subscribe: func(context.Context) error {
				called = true
				return errors.New("unexpected subscription")
			}}
			m := New(Broker(bro), Locator(lifecycleLocator{}), Prefix(tc.prefix))
			app := meta.New(meta.Name(tc.service), meta.ID(tc.id))
			require.Error(t, m.Start(meta.NewContext(t.Context(), app)))
			require.False(t, called)
		})
	}
}

func BenchmarkRequestService(b *testing.B) {
	for _, target := range []string{"subject", "service"} {
		b.Run(target, func(b *testing.B) {
			reg := &serviceRegistry{initial: []*registry.ServiceInstance{{Name: "room", ID: "a"}}}
			result := &broker.Message{Header: broker.Header{"code": {"200"}}}
			bro := &rpcRequestBroker{request: func(context.Context, string, []byte, broker.Header) (*broker.Message, error) { return result, nil }}
			m := newServiceMesh(b, reg, bro)
			request, name := m.Request, "meta.game.room.a"
			if target == "service" {
				request, name = m.RequestService, "room"
			}
			payload := make([]byte, 128)
			ctx := m.ctx
			_, _, _, err := request(ctx, name, "create", "1", payload)
			require.NoError(b, err)
			initial := reg.queries.Load()
			b.ReportAllocs()
			for b.Loop() {
				if _, _, _, err := request(ctx, name, "create", "1", payload); err != nil {
					b.Fatal(err)
				}
			}
			require.Equal(b, initial, reg.queries.Load())
		})
	}
}
