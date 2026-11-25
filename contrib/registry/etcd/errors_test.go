package etcd

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/byteweap/meta/component/registry"
)

type readKV struct {
	clientv3.KV
	values []*mvccpb.KeyValue
	err    error
}

func (f readKV) Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return &clientv3.GetResponse{Kvs: f.values}, f.err
}

func TestReadsRejectBackendErrorsAndMalformedRecords(t *testing.T) {
	failure := errors.New("KV unavailable")
	for _, tc := range []struct {
		kv   readKV
		want error
	}{
		{kv: readKV{err: failure}, want: failure},
		{kv: readKV{values: []*mvccpb.KeyValue{{Value: []byte("{")}}}},
	} {
		r := &Registry{opts: &options{}, kv: tc.kv}
		w := &watcher{ctx: t.Context(), kv: tc.kv, first: true, serviceName: "game"}
		for _, read := range []func() ([]*registry.ServiceInstance, error){
			func() ([]*registry.ServiceInstance, error) { return r.GetService(t.Context(), "game") },
			w.Next,
		} {
			got, err := read()
			if got != nil || err == nil || (tc.want != nil && !errors.Is(err, tc.want)) {
				t.Fatalf("read = %v, %v, want failure %v", got, err, tc.want)
			}
		}
	}

	wrapped := readKV{values: []*mvccpb.KeyValue{
		{Value: []byte(`{"Name":"game-other","ID":"skip"}`)},
		{Value: []byte(`{"Name":"game","ID":"keep"}`)},
	}}
	r := &Registry{opts: &options{}, kv: wrapped}
	w := &watcher{ctx: t.Context(), kv: wrapped, first: true, serviceName: "game"}
	for _, read := range []func() ([]*registry.ServiceInstance, error){
		func() ([]*registry.ServiceInstance, error) { return r.GetService(t.Context(), "game") },
		w.Next,
	} {
		got, err := read()
		if err != nil || len(got) != 1 || got[0].ID != "keep" {
			t.Fatalf("prefix lookup returned %v, %v", got, err)
		}
	}
}

type heartbeatLease struct {
	rollbackLease
	keep func(context.Context, clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error)
}

func (l *heartbeatLease) KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	return l.keep(ctx, id)
}

func TestHeartbeatRetryAfterLeaseLoss(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		closed := make(chan *clientv3.LeaseKeepAliveResponse)
		close(closed)
		leaseCalls := 0
		var keepIDs []clientv3.LeaseID
		lease := &heartbeatLease{
			rollbackLease: rollbackLease{grant: func(context.Context, int64) (*clientv3.LeaseGrantResponse, error) {
				leaseCalls++
				if leaseCalls == 1 {
					return nil, errors.New("temporary grant failure")
				}
				return &clientv3.LeaseGrantResponse{ID: clientv3.LeaseID(10 + leaseCalls)}, nil
			}},
			keep: func(_ context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
				keepIDs = append(keepIDs, id)
				if len(keepIDs) <= 2 {
					return closed, errors.New("keepalive failure")
				}
				cancel()
				return closed, nil
			},
		}
		kv := &rollbackKV{put: func(context.Context, string, string, ...clientv3.OpOption) (*clientv3.PutResponse, error) {
			return &clientv3.PutResponse{}, nil
		}}
		r := &Registry{
			opts: &options{ctx: ctx, ttl: time.Second, maxRetry: 3}, lease: lease,
			client: &clientv3.Client{Lease: lease}, kv: kv,
		}
		r.heartBeat(ctx, 1, "game/node", "record")
		if len(keepIDs) != 3 || keepIDs[0] != 1 || keepIDs[1] != 12 || keepIDs[2] != 13 {
			t.Fatalf("keepalive lease sequence = %v", keepIDs)
		}
	})
}

func TestHeartbeatStopsWhenGrantTimesOutOrContextAlreadyCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, isCanceled := range []bool{false, true} {
			ctx, cancel := context.WithCancel(t.Context())
			if isCanceled {
				cancel()
			}
			closed := make(chan *clientv3.LeaseKeepAliveResponse)
			close(closed)
			var calls int
			lease := &heartbeatLease{
				rollbackLease: rollbackLease{grant: func(ctx context.Context, _ int64) (*clientv3.LeaseGrantResponse, error) {
					calls++
					<-ctx.Done()
					return nil, ctx.Err()
				}},
				keep: func(context.Context, clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
					return closed, context.DeadlineExceeded
				},
			}
			r := &Registry{opts: &options{ctx: ctx, ttl: time.Second, maxRetry: 1}, client: &clientv3.Client{Lease: lease}, lease: lease}
			r.heartBeat(ctx, 1, "game/node", "record")
			cancel()
			synctest.Wait()
			if (calls == 0) != isCanceled {
				t.Fatalf("canceled=%v grant calls=%d", isCanceled, calls)
			}
		}
	})
}

func TestRegistryOptionsAndCanceledWatch(t *testing.T) {
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:2379"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	r := New(client, Context(ctx), Namespace("/other"))
	if r.ID() != "etcd" || r.registerKey(&registry.ServiceInstance{Name: "game", ID: "node"}) != "/other/game/node" {
		t.Fatal("constructor did not apply namespace")
	}
	if _, err := r.Watch(ctx, "game"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Watch returned %v", err)
	}
	for range 2 {
		if err := r.Register(ctx, &registry.ServiceInstance{Name: "game", ID: "node"}); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Register returned %v", err)
		}
	}
}

func TestHeartbeatStopsWhenRegistryContextEnds(t *testing.T) {
	lifetime, cancel := context.WithCancel(t.Context())
	connected := make(chan struct{})
	responses := make(chan *clientv3.LeaseKeepAliveResponse)
	lease := &heartbeatLease{keep: func(_ context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
		if id != 42 {
			t.Errorf("KeepAlive received lease %d, want 42", id)
		}
		close(connected)
		return responses, nil
	}}
	r := &Registry{opts: &options{ctx: lifetime}, client: &clientv3.Client{Lease: lease}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.heartBeat(t.Context(), 42, "game/node", "record")
	}()
	<-connected
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not stop when registry context ended")
	}
}
