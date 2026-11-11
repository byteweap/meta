package servicecomb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-chassis/cari/discovery"
	"github.com/go-chassis/sc-client"

	"github.com/byteweap/meta/component/registry"
)

type snapshotClient struct {
	RegistryClient
	find func(string, sc.CallOptions) ([]*discovery.MicroServiceInstance, error)
}

func (c *snapshotClient) FindMicroServiceInstances(_, _, name, _ string, opts ...sc.CallOption) ([]*discovery.MicroServiceInstance, error) {
	var options sc.CallOptions
	for _, opt := range opts {
		opt(&options)
	}
	return c.find(name, options)
}

func snapshotRegistry(t *testing.T, find func(string) ([]*discovery.MicroServiceInstance, error)) *Registry {
	t.Helper()
	return NewRegistry(&snapshotClient{find: func(name string, options sc.CallOptions) ([]*discovery.MicroServiceInstance, error) {
		if !options.WithoutRevision {
			t.Error("full snapshot query did not disable revision filtering")
		}
		return find(name)
	}})
}

func snapshotWatcher(t *testing.T, r *Registry, ctx context.Context, name string) registry.Watcher {
	t.Helper()
	w, err := r.Watch(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.Stop(); err != nil {
			t.Error(err)
		}
	})
	return w
}

func requireSnapshotIDs(t *testing.T, instances []*registry.ServiceInstance, err error, ids ...string) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	if len(instances) != len(ids) {
		t.Fatalf("snapshot has %d instances, want %v", len(instances), ids)
	}
	for i, id := range ids {
		if instances[i].ID != id {
			t.Fatalf("snapshot[%d].ID = %q, want %q", i, instances[i].ID, id)
		}
	}
}

func TestServiceCombSnapshotLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		instances := []*discovery.MicroServiceInstance{
			{InstanceId: "node-1", ServiceId: "service-id", Version: "v2", Properties: map[string]string{"zone": "a"}, Endpoints: []string{"grpc://127.0.0.1:9000"}},
			nil,
			{InstanceId: "node-2"},
		}
		queries := 0
		var started, finished time.Time
		r := snapshotRegistry(t, func(name string) ([]*discovery.MicroServiceInstance, error) {
			if name != "game" {
				t.Errorf("query name = %q, want game", name)
			}
			queries++
			started = time.Now()
			time.Sleep(200 * time.Millisecond) // Fake time includes SDK request duration.
			finished = time.Now()
			return instances, nil
		})
		w := snapshotWatcher(t, r, context.Background(), "game")
		if queries != 0 {
			t.Fatal("Watch queried before Next")
		}
		before := time.Now()
		first, err := w.Next()
		requireSnapshotIDs(t, first, err, "node-1", "node-2")
		if !started.Equal(before) {
			t.Fatal("first Next delayed the initial query")
		}
		want := &registry.ServiceInstance{
			ID: "node-1", Name: "game", Version: "v2",
			Metadata: map[string]string{"zone": "a"}, Endpoints: []string{"grpc://127.0.0.1:9000"},
		}
		if !reflect.DeepEqual(first[0], want) {
			t.Fatalf("converted instance = %#v, want %#v", first[0], want)
		}
		second, err := r.GetService(context.Background(), "game")
		requireSnapshotIDs(t, second, err, "node-1", "node-2")
		first[0].Metadata["zone"] = "changed"
		first[0].Endpoints[0] = "changed"
		if !reflect.DeepEqual(second[0], want) || instances[0].Properties["zone"] != "a" || instances[0].Endpoints[0] != want.Endpoints[0] {
			t.Fatal("returned metadata or endpoints alias another snapshot or the client")
		}

		instances = []*discovery.MicroServiceInstance{{InstanceId: "node-2"}}
		services, err := w.Next()
		requireSnapshotIDs(t, services, err, "node-2")
		if started.Sub(before) != 1200*time.Millisecond {
			t.Fatalf("second watch query began after %v, want 1.2s", started.Sub(before))
		}
		instances = nil
		previousFinish := finished
		services, err = w.Next()
		requireSnapshotIDs(t, services, err)
		if started.Sub(previousFinish) != time.Second {
			t.Fatalf("poll interval = %v, want 1s after request completion", started.Sub(previousFinish))
		}
		instances = []*discovery.MicroServiceInstance{{InstanceId: "node-3"}}
		services, err = w.Next()
		requireSnapshotIDs(t, services, err, "node-3")
	})
}

func TestServiceCombSnapshotEmptyAndErrors(t *testing.T) {
	queryErr := errors.New("query failed")
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{name: "empty"},
		{name: "missing service", err: sc.ErrMicroServiceNotExists},
		{name: "wrapped missing service", err: fmt.Errorf("query: %w", sc.ErrMicroServiceNotExists)},
		{name: "query failure", err: queryErr, want: queryErr},
		{name: "unexpected not modified", err: sc.ErrNotModified, want: sc.ErrNotModified},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := snapshotRegistry(t, func(string) ([]*discovery.MicroServiceInstance, error) { return nil, tc.err })
			w := snapshotWatcher(t, r, context.Background(), "game")
			for _, query := range []func() ([]*registry.ServiceInstance, error){
				func() ([]*registry.ServiceInstance, error) { return r.GetService(context.Background(), "game") },
				w.Next,
			} {
				services, err := query()
				if !errors.Is(err, tc.want) || len(services) != 0 {
					t.Fatalf("query = (%v, %v), want empty and %v", services, err, tc.want)
				}
			}
		})
	}
}

func TestServiceCombSnapshotMultipleServicesAndSerialization(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan string, 3)
		release := make(chan struct{})
		r := snapshotRegistry(t, func(name string) ([]*discovery.MicroServiceInstance, error) {
			entered <- name
			<-release
			return []*discovery.MicroServiceInstance{{InstanceId: name + "-1"}}, nil
		})
		game := snapshotWatcher(t, r, context.Background(), "game")
		chat := snapshotWatcher(t, r, context.Background(), "chat")
		type result struct {
			services []*registry.ServiceInstance
			err      error
		}
		gameResult, chatResult := make(chan result, 1), make(chan result, 1)
		go func() { services, err := game.Next(); gameResult <- result{services, err} }()
		if got := <-entered; got != "game" {
			t.Fatalf("first query = %q, want game", got)
		}
		go func() { services, err := chat.Next(); chatResult <- result{services, err} }()
		ctx, cancel := context.WithCancel(context.Background())
		lookupResult := make(chan error, 1)
		go func() { _, err := r.GetService(ctx, "queued"); lookupResult <- err }()
		synctest.Wait()
		select {
		case got := <-entered:
			t.Fatalf("concurrent SDK query entered for %q", got)
		default:
		}
		cancel()
		if err := <-lookupResult; !errors.Is(err, context.Canceled) {
			t.Fatalf("queued lookup cancellation = %v", err)
		}
		release <- struct{}{}
		got := <-gameResult
		requireSnapshotIDs(t, got.services, got.err, "game-1")
		if name := <-entered; name != "chat" {
			t.Fatalf("next query = %q, want chat", name)
		}
		release <- struct{}{}
		got = <-chatResult
		requireSnapshotIDs(t, got.services, got.err, "chat-1")
	})
}

func TestServiceCombSnapshotCancelIntervalAndRepeatedStop(t *testing.T) {
	for _, parentCancel := range []bool{false, true} {
		t.Run(fmt.Sprintf("parent cancel=%v", parentCancel), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queries := 0
				r := snapshotRegistry(t, func(string) ([]*discovery.MicroServiceInstance, error) { queries++; return nil, nil })
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				w := snapshotWatcher(t, r, ctx, "game")
				services, err := w.Next()
				requireSnapshotIDs(t, services, err)
				done := make(chan error, 1)
				go func() { _, err := w.Next(); done <- err }()
				synctest.Wait()
				if queries != 1 {
					t.Fatal("Next queried before its interval elapsed")
				}
				before := time.Now()
				if parentCancel {
					cancel()
				} else {
					var stops sync.WaitGroup
					for range 8 {
						stops.Go(func() {
							if err := w.Stop(); err != nil {
								t.Error(err)
							}
						})
					}
					stops.Wait()
				}
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatalf("waiting Next cancellation = %v", err)
				}
				if !time.Now().Equal(before) {
					t.Fatal("cancellation waited for the polling interval")
				}
				if _, err := w.Next(); !errors.Is(err, context.Canceled) || queries != 1 {
					t.Fatalf("Next after cancellation: err=%v, queries=%d", err, queries)
				}
				cancel()
				if stopped, err := r.Watch(ctx, "later"); stopped != nil || !errors.Is(err, context.Canceled) {
					t.Fatalf("Watch with canceled context = (%v, %v), want nil watcher and cancellation", stopped, err)
				}
			})
		})
	}
}

func TestServiceCombSnapshotStopDuringQuery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		queries := 0
		r := snapshotRegistry(t, func(string) ([]*discovery.MicroServiceInstance, error) {
			queries++
			close(entered)
			<-release
			return []*discovery.MicroServiceInstance{{InstanceId: "node-1"}}, nil
		})
		w := snapshotWatcher(t, r, context.Background(), "game")
		inFlight, queued := make(chan error, 1), make(chan error, 1)
		go func() { _, err := w.Next(); inFlight <- err }()
		<-entered
		go func() { _, err := w.Next(); queued <- err }()
		synctest.Wait()
		if err := w.Stop(); err != nil {
			t.Fatal(err)
		}
		if err := <-queued; !errors.Is(err, context.Canceled) {
			t.Fatalf("queued Next cancellation = %v", err)
		}
		select {
		case err := <-inFlight:
			t.Fatalf("SDK request returned before being released: %v", err)
		default:
		}
		close(release)
		if err := <-inFlight; !errors.Is(err, context.Canceled) {
			t.Fatalf("SDK completion after Stop = %v, want cancellation", err)
		}
		if queries != 1 {
			t.Fatalf("queued Next started another query: count=%d", queries)
		}
	})
}
