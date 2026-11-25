package eureka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/byteweap/meta/component/registry"
)

type discoveryTransport func(*http.Request) (*http.Response, error)

func (f discoveryTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func discoveryResponse(instances []Instance, status int) *http.Response {
	body, _ := json.Marshal(ApplicationsRootResponse{ApplicationsResponse: ApplicationsResponse{
		Applications: []Application{{Name: "game", Instance: instances}},
	}})
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body)))}
}

func discoveryNode(id string) Instance {
	return Instance{InstanceID: id, App: "game", Status: statusUp, Metadata: map[string]string{
		"ID": id, "Name": "game", "Version": "v2", "Endpoints": "http://127.0.0.1:9000", "zone": "a",
	}}
}

func discoveryRegistry(t *testing.T, transport discoveryTransport) (*Registry, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	client := NewClient([]string{"http://eureka.test"}, WithClientContext(ctx))
	client.client = &http.Client{Transport: transport}
	return &Registry{ctx: ctx, api: NewAPI(ctx, client, time.Hour)}, cancel
}

func discoveryWatch(t *testing.T, r *Registry, ctx context.Context) registry.Watcher {
	t.Helper()
	w, err := r.Watch(ctx, "game")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	return w
}

func requireDiscoveryIDs(t *testing.T, got []*registry.ServiceInstance, err error, ids ...string) {
	t.Helper()
	if err != nil || len(got) != len(ids) {
		t.Fatalf("snapshot = %#v, %v; want IDs %v", got, err, ids)
	}
	for i, id := range ids {
		if got[i].ID != id {
			t.Fatalf("snapshot[%d].ID = %q, want %q", i, got[i].ID, id)
		}
	}
}

func TestEurekaDiscoverySnapshotsAndIsolation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		instances := []Instance{discoveryNode("node-1"), discoveryNode("node-2")}
		r, _ := discoveryRegistry(t, func(req *http.Request) (*http.Response, error) {
			if !strings.HasSuffix(req.URL.Path, "/apps") {
				t.Errorf("unexpected fallback query: %s", req.URL.Path)
			}
			return discoveryResponse(instances, http.StatusOK), nil
		})
		synctest.Wait()
		w := discoveryWatch(t, r, context.Background())
		synctest.Wait()
		first, err := w.Next()
		requireDiscoveryIDs(t, first, err, "node-1", "node-2")
		if first[0].Version != "v2" || first[0].Name != "game" {
			t.Fatalf("lost discovery fields: %#v", first[0])
		}
		first[0].Metadata["zone"] = "changed"
		first[0].Endpoints[0] = "changed"
		fresh, err := r.GetService(context.Background(), "GAME")
		requireDiscoveryIDs(t, fresh, err, "node-1", "node-2")
		if fresh[0].Metadata["zone"] != "a" || fresh[0].Endpoints[0] != "http://127.0.0.1:9000" {
			t.Fatal("returned snapshot aliases cached data")
		}

		for _, next := range [][]Instance{{discoveryNode("node-2")}, nil, {discoveryNode("node-3")}} {
			instances = next
			r.api.requestRefresh()
			synctest.Wait()
			got, err := w.Next()
			var ids []string
			for _, instance := range next {
				ids = append(ids, instance.InstanceID)
			}
			requireDiscoveryIDs(t, got, err, ids...)
			cached, err := r.GetService(context.Background(), "game")
			requireDiscoveryIDs(t, cached, err, ids...)
		}

		svc := &registry.ServiceInstance{
			ID: "registered", Name: "game", Version: "v1",
			Endpoints: []string{"http://127.0.0.1:9001", "http://127.0.0.1:9002"}, Metadata: map[string]string{"zone": "original"},
		}
		endpoints := r.Endpoints(svc)
		if !reflect.DeepEqual(svc.Metadata, map[string]string{"zone": "original"}) {
			t.Fatalf("Endpoints mutated caller metadata: %#v", svc.Metadata)
		}
		for i := range endpoints {
			if endpoints[i].MetaData["Endpoints"] != svc.Endpoints[i] {
				t.Fatal("endpoint metadata was overwritten by another endpoint")
			}
		}
		endpoints[0].MetaData["zone"] = "changed"
		if endpoints[1].MetaData["zone"] != "original" || svc.Metadata["zone"] != "original" {
			t.Fatal("endpoint metadata maps are shared")
		}
	})
}

func TestEurekaDiscoveryEmptyAndFailureRecovery(t *testing.T) {
	for _, initiallyFailed := range []bool{false, true} {
		t.Run(fmt.Sprintf("initial failure=%v", initiallyFailed), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				status := http.StatusOK
				if initiallyFailed {
					status = http.StatusServiceUnavailable
				}
				var instances []Instance
				r, _ := discoveryRegistry(t, func(*http.Request) (*http.Response, error) {
					return discoveryResponse(instances, status), nil
				})
				synctest.Wait()
				w := discoveryWatch(t, r, context.Background())
				synctest.Wait()
				got, err := w.Next()
				if initiallyFailed {
					if err == nil || !strings.Contains(err.Error(), "503") {
						t.Fatalf("initial refresh error lost: %v", err)
					}
				} else {
					requireDiscoveryIDs(t, got, err)
				}
				status, instances = http.StatusOK, []Instance{discoveryNode("node-1")}
				r.api.requestRefresh()
				synctest.Wait()
				got, err = w.Next()
				requireDiscoveryIDs(t, got, err, "node-1")
				status = http.StatusServiceUnavailable
				r.api.requestRefresh()
				synctest.Wait()
				got, err = r.GetService(context.Background(), "game")
				requireDiscoveryIDs(t, got, err, "node-1")
				status, instances = http.StatusOK, nil
				r.api.requestRefresh()
				synctest.Wait()
				got, err = w.Next()
				requireDiscoveryIDs(t, got, err)
			})
		})
	}
}

func TestEurekaDiscoveryIndependentWatchers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		instances := []Instance{discoveryNode("first")}
		r, cancelRegistry := discoveryRegistry(t, func(*http.Request) (*http.Response, error) {
			return discoveryResponse(instances, http.StatusOK), nil
		})
		synctest.Wait()
		ctx, cancelWatcher := context.WithCancel(context.Background())
		first := discoveryWatch(t, r, ctx)
		second := discoveryWatch(t, r, context.Background())
		slow := discoveryWatch(t, r, context.Background())
		synctest.Wait()
		for _, w := range []registry.Watcher{first, second} {
			got, err := w.Next()
			requireDiscoveryIDs(t, got, err, "first")
		}
		cancelWatcher()
		synctest.Wait()
		if _, err := first.Next(); !errors.Is(err, context.Canceled) {
			t.Fatalf("parent cancellation = %v", err)
		}
		var stops sync.WaitGroup
		for range 8 {
			stops.Go(func() {
				if err := first.Stop(); err != nil {
					t.Error(err)
				}
			})
		}
		stops.Wait()
		for i := 0; i < 8; i++ {
			instances = []Instance{discoveryNode(fmt.Sprint(i))}
			r.api.requestRefresh()
			synctest.Wait()
			got, err := second.Next()
			requireDiscoveryIDs(t, got, err, fmt.Sprint(i))
		}
		got, err := slow.Next()
		requireDiscoveryIDs(t, got, err, "7")
		cancelRegistry()
		synctest.Wait()
		if _, err := second.Next(); !errors.Is(err, context.Canceled) {
			t.Fatalf("registry cancellation = %v", err)
		}
		if w, err := r.Watch(ctx, "game"); w != nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Watch = %v, %v; want nil and cancellation", w, err)
		}
	})
}

func TestEurekaDiscoveryRefreshSerializationAndCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{}, 4)
		replies := make(chan []Instance)
		var calls atomic.Int32
		var canceled atomic.Bool
		r, cancel := discoveryRegistry(t, func(req *http.Request) (*http.Response, error) {
			calls.Add(1)
			entered <- struct{}{}
			select {
			case instances := <-replies:
				return discoveryResponse(instances, http.StatusOK), nil
			case <-req.Context().Done():
				canceled.Store(true)
				return nil, req.Context().Err()
			}
		})
		<-entered
		lookupCtx, cancelLookup := context.WithCancel(context.Background())
		lookupResult := make(chan error, 1)
		go func() {
			_, err := r.GetService(lookupCtx, "game")
			lookupResult <- err
		}()
		synctest.Wait()
		select {
		case err := <-lookupResult:
			t.Fatalf("lookup returned before the initial refresh completed: %v", err)
		default:
		}
		cancelLookup()
		if err := <-lookupResult; !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting lookup cancellation = %v", err)
		}
		if canceled.Load() || calls.Load() != 1 {
			t.Fatal("canceling one lookup interrupted or duplicated the shared refresh")
		}
		for range 8 {
			r.api.requestRefresh()
		}
		synctest.Wait()
		if calls.Load() != 1 {
			t.Fatal("refresh queries overlapped")
		}
		replies <- []Instance{discoveryNode("old")}
		<-entered
		got, err := r.GetService(context.Background(), "game")
		requireDiscoveryIDs(t, got, err, "old")
		replies <- []Instance{discoveryNode("new")}
		synctest.Wait()
		got, err = r.GetService(context.Background(), "game")
		requireDiscoveryIDs(t, got, err, "new")
		if calls.Load() != 2 {
			t.Fatalf("coalesced refresh made %d requests, want 2", calls.Load())
		}
		r.api.requestRefresh()
		<-entered
		cancel()
		synctest.Wait()
		if !canceled.Load() {
			t.Fatal("registry cancellation did not cancel the in-flight HTTP request")
		}
	})
}

func TestEurekaDiscoveryConcurrentReadersAndSubscriptions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, _ := discoveryRegistry(t, func(*http.Request) (*http.Response, error) {
			return discoveryResponse([]Instance{discoveryNode("node-1")}, http.StatusOK), nil
		})
		synctest.Wait()
		var workers sync.WaitGroup
		for range 32 {
			workers.Go(func() {
				w, err := r.Watch(context.Background(), "game")
				if err != nil {
					t.Error(err)
					return
				}
				defer w.Stop()
				got, err := w.Next()
				if err != nil || len(got) != 1 {
					t.Errorf("watch snapshot = %v, %v", got, err)
					return
				}
				got[0].Metadata["zone"] = "private"
				for range 4 {
					r.api.requestRefresh()
					got, err := r.GetService(context.Background(), "game")
					if err != nil || len(got) != 1 || got[0].Metadata["zone"] != "a" {
						t.Errorf("shared snapshot was changed: %v, %v", got, err)
						return
					}
					got[0].Metadata["zone"] = "private"
				}
			})
		}
		workers.Wait()
	})
}
