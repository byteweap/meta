package consul

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"

	"github.com/byteweap/meta/component/registry"
)

type snapshotTransport func(*http.Request) (*http.Response, error)

func (f snapshotTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newSnapshotRegistry(t *testing.T, transport snapshotTransport, opts ...Option) *Registry {
	t.Helper()
	cli, err := api.NewClient(&api.Config{
		Address:    "http://consul.test",
		HttpClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatal(err)
	}
	return New(cli, opts...)
}

func snapshotResponse(index, body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"X-Consul-Index": []string{index}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func snapshotContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func snapshotWatch(t *testing.T, ctx context.Context, r *Registry) registry.Watcher {
	t.Helper()
	w, err := r.Watch(ctx, "game")
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

func requireSnapshot(t *testing.T, services []*registry.ServiceInstance, err error, id string) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		if len(services) != 0 {
			t.Fatalf("want empty snapshot, got %#v", services)
		}
		return
	}
	if len(services) != 1 || services[0].ID != id {
		t.Fatalf("want node %q, got %#v", id, services)
	}
}

func TestConsulEmptySnapshotInitialAndCached(t *testing.T) {
	for _, nilResolver := range []bool{false, true} {
		name := "empty slice"
		if nilResolver {
			name = "typed nil"
		}
		t.Run(name, func(t *testing.T) {
			ctx := snapshotContext(t)
			var initialQueries atomic.Int32
			r := newSnapshotRegistry(t, func(req *http.Request) (*http.Response, error) {
				if req.URL.Query().Get("index") != "" {
					<-req.Context().Done()
					return nil, req.Context().Err()
				}
				initialQueries.Add(1)
				return snapshotResponse("1", "[]"), nil
			})
			if nilResolver {
				r.cli.resolver = func(context.Context, []*api.ServiceEntry) []*registry.ServiceInstance {
					return nil
				}
			}

			first := snapshotWatch(t, ctx, r)
			services, err := first.Next()
			requireSnapshot(t, services, err, "")
			services, err = r.GetService(ctx, "game")
			requireSnapshot(t, services, err, "")
			second := snapshotWatch(t, ctx, r)
			services, err = second.Next()
			requireSnapshot(t, services, err, "")
			if got := initialQueries.Load(); got != 1 {
				t.Fatalf("cached empty snapshot caused %d initial queries, want 1", got)
			}
			all, err := r.ListServices()
			if err != nil {
				t.Fatal(err)
			}
			if services, ok := all["game"]; !ok || len(services) != 0 {
				t.Fatalf("ListServices omitted or changed cached empty snapshot: %#v", all)
			}
		})
	}
}

func TestConsulEmptySnapshotRemoteLookup(t *testing.T) {
	queryErr := errors.New("Consul query failed")
	for _, uninitialized := range []bool{false, true} {
		name := "without watcher"
		if uninitialized {
			name = "uninitialized cache"
		}
		t.Run(name, func(t *testing.T) {
			for _, result := range []string{"empty", "transport error", "HTTP error", "canceled"} {
				t.Run(result, func(t *testing.T) {
					ctx := snapshotContext(t)
					if result == "canceled" {
						canceled, cancel := context.WithCancel(ctx)
						cancel()
						ctx = canceled
					}
					r := newSnapshotRegistry(t, func(req *http.Request) (*http.Response, error) {
						if err := req.Context().Err(); err != nil {
							return nil, err
						}
						switch result {
						case "transport error":
							return nil, queryErr
						case "HTTP error":
							response := snapshotResponse("1", "Consul unavailable")
							response.StatusCode = http.StatusServiceUnavailable
							return response, nil
						default:
							return snapshotResponse("1", "[]"), nil
						}
					})
					if uninitialized {
						r.registry["game"] = &serviceSet{services: &atomic.Value{}}
					}
					services, err := r.GetService(ctx, "game")
					switch result {
					case "empty":
						requireSnapshot(t, services, err, "")
					case "transport error":
						if !errors.Is(err, queryErr) {
							t.Fatalf("query error lost: %v", err)
						}
					case "HTTP error":
						if err == nil || !strings.Contains(err.Error(), "503") || !strings.Contains(err.Error(), "Consul unavailable") {
							t.Fatalf("HTTP query error lost: %v", err)
						}
					case "canceled":
						if !errors.Is(err, context.Canceled) {
							t.Fatalf("cancellation lost: %v", err)
						}
					}
				})
			}
		})
	}
}

func TestConsulEmptySnapshotRemovalRecoveryAndQueryFailure(t *testing.T) {
	ctx := snapshotContext(t)
	requests := make(chan string)
	type reply struct {
		index string
		body  string
		err   error
	}
	replies := make(chan reply)
	r := newSnapshotRegistry(t, func(req *http.Request) (*http.Response, error) {
		index := req.URL.Query().Get("index")
		if index == "" {
			return snapshotResponse("1", `[{"Service":{"ID":"node-1","Service":"game"}}]`), nil
		}
		select {
		case requests <- index:
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
		select {
		case result := <-replies:
			if result.err != nil {
				return nil, result.err
			}
			return snapshotResponse(result.index, result.body), nil
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	})
	w := snapshotWatch(t, ctx, r)
	services, err := w.Next()
	requireSnapshot(t, services, err, "node-1")

	awaitQuery := func(want string) {
		t.Helper()
		select {
		case index := <-requests:
			if index != want {
				t.Fatalf("query index = %q, want %q", index, want)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	respond := func(result reply) {
		t.Helper()
		select {
		case replies <- result:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}

	awaitQuery("1")
	respond(reply{err: errors.New("temporary Consul failure")})
	awaitQuery("1") // A failed query must retain both the index and the last snapshot.
	services, err = r.GetService(ctx, "game")
	requireSnapshot(t, services, err, "node-1")
	select {
	case <-w.(*watcher).event:
		t.Fatal("query failure published a snapshot")
	default:
	}

	respond(reply{index: "2", body: "[]"})
	services, err = w.Next()
	requireSnapshot(t, services, err, "")
	services, err = r.GetService(ctx, "game")
	requireSnapshot(t, services, err, "")

	awaitQuery("2")
	respond(reply{index: "3", body: `[{"Service":{"ID":"node-2","Service":"game"}}]`})
	services, err = w.Next()
	requireSnapshot(t, services, err, "node-2")
	services, err = r.GetService(ctx, "game")
	requireSnapshot(t, services, err, "node-2")
}
