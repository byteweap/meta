package servicecomb

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chassis/sc-client"

	"github.com/byteweap/meta/component/registry"
)

func TestServiceCombSDKCompleteSnapshots(t *testing.T) {
	var queries atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet || !strings.HasSuffix(req.URL.Path, "/registry/instances") {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		queries.Add(1)
		// A revision-based query would return 304 instead of a full snapshot.
		if req.URL.Query().Has("rev") {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch req.URL.Query().Get("serviceName") {
		case "game":
			w.Header().Set(sc.HeaderRevision, "revision-7")
			fmt.Fprint(w, `{"instances":[{"instanceId":"node-1","serviceId":"service-id","version":"2.1.0","properties":{"zone":"blue"},"endpoints":["tcp://127.0.0.1:9000"]},{"instanceId":"node-2","serviceId":"service-id","version":"2.1.0","endpoints":["tcp://127.0.0.1:9001"]}]}`)
		case "missing":
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"errorCode":"400012","errorMessage":"microservice missing"}`)
		case "failure":
			http.Error(w, "service center unavailable", http.StatusServiceUnavailable)
		default:
			http.Error(w, "unexpected service", http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	client, err := sc.NewClient(sc.Options{Endpoints: []string{strings.TrimPrefix(server.URL, "http://")}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	r := NewRegistry(client)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	t.Run("unchanged revision still returns full snapshots", func(t *testing.T) {
		want := []*registry.ServiceInstance{
			{ID: "node-1", Name: "game", Version: "2.1.0", Metadata: map[string]string{"zone": "blue"}, Endpoints: []string{"tcp://127.0.0.1:9000"}},
			{ID: "node-2", Name: "game", Version: "2.1.0", Endpoints: []string{"tcp://127.0.0.1:9001"}},
		}
		for i := 0; i < 2; i++ {
			got, err := r.GetService(ctx, "game")
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("GetService = %#v, %v; want complete snapshot %#v", got, err, want)
			}
		}
		before := queries.Load()
		watch, err := r.Watch(ctx, "game")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = watch.Stop() })
		if got := queries.Load(); got != before {
			t.Fatalf("Watch queried before Next: %d -> %d", before, got)
		}
		for i := 0; i < 2; i++ {
			got, err := watch.Next()
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("Next = %#v, %v; want complete snapshot %#v", got, err, want)
			}
		}
		if got := queries.Load(); got != before+2 {
			t.Fatalf("two Next calls made %d requests, want 2", got-before)
		}
	})

	t.Run("missing service is an empty snapshot", func(t *testing.T) {
		_, err := client.FindMicroServiceInstances("", appID, "missing", "", sc.WithoutRevision())
		if !errors.Is(err, sc.ErrMicroServiceNotExists) {
			t.Fatalf("SDK missing-service error = %v", err)
		}
		got, err := r.GetService(ctx, "missing")
		if err != nil || len(got) != 0 {
			t.Fatalf("GetService missing = %#v, %v; want empty snapshot", got, err)
		}
		watch, err := r.Watch(ctx, "missing")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = watch.Stop() })
		got, err = watch.Next()
		if err != nil || len(got) != 0 {
			t.Fatalf("Next missing = %#v, %v; want empty snapshot", got, err)
		}
	})

	t.Run("HTTP failure remains an error", func(t *testing.T) {
		for _, lookup := range []func() ([]*registry.ServiceInstance, error){
			func() ([]*registry.ServiceInstance, error) { return r.GetService(ctx, "failure") },
			func() ([]*registry.ServiceInstance, error) {
				watch, err := r.Watch(ctx, "failure")
				if err != nil {
					return nil, err
				}
				defer watch.Stop()
				return watch.Next()
			},
		} {
			got, err := lookup()
			if len(got) != 0 || err == nil || !strings.Contains(err.Error(), "503") || !strings.Contains(err.Error(), "service center unavailable") {
				t.Fatalf("lookup failure = %#v, %v; want original HTTP error", got, err)
			}
		}
	})
}
