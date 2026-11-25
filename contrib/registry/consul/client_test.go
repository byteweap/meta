package consul

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hashicorp/consul/api"

	"github.com/byteweap/meta/component/registry"
)

type testTransport func(*http.Request) (*http.Response, error)

func (f testTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func consulResponse(code int, body, index string) *http.Response {
	return &http.Response{StatusCode: code, Header: http.Header{"X-Consul-Index": {index}}, Body: io.NopCloser(strings.NewReader(body))}
}

func consulClientForTest(t *testing.T, transport testTransport) *api.Client {
	t.Helper()
	client, err := api.NewClient(&api.Config{Address: "consul.test", HttpClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestMultiDatacenterLookup(t *testing.T) {
	for _, fail := range []string{"", "catalog", "dc2"} {
		t.Run(fail, func(t *testing.T) {
			var centers []string
			client := consulClientForTest(t, func(req *http.Request) (*http.Response, error) {
				if req.URL.Path == "/v1/catalog/datacenters" {
					if fail == "catalog" {
						return consulResponse(500, "unavailable", "0"), nil
					}
					return consulResponse(200, `["dc1","dc2"]`, "0"), nil
				}
				if req.URL.Path != "/v1/health/service/game" || !req.URL.Query().Has("passing") {
					t.Fatalf("unexpected service query: %s", req.URL)
				}
				center := req.URL.Query().Get("dc")
				centers = append(centers, center)
				if center == fail {
					return consulResponse(500, "unavailable", "0"), nil
				}
				index, next := "3", "4"
				if center == "dc2" {
					index, next = "4", "5"
				}
				if got := req.URL.Query().Get("index"); got != index {
					t.Fatalf("query index for %s = %s, want %s", center, got, index)
				}
				return consulResponse(200, `[{"Service":{"ID":"node","Service":"game","Address":"::1","Port":9000,"Tags":["version=v1"]}}]`, next), nil
			})
			r := New(client, WithDatacenter(MultiDatacenter), WithServiceResolver(defaultResolver))
			got, index, err := r.cli.Service(t.Context(), "game", 3, true)
			if fail != "" {
				if err == nil || got != nil || index != 0 {
					t.Fatalf("failed lookup = %v, %d, %v", got, index, err)
				}
				return
			}
			if err != nil || index != 5 || !slices.Equal(centers, []string{"dc1", "dc2"}) || len(got) != 2 {
				t.Fatalf("lookup = %v, %d, %v, datacenters %v", got, index, err, centers)
			}
			for i, instance := range got {
				if instance.Metadata["dc"] != centers[i] || instance.Version != "v1" ||
					!slices.Equal(instance.Endpoints, []string{"http://[::1]:9000"}) {
					t.Fatalf("unexpected instance %+v", instance)
				}
			}
		})
	}
}

func TestRegisterOptionsAndFailures(t *testing.T) {
	check := &api.AgentServiceCheck{HTTP: "http://127.0.0.1/ready", Interval: "1s"}
	var sent api.AgentServiceRegistration
	client := consulClientForTest(t, func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/v1/agent/service/register" {
			return nil, errors.New("unexpected Consul request")
		}
		if err := json.NewDecoder(req.Body).Decode(&sent); err != nil {
			return nil, err
		}
		return consulResponse(500, "registration rejected", "0"), nil
	})
	r := New(client, WithServiceCheck(check), WithTags([]string{"example"}), WithDeregisterCriticalServiceAfter(90), WithHealthCheckInterval(2))
	if r.ID() != "consul" {
		t.Fatalf("unexpected ID %q", r.ID())
	}
	bad := &registry.ServiceInstance{ID: "bad", Endpoints: []string{":invalid"}}
	if err := r.Register(t.Context(), bad); err == nil {
		t.Fatal("invalid endpoint accepted")
	}
	service := &registry.ServiceInstance{ID: "node", Name: "game", Version: "v1", Endpoints: []string{"http://127.0.0.1:9000"}}
	if err := r.Register(t.Context(), service); err == nil {
		t.Fatal("registration failure lost")
	}
	if !slices.Equal(sent.Tags, []string{"version=v1", "example"}) || len(sent.Checks) != 3 ||
		sent.Checks[0].DeregisterCriticalServiceAfter != "90s" || sent.Checks[1].HTTP != check.HTTP || sent.Checks[2].TTL != "4s" {
		t.Fatalf("registration options lost: %+v", sent)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := r.Watch(canceled, "game"); got != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled watch = %v, %v", got, err)
	}
	if got, err := r.Watch(t.Context(), "game"); got != nil || err == nil {
		t.Fatalf("watch initial lookup failure = %v, %v", got, err)
	}
	if services, err := r.ListServices(); err != nil || len(services) != 0 {
		t.Fatalf("uninitialized snapshot appeared in service list: %v, %v", services, err)
	}
}

func TestHeartbeatReregistersOnFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var registrations, updates atomic.Int32
		recovered := make(chan struct{}, 2)
		client := consulClientForTest(t, func(req *http.Request) (*http.Response, error) {
			switch {
			case strings.Contains(req.URL.Path, "/check/update/"):
				updates.Add(1)
				return consulResponse(500, "cannot update TTL", "0"), nil
			case req.URL.Path == "/v1/agent/service/register":
				count := registrations.Add(1)
				if count > 1 {
					recovered <- struct{}{}
				}
				if count == 2 {
					return consulResponse(500, "cannot register", "0"), nil
				}
				return consulResponse(200, "", "0"), nil
			case strings.Contains(req.URL.Path, "/service/deregister/"):
				return consulResponse(200, "", "0"), nil
			default:
				return nil, errors.New("unexpected Consul request: " + req.URL.Path)
			}
		})
		r := New(client, WithHealthCheckInterval(1))
		svc := &registry.ServiceInstance{ID: "node", Name: "game"}
		if err := r.Register(t.Context(), svc); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = r.Deregister(t.Context(), svc) }()
		for range 2 {
			select {
			case <-recovered:
				synctest.Wait()
			case <-time.After(8 * time.Second):
				t.Fatal("heartbeat did not re-register the service")
			}
		}
		if got := updates.Load(); got < 3 {
			t.Fatalf("only %d heartbeat attempts", got)
		}
	})
}
