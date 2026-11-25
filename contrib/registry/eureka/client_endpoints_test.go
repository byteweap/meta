package eureka

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/byteweap/meta/component/registry"
)

func TestClientQueriesAndChangesInstanceStatus(t *testing.T) {
	client := NewClient([]string{"http://eureka.test"})
	var calls []string
	client.client.Transport = clientTestTransport(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.Method+" "+req.URL.RequestURI())
		switch req.URL.Path {
		case "/eureka/v2/apps/GAME":
			return clientTestResponse(http.StatusOK, `{"name":"GAME","instance":[{"instanceId":"one","status":"UP"},{"instanceId":"two","status":"DOWN"}]}`), nil
		case "/eureka/v2/apps/GAME/one", "/eureka/v2/instances/one":
			return clientTestResponse(http.StatusOK, `{"instanceId":"one","status":"UP"}`), nil
		case "/eureka/v2/apps/GAME/one/status":
			return clientTestResponse(http.StatusNoContent, ""), nil
		default:
			return clientTestResponse(http.StatusNotFound, ""), nil
		}
	})
	ctx := t.Context()
	instances, err := client.FetchAppUpInstances(ctx, "GAME")
	if err != nil || len(instances) != 1 || instances[0].Status != statusUp {
		t.Fatalf("FetchAppUpInstances returned %v, %v", instances, err)
	}
	if result, err := client.FetchAppInstance(ctx, "GAME", "one"); err != nil || result.Status != statusUp {
		t.Fatalf("FetchAppInstance returned %v, %v", result, err)
	}
	if result, err := client.FetchInstance(ctx, "one"); err != nil || result.Status != statusUp {
		t.Fatalf("FetchInstance returned %v, %v", result, err)
	}
	if err := client.Out(ctx, "GAME", "one"); err != nil {
		t.Fatal(err)
	}
	if err := client.Down(ctx, "GAME", "one"); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 5 ||
		calls[3] != "PUT /eureka/v2/apps/GAME/one/status?value=OUT_OF_SERVICE" ||
		calls[4] != "PUT /eureka/v2/apps/GAME/one/status?value=DOWN" {
		t.Fatalf("instance operations requested %v", calls)
	}
	if result, err := client.FetchAppUpInstances(ctx, "ABSENT"); result != nil || err == nil {
		t.Fatalf("missing application returned %v, %v", result, err)
	}
}

type brokenBody struct{}

func (brokenBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestClientRejectsInvalidRequestsAndResponseBodies(t *testing.T) {
	if err := NewClient(nil).Down(t.Context(), "GAME", "one"); err == nil {
		t.Fatal("no Eureka servers accepted")
	}
	if client, err := New(nil); client != nil || err == nil {
		t.Fatalf("New(nil) returned %v, %v", client, err)
	}
	if client, err := New([]string{"http://eureka.test"}, WithRefresh(0)); client != nil || err == nil {
		t.Fatalf("New with zero refresh returned %v, %v", client, err)
	}
	if err := NewClient([]string{":invalid"}).Down(t.Context(), "GAME", "one"); err == nil {
		t.Fatal("invalid server URL accepted")
	}

	client := NewClient([]string{"http://eureka.test"})
	if err := client.do(t.Context(), http.MethodPut, nil, brokenBody{}, nil); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("request body error = %v", err)
	}
	for _, body := range []io.Reader{brokenBody{}, strings.NewReader("{")} {
		client.client.Transport = clientTestTransport(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(body)}, nil
		})
		if _, err := client.FetchApps(t.Context()); err == nil {
			t.Fatal("invalid response body accepted")
		}
	}
	if err := NewClient([]string{"http://eureka.test"}, WithHeartbeatInterval(-time.Second)).
		Register(t.Context(), Endpoint{AppID: "GAME", InstanceID: "one"}); err == nil {
		t.Fatal("invalid heartbeat accepted")
	}
}

func TestRegistryEndpointMetadataOverrides(t *testing.T) {
	r := &Registry{}
	metadata := map[string]string{
		"securePort": "8443", "homePageURL": "/home", "statusPageURL": "/status", "healthCheckURL": "/probe",
	}
	endpoints := r.Endpoints(&registry.ServiceInstance{Name: "game", ID: "one", Endpoints: []string{"http://127.0.0.1:9000"}, Metadata: metadata})
	if r.ID() != "eureka" || len(endpoints) != 1 {
		t.Fatalf("unexpected registry ID or endpoint: %v", endpoints)
	}
	got := endpoints[0]
	if got.SecurePort != 8443 || got.HomePageURL != "/home" || got.StatusPageURL != "/status" || got.HealthCheckURL != "/probe" {
		t.Fatalf("endpoint metadata not applied: %+v", got)
	}
}

func TestRegistryLifecycleUsesConfiguredNamespace(t *testing.T) {
	var registered, removed atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.Method + " " + req.URL.Path {
		case "GET /fixture/apps":
			_, _ = io.WriteString(w, `{"application":[]}`)
		case "POST /fixture/apps/GAME":
			var request struct {
				Instance Instance `json:"instance"`
			}
			if err := json.NewDecoder(req.Body).Decode(&request); err != nil || request.Instance.Metadata["ID"] != "app" {
				t.Errorf("unexpected registration body: %+v, %v", request, err)
				http.Error(w, "invalid instance", http.StatusBadRequest)
				return
			}
			registered.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case "DELETE /fixture/apps/GAME/127.0.0.1:GAME:9100":
			if removed.Add(1) == 1 {
				http.Error(w, "registration still present", http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request %s %s", req.Method, req.URL)
			http.NotFound(w, req)
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r, err := New([]string{server.URL}, WithContext(ctx), WithHeartbeat(time.Hour), WithRefresh(time.Hour), WithEurekaPath("fixture"))
	if err != nil {
		t.Fatal(err)
	}
	svc := &registry.ServiceInstance{ID: "app", Name: "game", Version: "v1", Endpoints: []string{"http://127.0.0.1:9100"}}
	if err := r.Register(ctx, svc); err != nil {
		t.Fatal(err)
	}
	if err := r.Deregister(ctx, svc); err == nil {
		t.Fatal("deregistration lost backend error")
	}
	if err := r.Deregister(ctx, svc); err != nil {
		t.Fatal(err)
	}
	if registered.Load() != 1 || removed.Load() != 2 {
		t.Fatalf("expected one registration and two deregistration attempts, got %d and %d", registered.Load(), removed.Load())
	}
}
