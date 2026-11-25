package polaris

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/polarismesh/polaris-go/api"
	"github.com/polarismesh/polaris-go/pkg/model"

	"github.com/byteweap/meta/component/registry"
)

type fakePolarisProvider struct {
	api.ProviderAPI
	register   func(*api.InstanceRegisterRequest, bool) (*model.InstanceRegisterResponse, error)
	deregister func(*api.InstanceDeRegisterRequest) error
}

func (p *fakePolarisProvider) RegisterInstance(req *api.InstanceRegisterRequest) (*model.InstanceRegisterResponse, error) {
	return p.register(req, true)
}

func (p *fakePolarisProvider) Register(req *api.InstanceRegisterRequest) (*model.InstanceRegisterResponse, error) {
	return p.register(req, false)
}

func (p *fakePolarisProvider) Deregister(req *api.InstanceDeRegisterRequest) error {
	return p.deregister(req)
}

type fakePolarisConsumer struct {
	api.ConsumerAPI
	getAll func(*api.GetAllInstancesRequest) (*model.InstancesResponse, error)
	watch  func(*api.WatchServiceRequest) (*model.WatchServiceResponse, error)
}

func (c *fakePolarisConsumer) GetAllInstances(req *api.GetAllInstancesRequest) (*model.InstancesResponse, error) {
	return c.getAll(req)
}

func (c *fakePolarisConsumer) WatchService(req *api.WatchServiceRequest) (*model.WatchServiceResponse, error) {
	return c.watch(req)
}

type polarisTestInstance struct {
	model.Instance
	id, service, host string
	port              uint32
	metadata          map[string]string
	healthy, isolated bool
	weight            int
}

func newPolarisTestInstance(id, appID string) *polarisTestInstance {
	return &polarisTestInstance{
		id: id, service: "game", host: "127.0.0.1", port: 9000,
		metadata: map[string]string{"id": appID, "kind": "tcp", "version": "v1"},
		healthy:  true, weight: 100,
	}
}

func (i *polarisTestInstance) GetId() string                  { return i.id }
func (i *polarisTestInstance) GetService() string             { return i.service }
func (i *polarisTestInstance) GetHost() string                { return i.host }
func (i *polarisTestInstance) GetPort() uint32                { return i.port }
func (i *polarisTestInstance) GetMetadata() map[string]string { return i.metadata }
func (i *polarisTestInstance) IsHealthy() bool                { return i.healthy }
func (i *polarisTestInstance) IsIsolated() bool               { return i.isolated }
func (i *polarisTestInstance) GetWeight() int                 { return i.weight }

func polarisTestService() *registry.ServiceInstance {
	return &registry.ServiceInstance{
		ID: "app-with-hyphens", Name: "game", Version: "v1",
		Metadata:  map[string]string{"zone": "east", "id": "reserved", "kind": "reserved", "version": "reserved"},
		Endpoints: []string{"tcp://127.0.0.1:9000", "grpc://[::1]:9001"},
	}
}

func TestPolarisRegistrationIdentityAndDefaults(t *testing.T) {
	service := polarisTestService()
	before := polarisTestService()
	var registrations []*api.InstanceRegisterRequest
	var removals []*api.InstanceDeRegisterRequest
	provider := &fakePolarisProvider{
		register: func(req *api.InstanceRegisterRequest, managed bool) (*model.InstanceRegisterResponse, error) {
			if !managed {
				t.Error("default registration must use SDK managed heartbeat")
			}
			if err := req.Validate(); err != nil {
				t.Fatalf("request rejected by SDK validation: %v", err)
			}
			if req.Service != "game" || req.Namespace != "default" || req.InstanceId != "" {
				t.Errorf("unexpected registration identity: %+v", req)
			}
			if req.Weight == nil || *req.Weight != 100 || req.TTL != nil || req.Timeout != nil || req.RetryCount != nil {
				t.Errorf("unexpected defaults: %+v", req)
			}
			if req.Metadata["id"] != service.ID || req.Metadata["version"] != "v1" || req.Metadata["zone"] != "east" {
				t.Errorf("unexpected metadata: %v", req.Metadata)
			}
			registrations = append(registrations, req)
			// The SDK applies its default only after validation and retains this request for heartbeats.
			req.SetDefaultTTL()
			return &model.InstanceRegisterResponse{InstanceID: fmt.Sprintf("sdk-id-%d", len(registrations))}, nil
		},
		deregister: func(req *api.InstanceDeRegisterRequest) error {
			if err := req.Validate(); err != nil {
				t.Fatalf("invalid deregistration: %v", err)
			}
			if req.InstanceID != "" || req.Service != "game" || req.Namespace != "default" {
				t.Errorf("deregistration must use the original service/address tuple: %+v", req)
			}
			removals = append(removals, req)
			return nil
		},
	}
	r := NewRegistry(provider, nil)
	if err := r.Register(context.Background(), service); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(service, before) {
		t.Fatalf("registration changed caller data: got %+v, want %+v", service, before)
	}
	if len(registrations) != 2 || registrations[0].Metadata["kind"] != "tcp" || registrations[1].Metadata["kind"] != "grpc" {
		t.Fatalf("incorrect endpoint metadata: %+v", registrations)
	}
	registrations[0].Metadata["zone"] = "changed"
	*registrations[0].Version = "changed"
	if registrations[1].Metadata["zone"] != "east" || *registrations[1].Version != "v1" || service.Version != "v1" || service.Metadata["zone"] != "east" {
		t.Fatal("registration requests share mutable caller or endpoint data")
	}
	if err := r.Deregister(context.Background(), service); err != nil {
		t.Fatal(err)
	}
	if len(removals) != 2 || removals[0].Host != "127.0.0.1" || removals[0].Port != 9000 || removals[1].Host != "::1" || removals[1].Port != 9001 {
		t.Fatalf("unexpected deregistration addresses: %+v", removals)
	}
}

func TestPolarisHeartbeatAndOptionalConfiguration(t *testing.T) {
	for _, managed := range []bool{true, false} {
		t.Run(fmt.Sprint(managed), func(t *testing.T) {
			calls := 0
			provider := &fakePolarisProvider{register: func(req *api.InstanceRegisterRequest, gotManaged bool) (*model.InstanceRegisterResponse, error) {
				calls++
				if gotManaged != managed || req.Timeout == nil || *req.Timeout != time.Second || req.RetryCount == nil || *req.RetryCount != 0 {
					t.Errorf("optional configuration not preserved: managed=%v request=%+v", gotManaged, req)
				}
				if managed && (req.TTL == nil || *req.TTL != 7) || !managed && req.TTL != nil {
					t.Errorf("unexpected TTL with managed=%v: %v", managed, req.TTL)
				}
				if req.Protocol == nil || *req.Protocol != "grpc" || req.ServiceToken != "token" || req.Namespace != "test" {
					t.Errorf("configured scope not preserved: %+v", req)
				}
				return &model.InstanceRegisterResponse{InstanceID: "sdk-instance"}, nil
			}}
			r := NewRegistry(provider, nil, WithHeartbeat(managed), WithTTL(7), WithTimeout(time.Second), WithRetryCount(0), WithProtocol("grpc"), WithNamespace("test"), WithServiceToken("token"))
			service := polarisTestService()
			service.Metadata = nil
			if err := r.Register(context.Background(), service); err != nil || calls != 2 {
				t.Fatalf("register calls=%d, err=%v", calls, err)
			}
			if service.Metadata != nil {
				t.Fatal("registration populated caller metadata")
			}
		})
	}
}

func TestPolarisValidatesBeforeSideEffects(t *testing.T) {
	cases := []struct {
		name   string
		change func(*registry.ServiceInstance)
		opts   []Option
	}{
		{name: "empty ID", change: func(s *registry.ServiceInstance) { s.ID = "" }},
		{name: "empty name", change: func(s *registry.ServiceInstance) { s.Name = "" }},
		{name: "no endpoints", change: func(s *registry.ServiceInstance) { s.Endpoints = nil }},
		{name: "missing scheme", change: func(s *registry.ServiceInstance) { s.Endpoints[1] = "127.0.0.1:9001" }},
		{name: "missing host", change: func(s *registry.ServiceInstance) { s.Endpoints[1] = "tcp://:9001" }},
		{name: "missing port", change: func(s *registry.ServiceInstance) { s.Endpoints[1] = "tcp://localhost" }},
		{name: "zero port", change: func(s *registry.ServiceInstance) { s.Endpoints[1] = "tcp://localhost:0" }},
		{name: "large port", change: func(s *registry.ServiceInstance) { s.Endpoints[1] = "tcp://localhost:65536" }},
		{name: "credentials", change: func(s *registry.ServiceInstance) { s.Endpoints[1] = "tcp://user@localhost:9001" }},
		{name: "path", change: func(s *registry.ServiceInstance) { s.Endpoints[1] = "tcp://localhost:9001/path" }},
		{name: "query", change: func(s *registry.ServiceInstance) { s.Endpoints[1] = "tcp://localhost:9001?q=x" }},
		{name: "fragment", change: func(s *registry.ServiceInstance) { s.Endpoints[1] = "tcp://localhost:9001#fragment" }},
		{name: "duplicate address", change: func(s *registry.ServiceInstance) { s.Endpoints[1] = "grpc://127.0.0.1:9000" }},
		{name: "empty namespace", opts: []Option{WithNamespace("")}},
		{name: "negative weight", opts: []Option{WithWeight(-1)}},
		{name: "large weight", opts: []Option{WithWeight(10001)}},
		{name: "zero TTL", opts: []Option{WithTTL(0)}},
		{name: "negative TTL", opts: []Option{WithTTL(-1)}},
		{name: "zero timeout", opts: []Option{WithTimeout(0)}},
		{name: "negative timeout", opts: []Option{WithTimeout(-time.Second)}},
		{name: "negative retries", opts: []Option{WithRetryCount(-1)}},
		{name: "empty metadata key", change: func(s *registry.ServiceInstance) { s.Metadata[""] = "value" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			provider := &fakePolarisProvider{register: func(*api.InstanceRegisterRequest, bool) (*model.InstanceRegisterResponse, error) {
				calls++
				return &model.InstanceRegisterResponse{}, nil
			}}
			service := polarisTestService()
			if tc.change != nil {
				tc.change(service)
			}
			if err := NewRegistry(provider, nil, tc.opts...).Register(context.Background(), service); err == nil || calls != 0 {
				t.Fatalf("invalid input must fail before any registration: calls=%d, err=%v", calls, err)
			}
		})
	}
	r := NewRegistry(&fakePolarisProvider{}, nil)
	if err := r.Register(context.Background(), nil); err == nil {
		t.Fatal("nil service accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Register(ctx, polarisTestService()); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled registration: %v", err)
	}
}

func TestPolarisRegistrationRollback(t *testing.T) {
	registerErr := errors.New("registration failed")
	cleanupErr := errors.New("cleanup failed")
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("canceled=%v", canceled), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			service := polarisTestService()
			service.Endpoints = append(service.Endpoints, "tcp://127.0.0.1:9002")
			calls := 0
			var removed []int
			provider := &fakePolarisProvider{
				register: func(req *api.InstanceRegisterRequest, managed bool) (*model.InstanceRegisterResponse, error) {
					calls++
					if calls == 3 {
						return nil, registerErr
					}
					if calls == 2 && canceled {
						cancel()
					}
					return &model.InstanceRegisterResponse{InstanceID: fmt.Sprint(req.Port), Existed: calls == 1}, nil
				},
				deregister: func(req *api.InstanceDeRegisterRequest) error {
					removed = append(removed, req.Port)
					if req.Namespace != "default" || req.Service != "game" || req.Host != "::1" {
						t.Errorf("rollback omitted SDK task key: %+v", req)
					}
					return cleanupErr
				},
			}
			err := NewRegistry(provider, nil).Register(ctx, service)
			cause := registerErr
			wantCalls := 3
			if canceled {
				cause = context.Canceled
				wantCalls = 2
			}
			if !errors.Is(err, cause) || !errors.Is(err, cleanupErr) || calls != wantCalls || !reflect.DeepEqual(removed, []int{9001}) {
				t.Fatalf("rollback must retain existing instance and both errors: calls=%d removed=%v err=%v", calls, removed, err)
			}
			if service.ID != "app-with-hyphens" {
				t.Fatalf("failed registration changed business ID: %s", service.ID)
			}
		})
	}
}

func TestPolarisDeregisterValidationAndFailures(t *testing.T) {
	firstErr, secondErr := errors.New("first endpoint failed"), errors.New("second endpoint failed")
	calls := 0
	provider := &fakePolarisProvider{deregister: func(req *api.InstanceDeRegisterRequest) error {
		calls++
		if calls == 1 {
			return firstErr
		}
		return secondErr
	}}
	r := NewRegistry(provider, nil)
	invalid := polarisTestService()
	invalid.Endpoints[1] = "tcp://localhost:0"
	if err := r.Deregister(context.Background(), invalid); err == nil || calls != 0 {
		t.Fatalf("invalid endpoint caused partial removal: calls=%d err=%v", calls, err)
	}
	if err := r.Deregister(context.Background(), polarisTestService()); !errors.Is(err, firstErr) || !errors.Is(err, secondErr) || calls != 2 {
		t.Fatalf("must attempt all removals and retain failures: calls=%d err=%v", calls, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Deregister(ctx, polarisTestService()); !errors.Is(err, context.Canceled) || calls != 2 {
		t.Fatalf("pre-canceled removal must have no effect: calls=%d err=%v", calls, err)
	}
}

func TestPolarisDiscoveryIdentityFilteringAndIsolation(t *testing.T) {
	good := newPolarisTestInstance("sdk-1", "application-1")
	good.host, good.port = "::1", 9001
	unhealthy := newPolarisTestInstance("sdk-2", "application-2")
	unhealthy.healthy = false
	isolated := newPolarisTestInstance("sdk-3", "application-3")
	isolated.isolated = true
	zeroWeight := newPolarisTestInstance("sdk-4", "application-4")
	zeroWeight.weight = 0
	legacy := newPolarisTestInstance("sdk-5", "")
	noKind := newPolarisTestInstance("sdk-6", "application-6")
	delete(noKind.metadata, "kind")
	response := &model.InstancesResponse{Instances: []model.Instance{good, unhealthy, isolated, zeroWeight, legacy, noKind}}
	calls := 0
	consumer := &fakePolarisConsumer{getAll: func(req *api.GetAllInstancesRequest) (*model.InstancesResponse, error) {
		calls++
		if req.Service != "game" || req.Namespace != "test" || req.Timeout != nil || req.RetryCount != nil {
			t.Errorf("unexpected discovery request: %+v", req)
		}
		return response, nil
	}}
	r := NewRegistry(nil, consumer, WithNamespace("test"))
	instances, err := r.GetService(context.Background(), "game")
	if err != nil || len(instances) != 1 {
		t.Fatalf("discovery returned %v, %v", instances, err)
	}
	if instances[0].ID != "application-1" || instances[0].Name != "game" || instances[0].Version != "v1" || !reflect.DeepEqual(instances[0].Endpoints, []string{"tcp://[::1]:9001"}) {
		t.Fatalf("discovery changed business identity: %+v", instances[0])
	}
	instances[0].Metadata["id"] = "changed"
	instances[0].Endpoints[0] = "changed"
	again, err := r.GetService(context.Background(), "game")
	if err != nil || again[0].ID != "application-1" || again[0].Metadata["id"] != "application-1" || again[0].Endpoints[0] != "tcp://[::1]:9001" {
		t.Fatalf("caller mutation leaked into discovery: %v %v", again, err)
	}
	response.Instances = nil
	if empty, err := r.GetService(context.Background(), "game"); err != nil || len(empty) != 0 {
		t.Fatalf("empty snapshot: %v %v", empty, err)
	}
	queryErr := errors.New("discovery unavailable")
	consumer.getAll = func(*api.GetAllInstancesRequest) (*model.InstancesResponse, error) { return nil, queryErr }
	if _, err := r.GetService(context.Background(), "game"); !errors.Is(err, queryErr) {
		t.Fatalf("lost query error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.GetService(ctx, "game"); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled query: %v", err)
	}
	if calls != 3 {
		t.Fatalf("unexpected discovery calls: %d", calls)
	}
}

func TestPolarisConcurrentRequestsDoNotShareOptions(t *testing.T) {
	var registrations, removals, queries atomic.Int32
	mutateControl := func(timeout *time.Duration, retries *int) {
		if timeout == nil || retries == nil {
			t.Error("explicit control parameters missing")
			return
		}
		if *timeout != time.Second || *retries != 2 {
			t.Errorf("SDK mutation leaked into a later request: timeout=%v retries=%v", *timeout, *retries)
		}
		*timeout, *retries = time.Minute, 9
	}
	provider := &fakePolarisProvider{
		register: func(req *api.InstanceRegisterRequest, managed bool) (*model.InstanceRegisterResponse, error) {
			registrations.Add(1)
			mutateControl(req.Timeout, req.RetryCount)
			if *req.TTL != 7 || *req.Protocol != "tcp" || *req.Weight != 50 || *req.Version != "v1" {
				t.Errorf("registration options were shared: %+v", req)
			}
			*req.TTL, *req.Protocol, *req.Weight, *req.Version = 20, "changed", 1, "changed"
			req.Metadata["zone"] = "changed"
			return &model.InstanceRegisterResponse{InstanceID: "sdk-id"}, nil
		},
		deregister: func(req *api.InstanceDeRegisterRequest) error {
			removals.Add(1)
			mutateControl(req.Timeout, req.RetryCount)
			return nil
		},
	}
	consumer := &fakePolarisConsumer{getAll: func(req *api.GetAllInstancesRequest) (*model.InstancesResponse, error) {
		queries.Add(1)
		mutateControl(req.Timeout, req.RetryCount)
		return &model.InstancesResponse{}, nil
	}}
	r := NewRegistry(provider, consumer, WithTTL(7), WithProtocol("tcp"), WithWeight(50), WithTimeout(time.Second), WithRetryCount(2))
	service := polarisTestService()
	metadata := maps.Clone(service.Metadata)
	var wg sync.WaitGroup
	for range 24 {
		wg.Go(func() {
			if err := r.Register(context.Background(), service); err != nil {
				t.Error(err)
			}
			if _, err := r.GetService(context.Background(), "game"); err != nil {
				t.Error(err)
			}
			if err := r.Deregister(context.Background(), service); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if registrations.Load() != 48 || removals.Load() != 48 || queries.Load() != 24 || !maps.Equal(metadata, service.Metadata) || service.Version != "v1" {
		t.Fatalf("concurrent operations changed input or lost requests: registrations=%d removals=%d queries=%d service=%+v", registrations.Load(), removals.Load(), queries.Load(), service)
	}
}

func TestPolarisLookupAndRemovalFailures(t *testing.T) {
	failure := errors.New("SDK unavailable")
	consumer := &fakePolarisConsumer{getAll: func(*api.GetAllInstancesRequest) (*model.InstancesResponse, error) {
		return nil, failure
	}}
	r := NewRegistry(nil, consumer)
	if r.ID() != "polaris" {
		t.Fatalf("unexpected registry ID: %q", r.ID())
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, check := range []struct {
		reg  *Registry
		ctx  context.Context
		name string
		want error
	}{
		{r, canceled, "game", context.Canceled},
		{r, t.Context(), "", nil},
		{r, t.Context(), "game", failure},
		{NewRegistry(nil, consumer, WithNamespace("")), t.Context(), "game", nil},
	} {
		instances, err := check.reg.GetService(check.ctx, check.name)
		if instances != nil || err == nil || (check.want != nil && !errors.Is(err, check.want)) {
			t.Fatalf("GetService(%q) = %v, %v, want failure %v", check.name, instances, err, check.want)
		}
	}
	if err := NewRegistry(nil, nil, WithNamespace("")).Deregister(t.Context(), polarisTestService()); err == nil {
		t.Fatal("Deregister accepted an empty namespace")
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	provider := &fakePolarisProvider{deregister: func(*api.InstanceDeRegisterRequest) error {
		calls++
		cancel()
		return failure
	}}
	if err := NewRegistry(provider, consumer).Deregister(ctx, polarisTestService()); calls != 1 ||
		!errors.Is(err, context.Canceled) || !errors.Is(err, failure) {
		t.Fatalf("Deregister returned %v after %d calls", err, calls)
	}
}

func TestPolarisRejectsEmptyRegistrationAndAppliesHealthOptions(t *testing.T) {
	provider := &fakePolarisProvider{register: func(req *api.InstanceRegisterRequest, _ bool) (*model.InstanceRegisterResponse, error) {
		if req.Healthy == nil || *req.Healthy || req.Isolate == nil || !*req.Isolate {
			t.Fatalf("unexpected health options: %+v", req)
		}
		return nil, nil
	}}
	err := NewRegistry(provider, nil, WithHealthy(false), WithIsolate(true)).Register(t.Context(), polarisTestService())
	if err == nil || !strings.Contains(err.Error(), "empty registration response") {
		t.Fatalf("Register returned %v", err)
	}
}

func TestPolarisRejectsInvalidTimeoutAndDropsLateLookupResponse(t *testing.T) {
	r := NewRegistry(nil, nil, WithTimeout(0))
	if _, err := r.GetService(t.Context(), "game"); err == nil {
		t.Fatal("GetService accepted a zero timeout")
	}
	if err := r.Deregister(t.Context(), polarisTestService()); err == nil {
		t.Fatal("Deregister accepted a zero timeout")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	consumer := &fakePolarisConsumer{getAll: func(*api.GetAllInstancesRequest) (*model.InstancesResponse, error) {
		cancel()
		return &model.InstancesResponse{Instances: []model.Instance{newPolarisTestInstance("sdk", "app")}}, nil
	}}
	if instances, err := NewRegistry(nil, consumer).GetService(ctx, "game"); instances != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("late GetService response = %v, %v", instances, err)
	}
}
