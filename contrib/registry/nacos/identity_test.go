package nacos

import (
	"context"
	"errors"
	"maps"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/nacos-group/nacos-sdk-go/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/model"
	"github.com/nacos-group/nacos-sdk-go/vo"

	"github.com/byteweap/meta/component/registry"
)

// Return the SDK-owned metadata directly so the tests detect accidental writes
// and shared maps in converted results.
type identityClient struct {
	naming_client.INamingClient
	registered     []vo.RegisterInstanceParam
	deregistered   []vo.DeregisterInstanceParam
	register       func(vo.RegisterInstanceParam) (bool, error)
	deregister     func(vo.DeregisterInstanceParam) (bool, error)
	instances      []model.Instance
	queries        []vo.SelectAllInstancesParam
	subscription   *vo.SubscribeParam
	unsubscription *vo.SubscribeParam
	queryErr       error
	subscribeErr   error
	unsubscribeErr error
}

func (c *identityClient) RegisterInstance(param vo.RegisterInstanceParam) (bool, error) {
	c.registered = append(c.registered, param)
	if c.register != nil {
		return c.register(param)
	}
	return true, nil
}

func (c *identityClient) DeregisterInstance(param vo.DeregisterInstanceParam) (bool, error) {
	c.deregistered = append(c.deregistered, param)
	if c.deregister != nil {
		return c.deregister(param)
	}
	return true, nil
}

func (c *identityClient) SelectAllInstances(param vo.SelectAllInstancesParam) ([]model.Instance, error) {
	c.queries = append(c.queries, param)
	return c.instances, c.queryErr
}

func (c *identityClient) Subscribe(param *vo.SubscribeParam) error {
	c.subscription = param
	return c.subscribeErr
}

func (c *identityClient) Unsubscribe(param *vo.SubscribeParam) error {
	c.unsubscription = param
	return c.unsubscribeErr
}

func identityInstance() model.Instance {
	return model.Instance{
		InstanceId: "127.0.0.1#9000#blue#team@@game", ServiceName: "team@@game",
		Ip: "127.0.0.1", Port: 9000, ClusterName: "blue",
		Healthy: true, Enable: true, Weight: 2.5,
		Metadata: map[string]string{"id": "app-7", "version": "v2", "kind": "ws", "label": "sdk"},
	}
}

func TestNacosIdentityRegistrationRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name     string
		metadata map[string]string
		weight   float64
	}{
		{"nil metadata", nil, 100},
		{"empty metadata", map[string]string{}, 100},
		{"existing metadata", map[string]string{"id": "caller-id", "version": "caller-version", "kind": "caller-kind", "label": "caller", "weight": "2.5"}, 2.5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli := &identityClient{}
			r := New(cli, WithGroup("team"), WithCluster("blue"))
			si := &registry.ServiceInstance{
				ID: "app-7", Name: "game", Version: "v2", Metadata: tc.metadata,
				Endpoints: []string{"ws://127.0.0.1:9000"},
			}
			original := maps.Clone(si.Metadata)
			if err := r.Register(context.Background(), si); err != nil {
				t.Fatal(err)
			}
			if len(cli.registered) != 1 {
				t.Fatalf("registered %d instances; want 1", len(cli.registered))
			}
			param := cli.registered[0]
			if param.Metadata["id"] != si.ID || param.Metadata["kind"] != "ws" || param.Metadata["version"] != si.Version {
				t.Fatalf("registration lost application identity: %v", param.Metadata)
			}
			if param.ServiceName != "game" || param.GroupName != "team" || param.ClusterName != "blue" || param.Weight != tc.weight {
				t.Fatalf("incorrect registration scope or weight: %+v", param)
			}
			if !reflect.DeepEqual(si.Metadata, original) {
				t.Fatalf("Register mutated caller metadata: %v; want %v", si.Metadata, original)
			}
			in := identityInstance()
			in.Metadata, in.Weight = param.Metadata, param.Weight
			cli.instances = []model.Instance{in}
			sdkMetadata := maps.Clone(in.Metadata)
			got, err := r.GetService(context.Background(), "game")
			if err != nil || len(got) != 1 {
				t.Fatalf("GetService = %v, %v", got, err)
			}
			if got[0].ID != si.ID || got[0].Name != si.Name || got[0].Version != si.Version || !reflect.DeepEqual(got[0].Endpoints, si.Endpoints) {
				t.Fatalf("identity did not round trip: %+v", got[0])
			}
			wantWeight := "100"
			if tc.weight == 2.5 {
				wantWeight = "3"
			}
			if got[0].Metadata["weight"] != wantWeight || !reflect.DeepEqual(in.Metadata, sdkMetadata) {
				t.Fatalf("weight conversion mutated SDK metadata: result=%v, SDK=%v", got[0].Metadata, in.Metadata)
			}
			got[0].Metadata["label"] = "consumer"
			param.Metadata["kind"] = "sdk-changed"
			if in.Metadata["label"] == "consumer" || !reflect.DeepEqual(si.Metadata, original) {
				t.Fatal("caller, SDK and result metadata must be independent")
			}
		})
	}
}

func TestNacosIdentityRejectsEmptyRegistrationID(t *testing.T) {
	cli := &identityClient{}
	err := New(cli).Register(context.Background(), &registry.ServiceInstance{Name: "game", Endpoints: []string{"grpc://127.0.0.1:9000"}})
	if !errors.Is(err, ErrServiceInstanceIDEmpty) || len(cli.registered) != 0 {
		t.Fatalf("empty ID registration = %v, SDK calls=%d", err, len(cli.registered))
	}
}

func TestNacosIdentityFiltersAndCopiesResults(t *testing.T) {
	valid := identityInstance()
	valid.Metadata["kind"] = "" // An empty override uses the configured protocol.
	valid.Ip = "::1"
	cli := &identityClient{instances: []model.Instance{valid}}
	for _, invalidate := range []func(*model.Instance){
		func(in *model.Instance) { in.Healthy = false },
		func(in *model.Instance) { in.Enable = false },
		func(in *model.Instance) { in.Weight = 0 },
		func(in *model.Instance) { in.Weight = -1 },
		func(in *model.Instance) { in.Weight = math.NaN() },
		func(in *model.Instance) { in.Weight = math.Inf(1) },
		func(in *model.Instance) { in.Weight = math.Inf(-1) },
		func(in *model.Instance) { in.Metadata = nil },
		func(in *model.Instance) { delete(in.Metadata, "id") },
		func(in *model.Instance) { in.Metadata["id"] = "" },
	} {
		in := valid
		in.Metadata = maps.Clone(valid.Metadata)
		invalidate(&in)
		cli.instances = append(cli.instances, in)
	}
	r := New(cli, WithGroup("team"), WithCluster("blue"), WithDefaultKind("grpc"))
	first, err := r.GetService(context.Background(), "game")
	if err != nil || len(first) != 1 {
		t.Fatalf("GetService returned invalid instances: %v, %v", first, err)
	}
	want := &registry.ServiceInstance{
		ID: "app-7", Name: "game", Version: "v2", Endpoints: []string{"grpc://[::1]:9000"},
		Metadata: map[string]string{"id": "app-7", "version": "v2", "kind": "", "label": "sdk", "weight": "3"},
	}
	if !reflect.DeepEqual(first[0], want) {
		t.Fatalf("conversion = %+v; want %+v", first[0], want)
	}
	second, err := r.GetService(context.Background(), "game")
	if err != nil || len(second) != 1 {
		t.Fatalf("second GetService = %v, %v", second, err)
	}
	first[0].Metadata["label"] = "first-result"
	if second[0].Metadata["label"] != "sdk" || valid.Metadata["label"] != "sdk" {
		t.Fatal("separate queries share result metadata with each other or with the SDK")
	}
	for _, query := range cli.queries {
		if query.ServiceName != "game" || query.GroupName != "team" || !reflect.DeepEqual(query.Clusters, []string{"blue"}) {
			t.Fatalf("incorrect discovery scope: %+v", query)
		}
	}
}

func TestNacosIdentityWatchMatchesQueryAndEmptyUpdates(t *testing.T) {
	cli := &identityClient{instances: []model.Instance{identityInstance()}}
	r := New(cli, WithGroup("team"), WithCluster("blue"))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	w, err := r.Watch(ctx, "game")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Stop(); err != nil {
			t.Error(err)
		}
		if cli.unsubscription != cli.subscription {
			t.Error("Stop must unsubscribe the same subscription")
		}
	}()
	if cli.subscription == nil || cli.subscription.ServiceName != "game" || cli.subscription.GroupName != "team" || !reflect.DeepEqual(cli.subscription.Clusters, []string{"blue"}) {
		t.Fatalf("incorrect watch scope: %+v", cli.subscription)
	}
	for i, instances := range [][]model.Instance{cli.instances, {}, nil} {
		cli.instances = instances
		// Callback payload is a change notification, not an independently converted snapshot.
		if i > 0 {
			cli.subscription.SubscribeCallback(nil, nil)
		}
		queried, queryErr := r.GetService(ctx, "game")
		watched, watchErr := w.Next()
		if queryErr != nil || watchErr != nil || !reflect.DeepEqual(watched, queried) {
			t.Fatalf("query/watch differ: query=%v (%v), watch=%v (%v)", queried, queryErr, watched, watchErr)
		}
		if len(instances) == 0 && len(watched) != 0 {
			t.Fatalf("empty update retained old nodes: %v", watched)
		}
		if len(watched) > 0 {
			watched[0].Metadata["label"] = "watch-consumer"
			if queried[0].Metadata["label"] != "sdk" || cli.instances[0].Metadata["label"] != "sdk" {
				t.Fatal("watch result shares metadata with query or SDK")
			}
		}
	}
	for _, query := range cli.queries {
		if query.ServiceName != "game" || query.GroupName != "team" || !reflect.DeepEqual(query.Clusters, []string{"blue"}) {
			t.Fatalf("query/watch changed scope: %+v", query)
		}
	}
}

func TestNacosIdentityPropagatesSDKErrors(t *testing.T) {
	failure := errors.New("SDK lookup failed")
	cli := &identityClient{queryErr: failure}
	r := New(cli)
	if _, err := r.GetService(context.Background(), "game"); !errors.Is(err, failure) {
		t.Fatalf("GetService lost SDK error: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	w, err := r.Watch(ctx, "game")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Next(); !errors.Is(err, failure) {
		t.Fatalf("Next lost SDK error: %v", err)
	}
	cli.unsubscribeErr = errors.New("SDK unsubscribe failed")
	if err := w.Stop(); !errors.Is(err, cli.unsubscribeErr) {
		t.Fatalf("Stop lost SDK error: %v", err)
	}
	cli.subscribeErr = errors.New("SDK subscribe failed")
	w, err = r.Watch(ctx, "game")
	if !errors.Is(err, cli.subscribeErr) {
		t.Fatalf("Watch lost SDK error: %v", err)
	}
	if w != nil {
		_ = w.Stop()
	}
}

type cancelingQueryClient struct {
	*identityClient
	cancel context.CancelFunc
}

func (c cancelingQueryClient) SelectAllInstances(param vo.SelectAllInstancesParam) ([]model.Instance, error) {
	instances, err := c.identityClient.SelectAllInstances(param)
	c.cancel()
	return instances, err
}

func TestRegistryHandlesInvalidWeightAndCancellation(t *testing.T) {
	cli := &identityClient{}
	r := New(cli, WithWeight(12))
	instance := &registry.ServiceInstance{
		Name: "game", ID: "app", Endpoints: []string{"grpc://127.0.0.1:9000"},
		Metadata: map[string]string{"weight": "invalid"},
	}
	if r.ID() != "nacos" {
		t.Fatalf("registry ID = %q", r.ID())
	}
	if err := r.Register(t.Context(), instance); err != nil {
		t.Fatal(err)
	}
	if len(cli.registered) != 1 || cli.registered[0].Weight != 12 {
		t.Fatalf("registration did not use default weight: %v", cli.registered)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if result, err := r.GetService(ctx, "game"); result != nil || !errors.Is(err, context.Canceled) || len(cli.queries) != 0 {
		t.Fatalf("already canceled lookup returned %v, %v with %d SDK calls", result, err, len(cli.queries))
	}

	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()
	cli.instances = []model.Instance{identityInstance()}
	r = New(cancelingQueryClient{identityClient: cli, cancel: cancel})
	if result, err := r.GetService(ctx, "game"); result != nil || !errors.Is(err, context.Canceled) || len(cli.queries) != 1 {
		t.Fatalf("late canceled lookup returned %v, %v with %d SDK calls", result, err, len(cli.queries))
	}
}

func TestDeregisterPropagatesSDKFailure(t *testing.T) {
	failure := errors.New("deregister rejected")
	cli := &identityClient{deregister: func(vo.DeregisterInstanceParam) (bool, error) { return false, failure }}
	r := New(cli)
	instance := &registry.ServiceInstance{Name: "game", Endpoints: []string{"grpc://127.0.0.1:9000", "grpc://127.0.0.1:9001"}}
	if err := r.Deregister(t.Context(), instance); !errors.Is(err, failure) || len(cli.deregistered) != 1 {
		t.Fatalf("Deregister returned %v after %d SDK calls", err, len(cli.deregistered))
	}
}

func TestDeregisterRejectsMalformedEndpoints(t *testing.T) {
	cli := &identityClient{}
	r := New(cli)
	for _, endpoint := range []string{"grpc://%", "grpc://127.0.0.1", "grpc://127.0.0.1:999999999999999999999999"} {
		instance := &registry.ServiceInstance{Name: "game", Endpoints: []string{endpoint}}
		if err := r.Deregister(t.Context(), instance); err == nil || len(cli.deregistered) != 0 {
			t.Fatalf("Deregister(%q) returned %v with %d SDK calls", endpoint, err, len(cli.deregistered))
		}
		if err := r.Register(t.Context(), &registry.ServiceInstance{Name: "game", ID: "app", Endpoints: []string{endpoint}}); err == nil {
			t.Fatalf("Register(%q) accepted an invalid endpoint", endpoint)
		}
	}
}
