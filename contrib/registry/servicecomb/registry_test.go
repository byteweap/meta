package servicecomb

import (
	"context"
	"maps"
	"testing"

	pb "github.com/go-chassis/cari/discovery"
	"github.com/go-chassis/sc-client"
	"github.com/gofrs/uuid"

	"github.com/byteweap/meta/component/registry"
)

type mockClient struct{ instance *pb.MicroServiceInstance }

// nolint
func (receiver *mockClient) FindMicroServiceInstances(_,
	_, microServiceName, _ string, _ ...sc.CallOption,
) ([]*pb.MicroServiceInstance, error) {
	if microServiceName == "MetaServicecomb" {
		return []*pb.MicroServiceInstance{receiver.instance}, nil
	}
	return nil, nil
}

func (receiver *mockClient) RegisterService(_ *pb.MicroService) (string, error) {
	return "", nil
}

func (receiver *mockClient) RegisterMicroServiceInstance(instance *pb.MicroServiceInstance) (string, error) {
	receiver.instance = instance
	return "", nil
}

func (receiver *mockClient) Heartbeat(_, _ string) (bool, error) {
	return true, nil
}

func (receiver *mockClient) UnregisterMicroServiceInstance(_, _ string) (bool, error) {
	return true, nil
}

func (receiver *mockClient) GetMicroServiceID(_, _, _, _ string, _ ...sc.CallOption) (string, error) {
	return "", nil
}

func TestRegistry(t *testing.T) {
	r := NewRegistry(&mockClient{})
	instanceID, err := uuid.NewV4()
	if err != nil {
		t.Fatal(err)
	}
	metadata := map[string]string{
		"app": "meta", registry.MetadataStateless: "true", appIDKey: "caller-app", envKey: "caller-env",
	}
	svc := &registry.ServiceInstance{
		Name:      "MetaServicecomb",
		Version:   "0.0.1",
		Metadata:  maps.Clone(metadata),
		Endpoints: []string{"tcp://127.0.0.1:9000?isSecure=false"},
		ID:        instanceID.String(),
	}
	ctx := context.TODO()
	t.Run("Register test, expected: success.", func(t *testing.T) {
		err = r.Register(ctx, svc)
		if err != nil {
			t.Error(err)
		}
		if !maps.Equal(svc.Metadata, metadata) {
			t.Errorf("registration mutated caller metadata: %v", svc.Metadata)
		}
	})
	t.Run("GetService test, expected: success.", func(t *testing.T) {
		var insts []*registry.ServiceInstance
		insts, err = r.GetService(ctx, svc.Name)
		if err != nil {
			t.Fatal(err)
		}
		if len(insts) != 1 {
			t.Fatalf("got %d instances, want 1", len(insts))
		}
		want := maps.Clone(metadata)
		want[appIDKey], want[envKey] = appID, env
		if !maps.Equal(insts[0].Metadata, want) {
			t.Errorf("discovered metadata = %v, want %v", insts[0].Metadata, want)
		}
	})
	t.Run("Deregister test, expected: success.", func(t *testing.T) {
		svc.ID = instanceID.String()
		err = r.Deregister(ctx, svc)
		if err != nil {
			t.Error(err)
		}
	})
}
