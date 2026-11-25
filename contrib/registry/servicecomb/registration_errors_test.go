package servicecomb

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/go-chassis/cari/discovery"
	"github.com/go-chassis/cari/pkg/errsvc"
	"github.com/go-chassis/sc-client"

	"github.com/byteweap/meta/component/registry"
)

type failureClient struct {
	mockClient
	serviceErr, idErr, instanceErr, removalErr error
	lookupCalls, instanceCalls                 int
}

func (c *failureClient) RegisterService(*discovery.MicroService) (string, error) {
	return "new-service", c.serviceErr
}

func (c *failureClient) GetMicroServiceID(string, string, string, string, ...sc.CallOption) (string, error) {
	c.lookupCalls++
	return "existing-service", c.idErr
}

func (c *failureClient) RegisterMicroServiceInstance(instance *discovery.MicroServiceInstance) (string, error) {
	c.instanceCalls++
	c.instance = instance
	return instance.InstanceId, c.instanceErr
}

func (c *failureClient) UnregisterMicroServiceInstance(string, string) (bool, error) {
	return c.removalErr == nil, c.removalErr
}

func TestRegistrationHandlesExistingServiceAndPreservesErrors(t *testing.T) {
	failure := errors.New("backend failed")
	conflictJSON, err := json.Marshal(errsvc.Error{Code: discovery.ErrServiceAlreadyExists})
	if err != nil {
		t.Fatal(err)
	}
	conflict := &sc.RegistryException{Message: string(conflictJSON)}
	wrongCode := &sc.RegistryException{Message: `{"code":"unexpected"}`}

	for _, tt := range []struct {
		name                  string
		client                *failureClient
		want                  error
		expectJSONSyntaxError bool
		wantLookup, wantCalls int
	}{
		{"transport", &failureClient{serviceErr: failure}, failure, false, 0, 0},
		{"invalid JSON", &failureClient{serviceErr: &sc.RegistryException{Message: "{"}}, nil, true, 0, 0},
		{"different service error", &failureClient{serviceErr: wrongCode}, wrongCode, false, 0, 0},
		{"lookup error", &failureClient{serviceErr: conflict, idErr: failure}, failure, false, 1, 0},
		{"instance error", &failureClient{serviceErr: conflict, instanceErr: failure}, failure, false, 1, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRegistry(tt.client)
			svc := &registry.ServiceInstance{Name: "game", Version: "v1"}
			err := r.Register(t.Context(), svc)
			if tt.expectJSONSyntaxError {
				var syntax *json.SyntaxError
				if !errors.As(err, &syntax) {
					t.Fatalf("want JSON syntax error, got %v", err)
				}
			} else if !errors.Is(err, tt.want) {
				t.Fatalf("Register returned %v, want %v", err, tt.want)
			}
			if tt.client.lookupCalls != tt.wantLookup || tt.client.instanceCalls != tt.wantCalls {
				t.Fatalf("lookup=%d registration=%d", tt.client.lookupCalls, tt.client.instanceCalls)
			}
			if tt.wantCalls > 0 && (svc.ID == "" || tt.client.instance.InstanceId != svc.ID || tt.client.instance.ServiceId != "existing-service") {
				t.Fatalf("generated or registered identity is invalid: %v, %v", svc, tt.client.instance)
			}
		})
	}
}

func TestDeregisterPropagatesLookupAndRemovalErrors(t *testing.T) {
	failure := errors.New("backend failed")
	svc := &registry.ServiceInstance{Name: "game", ID: "instance"}
	for _, client := range []*failureClient{{idErr: failure}, {removalErr: failure}} {
		r := NewRegistry(client)
		if r.ID() != "servicecomb" || !errors.Is(r.Deregister(t.Context(), svc), failure) {
			t.Fatal("deregistration lost registry identity or backend failure")
		}
	}
}
