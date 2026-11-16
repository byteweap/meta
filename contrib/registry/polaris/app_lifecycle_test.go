package polaris

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"testing"

	"github.com/byteweap/meta"
	"github.com/polarismesh/polaris-go/api"
	"github.com/polarismesh/polaris-go/pkg/model"
)

func TestPolarisAppFailedRegistrationPreservesExistingInstances(t *testing.T) {
	registerErr := errors.New("registration failed")
	rollbackErr := errors.New("rollback failed")
	for _, canceled := range []bool{false, true} {
		for _, cleanupFails := range []bool{false, true} {
			t.Run(fmt.Sprintf("canceled=%v/cleanupFails=%v", canceled, cleanupFails), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				remote := map[int]bool{9000: true}
				var registered, removed []int
				provider := &fakePolarisProvider{
					register: func(req *api.InstanceRegisterRequest, _ bool) (*model.InstanceRegisterResponse, error) {
						registered = append(registered, req.Port)
						if req.Port == 9002 {
							return nil, registerErr
						}
						existed := remote[req.Port]
						remote[req.Port] = true
						if req.Port == 9001 && canceled {
							cancel()
						}
						return &model.InstanceRegisterResponse{InstanceID: fmt.Sprint(req.Port), Existed: existed}, nil
					},
					deregister: func(req *api.InstanceDeRegisterRequest) error {
						removed = append(removed, req.Port)
						if req.Port == 9001 && cleanupFails {
							return rollbackErr
						}
						delete(remote, req.Port)
						return nil
					},
				}
				app := newPolarisTestApp(ctx, provider, []int{9000, 9001, 9002})
				err := app.Run()
				cause := registerErr
				wantRegistered := []int{9000, 9001, 9002}
				if canceled {
					cause = context.Canceled
					wantRegistered = []int{9000, 9001}
				}
				if !errors.Is(err, cause) || errors.Is(err, rollbackErr) != cleanupFails {
					t.Errorf("registration and rollback errors must survive App cleanup: %v", err)
				}
				if !reflect.DeepEqual(registered, wantRegistered) {
					t.Errorf("registered %v, want %v", registered, wantRegistered)
				}
				if !reflect.DeepEqual(removed, []int{9001}) {
					t.Errorf("only the newly created instance should be rolled back once: removed %v", removed)
				}
				wantRemote := map[int]bool{9000: true}
				if cleanupFails {
					wantRemote[9001] = true
				}
				if !reflect.DeepEqual(remote, wantRemote) {
					t.Errorf("failed startup changed pre-existing registrations: got %v, want %v", remote, wantRemote)
				}
			})
		}
	}
}

func TestPolarisAppSuccessfulRegistrationIsDeregistered(t *testing.T) {
	postStartErr := errors.New("post-start failed")
	for _, postStartFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("postStartFails=%v", postStartFails), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			remote := map[int]bool{9000: true}
			var registered, removed []int
			provider := &fakePolarisProvider{
				register: func(req *api.InstanceRegisterRequest, _ bool) (*model.InstanceRegisterResponse, error) {
					registered = append(registered, req.Port)
					existed := remote[req.Port]
					remote[req.Port] = true
					return &model.InstanceRegisterResponse{InstanceID: fmt.Sprint(req.Port), Existed: existed}, nil
				},
				deregister: func(req *api.InstanceDeRegisterRequest) error {
					removed = append(removed, req.Port)
					delete(remote, req.Port)
					return nil
				},
			}
			app := newPolarisTestApp(ctx, provider, []int{9000, 9001}, meta.PostStart(func(context.Context) error {
				if postStartFails {
					return postStartErr
				}
				cancel()
				return nil
			}))
			err := app.Run()
			if postStartFails && !errors.Is(err, postStartErr) || !postStartFails && err != nil {
				t.Errorf("unexpected App result: %v", err)
			}
			if !reflect.DeepEqual(registered, []int{9000, 9001}) || !reflect.DeepEqual(removed, registered) || len(remote) != 0 {
				t.Errorf("successful registration must be fully removed: registered=%v removed=%v remote=%v", registered, removed, remote)
			}
		})
	}
}

func newPolarisTestApp(ctx context.Context, provider *fakePolarisProvider, ports []int, opts ...meta.Option) *meta.App {
	endpoints := make([]*url.URL, 0, len(ports))
	for _, port := range ports {
		endpoints = append(endpoints, &url.URL{Scheme: "tcp", Host: fmt.Sprintf("127.0.0.1:%d", port)})
	}
	return meta.New(append([]meta.Option{
		meta.Context(ctx), meta.Signal(), meta.ID("app-under-test"), meta.Name("game"),
		meta.Endpoint(endpoints...), meta.Registry(NewRegistry(provider, nil)),
	}, opts...)...)
}
