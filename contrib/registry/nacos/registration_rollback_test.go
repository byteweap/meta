package nacos

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/nacos-group/nacos-sdk-go/vo"

	"github.com/byteweap/meta/component/registry"
)

func TestNacosRegistrationValidatesBeforeSideEffects(t *testing.T) {
	for _, invalid := range []string{"grpc://127.0.0.1", "grpc://127.0.0.1:invalid", "grpc://%"} {
		t.Run(invalid, func(t *testing.T) {
			cli := &identityClient{}
			err := New(cli).Register(context.Background(), &registry.ServiceInstance{
				ID: "app-1", Name: "game", Endpoints: []string{"grpc://127.0.0.1:9000", invalid},
			})
			if err == nil || len(cli.registered) != 0 || len(cli.deregistered) != 0 {
				t.Fatalf("invalid endpoint must fail before registration: err=%v registered=%v deregistered=%v", err, cli.registered, cli.deregistered)
			}
		})
	}
}

func TestNacosRegistrationRollback(t *testing.T) {
	registrationErr := errors.New("registration failed")
	cleanupErrors := []error{errors.New("first rollback failed"), errors.New("second rollback failed")}
	for _, scenario := range []string{"registration failure", "cleanup failure", "canceled between endpoints"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cli := &identityClient{
				register: func(param vo.RegisterInstanceParam) (bool, error) {
					if param.Port == 9002 {
						return false, registrationErr
					}
					if param.Port == 9001 && scenario == "canceled between endpoints" {
						cancel()
					}
					return true, nil
				},
				deregister: func(param vo.DeregisterInstanceParam) (bool, error) {
					if scenario == "cleanup failure" {
						return false, cleanupErrors[param.Port-9000]
					}
					return true, nil
				},
			}
			service := &registry.ServiceInstance{
				ID: "app-1", Name: "game",
				Endpoints: []string{"grpc://127.0.0.1:9000", "grpc://127.0.0.1:9001", "grpc://127.0.0.1:9002", "grpc://127.0.0.1:9003"},
			}
			err := New(cli, WithGroup("team"), WithCluster("blue")).Register(ctx, service)
			cause, wantCalls := registrationErr, 3
			if scenario == "canceled between endpoints" {
				cause, wantCalls = context.Canceled, 2
			}
			if !errors.Is(err, cause) || len(cli.registered) != wantCalls {
				t.Fatalf("registration = %v, SDK calls=%d; want %v and %d calls", err, len(cli.registered), cause, wantCalls)
			}
			wantRemoved := []vo.DeregisterInstanceParam{
				{Ip: "127.0.0.1", Port: 9000, ServiceName: "game", GroupName: "team", Cluster: "blue", Ephemeral: true},
				{Ip: "127.0.0.1", Port: 9001, ServiceName: "game", GroupName: "team", Cluster: "blue", Ephemeral: true},
			}
			if !reflect.DeepEqual(cli.deregistered, wantRemoved) {
				t.Fatalf("rollback must remove only confirmed registrations: got %+v, want %+v", cli.deregistered, wantRemoved)
			}
			if scenario == "cleanup failure" {
				for _, cleanupErr := range cleanupErrors {
					if !errors.Is(err, cleanupErr) {
						t.Fatalf("rollback lost error %v: %v", cleanupErr, err)
					}
				}
			}
		})
	}
}

func TestNacosRegistrationCancellationBoundaries(t *testing.T) {
	for _, canceledBefore := range []bool{false, true} {
		t.Run(map[bool]string{false: "successful final request", true: "before registration"}[canceledBefore], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cli := &identityClient{register: func(vo.RegisterInstanceParam) (bool, error) {
				cancel()
				return true, nil
			}}
			if canceledBefore {
				cancel()
			}
			err := New(cli).Register(ctx, &registry.ServiceInstance{
				ID: "app-1", Name: "game", Endpoints: []string{"grpc://127.0.0.1:9000"},
			})
			if canceledBefore {
				if !errors.Is(err, context.Canceled) || len(cli.registered) != 0 {
					t.Fatalf("pre-canceled registration = %v, SDK calls=%d", err, len(cli.registered))
				}
			} else if err != nil || len(cli.registered) != 1 {
				t.Fatalf("confirmed registration must succeed so its caller can deregister: err=%v, SDK calls=%d", err, len(cli.registered))
			}
			if len(cli.deregistered) != 0 {
				t.Fatalf("unexpected rollback: %+v", cli.deregistered)
			}
		})
	}
}
