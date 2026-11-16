package eureka

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestEurekaRegistrationRollback(t *testing.T) {
	registerErr := errors.New("register failed")
	cleanupErr := errors.New("cleanup failed")
	for _, mode := range []string{"failure", "canceled", "cleanup failure", "cleanup timeout"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				lifetime, stop := context.WithCancel(context.Background())
				defer stop()
				ctx, cancel := context.WithCancel(lifetime)
				defer cancel()
				client := NewClient([]string{"http://eureka.test"}, WithClientContext(lifetime), WithMaxRetry(1), WithHeartbeatInterval(time.Hour))
				var registered, deleted []string
				client.client.Transport = clientTestTransport(func(req *http.Request) (*http.Response, error) {
					switch req.Method {
					case http.MethodPost:
						var body RequestInstance
						if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
							return nil, err
						}
						id := body.Instance.InstanceID
						registered = append(registered, id)
						if id == "failed" {
							if mode == "canceled" {
								cancel()
							}
							return nil, registerErr
						}
					case http.MethodDelete:
						id := req.URL.Path[strings.LastIndex(req.URL.Path, "/")+1:]
						deleted = append(deleted, id)
						if req.Context().Err() != nil {
							t.Error("rollback inherited registration cancellation")
						}
						if _, ok := req.Context().Deadline(); !ok {
							t.Error("rollback has no deadline")
						}
						if id == "one" {
							if mode == "cleanup failure" {
								return nil, cleanupErr
							}
							if mode == "cleanup timeout" {
								<-req.Context().Done()
								return nil, req.Context().Err()
							}
						}
					default:
						t.Errorf("unexpected request: %s", req.Method)
					}
					return clientTestResponse(http.StatusOK, ""), nil
				})
				api := &API{cli: client}
				err := api.Register(ctx,
					Endpoint{AppID: "GAME", InstanceID: "one"},
					Endpoint{AppID: "GAME", InstanceID: "two"},
					Endpoint{AppID: "GAME", InstanceID: "failed"},
					Endpoint{AppID: "GAME", InstanceID: "untouched"},
				)
				cause := registerErr
				if mode == "canceled" {
					cause = context.Canceled
				}
				if !errors.Is(err, cause) {
					t.Errorf("registration cause lost: %v", err)
				}
				if mode == "cleanup failure" && !errors.Is(err, cleanupErr) {
					t.Errorf("cleanup cause lost: %v", err)
				}
				if mode == "cleanup timeout" && !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("cleanup deadline lost: %v", err)
				}
				if !reflect.DeepEqual(registered, []string{"one", "two", "failed"}) || !reflect.DeepEqual(deleted, []string{"one", "two"}) {
					t.Errorf("incorrect rollback scope: registered=%v deleted=%v", registered, deleted)
				}
				synctest.Wait()
				client.lock.Lock()
				defer client.lock.Unlock()
				if len(client.registrations) != 0 {
					t.Errorf("failed registration retained heartbeat state: %d", len(client.registrations))
				}
			})
		})
	}
}

func TestEurekaSuccessfulRegistrationSurvivesLateCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lifetime, stop := context.WithCancel(context.Background())
		defer stop()
		ctx, cancel := context.WithCancel(lifetime)
		defer cancel()
		client := NewClient([]string{"http://eureka.test"}, WithClientContext(lifetime), WithHeartbeatInterval(time.Hour))
		var methods []string
		client.client.Transport = clientTestTransport(func(req *http.Request) (*http.Response, error) {
			methods = append(methods, req.Method)
			if req.Method == http.MethodPost {
				cancel() // The server has accepted the request before cancellation is observed.
			}
			return clientTestResponse(http.StatusOK, ""), nil
		})
		ep := Endpoint{AppID: "GAME", InstanceID: "one"}
		if err := client.Register(ctx, ep); err != nil {
			t.Fatalf("confirmed registration must succeed so its caller can deregister: %v", err)
		}
		if err := client.Deregister(context.Background(), ep.AppID, ep.InstanceID); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(methods, []string{http.MethodPost, http.MethodDelete}) {
			t.Fatalf("successful registration cleanup = %v", methods)
		}
		synctest.Wait()
		client.lock.Lock()
		defer client.lock.Unlock()
		if len(client.registrations) != 0 {
			t.Fatalf("completed registration retained heartbeat state: %d", len(client.registrations))
		}
	})
}
