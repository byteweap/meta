package eureka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

type clientTestTransport func(*http.Request) (*http.Response, error)

func (f clientTestTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func clientTestResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

func TestEurekaClientConcurrentRequestsAndRetryBody(t *testing.T) {
	urls := []string{"http://one.test", "http://two.test", "http://three.test"}
	wantURLs := append([]string(nil), urls...)
	client := NewClient(urls, WithMaxRetry(5))
	urls[0] = "http://changed.test"
	queryErr := errors.New("temporary transport failure")
	var mu sync.Mutex
	attempts := make(map[string][]string)
	client.client.Transport = clientTestTransport(func(req *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		defer mu.Unlock()
		key := string(body)
		attempts[key] = append(attempts[key], req.URL.Host)
		if key == "fail" || len(attempts[key]) == 1 {
			return nil, queryErr
		}
		return clientTestResponse(http.StatusOK, ""), nil
	})
	var requests sync.WaitGroup
	for i := range 16 {
		requests.Go(func() {
			body := fmt.Sprintf("payload-%d", i)
			if err := client.do(context.Background(), http.MethodPost, []string{"apps", "GAME"}, bytes.NewBufferString(body), nil); err != nil {
				t.Error(err)
			}
		})
	}
	requests.Wait()
	if err := client.do(context.Background(), http.MethodPost, []string{"apps"}, bytes.NewBufferString("fail"), nil); !errors.Is(err, queryErr) {
		t.Fatalf("transport error lost: %v", err)
	}
	if !reflect.DeepEqual(client.urls, wantURLs) {
		t.Fatalf("shared URL list changed: %v", client.urls)
	}
	if len(attempts) != 17 || len(attempts["fail"]) != 5 {
		t.Fatalf("request bodies or attempt limit changed: %v", attempts)
	}
	for body, hosts := range attempts {
		if body != "fail" && (len(hosts) != 2 || hosts[0] == hosts[1]) {
			t.Errorf("request %q retry hosts = %v", body, hosts)
		}
	}
}

func TestEurekaClientIndependentHeartbeats(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client := NewClient([]string{"http://eureka.test"}, WithClientContext(ctx), WithHeartbeatInterval(time.Second))
		var mu sync.Mutex
		beats := make(map[string]int)
		client.client.Transport = clientTestTransport(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPut {
				mu.Lock()
				beats[req.URL.Path]++
				mu.Unlock()
			}
			return clientTestResponse(http.StatusOK, ""), nil
		})
		first := Endpoint{AppID: "GAME", InstanceID: "one"}
		second := Endpoint{AppID: "GAME", InstanceID: "two"}
		for _, ep := range []Endpoint{first, second} {
			if err := client.Register(ctx, ep); err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		canceled, stop := context.WithCancel(ctx)
		stop()
		for range 16 {
			if err := client.Register(canceled, first); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if err := client.Deregister(canceled, first.AppID, first.InstanceID); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		}
		time.Sleep(time.Second)
		synctest.Wait()
		mu.Lock()
		if beats["/eureka/v2/apps/GAME/one"] != 1 || beats["/eureka/v2/apps/GAME/two"] != 1 {
			t.Errorf("canceled operations changed active heartbeats: %v", beats)
		}
		mu.Unlock()
		var stops sync.WaitGroup
		for range 8 {
			stops.Go(func() {
				if err := client.Deregister(ctx, first.AppID, first.InstanceID); err != nil {
					t.Error(err)
				}
			})
		}
		stops.Wait()
		time.Sleep(time.Second)
		synctest.Wait()
		mu.Lock()
		if beats["/eureka/v2/apps/GAME/one"] != 1 || beats["/eureka/v2/apps/GAME/two"] != 2 {
			t.Errorf("instance cancellation crossed heartbeat ownership: %v", beats)
		}
		mu.Unlock()
		cancel()
		synctest.Wait()
		client.lock.Lock()
		defer client.lock.Unlock()
		if len(client.registrations) != 0 {
			t.Fatalf("heartbeat records retained: %d", len(client.registrations))
		}
	})
}

func TestEurekaClientDeregisterOrdersReregistration(t *testing.T) {
	for _, cancelDelete := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel delete=%v", cancelDelete), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				client := NewClient([]string{"http://eureka.test"}, WithClientContext(ctx), WithHeartbeatInterval(time.Second))
				entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var mu sync.Mutex
				var sequence []string
				posts := 0
				client.client.Transport = clientTestTransport(func(req *http.Request) (*http.Response, error) {
					mu.Lock()
					sequence = append(sequence, req.Method)
					if req.Method == http.MethodPost {
						posts++
					}
					retrying := req.Method == http.MethodPost && posts == 2
					mu.Unlock()
					if req.Method == http.MethodPut {
						return clientTestResponse(http.StatusServiceUnavailable, ""), nil
					}
					if retrying {
						var body RequestInstance
						if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
							return nil, err
						}
						if body.Instance.Metadata["zone"] != "original" {
							t.Error("heartbeat metadata was changed by caller")
						}
						close(entered)
						<-req.Context().Done()
						close(canceled)
						<-release // Model a request that takes time to finish after cancellation.
						return nil, req.Context().Err()
					}
					return clientTestResponse(http.StatusOK, ""), nil
				})
				ep := Endpoint{AppID: "GAME", InstanceID: "one", MetaData: map[string]string{"zone": "original"}}
				if err := client.Register(ctx, ep); err != nil {
					t.Fatal(err)
				}
				ep.MetaData["zone"] = "caller changed"
				synctest.Wait()
				<-entered // Virtual time advances through four failed heartbeat requests.
				deleteCtx, cancelDeregister := context.WithCancel(ctx)
				defer cancelDeregister()
				deleted := make(chan error, 1)
				go func() { deleted <- client.Deregister(deleteCtx, ep.AppID, ep.InstanceID) }()
				<-canceled
				if cancelDelete {
					cancelDeregister()
					if err := <-deleted; !errors.Is(err, context.Canceled) {
						t.Fatalf("Deregister cancellation = %v", err)
					}
				}
				registered := make(chan error, 1)
				go func() { registered <- client.Register(ctx, ep) }()
				synctest.Wait()
				mu.Lock()
				if posts != 2 {
					t.Error("new registration overtook in-flight heartbeat")
				}
				mu.Unlock()
				close(release)
				if !cancelDelete {
					if err := <-deleted; err != nil {
						t.Fatal(err)
					}
				}
				if err := <-registered; err != nil {
					t.Fatal(err)
				}
				mu.Lock()
				want := []string{http.MethodPost, http.MethodPut, http.MethodPut, http.MethodPut, http.MethodPut, http.MethodPost}
				if !cancelDelete {
					want = append(want, http.MethodDelete)
				}
				want = append(want, http.MethodPost)
				if !reflect.DeepEqual(sequence, want) {
					t.Errorf("lifecycle requests = %v, want %v", sequence, want)
				}
				mu.Unlock()
				client.lock.Lock()
				if len(client.registrations) != 1 {
					t.Errorf("old worker removed replacement: %d", len(client.registrations))
				}
				client.lock.Unlock()
				cancel()
				synctest.Wait()
			})
		})
	}
}

func TestEurekaClientFetchErrorsAndCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewClient([]string{"http://eureka.test"}, WithClientContext(ctx))
	client.client.Transport = clientTestTransport(func(*http.Request) (*http.Response, error) {
		return clientTestResponse(http.StatusServiceUnavailable, ""), nil
	})
	if _, err := client.FetchAllUpInstances(ctx); err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("fetch error lost: %v", err)
	}
	entered := make(chan struct{})
	client.client.Transport = clientTestTransport(func(req *http.Request) (*http.Response, error) {
		close(entered)
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	done := make(chan error, 1)
	go func() { _, err := client.FetchAllUpInstances(ctx); done <- err }()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("registry cancellation lost: %v", err)
	}
}

func TestEurekaClientDeregisterAfterRegistryCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client := NewClient([]string{"http://eureka.test"}, WithClientContext(ctx), WithHeartbeatInterval(time.Second))
		var methods []string
		client.client.Transport = clientTestTransport(func(req *http.Request) (*http.Response, error) {
			methods = append(methods, req.Method)
			return clientTestResponse(http.StatusOK, ""), nil
		})
		ep := Endpoint{AppID: "GAME", InstanceID: "one"}
		if err := client.Register(ctx, ep); err != nil {
			t.Fatal(err)
		}
		cancel()
		synctest.Wait()
		if err := client.Deregister(context.Background(), ep.AppID, ep.InstanceID); err != nil {
			t.Fatal(err)
		}
		if err := client.Register(context.Background(), ep); !errors.Is(err, context.Canceled) {
			t.Fatalf("stopped registry accepted registration: %v", err)
		}
		if !reflect.DeepEqual(methods, []string{http.MethodPost, http.MethodDelete}) {
			t.Fatalf("cleanup requests = %v", methods)
		}
	})
}
