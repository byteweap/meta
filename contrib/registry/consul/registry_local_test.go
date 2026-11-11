package consul

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"

	"github.com/byteweap/meta/component/registry"
)

func TestRegistry_ShareServiceSet(t *testing.T) {
	var mu sync.Mutex
	lastIndex := uint64(0)
	serviceName := "share-service-set"
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/health/service/"+serviceName, func(w http.ResponseWriter, r *http.Request) {
		var index uint64
		if s := r.URL.Query().Get("index"); s != "" {
			val, err := strconv.ParseUint(s, 10, 64)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			index = val
		}

		mu.Lock()
		defer mu.Unlock()
		if index < lastIndex {
			msg := "repeated request, not the same ServiceSet"
			http.Error(w, msg, http.StatusBadRequest)
			t.Error(msg)
			return
		}

		lastIndex = index + 1
		w.Header().Set("X-Consul-Index", strconv.FormatUint(lastIndex, 10))

		out := []*api.ServiceEntry{
			{
				Service: &api.AgentService{
					ID:      "1",
					Service: serviceName,
				},
			},
		}
		err := json.NewEncoder(w).Encode(out)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli, err := api.NewClient(&api.Config{Address: ts.URL, WaitTime: 2 * time.Second})
	if err != nil {
		t.Fatalf("create consul client failed: %v", err)
	}

	var prev registry.Watcher
	r := New(cli, WithHealthCheck(false), WithHeartbeat(false))
	for i := 0; i < 100; i++ {
		w, err := r.Watch(context.Background(), serviceName) //nolint
		if err != nil {
			t.Error(err)
			return
		}
		// 关闭上一个 watcher
		if prev != nil {
			if err = prev.Stop(); err != nil {
				t.Error(err)
				return
			}
		}
		prev = w
	}

	time.Sleep(time.Second * 5)

	if prev != nil {
		if err = prev.Stop(); err != nil {
			t.Error(err)
			return
		}
	}
}
