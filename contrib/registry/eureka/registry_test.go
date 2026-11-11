//go:build integration

package eureka

import (
	"context"
	"testing"
	"time"

	"github.com/byteweap/meta/component/registry"
)

func TestRegistry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r, err := New([]string{"https://127.0.0.1:18761"}, WithContext(ctx), WithHeartbeat(time.Second), WithRefresh(time.Second), WithEurekaPath("eureka"))
	if err != nil {
		t.Fatal(err)
	}
	for _, service := range []*registry.ServiceInstance{
		{ID: "0", Name: "helloworld", Endpoints: []string{"http://127.0.0.1:1111"}},
		{ID: "1", Name: "helloworld2", Endpoints: []string{"http://127.0.0.1:222"}},
	} {
		t.Run(service.Name, func(t *testing.T) {
			watch, err := r.Watch(ctx, service.Name)
			if err != nil {
				t.Fatal(err)
			}
			defer watch.Stop()
			if err := r.Register(ctx, service); err != nil {
				t.Fatal(err)
			}
			defer r.Deregister(ctx, service)
			await := func(present bool) {
				t.Helper()
				for {
					instances, err := watch.Next()
					if err != nil {
						t.Fatal(err)
					}
					if !present && len(instances) == 0 {
						return
					}
					if present && len(instances) == 1 && instances[0].ID == service.ID && instances[0].Name == service.Name {
						return
					}
				}
			}
			await(true)
			if err := r.Deregister(ctx, service); err != nil {
				t.Fatal(err)
			}
			await(false)
			instances, err := r.GetService(ctx, service.Name)
			if err != nil || len(instances) != 0 {
				t.Fatalf("GetService after deregister = %v, %v; want empty snapshot", instances, err)
			}
		})
	}
}
