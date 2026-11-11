//go:build integration

package nacos

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/vo"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/byteweap/meta/component/registry"
)

var testServerConfig = []constant.ServerConfig{
	*constant.NewServerConfig("127.0.0.1", 8848),
}

func TestRegistry_Register(t *testing.T) {
	sc := testServerConfig

	cc := constant.ClientConfig{
		NamespaceId:          "public", // 命名空间 id
		TimeoutMs:            5000,
		NotLoadCacheAtStart:  true,
		UpdateCacheWhenEmpty: true,
		LogDir:               "/tmp/nacos/log",
		CacheDir:             "/tmp/nacos/cache",
		LogRollingConfig:     &lumberjack.Logger{},
		LogLevel:             "debug",
	}

	// 更稳妥的方式创建 naming client
	client, err := clients.NewNamingClient(
		vo.NacosClientParam{
			ClientConfig:  &cc,
			ServerConfigs: sc,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	r := New(client)

	testServer := &registry.ServiceInstance{
		ID:        "1",
		Name:      "test1",
		Version:   "v1.0.0",
		Endpoints: []string{"http://127.0.0.1:8080?isSecure=false"},
	}
	testServerWithMetadata := &registry.ServiceInstance{
		ID:        "1",
		Name:      "test1",
		Version:   "v1.0.0",
		Endpoints: []string{"http://127.0.0.1:8080?isSecure=false"},
		Metadata:  map[string]string{"idc": "shanghai-xs"},
	}
	type fields struct {
		registry *Registry
	}
	type args struct {
		ctx     context.Context
		service *registry.ServiceInstance
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantErr   bool
		deferFunc func(t *testing.T)
	}{
		{
			name: "normal",
			fields: fields{
				registry: New(client),
			},
			args: args{
				ctx:     context.Background(),
				service: testServer,
			},
			wantErr: false,
			deferFunc: func(t *testing.T) {
				err = r.Deregister(context.Background(), testServer)
				if err != nil {
					t.Error(err)
				}
			},
		},
		{
			name: "withMetadata",
			fields: fields{
				registry: New(client),
			},
			args: args{
				ctx:     context.Background(),
				service: testServerWithMetadata,
			},
			wantErr: false,
			deferFunc: func(t *testing.T) {
				err = r.Deregister(context.Background(), testServerWithMetadata)
				if err != nil {
					t.Error(err)
				}
			},
		},
		{
			name: "error",
			fields: fields{
				registry: New(client),
			},
			args: args{
				ctx: context.Background(),
				service: &registry.ServiceInstance{
					ID:        "1",
					Name:      "",
					Version:   "v1.0.0",
					Endpoints: []string{"http://127.0.0.1:8080?isSecure=false"},
				},
			},
			wantErr: true,
		},
		{
			name: "urlError",
			fields: fields{
				registry: New(client),
			},
			args: args{
				ctx: context.Background(),
				service: &registry.ServiceInstance{
					ID:        "1",
					Name:      "test",
					Version:   "v1.0.0",
					Endpoints: []string{"127.0.0.1:8080"},
				},
			},
			wantErr: true,
		},
		{
			name: "portError",
			fields: fields{
				registry: New(client),
			},
			args: args{
				ctx: context.Background(),
				service: &registry.ServiceInstance{
					ID:        "1",
					Name:      "test",
					Version:   "v1.0.0",
					Endpoints: []string{"http://127.0.0.1888"},
				},
			},
			wantErr: true,
		},
		{
			name: "withCluster",
			fields: fields{
				registry: New(client, WithCluster("test")),
			},
			args: args{
				ctx:     context.Background(),
				service: testServer,
			},
			wantErr: false,
		},
		{
			name: "withGroup",
			fields: fields{
				registry: New(client, WithGroup("TEST_GROUP")),
			},
			args: args{
				ctx:     context.Background(),
				service: testServer,
			},
			wantErr: false,
		},
		{
			name: "withWeight",
			fields: fields{
				registry: New(client, WithWeight(200)),
			},
			args: args{
				ctx:     context.Background(),
				service: testServer,
			},
			wantErr: false,
		},
		{
			name: "withPrefix",
			fields: fields{
				registry: New(client, WithPrefix("test")),
			},
			args: args{
				ctx:     context.Background(),
				service: testServer,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := tt.fields.registry
			if err := r.Register(tt.args.ctx, tt.args.service); (err != nil) != tt.wantErr {
				t.Errorf("Register error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRegistry_Deregister(t *testing.T) {
	testServer := &registry.ServiceInstance{
		ID:        "1",
		Name:      "test2",
		Version:   "v1.0.0",
		Endpoints: []string{"http://127.0.0.1:8080?isSecure=false"},
	}

	type args struct {
		ctx     context.Context
		service *registry.ServiceInstance
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		preFunc func(t *testing.T)
	}{
		{
			name: "normal",
			args: args{
				ctx:     context.Background(),
				service: testServer,
			},
			wantErr: false,
			preFunc: func(t *testing.T) {
				sc := testServerConfig

				cc := constant.ClientConfig{
					NamespaceId:          "public", // 命名空间 id
					TimeoutMs:            5000,
					NotLoadCacheAtStart:  true,
					UpdateCacheWhenEmpty: true,
					LogDir:               "/tmp/nacos/log",
					CacheDir:             "/tmp/nacos/cache",
					LogRollingConfig:     &lumberjack.Logger{},
					LogLevel:             "debug",
				}

				// 更稳妥的方式创建 naming client
				client, err := clients.NewNamingClient(
					vo.NacosClientParam{
						ClientConfig:  &cc,
						ServerConfigs: sc,
					},
				)
				if err != nil {
					t.Fatal(err)
				}
				r := New(client)
				err = r.Register(context.Background(), testServer)
				if err != nil {
					t.Error(err)
				}
			},
		},
		{
			name: "error",
			args: args{
				ctx: context.Background(),
				service: &registry.ServiceInstance{
					ID:        "1",
					Name:      "test",
					Version:   "v1.0.0",
					Endpoints: []string{"127.0.0.1:8080"},
				},
			},
			wantErr: true,
		},
		{
			name: "errorPort",
			args: args{
				ctx: context.Background(),
				service: &registry.ServiceInstance{
					ID:        "1",
					Name:      "notExist",
					Version:   "v1.0.0",
					Endpoints: []string{"http://127.0.0.18080"},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := testServerConfig

			cc := constant.ClientConfig{
				NamespaceId:          "public", // 命名空间 id
				TimeoutMs:            5000,
				NotLoadCacheAtStart:  true,
				UpdateCacheWhenEmpty: true,
				LogDir:               "/tmp/nacos/log",
				CacheDir:             "/tmp/nacos/cache",
				LogRollingConfig:     &lumberjack.Logger{},
				LogLevel:             "debug",
			}

			// 更稳妥的方式创建 naming client
			client, err := clients.NewNamingClient(
				vo.NacosClientParam{
					ClientConfig:  &cc,
					ServerConfigs: sc,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			r := New(client)
			if tt.preFunc != nil {
				tt.preFunc(t)
			}
			if err := r.Deregister(tt.args.ctx, tt.args.service); (err != nil) != tt.wantErr {
				t.Errorf("Deregister error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRegistry_GetService(t *testing.T) {
	sc := testServerConfig

	cc := constant.ClientConfig{
		NamespaceId:          "public", // 命名空间 id
		TimeoutMs:            5000,
		NotLoadCacheAtStart:  true,
		UpdateCacheWhenEmpty: true,
		LogDir:               "/tmp/nacos/log",
		CacheDir:             "/tmp/nacos/cache",
		LogRollingConfig:     &lumberjack.Logger{},
		LogLevel:             "debug",
	}

	// 更稳妥的方式创建 naming client
	client, err := clients.NewNamingClient(
		vo.NacosClientParam{
			ClientConfig:  &cc,
			ServerConfigs: sc,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	r := New(client)
	testServer := &registry.ServiceInstance{
		ID:        "1",
		Name:      "test3",
		Version:   "v1.0.0",
		Endpoints: []string{"grpc://127.0.0.1:8080?isSecure=false"},
	}

	type fields struct {
		registry *Registry
	}
	type args struct {
		ctx         context.Context
		serviceName string
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		want      []*registry.ServiceInstance
		wantErr   bool
		preFunc   func(t *testing.T)
		deferFunc func(t *testing.T)
	}{
		{
			name: "normal",
			preFunc: func(t *testing.T) {
				err = r.Register(context.Background(), testServer)
				if err != nil {
					t.Error(err)
				}
				time.Sleep(time.Second * 3)
			},
			deferFunc: func(t *testing.T) {
				err = r.Deregister(context.Background(), testServer)
				if err != nil {
					t.Error(err)
				}
			},
			fields: fields{
				registry: r,
			},
			args: args{
				ctx:         context.Background(),
				serviceName: testServer.Name,
			},
			want: []*registry.ServiceInstance{{
				ID:        "1",
				Name:      testServer.Name,
				Version:   "v1.0.0",
				Metadata:  map[string]string{"id": "1", "version": "v1.0.0", "kind": "grpc", "weight": "100"},
				Endpoints: []string{"grpc://127.0.0.1:8080"},
			}},
			wantErr: false,
		},
		{
			name: "emptyService",
			fields: fields{
				registry: r,
			},
			args: args{
				ctx:         context.Background(),
				serviceName: "notExist",
			},
			want:    []*registry.ServiceInstance{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preFunc != nil {
				tt.preFunc(t)
			}
			if tt.deferFunc != nil {
				defer tt.deferFunc(t)
			}
			r := tt.fields.registry
			got, err := r.GetService(tt.args.ctx, tt.args.serviceName)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetService error = %v, wantErr %v", err, tt.wantErr)
				t.Errorf("GetService got = %v", got)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetService got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRegistry_Watch(t *testing.T) {
	sc := testServerConfig

	cc := constant.ClientConfig{
		NamespaceId:          "public", // 命名空间 id
		TimeoutMs:            5000,
		NotLoadCacheAtStart:  true,
		UpdateCacheWhenEmpty: true,
		LogDir:               "/tmp/nacos/log",
		CacheDir:             "/tmp/nacos/cache",
		LogRollingConfig:     &lumberjack.Logger{},
		LogLevel:             "debug",
	}

	// 更稳妥的方式创建 naming client
	client, err := clients.NewNamingClient(
		vo.NacosClientParam{
			ClientConfig:  &cc,
			ServerConfigs: sc,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	r := New(client)

	testServer := &registry.ServiceInstance{
		ID:        "1",
		Name:      "test4",
		Version:   "v1.0.0",
		Endpoints: []string{"grpc://127.0.0.1:8080?isSecure=false"},
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type fields struct {
		registry *Registry
	}
	type args struct {
		ctx         context.Context
		serviceName string
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantErr     bool
		want        []*registry.ServiceInstance
		processFunc func(t *testing.T)
	}{
		{
			name: "normal",
			fields: fields{
				registry: New(client),
			},
			args: args{
				ctx:         context.Background(),
				serviceName: testServer.Name,
			},
			wantErr: false,
			want: []*registry.ServiceInstance{{
				ID:        "1",
				Name:      testServer.Name,
				Version:   "v1.0.0",
				Metadata:  map[string]string{"id": "1", "version": "v1.0.0", "kind": "grpc", "weight": "100"},
				Endpoints: []string{"grpc://127.0.0.1:8080"},
			}},
			processFunc: func(t *testing.T) {
				if err := r.Register(context.Background(), testServer); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := r.Deregister(context.Background(), testServer); err != nil {
						t.Error(err)
					}
				})
			},
		},
		{
			name: "ctxCancel",
			fields: fields{
				registry: r,
			},
			args: args{
				ctx:         cancelCtx,
				serviceName: testServer.Name,
			},
			wantErr: true,
			want:    nil,
			processFunc: func(*testing.T) {
				cancel()
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := tt.fields.registry
			ctx, stop := context.WithTimeout(tt.args.ctx, 15*time.Second)
			defer stop()
			watch, err := r.Watch(ctx, tt.args.serviceName)
			if err != nil {
				t.Error(err)
				return
			}
			defer func() {
				err = watch.Stop()
				if err != nil {
					t.Error(err)
				}
			}()
			if tt.processFunc != nil {
				tt.processFunc(t)
			}

			// Registration propagation may emit an initial empty snapshot before
			// the registered instance appears. Wait within the watcher deadline.
			for {
				got, err := watch.Next()
				if err != nil {
					if !tt.wantErr {
						t.Fatalf("Watch error = %v, last snapshot = %v", err, got)
					}
					if !reflect.DeepEqual(got, tt.want) {
						t.Errorf("Watch snapshot = %v, want %v", got, tt.want)
					}
					return
				}
				if !tt.wantErr && reflect.DeepEqual(got, tt.want) {
					return
				}
			}
		})
	}
}
