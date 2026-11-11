package eureka

import (
	"context"
	"errors"
	"maps"
	"strconv"
	"strings"
	"time"

	"github.com/byteweap/meta/component/registry"
)

type Registry struct {
	ctx               context.Context
	api               *API
	heartbeatInterval time.Duration
	refreshInterval   time.Duration
	eurekaPath        string
}

var _ registry.Registry = (*Registry)(nil)

func (r *Registry) ID() string {
	return "eureka"
}

func New(eurekaUrls []string, opts ...Option) (*Registry, error) {
	r := &Registry{
		ctx:               context.Background(),
		heartbeatInterval: heartbeatTime,
		refreshInterval:   refreshTime,
		eurekaPath:        "eureka/v2",
	}

	for _, o := range opts {
		o(r)
	}
	if len(eurekaUrls) == 0 {
		return nil, errors.New("eureka: at least one server address is required")
	}
	if r.heartbeatInterval <= 0 || r.refreshInterval <= 0 {
		return nil, errors.New("eureka: heartbeat and refresh intervals must be positive")
	}

	client := NewClient(eurekaUrls,
		WithHeartbeatInterval(r.heartbeatInterval),
		WithClientContext(r.ctx),
		WithNamespace(r.eurekaPath),
	)
	r.api = NewAPI(r.ctx, client, r.refreshInterval)
	return r, nil
}

// Register 注册服务实例
func (r *Registry) Register(ctx context.Context, service *registry.ServiceInstance) error {
	return r.api.Register(ctx, r.Endpoints(service)...)
}

// Deregister 从 Eureka 注销服务实例
func (r *Registry) Deregister(ctx context.Context, service *registry.ServiceInstance) error {
	return r.api.Deregister(ctx, r.Endpoints(service))
}

// GetService 从 Eureka 获取服务
func (r *Registry) GetService(ctx context.Context, serviceName string) ([]*registry.ServiceInstance, error) {
	instances, err := r.api.GetService(ctx, serviceName)
	if err != nil {
		return nil, err
	}
	items := make([]*registry.ServiceInstance, 0, len(instances))
	for _, instance := range instances {
		items = append(items, &registry.ServiceInstance{
			ID:        instance.Metadata["ID"],
			Name:      instance.Metadata["Name"],
			Version:   instance.Metadata["Version"],
			Endpoints: []string{instance.Metadata["Endpoints"]},
			Metadata:  instance.Metadata,
		})
	}

	return items, nil
}

// Watch 创建服务监听器，使用独立的 ctx
func (r *Registry) Watch(ctx context.Context, serviceName string) (registry.Watcher, error) {
	w, err := newWatch(ctx, r, serviceName)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (r *Registry) Endpoints(service *registry.ServiceInstance) []Endpoint {
	res := make([]Endpoint, 0, len(service.Endpoints))
	for _, ep := range service.Endpoints {
		start := strings.Index(ep, "//")
		end := strings.LastIndex(ep, ":")
		appID := strings.ToUpper(service.Name)
		ip := ep[start+2 : end]
		sport := ep[end+1:]
		port, _ := strconv.Atoi(sport)
		securePort := 443
		homePageURL := ep + "/"
		statusPageURL := ep + "/info"
		healthCheckURL := ep + "/health"
		instanceID := strings.Join([]string{ip, appID, sport}, ":")
		metadata := maps.Clone(service.Metadata)
		if metadata == nil {
			metadata = make(map[string]string)
		}
		if s, ok := service.Metadata["securePort"]; ok {
			securePort, _ = strconv.Atoi(s)
		}
		if s, ok := service.Metadata["homePageURL"]; ok {
			homePageURL = s
		}
		if s, ok := service.Metadata["statusPageURL"]; ok {
			statusPageURL = s
		}
		if s, ok := service.Metadata["healthCheckURL"]; ok {
			healthCheckURL = s
		}
		metadata["ID"] = service.ID
		metadata["Name"] = service.Name
		metadata["Version"] = service.Version
		metadata["Endpoints"] = ep
		metadata["agent"] = "go-eureka-client"
		res = append(res, Endpoint{
			AppID:          appID,
			IP:             ip,
			Port:           port,
			SecurePort:     securePort,
			HomePageURL:    homePageURL,
			StatusPageURL:  statusPageURL,
			HealthCheckURL: healthCheckURL,
			InstanceID:     instanceID,
			MetaData:       metadata,
		})
	}

	return res
}
