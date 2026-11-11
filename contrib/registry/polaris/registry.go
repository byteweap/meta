package polaris

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/url"
	"strconv"

	"github.com/polarismesh/polaris-go/api"
	"github.com/polarismesh/polaris-go/pkg/config"
	"github.com/polarismesh/polaris-go/pkg/model"

	"github.com/byteweap/meta/component/registry"
)

// Registry 是 polaris 注册中心实现。
type Registry struct {
	opt      options
	provider api.ProviderAPI
	consumer api.ConsumerAPI
}

var _ registry.Registry = (*Registry)(nil)

func NewRegistry(provider api.ProviderAPI, consumer api.ConsumerAPI, opts ...Option) *Registry {
	op := options{Namespace: "default", Weight: 100, Healthy: true, Heartbeat: true}
	for _, option := range opts {
		option(&op)
	}
	return &Registry{opt: op, provider: provider, consumer: consumer}
}

func NewRegistryWithConfig(conf config.Configuration, opts ...Option) *Registry {
	provider, err := api.NewProviderAPIByConfig(conf)
	if err != nil {
		panic(err)
	}
	consumer, err := api.NewConsumerAPIByConfig(conf)
	if err != nil {
		panic(err)
	}
	return NewRegistry(provider, consumer, opts...)
}

func (r *Registry) ID() string { return "polaris" }

// Register 保留业务身份，由 SDK 管理心跳；失败时仅清理已确认的新建实例。
func (r *Registry) Register(ctx context.Context, service *registry.ServiceInstance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	addresses, err := parseEndpoints(service)
	if err != nil {
		return err
	}
	if service.ID == "" {
		return errors.New("meta/polaris: service ID must not be empty")
	}
	if err := r.validateOptions(); err != nil {
		return err
	}
	requests := make([]*api.InstanceRegisterRequest, 0, len(addresses))
	for _, address := range addresses {
		op := r.opt
		version := service.Version
		metadata := maps.Clone(service.Metadata)
		if metadata == nil {
			metadata = make(map[string]string)
		}
		metadata["id"], metadata["kind"], metadata["version"] = service.ID, address.kind, version
		request := &api.InstanceRegisterRequest{InstanceRegisterRequest: model.InstanceRegisterRequest{
			Service: service.Name, Namespace: op.Namespace, ServiceToken: op.ServiceToken,
			Host: address.host, Port: address.port,
			Protocol: clonePointer(op.Protocol), Weight: &op.Weight, Priority: &op.Priority,
			Version: &version, Metadata: metadata, Healthy: &op.Healthy, Isolate: &op.Isolate,
			Timeout: clonePointer(op.Timeout), RetryCount: clonePointer(op.RetryCount),
		}}
		if op.Heartbeat {
			request.TTL = clonePointer(op.TTL)
		}
		if err := request.Validate(); err != nil {
			return fmt.Errorf("meta/polaris: invalid endpoint %s: %w", address, err)
		}
		requests = append(requests, request)
	}

	register := r.provider.Register
	if r.opt.Heartbeat {
		register = r.provider.RegisterInstance
	}
	created := make([]endpoint, 0, len(addresses))
	for i, request := range requests {
		err = ctx.Err()
		if err == nil {
			var response *model.InstanceRegisterResponse
			response, err = register(request)
			if err == nil {
				if response == nil {
					err = errors.New("empty registration response")
				} else if !response.Existed {
					created = append(created, addresses[i])
				}
			}
		}
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			errs := []error{fmt.Errorf("meta/polaris: register %s: %w", addresses[i], err)}
			// 清理不使用已取消的调用 context；SDK 请求仍受自身超时限制。
			for _, address := range created {
				if cleanupErr := r.provider.Deregister(r.deregisterRequest(service.Name, address)); cleanupErr != nil {
					errs = append(errs, fmt.Errorf("meta/polaris: rollback %s: %w", address, cleanupErr))
				}
			}
			return errors.Join(errs...)
		}
	}
	return nil
}

// Deregister 按完整地址键注销，同时取消对应的 SDK 托管心跳任务。
func (r *Registry) Deregister(ctx context.Context, service *registry.ServiceInstance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	addresses, err := parseEndpoints(service)
	if err != nil {
		return err
	}
	if err := r.validateOptions(); err != nil {
		return err
	}
	requests := make([]*api.InstanceDeRegisterRequest, 0, len(addresses))
	for _, address := range addresses {
		request := r.deregisterRequest(service.Name, address)
		if err := request.Validate(); err != nil {
			return fmt.Errorf("meta/polaris: invalid endpoint %s: %w", address, err)
		}
		requests = append(requests, request)
	}
	var errs []error
	for i, request := range requests {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(errs, err)...)
		}
		if err := r.provider.Deregister(request); err != nil {
			errs = append(errs, fmt.Errorf("meta/polaris: deregister %s: %w", addresses[i], err))
		}
	}
	return errors.Join(append(errs, ctx.Err())...)
}

func (r *Registry) deregisterRequest(serviceName string, address endpoint) *api.InstanceDeRegisterRequest {
	return &api.InstanceDeRegisterRequest{InstanceDeRegisterRequest: model.InstanceDeRegisterRequest{
		Service: serviceName, Namespace: r.opt.Namespace, ServiceToken: r.opt.ServiceToken,
		Host: address.host, Port: address.port,
		Timeout: clonePointer(r.opt.Timeout), RetryCount: clonePointer(r.opt.RetryCount),
	}}
}

// GetService 按原服务名获取可路由实例。
func (r *Registry) GetService(ctx context.Context, serviceName string) ([]*registry.ServiceInstance, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := r.validateOptions(); err != nil {
		return nil, err
	}
	request := &api.GetAllInstancesRequest{GetAllInstancesRequest: model.GetAllInstancesRequest{
		Service: serviceName, Namespace: r.opt.Namespace,
		Timeout: clonePointer(r.opt.Timeout), RetryCount: clonePointer(r.opt.RetryCount),
	}}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	response, err := r.consumer.GetAllInstances(request)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return instancesToServiceInstances(response.GetInstances()), nil
}

func (r *Registry) Watch(ctx context.Context, serviceName string) (registry.Watcher, error) {
	w, err := newWatcher(ctx, r.opt.Namespace, serviceName, r.consumer)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func instancesToServiceInstances(instances []model.Instance) []*registry.ServiceInstance {
	services := make([]*registry.ServiceInstance, 0, len(instances))
	for _, instance := range instances {
		metadata := instance.GetMetadata()
		if !instance.IsHealthy() || instance.IsIsolated() || instance.GetWeight() <= 0 || metadata["id"] == "" || metadata["kind"] == "" {
			continue
		}
		services = append(services, instanceToServiceInstance(instance))
	}
	return services
}

func instanceToServiceInstance(instance model.Instance) *registry.ServiceInstance {
	metadata := maps.Clone(instance.GetMetadata())
	return &registry.ServiceInstance{
		ID: metadata["id"], Name: instance.GetService(), Version: metadata["version"], Metadata: metadata,
		Endpoints: []string{metadata["kind"] + "://" + net.JoinHostPort(instance.GetHost(), strconv.Itoa(int(instance.GetPort())))},
	}
}

type endpoint struct {
	kind string
	host string
	port int
}

func (e endpoint) String() string {
	return e.kind + "://" + net.JoinHostPort(e.host, strconv.Itoa(e.port))
}

func parseEndpoints(service *registry.ServiceInstance) ([]endpoint, error) {
	if service == nil || service.Name == "" || len(service.Endpoints) == 0 {
		return nil, errors.New("meta/polaris: service name and endpoints must not be empty")
	}
	addresses := make([]endpoint, 0, len(service.Endpoints))
	seen := make(map[string]bool, len(service.Endpoints))
	for _, raw := range service.Endpoints {
		u, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("meta/polaris: invalid endpoint %q: %w", raw, err)
		}
		if u.Scheme == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
			return nil, fmt.Errorf("meta/polaris: endpoint %q must be scheme://host:port", raw)
		}
		host, port, err := net.SplitHostPort(u.Host)
		if err != nil {
			return nil, fmt.Errorf("meta/polaris: invalid endpoint %q: %w", raw, err)
		}
		portNum, err := strconv.Atoi(port)
		if err != nil || host == "" || portNum < 1 || portNum > 65535 {
			return nil, fmt.Errorf("meta/polaris: endpoint %q requires a host and port in 1–65535", raw)
		}
		key := net.JoinHostPort(host, strconv.Itoa(portNum))
		if seen[key] {
			return nil, fmt.Errorf("meta/polaris: duplicate endpoint address %s", key)
		}
		seen[key] = true
		addresses = append(addresses, endpoint{kind: u.Scheme, host: host, port: portNum})
	}
	return addresses, nil
}

func (r *Registry) validateOptions() error {
	if r.opt.Timeout != nil && *r.opt.Timeout <= 0 {
		return errors.New("meta/polaris: timeout must be positive")
	}
	if r.opt.RetryCount != nil && *r.opt.RetryCount < 0 {
		return errors.New("meta/polaris: retry count must not be negative")
	}
	return nil
}

func clonePointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
