package nacos

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"net"
	"net/url"
	"strconv"

	"github.com/nacos-group/nacos-sdk-go/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/vo"

	"github.com/byteweap/meta/component/log"
	"github.com/byteweap/meta/component/registry"
)

var (
	ErrServiceInstanceNameEmpty = errors.New("meta/nacos: ServiceInstance.Name can not be empty")
	ErrServiceInstanceIDEmpty   = errors.New("meta/nacos: ServiceInstance.ID can not be empty")
)

// Registry 是 nacos 注册中心实现
type Registry struct {
	opts options
	cli  naming_client.INamingClient
}

var _ registry.Registry = (*Registry)(nil)

// New 创建 nacos 注册中心
func New(cli naming_client.INamingClient, opts ...Option) (r *Registry) {
	op := options{
		prefix:  "/microservices",
		cluster: "DEFAULT",
		group:   constant.DEFAULT_GROUP,
		weight:  100,
		kind:    "grpc",
	}
	for _, option := range opts {
		option(&op)
	}
	return &Registry{
		opts: op,
		cli:  cli,
	}
}

func (r *Registry) ID() string {
	return "nacos"
}

// Register 注册服务
func (r *Registry) Register(_ context.Context, si *registry.ServiceInstance) error {
	if si.Name == "" {
		return ErrServiceInstanceNameEmpty
	}
	if si.ID == "" {
		return ErrServiceInstanceIDEmpty
	}
	for _, endpoint := range si.Endpoints {
		u, err := url.Parse(endpoint)
		if err != nil {
			return err
		}
		host, port, err := net.SplitHostPort(u.Host)
		if err != nil {
			return err
		}
		p, err := strconv.Atoi(port)
		if err != nil {
			return err
		}
		weight := r.opts.weight
		rmd := maps.Clone(si.Metadata)
		if rmd == nil {
			rmd = make(map[string]string)
		}
		rmd["kind"] = u.Scheme
		rmd["version"] = si.Version
		rmd["id"] = si.ID
		if w, ok := si.Metadata["weight"]; ok {
			weight, err = strconv.ParseFloat(w, 64)
			if err != nil {
				weight = r.opts.weight
			}
		}
		_, e := r.cli.RegisterInstance(vo.RegisterInstanceParam{
			Ip:          host,
			Port:        uint64(p),
			ServiceName: si.Name,
			Weight:      weight,
			Enable:      true,
			Healthy:     true,
			Ephemeral:   true,
			Metadata:    rmd,
			ClusterName: r.opts.cluster,
			GroupName:   r.opts.group,
		})
		if e != nil {
			return fmt.Errorf("RegisterInstance err %v,%v", e, endpoint)
		}
	}
	return nil
}

// Deregister 注销服务
func (r *Registry) Deregister(_ context.Context, service *registry.ServiceInstance) error {
	for _, endpoint := range service.Endpoints {
		u, err := url.Parse(endpoint)
		if err != nil {
			return err
		}
		host, port, err := net.SplitHostPort(u.Host)
		if err != nil {
			return err
		}
		p, err := strconv.Atoi(port)
		if err != nil {
			return err
		}
		if _, err = r.cli.DeregisterInstance(vo.DeregisterInstanceParam{
			Ip:          host,
			Port:        uint64(p),
			ServiceName: service.Name,
			GroupName:   r.opts.group,
			Cluster:     r.opts.cluster,
			Ephemeral:   true,
		}); err != nil {
			return err
		}
	}
	return nil
}

// Watch 按服务名创建 watcher
func (r *Registry) Watch(ctx context.Context, serviceName string) (registry.Watcher, error) {
	return newWatcher(ctx, r, serviceName)
}

// GetService 按服务名获取实例列表
func (r *Registry) GetService(ctx context.Context, serviceName string) ([]*registry.ServiceInstance, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// SelectInstances 将空列表视作错误，使用全量查询后统一筛选可路由节点。
	res, err := r.cli.SelectAllInstances(vo.SelectAllInstancesParam{
		ServiceName: serviceName,
		GroupName:   r.opts.group,
		Clusters:    []string{r.opts.cluster},
	})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	items := make([]*registry.ServiceInstance, 0, len(res))
	for _, in := range res {
		if !in.Healthy || !in.Enable || !(in.Weight > 0) || math.IsInf(in.Weight, 0) {
			continue
		}
		id := in.Metadata["id"]
		if id == "" {
			log.Warnf("[nacos] skip instance without application ID, service: %s, instance: %s", serviceName, in.InstanceId)
			continue
		}
		metadata := maps.Clone(in.Metadata)
		kind := metadata["kind"]
		if kind == "" {
			kind = r.opts.kind
		}
		metadata["weight"] = strconv.FormatFloat(math.Ceil(in.Weight), 'f', -1, 64)
		items = append(items, &registry.ServiceInstance{
			ID:        id,
			Name:      serviceName,
			Version:   metadata["version"],
			Metadata:  metadata,
			Endpoints: []string{kind + "://" + net.JoinHostPort(in.Ip, strconv.FormatUint(in.Port, 10))},
		})
	}
	return items, nil
}
