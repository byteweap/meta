package servicecomb

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"slices"
	"time"

	"github.com/go-chassis/cari/discovery"
	"github.com/go-chassis/cari/pkg/errsvc"
	"github.com/go-chassis/sc-client"
	"github.com/gofrs/uuid"

	"github.com/byteweap/meta/component/log"
	"github.com/byteweap/meta/component/registry"
)

func init() {
	appID = os.Getenv(appIDVar)
	if appID == "" {
		appID = "default"
	}
	env = os.Getenv(envVar)
}

var (
	appID string
	env   string
)

const (
	appIDKey         = "appId"
	envKey           = "environment"
	envVar           = "CAS_ENVIRONMENT_ID"
	appIDVar         = "CAS_APPLICATION_NAME"
	frameWorkName    = "meta"
	frameWorkVersion = "v1"
)

// Registry 是 servicecomb 注册中心实现
type Registry struct {
	cli       RegistryClient
	queryGate chan struct{}
}

var _ registry.Registry = (*Registry)(nil)

func NewRegistry(client RegistryClient) *Registry {
	return &Registry{
		cli:       client,
		queryGate: make(chan struct{}, 1),
	}
}

func (r *Registry) ID() string {
	return "servicecomb"
}

func (r *Registry) GetService(ctx context.Context, serviceName string) ([]*registry.ServiceInstance, error) {
	// SDK 即使不发送 revision 也会读写共享状态，发现查询需要串行执行。
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r.queryGate <- struct{}{}:
	}
	defer func() { <-r.queryGate }()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	instances, err := r.cli.FindMicroServiceInstances("", appID, serviceName, "", sc.WithoutRevision())
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if errors.Is(err, sc.ErrMicroServiceNotExists) {
		return []*registry.ServiceInstance{}, nil
	}
	if err != nil {
		return nil, err
	}
	svcInstances := make([]*registry.ServiceInstance, 0, len(instances))
	for _, instance := range instances {
		if instance == nil {
			continue
		}
		svcInstances = append(svcInstances, &registry.ServiceInstance{
			ID:        instance.InstanceId,
			Name:      serviceName,
			Metadata:  maps.Clone(instance.Properties),
			Endpoints: slices.Clone(instance.Endpoints),
			Version:   instance.Version,
		})
	}
	return svcInstances, nil
}

func (r *Registry) Watch(ctx context.Context, serviceName string) (registry.Watcher, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return newWatcher(ctx, r, serviceName), nil
}

// Register 注册服务和实例，将实例元数据复制为 ServiceComb 实例属性
// appId 和 environment 由适配器根据环境变量设置
func (r *Registry) Register(_ context.Context, svcIns *registry.ServiceInstance) error {
	fw := &discovery.FrameWork{
		Name:    frameWorkName,
		Version: frameWorkVersion,
	}
	ms := &discovery.MicroService{
		ServiceName: svcIns.Name,
		AppId:       appID,
		Version:     svcIns.Version,
		Environment: env,
		Framework:   fw,
	}
	// 尝试注册微服务
	sid, err := r.cli.RegisterService(ms)
	// 失败时可能是服务已存在
	if err != nil {
		registryException, ok := err.(*sc.RegistryException)
		if !ok {
			return err
		}
		var svcErr errsvc.Error
		parseErr := json.Unmarshal([]byte(registryException.Message), &svcErr)
		if parseErr != nil {
			return parseErr
		}
		// 错误码不是服务已存在时直接返回
		if svcErr.Code != discovery.ErrServiceAlreadyExists {
			return err
		}
		sid, err = r.cli.GetMicroServiceID(appID, ms.ServiceName, ms.Version, ms.Environment)
		if err != nil {
			return err
		}
	}
	if svcIns.ID == "" {
		var id uuid.UUID
		id, err = uuid.NewV4()
		if err != nil {
			return err
		}
		svcIns.ID = id.String()
	}
	props := make(map[string]string, len(svcIns.Metadata)+2)
	maps.Copy(props, svcIns.Metadata)
	props[appIDKey] = appID
	props[envKey] = env
	_, err = r.cli.RegisterMicroServiceInstance(&discovery.MicroServiceInstance{
		InstanceId: svcIns.ID,
		ServiceId:  sid,
		Endpoints:  svcIns.Endpoints,
		HostName:   svcIns.ID,
		Properties: props,
		Version:    svcIns.Version,
	})
	if err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			<-ticker.C
			_, err = r.cli.Heartbeat(sid, svcIns.ID)
			if err != nil {
				log.Errorf("failed to send heartbeat: %v", err)
				continue
			}
		}
	}()
	return nil
}

func (r *Registry) Deregister(_ context.Context, svcIns *registry.ServiceInstance) error {
	sid, err := r.cli.GetMicroServiceID(appID, svcIns.Name, svcIns.Version, env)
	if err != nil {
		return err
	}
	_, err = r.cli.UnregisterMicroServiceInstance(sid, svcIns.ID)
	if err != nil {
		return err
	}
	return nil
}
