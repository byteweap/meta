package gate

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"

	"github.com/byteweap/meta/component/log"
	"github.com/byteweap/meta/component/registry"
	"github.com/byteweap/meta/component/selector"
)

var errServiceNotFound = errors.New("service not found")

// serviceDiscovery 将选择器和监听器作为同一代缓存维护。
type serviceDiscovery struct {
	selector.Selector
	mu      sync.RWMutex
	ids     map[string]struct{}
	watcher registry.Watcher
	stop    func() error
}

func (g *Gate) cachedDiscovery(service string) *serviceDiscovery {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.services[service]
}

// ensure 在首次请求时发现可用的服务实例，同名并发请求共用一次初始化
func (g *Gate) ensure(service string) (*serviceDiscovery, error) {
	if current := g.cachedDiscovery(service); current != nil && g.ctx.Err() == nil {
		return current, nil
	}
	// 服务名将作为 Broker 主题的一段以及注册中心的查询键
	if service == "" || service == "." || service == ".." || strings.ContainsAny(service, "/\\*> \t\r\n\x00") {
		return nil, errServiceNotFound
	}
	if !g.beginTask() {
		return nil, context.Canceled
	}
	defer g.tasks.Done()
	v, err, _ := g.sfg.Do(service, func() (any, error) {
		if current := g.cachedDiscovery(service); current != nil {
			return current, nil
		}
		return g.discover(service)
	})
	if err != nil {
		return nil, err
	}
	return v.(*serviceDiscovery), nil
}

// discover 仅为存在可用实例的服务建立监听
func (g *Gate) discover(service string) (*serviceDiscovery, error) {
	instances, err := g.opts.discovery.GetService(g.ctx, service)
	if err != nil {
		return nil, err
	}
	if ctxErr := g.ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if !slices.ContainsFunc(instances, func(i *registry.ServiceInstance) bool { return matchesService(service, i) }) {
		return nil, errServiceNotFound
	}
	ctx, cancel := context.WithCancel(g.ctx)
	w, watchErr := g.opts.discovery.Watch(ctx, service)
	stop := sync.OnceValue(func() error {
		cancel()
		if w == nil {
			return nil
		}
		closeErr := w.Stop()
		g.addStopError(closeErr)
		if closeErr != nil {
			log.Errorf("[websocket] close watcher error, service: %v, err: %v", service, closeErr)
		}
		return closeErr
	})
	if watchErr != nil {
		return nil, errors.Join(watchErr, stop())
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, errors.Join(ctxErr, stop())
	}
	// 订阅生效后再取快照，覆盖首次查询与建立监听之间的变化
	instances, err = g.opts.discovery.GetService(ctx, service)
	if err != nil {
		return nil, errors.Join(err, stop())
	}
	d := &serviceDiscovery{Selector: g.opts.selectorFunc(), watcher: w, stop: stop}
	if !d.update(service, instances) {
		return nil, errors.Join(errServiceNotFound, stop())
	}

	g.lifecycleMu.Lock()
	if g.stopped || g.ctx.Err() != nil {
		g.lifecycleMu.Unlock()
		return nil, errors.Join(context.Canceled, stop())
	}
	g.mu.Lock()
	g.services[service] = d
	g.mu.Unlock()
	g.tasks.Add(1)
	g.lifecycleMu.Unlock()
	go g.watchSelector(service, d)
	return d, nil
}

func (g *Gate) watchSelector(service string, d *serviceDiscovery) {
	defer g.tasks.Done()
	defer func() {
		d.update(service, nil)
		g.mu.Lock()
		if g.services[service] == d {
			delete(g.services, service)
		}
		g.mu.Unlock()
		_ = d.stop()
	}()
	for g.ctx.Err() == nil {
		instances, err := d.watcher.Next()
		if err != nil {
			if g.ctx.Err() == nil {
				log.Errorf("[websocket] watch service error, service: %v, err: %v", service, err)
			}
			return
		}
		if g.ctx.Err() != nil {
			return
		}
		if !d.update(service, instances) {
			return
		}
	}
}

func matchesService(service string, i *registry.ServiceInstance) bool {
	return i != nil && i.ID != "" && i.Name == service
}

func (d *serviceDiscovery) hasNode(id string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, ok := d.ids[id]
	return ok
}

func (d *serviceDiscovery) update(service string, instances []*registry.ServiceInstance) bool {
	ids := make(map[string]struct{}, len(instances))
	nodes := make([]selector.Node, 0, len(instances))
	for _, i := range instances {
		if !matchesService(service, i) {
			continue
		}
		if _, exists := ids[i.ID]; exists {
			continue
		}
		ids[i.ID] = struct{}{}
		nodes = append(nodes, selector.NewNode(i.ID, i.Name, i.Version, i.Metadata))
	}
	d.mu.Lock()
	d.ids = ids
	d.Update(nodes)
	d.mu.Unlock()
	return len(nodes) != 0
}
