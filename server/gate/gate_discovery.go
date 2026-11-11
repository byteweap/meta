package gate

import (
	"context"
	"errors"
	"sync"

	"github.com/byteweap/meta/component/log"
	"github.com/byteweap/meta/component/registry"
	"github.com/byteweap/meta/component/selector"
)

// serviceDiscovery 将选择器和监听器作为同一代缓存维护。
type serviceDiscovery struct {
	selector selector.Selector
	watcher  registry.Watcher
	stop     func() error
}

func (g *Gate) cachedDiscovery(service string) *serviceDiscovery {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.services[service]
}

// ensure 只返回完成初始快照加载的选择器。同服务的并发初始化共用一次查询。
func (g *Gate) ensure(service string) (selector.Selector, error) {
	if !g.beginTask() {
		return nil, context.Canceled
	}
	defer g.tasks.Done()
	if current := g.cachedDiscovery(service); current != nil {
		return current.selector, nil
	}

	v, err, _ := g.sfg.Do(service, func() (any, error) {
		if current := g.cachedDiscovery(service); current != nil {
			return current.selector, nil
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

		g.lifecycleMu.Lock()
		stopping := g.stopped || g.ctx.Err() != nil
		g.lifecycleMu.Unlock()
		if stopping {
			return nil, errors.Join(context.Canceled, stop())
		}
		instances, err := g.opts.discovery.GetService(ctx, service)
		if err != nil {
			return nil, errors.Join(err, stop())
		}
		d := &serviceDiscovery{selector: g.opts.selectorFunc(), watcher: w, stop: stop}
		d.update(instances)

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
		return d.selector, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(selector.Selector), nil
}

func (g *Gate) watchSelector(service string, d *serviceDiscovery) {
	defer g.tasks.Done()
	defer func() {
		d.selector.Update(nil)
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
		d.update(instances)
	}
}

func (d *serviceDiscovery) update(instances []*registry.ServiceInstance) {
	nodes := make([]selector.Node, 0, len(instances))
	for _, instance := range instances {
		nodes = append(nodes, selector.NewNode(instance.ID, instance.Name, instance.Version, instance.Metadata))
	}
	d.selector.Update(nodes)
}
