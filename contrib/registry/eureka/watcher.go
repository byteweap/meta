package eureka

import (
	"context"
	"sync"

	"github.com/byteweap/meta/component/registry"
)

type watcher struct {
	ctx            context.Context
	cancel         context.CancelFunc
	registry       *Registry
	watchChan      chan struct{}
	serverName     string
	stopOnce       sync.Once
	detachRegistry func() bool
}

var _ registry.Watcher = (*watcher)(nil)

func newWatch(ctx context.Context, r *Registry, serverName string) (*watcher, error) {
	ctx, cancel := context.WithCancel(ctx)
	w := &watcher{
		ctx:        ctx,
		cancel:     cancel,
		registry:   r,
		serverName: serverName,
		watchChan:  make(chan struct{}, 1),
	}
	w.detachRegistry = context.AfterFunc(r.ctx, cancel)
	if err := r.api.addWatch(w); err != nil {
		w.detachRegistry()
		cancel()
		return nil, err
	}
	context.AfterFunc(ctx, func() { _ = w.Stop() })
	return w, nil
}

func (w *watcher) notify() {
	select {
	case w.watchChan <- struct{}{}:
	default:
	}
}

func (w *watcher) Next() ([]*registry.ServiceInstance, error) {
	select {
	case <-w.ctx.Done():
		return nil, w.ctx.Err()
	case <-w.watchChan:
		return w.registry.GetService(w.ctx, w.serverName)
	}
}

func (w *watcher) Stop() error {
	w.stopOnce.Do(func() {
		w.cancel()
		w.detachRegistry()
		w.registry.api.removeWatch(w)
	})
	return nil
}
