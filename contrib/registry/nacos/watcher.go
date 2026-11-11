package nacos

import (
	"context"

	"github.com/nacos-group/nacos-sdk-go/model"
	"github.com/nacos-group/nacos-sdk-go/vo"

	"github.com/byteweap/meta/component/registry"
)

type watcher struct {
	serviceName    string
	ctx            context.Context
	cancel         context.CancelFunc
	watchChan      chan struct{}
	registry       *Registry
	subscribeParam *vo.SubscribeParam
}

var _ registry.Watcher = (*watcher)(nil)

func newWatcher(ctx context.Context, r *Registry, serviceName string) (*watcher, error) {
	w := &watcher{
		serviceName: serviceName,
		registry:    r,
		watchChan:   make(chan struct{}, 1),
	}
	w.ctx, w.cancel = context.WithCancel(ctx)

	w.subscribeParam = &vo.SubscribeParam{
		ServiceName: serviceName,
		Clusters:    []string{r.opts.cluster},
		GroupName:   r.opts.group,
		SubscribeCallback: func([]model.SubscribeService, error) {
			select {
			case w.watchChan <- struct{}{}:
			default:
			}
		},
	}
	e := r.cli.Subscribe(w.subscribeParam)
	select {
	case w.watchChan <- struct{}{}:
	default:
	}
	return w, e
}

func (w *watcher) Next() ([]*registry.ServiceInstance, error) {
	select {
	case <-w.ctx.Done():
		return nil, w.ctx.Err()
	case <-w.watchChan:
	}
	return w.registry.GetService(w.ctx, w.serviceName)
}

func (w *watcher) Stop() error {
	err := w.registry.cli.Unsubscribe(w.subscribeParam)
	w.cancel()
	return err
}
