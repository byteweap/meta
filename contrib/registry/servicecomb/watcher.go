package servicecomb

import (
	"context"
	"time"

	"github.com/byteweap/meta/component/registry"
)

const watchInterval = time.Second

type Watcher struct {
	registry    *Registry
	serviceName string
	ctx         context.Context
	cancel      context.CancelFunc
	nextGate    chan struct{}
	nextPoll    time.Time
}

var _ registry.Watcher = (*Watcher)(nil)

func newWatcher(ctx context.Context, r *Registry, serviceName string) *Watcher {
	ctx, cancel := context.WithCancel(ctx)
	return &Watcher{
		registry:    r,
		serviceName: serviceName,
		ctx:         ctx,
		cancel:      cancel,
		nextGate:    make(chan struct{}, 1),
	}
}

func (w *Watcher) Next() ([]*registry.ServiceInstance, error) {
	// 串行消费完整快照，排队和轮询等待均可取消。
	select {
	case <-w.ctx.Done():
		return nil, w.ctx.Err()
	case w.nextGate <- struct{}{}:
	}
	defer func() { <-w.nextGate }()
	if delay := time.Until(w.nextPoll); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-w.ctx.Done():
			return nil, w.ctx.Err()
		case <-timer.C:
		}
	}

	instances, err := w.registry.GetService(w.ctx, w.serviceName)
	w.nextPoll = time.Now().Add(watchInterval)
	return instances, err
}

func (w *Watcher) Stop() error {
	w.cancel()
	return nil
}
