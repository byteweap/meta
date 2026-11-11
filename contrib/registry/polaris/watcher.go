package polaris

import (
	"context"
	"io"
	"maps"
	"slices"

	"github.com/polarismesh/polaris-go/api"
	"github.com/polarismesh/polaris-go/pkg/model"

	"github.com/byteweap/meta/component/registry"
)

type watcher struct {
	ctx       context.Context
	cancel    context.CancelFunc
	events    <-chan model.SubScribeEvent
	next      chan struct{}
	instances map[string]model.Instance // Polaris IDs identify endpoint records, not applications.
	first     bool
}

var _ registry.Watcher = (*watcher)(nil)

func newWatcher(ctx context.Context, namespace, serviceName string, consumer api.ConsumerAPI) (*watcher, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	request := &api.WatchServiceRequest{
		WatchServiceRequest: model.WatchServiceRequest{
			Key: model.ServiceKey{Namespace: namespace, Service: serviceName},
		},
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	response, err := consumer.WatchService(request)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	w := &watcher{
		ctx:       ctx,
		cancel:    cancel,
		events:    response.EventChannel,
		next:      make(chan struct{}, 1),
		instances: make(map[string]model.Instance),
		first:     true,
	}
	for _, instance := range response.GetAllInstancesResp.GetInstances() {
		w.instances[instance.GetId()] = instance
	}
	return w, nil
}

// Next returns the initial snapshot (including empty), then each instance change.
func (w *watcher) Next() ([]*registry.ServiceInstance, error) {
	select {
	case w.next <- struct{}{}:
		defer func() { <-w.next }()
	case <-w.ctx.Done():
		return nil, w.ctx.Err()
	}
	if err := w.ctx.Err(); err != nil {
		return nil, err
	}
	if w.first {
		w.first = false
		return w.snapshot(), nil
	}
	for {
		select {
		case <-w.ctx.Done():
			return nil, w.ctx.Err()
		case event, ok := <-w.events:
			if err := w.ctx.Err(); err != nil {
				return nil, err
			}
			if !ok {
				return nil, io.EOF
			}
			change, ok := event.(*model.InstanceEvent)
			if !ok || change == nil {
				continue
			}
			if change.DeleteEvent != nil {
				for _, instance := range change.DeleteEvent.Instances {
					delete(w.instances, instance.GetId())
				}
			}
			if change.UpdateEvent != nil {
				for _, update := range change.UpdateEvent.UpdateList {
					delete(w.instances, update.Before.GetId())
					w.instances[update.After.GetId()] = update.After
				}
			}
			if change.AddEvent != nil {
				for _, instance := range change.AddEvent.Instances {
					w.instances[instance.GetId()] = instance
				}
			}
			return w.snapshot(), nil
		}
	}
}

func (w *watcher) snapshot() []*registry.ServiceInstance {
	instances := make([]model.Instance, 0, len(w.instances))
	for _, id := range slices.Sorted(maps.Keys(w.instances)) {
		instances = append(instances, w.instances[id])
	}
	return instancesToServiceInstances(instances)
}

// Stop stops local waiting; the SDK WatchService API has no unsubscribe operation.
func (w *watcher) Stop() error {
	w.cancel()
	return nil
}
