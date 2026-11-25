package polaris

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/polarismesh/polaris-go/api"
	"github.com/polarismesh/polaris-go/pkg/model"

	"github.com/byteweap/meta/component/registry"
)

func newPolarisWatcherForTest(t *testing.T, ctx context.Context, initial ...model.Instance) (registry.Watcher, chan model.SubScribeEvent) {
	t.Helper()
	events := make(chan model.SubScribeEvent, 64)
	consumer := &fakePolarisConsumer{watch: func(request *api.WatchServiceRequest) (*model.WatchServiceResponse, error) {
		if request.Key.Namespace != "default" || request.Key.Service != "game" {
			t.Fatalf("watch scope = %+v", request.Key)
		}
		return &model.WatchServiceResponse{
			EventChannel:        events,
			GetAllInstancesResp: &model.InstancesResponse{Instances: initial},
		}, nil
	}}
	w, err := NewRegistry(nil, consumer).Watch(ctx, "game")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	return w, events
}

func requirePolarisSnapshot(t *testing.T, w registry.Watcher, ids ...string) []*registry.ServiceInstance {
	t.Helper()
	instances, err := w.Next()
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(instances))
	for _, instance := range instances {
		got = append(got, instance.ID)
	}
	if !reflect.DeepEqual(got, append([]string{}, ids...)) {
		t.Fatalf("application IDs = %v, want %v", got, ids)
	}
	return instances
}

func TestPolarisWatcherSDKIdentityUpdates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first := newPolarisTestInstance("sdk-a", "app-with-hyphens")
	second := newPolarisTestInstance("sdk-b", "app-with-hyphens")
	second.port = 9001
	unhealthy := newPolarisTestInstance("sdk-c", "recovering-app")
	unhealthy.healthy = false
	w, events := newPolarisWatcherForTest(t, ctx, first, second, unhealthy)

	initial := requirePolarisSnapshot(t, w, "app-with-hyphens", "app-with-hyphens")
	initial[0].Metadata["version"] = "caller-changed"
	initial[0].Endpoints[0] = "caller-changed"
	initial[0].ID = "caller-changed"
	if first.metadata["version"] == "caller-changed" {
		t.Fatal("watch result aliases SDK metadata")
	}

	updated := newPolarisTestInstance("sdk-a", "app-with-hyphens")
	updated.metadata["version"] = "v2"
	events <- &model.InstanceEvent{UpdateEvent: &model.InstanceUpdateEvent{
		UpdateList: []model.OneInstanceUpdate{{Before: first, After: updated}},
	}}
	snapshot := requirePolarisSnapshot(t, w, "app-with-hyphens", "app-with-hyphens")
	if snapshot[0].Version != "v2" || snapshot[1].Endpoints[0] != "tcp://127.0.0.1:9001" {
		t.Fatalf("update matched the wrong SDK record: %+v", snapshot)
	}

	events <- &model.InstanceEvent{DeleteEvent: &model.InstanceDeleteEvent{Instances: []model.Instance{updated}}}
	snapshot = requirePolarisSnapshot(t, w, "app-with-hyphens")
	if snapshot[0].Endpoints[0] != "tcp://127.0.0.1:9001" {
		t.Fatal("deleting one SDK ID removed a different endpoint of the same application")
	}

	recovered := newPolarisTestInstance("sdk-c", "recovering-app")
	events <- &model.InstanceEvent{UpdateEvent: &model.InstanceUpdateEvent{
		UpdateList: []model.OneInstanceUpdate{{Before: unhealthy, After: recovered}},
	}}
	requirePolarisSnapshot(t, w, "app-with-hyphens", "recovering-app")

	isolated := newPolarisTestInstance("sdk-b", "app-with-hyphens")
	isolated.isolated = true
	events <- &model.InstanceEvent{UpdateEvent: &model.InstanceUpdateEvent{
		UpdateList: []model.OneInstanceUpdate{{Before: second, After: isolated}},
	}}
	requirePolarisSnapshot(t, w, "recovering-app")

	// Repeated additions replace one endpoint record instead of duplicating it.
	events <- &model.InstanceEvent{AddEvent: &model.InstanceAddEvent{Instances: []model.Instance{second, second}}}
	requirePolarisSnapshot(t, w, "app-with-hyphens", "recovering-app")
	events <- &model.InstanceEvent{DeleteEvent: &model.InstanceDeleteEvent{Instances: []model.Instance{second, recovered}}}
	requirePolarisSnapshot(t, w)
}

func TestPolarisWatcherInitialEmptyAndClosedChannel(t *testing.T) {
	w, events := newPolarisWatcherForTest(t, context.Background())
	requirePolarisSnapshot(t, w)
	close(events)
	for range 2 {
		if instances, err := w.Next(); instances != nil || !errors.Is(err, io.EOF) {
			t.Fatalf("closed channel Next = %v, %v", instances, err)
		}
	}
}

func TestPolarisWatcherCreationFailures(t *testing.T) {
	failure := errors.New("watch failed")
	consumer := &fakePolarisConsumer{watch: func(*api.WatchServiceRequest) (*model.WatchServiceResponse, error) {
		return nil, failure
	}}
	if w, err := NewRegistry(nil, consumer).Watch(context.Background(), "game"); w != nil || !errors.Is(err, failure) {
		t.Fatalf("Watch error = %v, %v", w, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if w, err := NewRegistry(nil, nil).Watch(ctx, "game"); w != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-cancelled Watch = %v, %v", w, err)
	}
	if w, err := NewRegistry(nil, nil).Watch(context.Background(), ""); w != nil || err == nil {
		t.Fatalf("invalid Watch = %v, %v", w, err)
	}

	ctx, cancel = context.WithCancel(context.Background())
	consumer.watch = func(*api.WatchServiceRequest) (*model.WatchServiceResponse, error) {
		cancel()
		return &model.WatchServiceResponse{}, nil
	}
	if w, err := NewRegistry(nil, consumer).Watch(ctx, "game"); w != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled during creation = %v, %v", w, err)
	}
}

func TestPolarisWatcherConcurrentNextAndStop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w, events := newPolarisWatcherForTest(t, ctx)
	requirePolarisSnapshot(t, w)
	var readers sync.WaitGroup
	for i := range 32 {
		readers.Go(func() {
			instances, err := w.Next()
			if err != nil || len(instances) != 1 {
				t.Errorf("concurrent Next = %v, %v", instances, err)
				return
			}
			instances[0].Metadata["caller"] = "changed"
			instances[0].Endpoints[0] = "changed"
		})
		instance := newPolarisTestInstance("sdk-node", "app-node")
		instance.port = uint32(9000 + i)
		events <- &model.InstanceEvent{AddEvent: &model.InstanceAddEvent{Instances: []model.Instance{instance}}}
	}
	readers.Wait()

	for range 16 {
		readers.Go(func() {
			if instances, err := w.Next(); instances != nil || !errors.Is(err, context.Canceled) {
				t.Errorf("stopped Next = %v, %v", instances, err)
			}
		})
	}
	var stoppers sync.WaitGroup
	for range 8 {
		stoppers.Go(func() {
			if err := w.Stop(); err != nil {
				t.Error(err)
			}
		})
	}
	stoppers.Wait()
	readers.Wait()
	// Stop must not close the SDK-owned channel.
	events <- &model.InstanceEvent{}
}

func TestPolarisWatcherParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	w, _ := newPolarisWatcherForTest(t, ctx)
	cancel()
	if instances, err := w.Next(); instances != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled initial Next = %v, %v", instances, err)
	}
}

func TestPolarisWatcherIgnoresNilChangesAndUnblocksConcurrentReads(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	w, events := newPolarisWatcherForTest(t, ctx)
	requirePolarisSnapshot(t, w)
	events <- (*model.InstanceEvent)(nil)
	close(events)
	if instances, err := w.Next(); instances != nil || !errors.Is(err, io.EOF) {
		t.Fatalf("Next returned a snapshot for a nil event: %v, %v", instances, err)
	}

	blocked, _ := newPolarisWatcherForTest(t, ctx)
	internal := blocked.(*watcher)
	internal.next <- struct{}{}
	defer func() { <-internal.next }()
	cancel()
	if snapshot, err := blocked.Next(); snapshot != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("queued Next after cancellation = %v, %v", snapshot, err)
	}
}
