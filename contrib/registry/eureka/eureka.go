package eureka

import (
	"context"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/byteweap/meta/component/log"
)

type API struct {
	ctx             context.Context
	cli             *Client
	allInstances    map[string][]Instance
	lastErr         error
	subscribers     map[*watcher]struct{}
	ready           chan struct{}
	refreshRequest  chan struct{}
	refreshInterval time.Duration
	lock            sync.RWMutex
}

func NewAPI(ctx context.Context, client *Client, refreshInterval time.Duration) *API {
	e := &API{
		ctx:             ctx,
		cli:             client,
		subscribers:     make(map[*watcher]struct{}),
		ready:           make(chan struct{}),
		refreshRequest:  make(chan struct{}, 1),
		refreshInterval: refreshInterval,
	}
	go e.refresh()
	return e
}

func (e *API) refresh() {
	e.broadcast()
	close(e.ready)
	ticker := time.NewTicker(e.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.broadcast()
		case <-e.refreshRequest:
			e.broadcast()
		}
	}
}

func (e *API) requestRefresh() {
	select {
	case e.refreshRequest <- struct{}{}:
	default:
	}
}

func (e *API) broadcast() {
	instances, err := e.cli.FetchAllUpInstances(e.ctx)
	if e.ctx.Err() != nil {
		return
	}
	var items map[string][]Instance
	if err == nil {
		items = make(map[string][]Instance)
		for _, instance := range instances {
			appID := e.ToAppID(instance.App)
			items[appID] = append(items[appID], instance)
		}
	} else {
		log.Warnf("[eureka] refresh failed: %v", err)
	}

	e.lock.Lock()
	defer e.lock.Unlock()
	e.lastErr = err
	if err == nil {
		e.allInstances = items
	}
	// 首次失败通知等待者；已有成功快照时保留原状态，等待后续刷新。
	if err == nil || e.allInstances == nil {
		for w := range e.subscribers {
			w.notify()
		}
	}
}

func (e *API) Register(ctx context.Context, endpoints ...Endpoint) error {
	for _, ep := range endpoints {
		if err := e.cli.Register(ctx, ep); err != nil {
			return err
		}
	}
	return nil
}

func (e *API) Deregister(ctx context.Context, endpoints []Endpoint) error {
	for _, ep := range endpoints {
		if err := e.cli.Deregister(ctx, ep.AppID, ep.InstanceID); err != nil {
			return err
		}
	}
	return nil
}

func (e *API) addWatch(w *watcher) error {
	e.lock.Lock()
	defer e.lock.Unlock()
	if err := e.ctx.Err(); err != nil {
		return err
	}
	if err := w.ctx.Err(); err != nil {
		return err
	}
	e.subscribers[w] = struct{}{}
	if e.allInstances != nil {
		w.notify()
	}
	e.requestRefresh()
	return nil
}

func (e *API) removeWatch(w *watcher) {
	e.lock.Lock()
	delete(e.subscribers, w)
	e.lock.Unlock()
}

func (e *API) GetService(ctx context.Context, serverName string) ([]Instance, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.ctx.Done():
		return nil, e.ctx.Err()
	case <-e.ready:
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := e.ctx.Err(); err != nil {
		return nil, err
	}
	e.lock.RLock()
	defer e.lock.RUnlock()
	if e.allInstances == nil {
		return nil, e.lastErr
	}
	instances := e.allInstances[e.ToAppID(serverName)]
	result := make([]Instance, len(instances))
	for i, instance := range instances {
		result[i] = instance
		result[i].Metadata = maps.Clone(instance.Metadata)
	}
	return result, nil
}

func (e *API) ToAppID(serverName string) string {
	return strings.ToUpper(serverName)
}
