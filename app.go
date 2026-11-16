package meta

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"sync"

	"github.com/byteweap/meta/component/log"
	"github.com/byteweap/meta/component/registry"
)

type AppInfo interface {
	ID() string
	Name() string
	Version() string
	Metadata() map[string]string
	Endpoint() []string
}

// App 代表应用实例
type App struct {
	opts     *options
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	instance *registry.ServiceInstance
	started  bool
}

// New 创建应用实例
func New(opts ...Option) *App {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}
	if o.logger != nil {
		log.SetLogger(o.logger)
	}
	ctx, cancel := context.WithCancel(o.ctx)
	return &App{
		opts:   o,
		ctx:    ctx,
		cancel: cancel,
	}
}

// ID 返回应用实例 ID
func (a *App) ID() string { return a.opts.id }

// Name 返回服务名
func (a *App) Name() string { return a.opts.name }

// Version 返回应用版本
func (a *App) Version() string { return a.opts.version }

// Metadata 返回服务元数据
func (a *App) Metadata() map[string]string { return a.opts.metadata }

// Endpoint 返回服务端点
func (a *App) Endpoint() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.instance != nil {
		return a.instance.Endpoints
	}
	return nil
}

// Run 运行一次应用生命周期，等待服务退出和清理完成并返回所有错误。
func (a *App) Run() (err error) {
	a.mu.Lock()
	if a.started {
		a.mu.Unlock()
		return errors.New("app already started")
	}
	a.started = true
	a.mu.Unlock()

	ctx := NewContext(a.ctx, a)
	if len(a.opts.sigs) > 0 {
		signalCtx, stopSignals := signal.NotifyContext(ctx, a.opts.sigs...)
		defer stopSignals()
		ctx = signalCtx
	}
	results := make(chan error, len(a.opts.servers))
	started, registered := 0, false
	defer func() {
		if err == context.Canceled && ctx.Err() != nil {
			err = nil
		}
		a.cancel()
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.opts.stopTimeout)
		defer cancel()
		err = errors.Join(err, a.cleanup(stopCtx, registered, results, started))
	}()

	if ctx.Err() != nil {
		return nil
	}
	if err := a.buildInstance(ctx); err != nil {
		return err
	}
	for _, fn := range a.opts.preStartHooks {
		if ctx.Err() != nil {
			return nil
		}
		if err := fn(ctx); err != nil {
			return err
		}
	}
	for _, srv := range a.opts.servers {
		if ctx.Err() != nil {
			return nil
		}
		started++
		go func() {
			if ctx.Err() != nil {
				results <- nil
				return
			}
			startErr := srv.Start(ctx)
			if startErr == context.Canceled && ctx.Err() != nil {
				startErr = nil
			} else if startErr == nil && ctx.Err() == nil {
				startErr = fmt.Errorf("%s server stopped unexpectedly", srv.Kind())
			}
			results <- startErr
			a.cancel()
		}()
	}
	for _, srv := range a.opts.servers {
		select {
		case <-ctx.Done():
			return nil
		case <-srv.Ready():
		}
	}
	if ctx.Err() != nil {
		return nil
	}
	if err := a.register(ctx); err != nil {
		return err
	}
	registered = a.opts.registry != nil
	for _, fn := range a.opts.postStartHooks {
		if ctx.Err() != nil {
			return nil
		}
		if err := fn(ctx); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return nil
}

// Stop 请求停止应用。Run 负责等待清理完成并返回清理错误。
func (a *App) Stop() error {
	a.cancel()
	return nil
}

func (a *App) cleanup(ctx context.Context, registered bool, results <-chan error, started int) error {
	var err error
	for _, fn := range a.opts.preStopHooks {
		err = errors.Join(err, fn(ctx))
	}
	if registered {
		err = errors.Join(err, a.deregister(ctx))
	}
	// Endpoint 也可能分配资源，尚未 Start 的服务同样需要停止。
	for _, srv := range a.opts.servers {
		err = errors.Join(err, srv.Stop(ctx))
	}
	err = errors.Join(err, waitServers(ctx, results, started))
	for _, fn := range a.opts.postStopHooks {
		err = errors.Join(err, fn(ctx))
	}
	return err
}

func waitServers(ctx context.Context, results <-chan error, count int) error {
	var err error
	for range count {
		select {
		case result := <-results:
			err = errors.Join(err, result)
		case <-ctx.Done():
			// 清理预算耗尽时，仍保留已经返回的启动错误。
			for {
				select {
				case result := <-results:
					err = errors.Join(err, result)
				default:
					return errors.Join(err, ctx.Err())
				}
			}
		}
	}
	return err
}

// buildInstance 构建服务实例
func (a *App) buildInstance(ctx context.Context) error {
	endpoints := make([]string, 0, len(a.opts.endpoints))
	for _, e := range a.opts.endpoints {
		endpoints = append(endpoints, e.String())
	}
	if len(endpoints) == 0 {
		for _, srv := range a.opts.servers {
			e, err := srv.Endpoint(ctx)
			if err != nil {
				return err
			}
			endpoints = append(endpoints, e.String())
		}
	}
	instance := &registry.ServiceInstance{
		ID:        a.opts.id,
		Name:      a.opts.name,
		Version:   a.opts.version,
		Metadata:  a.opts.metadata,
		Endpoints: endpoints,
	}
	a.mu.Lock()
	a.instance = instance
	a.mu.Unlock()

	return nil
}

// register 向注册中心注册服务
func (a *App) register(ctx context.Context) error {
	a.mu.Lock()
	instance := a.instance
	a.mu.Unlock()

	if a.opts.registry != nil && instance != nil {
		regCtx, regCancel := context.WithTimeout(ctx, a.opts.registryTimeout)
		defer regCancel()
		if err := a.opts.registry.Register(regCtx, instance); err != nil {
			return err
		}
	}
	return nil
}

// deregister 从注册中心注销服务
func (a *App) deregister(ctx context.Context) error {
	a.mu.Lock()
	instance := a.instance
	a.mu.Unlock()

	if a.opts.registry != nil && instance != nil {
		regCtx, regCancel := context.WithTimeout(ctx, a.opts.registryTimeout)
		defer regCancel()
		if err := a.opts.registry.Deregister(regCtx, instance); err != nil {
			return err
		}
	}
	return nil
}

// appKey 用于在 context 中存放 AppInfo
type appKey struct{}

// NewContext 返回携带 AppInfo 的新 Context
func NewContext(ctx context.Context, s AppInfo) context.Context {
	return context.WithValue(ctx, appKey{}, s)
}

// FromContext 从 Context 中取出 AppInfo
func FromContext(ctx context.Context) (s AppInfo, ok bool) {
	s, ok = ctx.Value(appKey{}).(AppInfo)
	return
}
