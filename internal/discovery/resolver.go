// Package discovery 在进程内按服务名共享注册中心快照和监听
package discovery

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"slices"
	"strings"
	"sync"

	"github.com/byteweap/meta/component/log"
	"github.com/byteweap/meta/component/registry"
	"github.com/byteweap/meta/component/selector"
)

// ErrNotFound 表示当前快照没有目标服务的可用实例
var ErrNotFound = fmt.Errorf("service not found: %w", selector.ErrNoAvailableNode)

// Service 持有同一代监听的选择器和实例投递地址
type Service struct {
	selector.Selector
	mu       sync.RWMutex
	subjects map[string]string
	watcher  registry.Watcher
	stop     func() error
}

// Subject 返回指定实例的投递地址，实例已下线时返回空字符串
func (s *Service) Subject(id string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.subjects[id]
}

// SelectSubject 从同一份快照选取一个实例并返回其投递地址
func (s *Service) SelectSubject(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	node, err := s.Select(key)
	if err != nil {
		return "", err
	}
	if node != nil {
		if subject := s.subjects[node.ID()]; subject != "" {
			return subject, nil
		}
	}
	return "", selector.ErrNoAvailableNode
}

func matches(name string, i *registry.ServiceInstance) bool {
	return i != nil && i.ID != "" && i.Name == name
}

func (r *Resolver) update(s *Service, name string, instances []*registry.ServiceInstance) bool {
	subjects := make(map[string]string, len(instances))
	nodes := make([]selector.Node, 0, len(instances))
	for _, i := range instances {
		if !matches(name, i) {
			continue
		}
		if _, ok := subjects[i.ID]; ok {
			continue
		}
		subject := r.subject(name, i.ID)
		if subject == "" {
			continue
		}
		subjects[i.ID] = subject
		nodes = append(nodes, selector.NewNode(i.ID, i.Name, i.Version, i.Metadata))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subjects = subjects
	s.Update(nodes)
	return len(nodes) != 0
}

type entry struct {
	ready   chan struct{}
	cancel  context.CancelFunc
	waiters int
	service *Service
	err     error
}

// Resolver 为每个使用中的服务维护一次初始化和一个监听
type Resolver struct {
	ctx         context.Context
	cancel      context.CancelFunc
	registry    registry.Registry
	newSelector func() selector.Selector
	subject     func(name, id string) string
	mu          sync.RWMutex
	entries     map[string]*entry
	tasks       sync.WaitGroup
	closeErr    error
}

// New 创建从属于 ctx 的发现缓存。subject 在快照更新时构造地址，返回空串时跳过该实例
func New(ctx context.Context, reg registry.Registry, newSelector func() selector.Selector, subject func(name, id string) string) *Resolver {
	ctx, cancel := context.WithCancel(ctx)
	return &Resolver{ctx: ctx, cancel: cancel, registry: reg, newSelector: newSelector, subject: subject, entries: make(map[string]*entry)}
}

// Cached 返回已初始化的服务，未就绪或已移除时返回 nil
func (r *Resolver) Cached(name string) *Service {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if e := r.entries[name]; e != nil {
		return e.service
	}
	return nil
}

// Resolve 等待可用快照，最后一个等待者取消时一并取消尚未完成的初始化
func (r *Resolver) Resolve(ctx context.Context, name string) (*Service, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	if s := r.Cached(name); s != nil {
		return s, nil
	}
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\\*> \t\r\n\x00") {
		return nil, ErrNotFound
	}
	r.mu.Lock()
	if err := r.ctx.Err(); err != nil {
		r.mu.Unlock()
		return nil, err
	}
	e := r.entries[name]
	if e == nil {
		lookup, cancel := context.WithCancel(r.ctx)
		e = &entry{ready: make(chan struct{}), cancel: cancel}
		r.entries[name] = e
		r.tasks.Go(func() { r.run(lookup, name, e) })
	}
	e.waiters++
	r.mu.Unlock()
	select {
	case <-ctx.Done():
		r.leave(name, e)
		return nil, ctx.Err()
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	case <-e.ready:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return e.service, e.err
	}
}

// leave 在最后一个等待者取消时释放未完成的查询，下一次请求可以立即重新发现
func (r *Resolver) leave(name string, e *entry) {
	r.mu.Lock()
	e.waiters--
	abandoned := e.waiters == 0 && e.service == nil && r.entries[name] == e
	if abandoned {
		delete(r.entries, name)
	}
	r.mu.Unlock()
	if abandoned {
		e.cancel()
	}
}

func (r *Resolver) run(ctx context.Context, name string, e *entry) {
	defer e.cancel()
	s, err := r.open(ctx, name, e.cancel)
	r.mu.Lock()
	if err == nil {
		err = ctx.Err()
		if r.entries[name] != e {
			err = context.Canceled
		}
	}
	if err != nil {
		if r.entries[name] == e {
			delete(r.entries, name)
		}
		r.mu.Unlock()
		if s != nil {
			err = errors.Join(err, s.stop())
		}
		e.err = err
		close(e.ready)
		return
	}
	e.service = s
	close(e.ready)
	r.mu.Unlock()

	if watchErr := r.watch(ctx, name, s); watchErr != nil && ctx.Err() == nil {
		log.Errorf("watch service %q: %v", name, watchErr)
	}
	r.mu.Lock()
	delete(r.entries, name)
	r.mu.Unlock()
	_ = s.stop()
}

func (r *Resolver) watch(ctx context.Context, name string, s *Service) (err error) {
	defer recoverDiscovery(&err)
	// 先清空旧地址，再移除缓存和关闭监听，旧引用也会立即失效
	defer r.update(s, name, nil)
	for ctx.Err() == nil {
		instances, nextErr := s.watcher.Next()
		if nextErr != nil {
			return nextErr
		}
		if ctx.Err() != nil || !r.update(s, name, instances) {
			break
		}
	}
	return nil
}

func (r *Resolver) open(ctx context.Context, name string, cancel context.CancelFunc) (s *Service, err error) {
	defer recoverDiscovery(&err)
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	instances, err := r.registry.GetService(ctx, name)
	if err != nil {
		return nil, err
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if !slices.ContainsFunc(instances, func(i *registry.ServiceInstance) bool {
		return matches(name, i) && r.subject(name, i.ID) != ""
	}) {
		return nil, ErrNotFound
	}
	w, err := r.registry.Watch(ctx, name)
	stop := sync.OnceValue(func() error {
		cancel()
		if w == nil {
			return nil
		}
		closeErr := stopWatcher(w)
		if closeErr != nil {
			r.mu.Lock()
			r.closeErr = errors.Join(r.closeErr, closeErr)
			r.mu.Unlock()
			log.Errorf("close service watcher %q: %v", name, closeErr)
		}
		return closeErr
	})
	if err != nil {
		return nil, errors.Join(err, stop())
	}
	if w == nil {
		cancel()
		return nil, errors.New("registry returned a nil watcher")
	}
	// 某些 Watcher 需要显式 Stop 才能唤醒 Next
	detach := context.AfterFunc(ctx, func() { _ = stop() })
	// 先保留监听器，后续快照或选择器回调失败时由 run 统一关闭
	s = &Service{watcher: w, stop: func() error {
		detach()
		return stop()
	}}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return s, ctxErr
	}
	// Watch 生效后再次查询，覆盖首次查询与建立监听之间的实例变化
	instances, err = r.registry.GetService(ctx, name)
	if err != nil {
		return s, err
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return s, ctxErr
	}
	s.Selector = r.newSelector()
	if s.Selector == nil {
		return s, errors.New("selector factory returned nil")
	}
	if !r.update(s, name, instances) {
		return s, ErrNotFound
	}
	return s, nil
}

func stopWatcher(w registry.Watcher) (err error) {
	defer recoverDiscovery(&err)
	return w.Stop()
}

// recoverDiscovery 将后台组件回调的 panic 纳入正常的失效和清理流程
func recoverDiscovery(err *error) {
	if v := recover(); v != nil {
		*err = fmt.Errorf("service discovery panic: %v", v)
		log.Errorf("%v\n%s", *err, debug.Stack())
	}
}

// Close 取消所有发现任务，等待查询和监听退出并返回累计的关闭错误
func (r *Resolver) Close() error {
	r.mu.Lock()
	r.cancel()
	r.mu.Unlock()
	r.tasks.Wait()
	return r.closeErr
}
