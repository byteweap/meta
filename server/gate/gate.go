package gate

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"

	"github.com/olahol/melody"
	"golang.org/x/sync/singleflight"

	"github.com/byteweap/meta"
	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/log"
	"github.com/byteweap/meta/component/registry"
	"github.com/byteweap/meta/component/selector"
	es "github.com/byteweap/meta/errors"
	"github.com/byteweap/meta/internal/cluster"
	"github.com/byteweap/meta/pkg/endpoint"
	"github.com/byteweap/meta/pkg/host"
	"github.com/byteweap/meta/server"
)

type Gate struct {
	*http.Server
	ln net.Listener

	ctx     context.Context
	appID   string // application ID
	appName string // application name

	opts     *options       // server options
	endpoint *url.URL       // server endpoint
	ws       *melody.Melody // WebSocket server
	sessions *Sessions      // player sessions

	lifecycleMu sync.Mutex
	started     bool
	stopped     bool
	ready       chan struct{}
	cancel      context.CancelFunc

	mu        sync.RWMutex
	selectors map[string]selector.Selector // 服务节点选择器 key: 服务名
	watchers  map[string]registry.Watcher  // 服务节点监听器 key: 服务名
	sfg       singleflight.Group
}

var _ server.Server = (*Gate)(nil)

func New(opts ...Option) *Gate {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	return &Gate{
		ctx:       context.Background(),
		opts:      o,
		Server:    &http.Server{},
		sessions:  newSessions(),
		selectors: make(map[string]selector.Selector),
		watchers:  make(map[string]registry.Watcher),
		ready:     make(chan struct{}),
	}
}

func (g *Gate) Kind() server.Kind {
	return server.KindGate
}

// Ready 返回本次启动的就绪信号。
func (g *Gate) Ready() <-chan struct{} { return g.ready }

func (g *Gate) validate() error {
	if g.opts == nil {
		return es.ErrOptionsRequired
	}
	if g.opts.userIDExtractor == nil {
		return es.ErrIDExtractorRequired
	}
	if g.opts.locator == nil {
		return es.ErrLocatorRequired
	}
	if g.opts.broker == nil {
		return es.ErrBrokerRequired
	}
	if g.opts.discovery == nil {
		return es.ErrDiscoveryRequired
	}
	if g.opts.selectorFunc == nil {
		return es.ErrSelectorRequired
	}
	return nil
}

func (g *Gate) setup(ctx context.Context, name, appID string) error {
	// base
	g.appID = appID
	g.appName = name
	g.ctx = ctx

	// 监听端口并设置 endpoint
	if err := g.listenAndEndpoint(); err != nil {
		return err
	}

	// websocket
	m, o := melody.New(), g.opts
	m.Config.WriteWait = o.writeTimeout
	m.Config.PongWait = o.pongTimeout
	m.Config.PingPeriod = o.pingInterval
	m.Config.MaxMessageSize = o.maxMessageSize
	m.Config.MessageBufferSize = o.websocketMessageBufferSize
	m.Config.ConcurrentMessageHandling = false

	m.HandleConnect(g.handleConnect)
	m.HandleDisconnect(g.handleDisconnect)
	m.HandleMessage(g.handleTextMessage)
	m.HandleMessageBinary(g.handleBinaryMessage)
	m.HandleError(g.handleError)
	m.HandleClose(g.handleClose)

	g.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != g.opts.path {
			http.NotFound(w, r)
			return
		}
		uid := g.opts.userIDExtractor(r)
		if uid <= 0 {
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return
		}
		_ = m.HandleRequestWithKeys(w, r, map[string]any{"uid": uid})
	})
	g.ws = m

	return nil
}

// Start 启动网关
func (g *Gate) Start(ctx context.Context) error {
	app, ok := meta.FromContext(ctx)
	if !ok {
		return es.ErrAppNotFound
	}
	// 验证参数
	if err := g.validate(); err != nil {
		return err
	}
	g.lifecycleMu.Lock()
	if g.stopped {
		g.lifecycleMu.Unlock()
		return context.Canceled
	}
	if g.started {
		g.lifecycleMu.Unlock()
		return errors.New("gate already started")
	}
	if err := ctx.Err(); err != nil {
		g.lifecycleMu.Unlock()
		return err
	}
	g.started = true
	runCtx, cancel := context.WithCancel(ctx)
	g.cancel = cancel
	// 启动前设置
	err := g.setup(runCtx, app.Name(), app.ID())
	ln := g.ln
	g.lifecycleMu.Unlock()
	defer cancel()
	if err != nil {
		return fmt.Errorf("setup failed: %w", err)
	}
	// 循环(常驻协程)
	if err := g.loop(); err != nil {
		if err == runCtx.Err() {
			return err
		}
		return fmt.Errorf("loop failed: %w", err)
	}
	g.lifecycleMu.Lock()
	if err := runCtx.Err(); err != nil {
		g.lifecycleMu.Unlock()
		return err
	}
	close(g.ready)
	g.lifecycleMu.Unlock()

	log.Infof("[gate] server started")
	log.Infof("[websocket] server listening on: %s", ln.Addr().String())

	// 启动服务
	if err := g.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Stop 停止网关
func (g *Gate) Stop(ctx context.Context) error {
	g.lifecycleMu.Lock()
	if g.stopped {
		g.lifecycleMu.Unlock()
		return nil
	}
	g.stopped = true
	if g.cancel != nil {
		g.cancel()
	}
	ln, ws := g.ln, g.ws
	g.lifecycleMu.Unlock()

	// 1. Shutdown http server
	e1 := g.Shutdown(ctx)
	if e1 != nil && ctx.Err() != nil {
		log.Warn("[websocket] server couldn't stop gracefully in time, doing force stop")
		e1 = errors.Join(e1, g.Close())
	}
	// Endpoint 可以在 Serve 前创建监听器，Shutdown 不会关闭这类监听器。
	if ln != nil {
		if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			e1 = errors.Join(e1, err)
		}
	}

	// 2. Close melody
	var e2 error
	if ws != nil {
		e2 = ws.Close()
	}

	err := errors.Join(e1, e2)

	// 3. 停止监听器
	g.mu.Lock()
	for _, watcher := range g.watchers {
		if e := watcher.Stop(); e != nil {
			err = errors.Join(err, e)
		}
	}
	g.mu.Unlock()

	if err != nil {
		return err
	}
	log.Info("[gate] server stopped")
	return nil
}

// Endpoint 获取网关地址
func (g *Gate) Endpoint(ctx context.Context) (*url.URL, error) {
	g.lifecycleMu.Lock()
	defer g.lifecycleMu.Unlock()
	if g.stopped {
		return nil, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := g.listenAndEndpoint(); err != nil {
		return nil, err
	}
	return g.endpoint, nil
}

// 监听端口并设置 endpoint
func (g *Gate) listenAndEndpoint() error {
	if g.ln == nil {
		ln, err := net.Listen("tcp", g.opts.addr)
		if err != nil {
			return err
		}
		g.ln = ln
	}
	if g.endpoint == nil {
		addr, err := host.Extract(g.opts.addr, g.ln)
		if err != nil {
			return err
		}
		g.endpoint = endpoint.NewEndpoint(endpoint.Scheme("ws", false), addr)
	}
	return nil
}

// 循环处理来自其它服务的消息
func (g *Gate) loop() error {
	var (
		o       = g.opts
		bro     = g.opts.broker
		msgChan = make(chan *broker.Message, o.brokerMessageBufferSize)
		subject = cluster.Subject(o.prefix, "*", g.appName, g.appID)
	)

	// 订阅消息
	sub, err := bro.Sub(g.ctx, subject, func(msg *broker.Message) {
		select {
		case msgChan <- msg:
		case <-g.ctx.Done():
		}
	})
	if err != nil {
		return err
	}

	// 处理收到的消息
	go func(ctx context.Context, sub broker.Subscription, ch <-chan *broker.Message) {
		defer func() {
			if err := sub.Close(); err != nil {
				log.Errorf("gate close subscription error: %v", err)
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-ch:
				if msg == nil {
					continue
				}
				g.handleMessage(msg)
			}
		}
	}(g.ctx, sub, msgChan)

	return nil
}

// Subject 获取当前服务主题
func (g *Gate) Subject(fromApp string) string {
	return cluster.Subject(g.opts.prefix, fromApp, g.appName, g.appID)
}

// 确保选择器
func (g *Gate) ensure(service string) (selector.Selector, error) {
	g.mu.RLock()
	sel := g.selectors[service]
	g.mu.RUnlock()
	if sel != nil {
		return sel, nil
	}

	v, err, _ := g.sfg.Do(service, func() (any, error) {
		g.mu.RLock()
		if existing := g.selectors[service]; existing != nil {
			g.mu.RUnlock()
			return existing, nil
		}
		g.mu.RUnlock()

		created := g.opts.selectorFunc()
		w, err := g.opts.discovery.Watch(g.ctx, service)
		if err != nil {
			log.Errorf("[websocket] ensure watch service error, service: %v, err: %v", service, err)
			return nil, err
		}

		g.mu.Lock()
		if existing := g.selectors[service]; existing != nil {
			g.mu.Unlock()
			if stopErr := w.Stop(); stopErr != nil {
				log.Errorf("[websocket] ensure stop redundant watcher error, service: %v, err: %v", service, stopErr)
			}
			return existing, nil
		}
		g.selectors[service] = created
		g.watchers[service] = w
		g.mu.Unlock()

		go g.watchSelector(created, w)
		return created, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(selector.Selector), nil
}

func (g *Gate) watchSelector(sel selector.Selector, w registry.Watcher) {
	for {
		select {
		case <-g.ctx.Done():
			return
		default:
			instances, err := w.Next()
			if err != nil {
				return
			}
			nodes := make([]selector.Node, 0, len(instances))
			for _, instance := range instances {
				nodes = append(nodes,
					selector.NewNode(
						instance.ID,
						instance.Name,
						instance.Version,
						instance.Metadata,
					),
				)
			}
			sel.Update(nodes) // 更新节点
		}
	}
}
