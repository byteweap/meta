package mesh

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"runtime/debug"
	"sync"

	"github.com/byteweap/meta"
	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/log"
	"github.com/byteweap/meta/encoding/proto"
	"github.com/byteweap/meta/envelope"
	es "github.com/byteweap/meta/errors"
	"github.com/byteweap/meta/internal/cluster"
	"github.com/byteweap/meta/pkg/async"
	"github.com/byteweap/meta/pkg/conv"
	"github.com/byteweap/meta/server"
)

type Mesh struct {
	ctx     context.Context
	appID   string // application ID
	appName string // application name
	running bool
	started bool
	stopped bool
	ready   chan struct{}

	opts          *options
	routes        sync.Map // key: cmd<<32|version (uint64), value: MessageHandler
	requestRoutes sync.Map // key: cmd.version (string), value: RPCMessageHandler

	onlineHandler    func(uid int64) // 玩家上线
	offlineHandler   func(uid int64) // 玩家掉线
	reconnectHandler func(uid int64) // 玩家重连

	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
}

const meshScheme = "mesh"

var _ server.Server = (*Mesh)(nil)

// New 创建 Mesh 服务实例
func New(opts ...Option) *Mesh {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}
	return &Mesh{opts: o, ready: make(chan struct{})}
}

// Kind 返回服务类型
func (*Mesh) Kind() server.Kind {
	return server.KindMesh
}

// Ready 返回本次启动的就绪信号。
func (m *Mesh) Ready() <-chan struct{} { return m.ready }

// Start 启动 Mesh 服务
func (m *Mesh) Start(ctx context.Context) error {
	app, ok := meta.FromContext(ctx)
	if !ok {
		return es.ErrAppNotFound
	}

	if m.opts.broker == nil {
		return es.ErrBrokerRequired
	}
	if m.opts.locator == nil {
		return es.ErrLocatorRequired
	}
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return context.Canceled
	}
	if m.started {
		m.mu.Unlock()
		return fmt.Errorf("mesh already started")
	}
	if err := ctx.Err(); err != nil {
		m.mu.Unlock()
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	m.ctx = runCtx
	m.cancel = cancel
	m.appID = app.ID()
	m.appName = app.Name()
	m.done = make(chan struct{})
	m.running = true
	m.started = true
	m.mu.Unlock()

	defer func() {
		cancel()
		m.mu.Lock()
		if m.done != nil {
			close(m.done)
			m.done = nil
		}
		m.running = false
		m.cancel = nil
		m.mu.Unlock()
	}()

	// 启动常驻协程
	return m.loop()
}

// Stop 停止 Mesh 服务
func (m *Mesh) Stop(ctx context.Context) error {
	m.mu.Lock()
	m.stopped = true
	if !m.running {
		m.mu.Unlock()
		return nil
	}
	cancel := m.cancel
	done := m.done
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done == nil {
		return nil
	}
	select {
	case <-done:
		log.Infof("%s.%s server stop success", m.appName, m.appID)
		return nil
	case <-ctx.Done():
		log.Warnf("%s.%s server stop timeout", m.appName, m.appID)
		return ctx.Err()
	}
}

// Endpoint 返回服务监听地址
func (m *Mesh) Endpoint(ctx context.Context) (*url.URL, error) {
	app, ok := meta.FromContext(ctx)
	if !ok {
		return nil, es.ErrAppNotFound
	}
	host := app.Name() + "." + app.ID()
	return &url.URL{
		Scheme: meshScheme,
		Host:   host + ":0000",
	}, nil
}

// routeKey 将 cmd/version 打包为 uint64 路由键
func routeKey(cmd, version uint32) uint64 {
	return uint64(cmd)<<32 | uint64(version)
}

func requestRouteKey(cmd, version string) string {
	return cmd + "." + version
}

// OnlineHandler 玩家上线事件处理器
func (m *Mesh) OnlineHandler(handler func(uid int64)) {
	if handler == nil {
		return
	}
	m.onlineHandler = handler
}

// OfflineHandler 玩家掉线事件处理器
func (m *Mesh) OfflineHandler(handler func(uid int64)) {
	if handler == nil {
		return
	}
	m.offlineHandler = handler
}

// ReconnectHandler 玩家重连事件处理器
func (m *Mesh) ReconnectHandler(handler func(uid int64)) {
	if handler == nil {
		return
	}
	m.reconnectHandler = handler
}

// RouteX 注册业务路由处理器(Gate pub-sub)
// cmd/version 共同确定唯一路由
// handler 支持两种写法，推荐直接传业务函数
//
// 1) 推荐写法
// func(ctx *Context, req *Request)
// 示例: mesh.RouteX(cmd, version, EnterGame)
//
// 2) 兼容写法
// MessageHandler
// 示例: mesh.RouteX(cmd, version, mesh.Wrap(EnterGame))
//
// 如果 handler 签名不合法，函数会 panic
// 注意: 使用反射, 热点路由请使用 Route
func (m *Mesh) RouteX(cmd, version uint32, handler any) {
	mh, err := adaptMessageHandler(handler)
	if err != nil {
		panic(err)
	}
	key := routeKey(cmd, version)
	m.routes.Store(key, mh)
}

// Route 注册业务路由处理器
// 该方法要求显式传入 MessageHandler（通常通过 mesh.Wrap 构造）
// 运行期不经过反射调用，适合高频热点路由
func (m *Mesh) Route(cmd, version uint32, handler MessageHandler) {
	if handler == nil {
		panic("mesh: handler is nil")
	}
	key := routeKey(cmd, version)
	m.routes.Store(key, handler)
}

// RPCRouteX 注册 request-reply 路由处理器
// cmd/version 共同确定唯一路由
// handler 支持两种写法，推荐直接传业务函数
//
// 1) 推荐写法
// func(ctx *RPCContext, req *Request) ([]byte, string, int)
// 示例: mesh.RPCRouteX(cmd, version, HandleRequest)
//
// 2) 兼容写法
// RPCMessageHandler
// 示例: mesh.RPCRouteX(cmd, version, mesh.WrapRPC(HandleRequest))
//
// 如果 handler 签名不合法，函数会 panic
// 注意: 使用反射, 热点路由请使用 RPCRoute
func (m *Mesh) RPCRouteX(cmd, version string, handler any) {
	mh, err := adaptRPCMessageHandler(handler)
	if err != nil {
		panic(err)
	}
	key := requestRouteKey(cmd, version)
	m.requestRoutes.Store(key, mh)
}

// RPCRoute 注册 request-reply 路由处理器
// 该方法要求显式传入 RPCMessageHandler（通常通过 mesh.WrapRPC 构造）
// 运行期不经过反射调用，适合高频热点路由
func (m *Mesh) RPCRoute(cmd, version string, handler RPCMessageHandler) {
	if handler == nil {
		panic("mesh: request-reply handler is nil")
	}
	key := requestRouteKey(cmd, version)
	m.requestRoutes.Store(key, handler)
}

// loop 循环
func (m *Mesh) loop() error {
	var (
		o       = m.opts
		subject = cluster.Subject(o.prefix, "*", m.appName, m.appID)
		msgChan = make(chan *broker.Message, o.messageBufferSize)
	)

	// 订阅
	sub, err := o.broker.Sub(m.ctx, subject, func(msg *broker.Message) {
		select {
		case msgChan <- msg:
		case <-m.ctx.Done():
		}
	})
	if err != nil {
		return err
	}

	defer func() {
		if err := sub.Close(); err != nil {
			log.Errorf("mesh close subscription error: %v", err)
		}
	}()
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return context.Canceled
	}
	if err := m.ctx.Err(); err != nil {
		m.mu.Unlock()
		return err
	}
	close(m.ready)
	m.mu.Unlock()

	log.Infof("%s.%s server start success", m.appName, m.appID)

	for {
		select {
		case <-m.ctx.Done():
			return nil
		case msg := <-msgChan:
			if msg == nil {
				continue
			}
			m.handlerMessage(msg)
		}
	}
}

// Request 发送请求并等待回复
// 入参
//   - subject: 目标服务
//   - cmd: 命令
//   - version: 版本
//   - data: 业务数据
//
// 出参
//   - data: 业务数据
//   - tip: 提示信息
//   - code: 业务状态码
//   - error: 错误信息
func (m *Mesh) Request(subject, cmd, version string, data []byte) ([]byte, string, int, error) {
	header := broker.Header{}
	header.Set("cmd", cmd)
	header.Set("version", version)
	result, err := m.opts.broker.Request(m.ctx, subject, data, broker.RequestHeader(header))
	if err != nil {
		return nil, "", 0, err
	}
	code := result.Header.Get("code")
	tip := result.Header.Get("tip")
	return result.Data, tip, conv.Int(code), nil
}

// sendMessage 发送消息
func (m *Mesh) sendMessage(subject, toService string, bytes []byte, uids ...int64) error {
	var err error
	for _, uid := range uids {
		header := cluster.BuildHeader(uid, cluster.EventBusiness, "", m.appName, toService)
		if e := m.opts.broker.Pub(m.ctx, subject, bytes, broker.PubHeader(header)); e != nil {
			err = errors.Join(err, e)
		}
	}
	return err
}

// okReply 发送成功回复
func (m *Mesh) okReply(reqMsg *broker.Message, data []byte) error {
	if reqMsg == nil {
		return nil
	}
	if reqMsg.Reply == "" {
		return errors.New("reply subject is empty")
	}
	header := reqMsg.Header
	if header == nil {
		header = broker.Header{}
	}
	header.Set("code", "200")
	header.Set("tip", "ok")
	return m.opts.broker.Reply(m.ctx, reqMsg, data, broker.ReplyHeader(header))
}

// errReply 发送错误回复
func (m *Mesh) errReply(reqMsg *broker.Message, code int, tip string) error {
	if reqMsg == nil {
		return errors.New("request message is nil")
	}
	if reqMsg.Reply == "" {
		return errors.New("reply subject is empty")
	}
	header := reqMsg.Header
	if header == nil {
		header = broker.Header{}
	}
	header.Set("code", conv.String(code))
	header.Set("tip", tip)
	return m.opts.broker.Reply(m.ctx, reqMsg, nil, broker.ReplyHeader(header))
}

// handlerRequestReplyMessage 来自其它服务的(request-reply)消息
func (m *Mesh) handlerRequestReplyMessage(msg *broker.Message) {
	if msg == nil {
		return
	}
	header := msg.Header
	if header == nil {
		if err := m.errReply(msg, 100, "header is nil"); err != nil {
			log.Errorf("mesh [handlerRequestReplyMessage] reply error: %v", err)
		}
		return
	}
	cmd, version := header.Get("cmd"), header.Get("version")
	if handler, ok := m.requestRoutes.Load(requestRouteKey(cmd, version)); ok {
		data, tip, code := m.invokeRPC(handler.(RPCMessageHandler), msg)
		m.replyRequestResult(msg, data, tip, code)
	} else {
		if err := m.errReply(msg, 404, fmt.Sprintf("cmd:%s version:%s not found", cmd, version)); err != nil {
			log.Errorf("mesh [handlerRequestReplyMessage] reply error: %v", err)
		}
	}
}

func (m *Mesh) replyRequestResult(msg *broker.Message, data []byte, tip string, code int) {
	if code == http.StatusOK {
		if err := m.okReply(msg, data); err != nil {
			log.Errorf("mesh request-reply ok reply error: %v", err)
		}
		return
	}
	if tip == "" {
		tip = "request failed"
	}
	if code == 0 {
		code = http.StatusInternalServerError
	}
	if err := m.errReply(msg, code, tip); err != nil {
		log.Errorf("mesh request-reply err reply error: %v", err)
	}
}

// handlerPubSubMessage 来自Gate的(pub-sub)消息
func (m *Mesh) handlerPubSubMessage(msg *broker.Message) {
	switch cluster.GetEventBy(msg.Header) {
	case cluster.EventOnline:
		m.handleSessionEvent(msg, m.onlineHandler)
	case cluster.EventOffline:
		m.handleSessionEvent(msg, m.offlineHandler)
	case cluster.EventReconnect:
		m.handleSessionEvent(msg, m.reconnectHandler)
	default:
		e := &envelope.IMessage{}
		if err := proto.Unmarshal(msg.Data, e); err != nil {
			log.Errorf("mesh unmarshal Gate2MeshEnvelope error: %v", err)
			return
		}
		header := e.GetHeader()
		if header == nil {
			log.Error("mesh Gate2MeshEnvelope header is required")
			return
		}
		cmd, version := header.GetCmd(), header.GetVersion()
		if handler, ok := m.routes.Load(routeKey(cmd, version)); ok {
			handler.(MessageHandler)(m, msg, e)
		}
	}
}

// handleSessionEvent 只分发读取时仍与当前会话状态一致的事件。
// 定位器查询与业务回调不是同一个事务，回调执行期间仍可能发生重连。
func (m *Mesh) handleSessionEvent(msg *broker.Message, handler func(int64)) {
	if handler == nil {
		return
	}
	uid := cluster.GetUIDBy(msg.Header)
	service := cluster.GetFromServiceBy(msg.Header)
	sessionID := cluster.GetSessionIDBy(msg.Header)
	if uid <= 0 || service == "" || sessionID == "" {
		return
	}
	current, err := m.opts.locator.Session(m.ctx, uid, service)
	if err != nil {
		log.Errorf("mesh session event lookup failed, uid: %v, service: %v, err: %v", uid, service, err)
		return
	}
	online := cluster.GetEventBy(msg.Header) != cluster.EventOffline
	if current.ID != sessionID || (current.Node != "") != online {
		return
	}
	handler(uid)
}

// 处理消息
func (m *Mesh) handlerMessage(msg *broker.Message) {
	defer async.Recover(func(r any) {
		log.Errorf("mesh message handler panic: %v\n%s", r, debug.Stack())
	})
	if msg.Reply != "" {
		m.handlerRequestReplyMessage(msg)
	} else {
		m.handlerPubSubMessage(msg)
	}
}
