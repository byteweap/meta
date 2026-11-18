package mesh

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/byteweap/meta"
	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/log"
	"github.com/byteweap/meta/encoding/proto"
	"github.com/byteweap/meta/envelope"
	es "github.com/byteweap/meta/errors"
	"github.com/byteweap/meta/internal/cluster"
	"github.com/byteweap/meta/internal/discovery"
	"github.com/byteweap/meta/pkg/async"
	"github.com/byteweap/meta/pkg/conv"
	"github.com/byteweap/meta/server"
)

type Mesh struct {
	ctx     context.Context
	appID   string // application ID
	appName string // application name
	started bool
	stopped bool
	ready   chan struct{}

	opts          *options
	resolver      *discovery.Resolver
	routes        sync.Map // key: cmd<<32|version (uint64), value: MessageHandler
	requestRoutes sync.Map // key: cmd.version (string), value: RPCMessageHandler

	replyCtx    context.Context // 仅由消息循环使用
	cancelReply context.CancelFunc

	onlineHandler    func(uid int64) // 玩家上线
	offlineHandler   func(uid int64) // 玩家掉线
	reconnectHandler func(uid int64) // 玩家重连

	cancel  context.CancelFunc
	done    chan struct{}
	mu      sync.Mutex
	stopErr error
}

const meshScheme = "mesh"

var _ server.Server = (*Mesh)(nil)

// New 创建 Mesh 服务实例
func New(opts ...Option) *Mesh {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Mesh{ctx: ctx, cancel: cancel, opts: o, ready: make(chan struct{})}
}

// Kind 返回服务类型
func (*Mesh) Kind() server.Kind {
	return server.KindMesh
}

// Ready 返回本次启动的就绪信号。
func (m *Mesh) Ready() <-chan struct{} { return m.ready }

// Start 启动 Mesh 服务。前缀、服务名和实例 ID 含 Broker 通配符时返回错误
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
	name, id := app.Name(), app.ID()
	// 入站订阅只允许框架在发送方字段放置通配符，否则定点请求也可能被其他实例处理
	if strings.ContainsAny(m.opts.prefix, "*>") || strings.ContainsAny(name, "*>") || strings.ContainsAny(id, "*>") {
		m.mu.Unlock()
		return fmt.Errorf("wildcard in mesh address: prefix=%q service=%q id=%q", m.opts.prefix, name, id)
	}
	runCtx, cancel := context.WithCancel(ctx)
	// 在启动前发出的 RPC 也随本次运行结束而取消
	context.AfterFunc(runCtx, m.cancel)
	m.ctx = runCtx
	m.cancel = cancel
	m.appID = id
	m.appName = name
	if m.opts.discovery != nil && m.opts.selectorFunc != nil {
		m.resolver = discovery.New(runCtx, m.opts.discovery, m.opts.selectorFunc, func(service, id string) string {
			if !validSubjectToken(id) {
				return ""
			}
			return cluster.Subject(m.opts.prefix, m.appName, service, id)
		})
	}
	m.done = make(chan struct{})
	m.started = true
	m.mu.Unlock()

	defer func() {
		if m.cancelReply != nil {
			m.cancelReply()
		}
		cancel()
		var closeErr error
		if m.resolver != nil {
			closeErr = m.resolver.Close()
		}
		m.mu.Lock()
		m.stopErr = closeErr
		if m.done != nil {
			close(m.done)
			m.done = nil
		}
		m.cancel = nil
		m.mu.Unlock()
	}()

	// 启动常驻协程
	return m.loop()
}

// Stop 停止 Mesh 服务，在 ctx 的期限内等待清理，并返回服务发现监听器的关闭错误
func (m *Mesh) Stop(ctx context.Context) error {
	m.mu.Lock()
	m.stopped = true
	cancel := m.cancel
	done := m.done
	closeErr := m.stopErr
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done == nil {
		return closeErr
	}
	select {
	case <-done:
		// 关闭 done 前已经写入最终结果，Mesh 实例只启动一次
		if m.stopErr != nil {
			return m.stopErr
		}
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

// Route 注册客户端业务路由。handler 为 nil 或 cmd/version 已注册时 panic，原注册仍有效
// Handle 和 Wrap 均支持类型化业务函数，运行期直接调用
func (m *Mesh) Route(cmd, version uint32, handler MessageHandler) {
	if handler == nil {
		panic("mesh: handler is nil")
	}
	key := routeKey(cmd, version)
	if _, loaded := m.routes.LoadOrStore(key, handler); loaded {
		panic(fmt.Sprintf("mesh: duplicate client route cmd=%d version=%d", cmd, version))
	}
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

const (
	rpcDeadlineHeader    = "deadline_unix_ms"
	rpcMissingHeaderCode = 100
	rpcReplyTimeout      = time.Second
)

// Request 向 subject 发送 RPC，返回响应数据、提示、业务状态码和传输错误
// ctx 必须非 nil。调用方取消或 Mesh 停止时结束等待。有截止时间时将其按 Unix 毫秒传给下游
// 主动取消只影响本地等待，已发出的业务可能继续执行。可在启动前调用，未配置 Broker 时返回 ErrBrokerRequired
func (m *Mesh) Request(ctx context.Context, subject, cmd, version string, data []byte) ([]byte, string, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", 0, err
	}
	m.mu.Lock()
	runCtx, stopped := m.ctx, m.stopped
	m.mu.Unlock()
	if stopped {
		return nil, "", 0, context.Canceled
	}
	if err := runCtx.Err(); err != nil {
		return nil, "", 0, err
	}
	if m.opts.broker == nil {
		return nil, "", 0, es.ErrBrokerRequired
	}

	// 外部传入的上下文也要随本 Mesh 停止，使用同一取消信号时直接复用
	if done := runCtx.Done(); done != nil && done != ctx.Done() {
		var cancel context.CancelFunc
		if limit, ok := runCtx.Deadline(); ok {
			ctx, cancel = context.WithDeadline(ctx, limit)
		} else {
			ctx, cancel = context.WithCancel(ctx)
		}
		stop := context.AfterFunc(runCtx, func() {
			// 到期由请求自身的截止时间处理，保留 DeadlineExceeded
			if runCtx.Err() == context.Canceled {
				cancel()
			}
		})
		defer cancel()
		defer stop()
	}
	header := broker.Header{"cmd": {cmd}, "version": {version}}
	if deadline, ok := ctx.Deadline(); ok {
		if time.Until(deadline) <= 0 {
			return nil, "", 0, context.DeadlineExceeded
		}
		header.Set(rpcDeadlineHeader, strconv.FormatInt(deadline.UnixMilli(), 10))
	}
	result, err := m.opts.broker.Request(ctx, subject, data, broker.RequestHeader(header))
	if err != nil {
		return nil, "", 0, err
	}
	return result.Data, result.Header.Get("tip"), conv.Int(result.Header.Get("code")), nil
}

func (m *Mesh) sendResponse(subject, toService string, data []byte, uid int64, sessionID string) error {
	header := cluster.BuildHeader(uid, cluster.EventBusiness, "", m.appName, toService)
	header.Set(cluster.FieldNameDelivery, cluster.DeliveryResponse)
	if sessionID != "" {
		header.Set(cluster.FieldNameSessionID, sessionID)
	}
	return m.opts.broker.Pub(m.ctx, subject, data, broker.PubHeader(header))
}

// handlerRequestReplyMessage 来自其它服务的(request-reply)消息
func (m *Mesh) handlerRequestReplyMessage(msg *broker.Message) {
	if msg == nil {
		return
	}
	data, tip, code := m.dispatchRPC(msg)
	m.replyRequestResult(msg, data, tip, code)
}

// dispatchRPC 在出队后校验绝对截止时间，收到请求之前的等待也计入调用预算
func (m *Mesh) dispatchRPC(msg *broker.Message) ([]byte, string, int) {
	header := msg.Header
	if header == nil {
		return nil, "header is nil", rpcMissingHeaderCode
	}
	ctx := m.ctx
	if err := ctx.Err(); err != nil {
		if err == context.DeadlineExceeded {
			return nil, http.StatusText(http.StatusGatewayTimeout), http.StatusGatewayTimeout
		}
		return nil, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable
	}
	var deadline time.Time
	if values, ok := header[rpcDeadlineHeader]; ok {
		if len(values) != 1 {
			return nil, "invalid RPC deadline", http.StatusBadRequest
		}
		ms, err := strconv.ParseInt(values[0], 10, 64)
		if err != nil {
			return nil, "invalid RPC deadline", http.StatusBadRequest
		}
		deadline = time.UnixMilli(ms)
		if !time.Now().Before(deadline) {
			return nil, http.StatusText(http.StatusGatewayTimeout), http.StatusGatewayTimeout
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	cmd, version := header.Get("cmd"), header.Get("version")
	handler, ok := m.requestRoutes.Load(requestRouteKey(cmd, version))
	if !ok {
		return nil, fmt.Sprintf("cmd:%s version:%s not found", cmd, version), http.StatusNotFound
	}
	data, tip, code := m.invoke(ctx, handler.(RPCMessageHandler), msg)
	if ctx.Err() == context.DeadlineExceeded || (!deadline.IsZero() && !time.Now().Before(deadline)) {
		return nil, http.StatusText(http.StatusGatewayTimeout), http.StatusGatewayTimeout
	}
	return data, tip, code
}

func (m *Mesh) replyRequestResult(msg *broker.Message, data []byte, tip string, code int) {
	if msg.Reply == "" {
		log.Error("mesh request-reply: reply subject is empty")
		return
	}
	header := msg.Header
	if header == nil {
		header = broker.Header{}
	}
	if code == http.StatusOK {
		header.Set("code", "200")
		tip = "ok"
	} else {
		data = nil
		if code == 0 {
			code = http.StatusInternalServerError
		}
		header.Set("code", strconv.Itoa(code))
		if tip == "" {
			tip = "request failed"
		}
	}
	header.Set("tip", tip)

	if err := m.opts.broker.Reply(m.replyContext(), msg, data, broker.ReplyHeader(header)); err != nil {
		log.Errorf("mesh request-reply error: %v", err)
	}
}

// replyContext 为串行回包复用独立的发送期限，剩余时间不足一半时更换定时器
func (m *Mesh) replyContext() context.Context {
	if m.replyCtx != nil {
		deadline, _ := m.replyCtx.Deadline()
		if time.Until(deadline) >= rpcReplyTimeout/2 {
			return m.replyCtx
		}
		m.cancelReply()
	}
	m.replyCtx, m.cancelReply = context.WithTimeout(context.WithoutCancel(m.ctx), rpcReplyTimeout)
	return m.replyCtx
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
		e := &envelope.ClientEnvelope{}
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
		} else {
			ctx := newContext(m, msg, e)
			defer ctx.release()
			ctx.ErrResp(http.StatusNotFound, "route not found")
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
