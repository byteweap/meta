package mesh

import (
	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/locator"
	"github.com/byteweap/meta/component/observe"
	"github.com/byteweap/meta/component/registry"
	"github.com/byteweap/meta/component/selector"
	"github.com/byteweap/meta/internal/observation"
)

const (
	defaultPrefix            = "meta"
	defaultMessageBufferSize = 256
)

// options 选项
type options struct {
	prefix            string // subject \ redis key 前缀
	messageBufferSize int    // 消息缓冲区大小
	rpcWorkers        int
	rpcQueueSize      int
	gateService       string
	locator           locator.SessionLocator // 玩家位置与会话定位器
	broker            broker.Broker          // 消息传输代理
	discovery         registry.Registry
	selectorFunc      func() selector.Selector
	observer          observe.Observer
}

// Option 定义 Mesh 可选配置函数
type Option func(*options)

// Observer 设置 Mesh 的入站请求观测回调。默认为 nil，nil 关闭观测
func Observer(observer observe.Observer) Option {
	return func(o *options) { o.observer = observation.Protect(observer) }
}

// defaultOptions 返回默认配置
func defaultOptions() *options {
	return &options{
		prefix:            defaultPrefix,
		gateService:       "gate",
		messageBufferSize: defaultMessageBufferSize,
	}
}

// Prefix 设置 broker subject 前缀, 默认: meta
func Prefix(prefix string) Option {
	return func(o *options) {
		if prefix != "" {
			o.prefix = prefix
		}
	}
}

// MessageBufferSize 设置消息缓冲区大小, 默认 256
func MessageBufferSize(size int) Option {
	return func(o *options) {
		if size > 0 {
			o.messageBufferSize = size
		}
	}
}

// RPCConcurrency 设置入站服务 RPC 的固定 worker 数和等待队列容量
// 默认 (0, 0) 让 RPC、客户端消息和会话事件串行执行。workers > 0 时只将 RPC 交给 worker
// queueSize 为 0 时只使用 worker 的执行名额。执行与排队名额均用尽时回复 503
// 参数为负、总容量溢出或仅配置队列时 Start 返回错误。开启后业务须同步与 RPC 共享的可变状态
func RPCConcurrency(workers, queueSize int) Option {
	return func(o *options) {
		o.rpcWorkers, o.rpcQueueSize = workers, queueSize
	}
}

// GateService 设置 Push 定位会话使用的 Gate 服务名，默认为 gate
// 名称须与 Gate 的应用名一致，空名称或非法的 Broker 主题段会使 Start 返回错误
func GateService(name string) Option {
	return func(o *options) { o.gateService = name }
}

// Locator 设置玩家位置定位器
func Locator(locator locator.SessionLocator) Option {
	return func(o *options) {
		if locator != nil {
			o.locator = locator
		}
	}
}

// Broker 设置消息传输代理
func Broker(broker broker.Broker) Option {
	return func(o *options) {
		if broker != nil {
			o.broker = broker
		}
	}
}

// Discovery 设置 RequestService 使用的注册中心
func Discovery(reg registry.Registry) Option {
	return func(o *options) {
		if reg != nil {
			o.discovery = reg
		}
	}
}

// SelectorFunc 为每个目标服务创建独立的选择器，RequestService 以空 key 选点
func SelectorFunc(newSelector func() selector.Selector) Option {
	return func(o *options) {
		if newSelector != nil {
			o.selectorFunc = newSelector
		}
	}
}
