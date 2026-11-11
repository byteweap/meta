package gate

import (
	"net/http"
	"time"

	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/locator"
	"github.com/byteweap/meta/component/registry"
	"github.com/byteweap/meta/component/selector"
)

const (
	defaultPrefix                     = "meta"
	defaultPath                       = "/"
	defaultAddr                       = ":9000"
	defaultWriteTimeout               = 5 * time.Second
	defaultPongTimeout                = 60 * time.Second
	defaultPingInterval               = 10 * time.Second
	defaultMaxMessageSize             = 1024 * 2
	defaultWebsocketMessageBufferSize = 256
	defaultBrokerMessageBufferSize    = 256
)

// IDExtractor 在 WebSocket 握手前验证请求凭据。
// 返回认证用户的正数 ID，非正数表示认证失败。
type IDExtractor func(r *http.Request) int64

// options 选项
type options struct {
	// app
	prefix          string      // subject / redis key 前缀
	userIDExtractor IDExtractor // 用户 id 提取器

	// websocket
	path                       string        // ws 路径
	addr                       string        // ws 地址
	writeTimeout               time.Duration // write 超时时间
	pongTimeout                time.Duration // Pong 超时时间
	pingInterval               time.Duration // Ping 间隔时间
	maxMessageSize             int64         // 最大消息大小
	websocketMessageBufferSize int           // websocket 消息缓冲区大小
	brokerMessageBufferSize    int           // broker 消息缓冲区大小

	// component
	locator      locator.Locator          // 玩家位置定位器
	broker       broker.Broker            // 消息传输代理
	discovery    registry.Registry        // 服务发现
	selectorFunc func() selector.Selector // 选择器创建函数
}

type Option func(*options)

func defaultOptions() *options {
	return &options{
		prefix:                     defaultPrefix,
		path:                       defaultPath,
		addr:                       defaultAddr,
		writeTimeout:               defaultWriteTimeout,
		pongTimeout:                defaultPongTimeout,
		pingInterval:               defaultPingInterval,
		maxMessageSize:             defaultMaxMessageSize, // 2k
		websocketMessageBufferSize: defaultWebsocketMessageBufferSize,
		brokerMessageBufferSize:    defaultBrokerMessageBufferSize,
	}
}

// Prefix 设置前缀(subject、redis key), 默认: meta
func Prefix(prefix string) Option {
	return func(o *options) {
		if prefix != "" {
			o.prefix = prefix
		}
	}
}

// Addr 设置 websocket 地址
func Addr(addr string) Option {
	return func(o *options) {
		if addr != "" {
			o.addr = addr
		}
	}
}

// Path 设置 websocket 路径
func Path(path string) Option {
	return func(o *options) {
		if path != "" {
			o.path = path
		}
	}
}

// WriteTimeout 设置 websocket write 超时时间
func WriteTimeout(timeout time.Duration) Option {
	return func(o *options) {
		if timeout > 0 {
			o.writeTimeout = timeout
		}
	}
}

// PongTimeout 设置 websocket Pong 超时时间
func PongTimeout(timeout time.Duration) Option {
	return func(o *options) {
		if timeout > 0 {
			o.pongTimeout = timeout
		}
	}
}

// PingInterval 设置 websocket Ping 间隔时间
func PingInterval(interval time.Duration) Option {
	return func(o *options) {
		if interval > 0 {
			o.pingInterval = interval
		}
	}
}

// MaxMessageSize 设置 websocket 最大消息大小, 单位:字节, 默认: 2KB
func MaxMessageSize(size int64) Option {
	return func(o *options) {
		if size > 0 {
			o.maxMessageSize = size
		}
	}
}

// MessageBufferSize 设置 websocket 和 broker 消息缓冲区大小, 默认: 256
func MessageBufferSize(size int) Option {
	return func(o *options) {
		if size > 0 {
			o.websocketMessageBufferSize = size
			o.brokerMessageBufferSize = size
		}
	}
}

// WebsocketMessageBufferSize 设置 websocket 消息缓冲区大小, 默认: 256
func WebsocketMessageBufferSize(size int) Option {
	return func(o *options) {
		if size > 0 {
			o.websocketMessageBufferSize = size
		}
	}
}

// BrokerMessageBufferSize 设置 broker 消息缓冲区大小, 默认: 256
func BrokerMessageBufferSize(size int) Option {
	return func(o *options) {
		if size > 0 {
			o.brokerMessageBufferSize = size
		}
	}
}

// UserIDExtractor 设置必填的身份验证函数，每次握手前调用一次。
// 验证函数应校验请求凭据并返回用户 ID，不能直接信任客户端提供的用户 ID。
func UserIDExtractor(extractor IDExtractor) Option {
	return func(o *options) {
		o.userIDExtractor = extractor
	}
}

// Locator 设置玩家位置定位器
func Locator(locator locator.Locator) Option {
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

// Discovery 设置服务发现
func Discovery(discovery registry.Registry) Option {
	return func(o *options) {
		if discovery != nil {
			o.discovery = discovery
		}
	}
}

// SelectorFunc 设置选择器创建函数
func SelectorFunc(selectorFunc func() selector.Selector) Option {
	return func(o *options) {
		if selectorFunc != nil {
			o.selectorFunc = selectorFunc
		}
	}
}
