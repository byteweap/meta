package mesh

import (
	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/locator"
	"github.com/byteweap/meta/component/registry"
	"github.com/byteweap/meta/component/selector"
)

const (
	defaultPrefix            = "meta"
	defaultMessageBufferSize = 256
)

// options 选项
type options struct {
	prefix            string                 // subject \ redis key 前缀
	messageBufferSize int                    // 消息缓冲区大小
	locator           locator.SessionLocator // 玩家位置与会话定位器
	broker            broker.Broker          // 消息传输代理
	discovery         registry.Registry
	selectorFunc      func() selector.Selector
}

// Option 定义 Mesh 可选配置函数
type Option func(*options)

// defaultOptions 返回默认配置
func defaultOptions() *options {
	return &options{
		prefix:            defaultPrefix,
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
