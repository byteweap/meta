package mesh

import (
	"context"
	"sync"

	"github.com/byteweap/meta/component/broker"
)

// RPCContext 提供服务间 RPC 的执行上下文和路由信息，仅在当前 handler 调用期间有效
type RPCContext struct {
	ctx                   context.Context
	subject, cmd, version string
}

var reqCtxPool = sync.Pool{
	New: func() any {
		return &RPCContext{}
	},
}

// newRPCContext 从对象池获取上下文并重置字段
func newRPCContext(ctx context.Context, msg *broker.Message) *RPCContext {
	c := reqCtxPool.Get().(*RPCContext)
	c.ctx = ctx
	c.subject = msg.Subject
	c.cmd = msg.Header.Get("cmd")
	c.version = msg.Header.Get("version")
	return c
}

func (ctx *RPCContext) release() {
	*ctx = RPCContext{}
	reqCtxPool.Put(ctx)
}

// Context 返回可传给存储操作或嵌套 RPC 的标准上下文
// 请求指定期限时在到期、Mesh 停止或处理结束时取消。未指定期限时沿用 Mesh 的上下文
func (ctx *RPCContext) Context() context.Context { return ctx.ctx }

// Subject 获取当前请求的主题
func (ctx *RPCContext) Subject() string {
	return ctx.subject
}

// Cmd 指令(路由)
func (ctx *RPCContext) Cmd() string {
	return ctx.cmd
}

// Version 版本
func (ctx *RPCContext) Version() string {
	return ctx.version
}
