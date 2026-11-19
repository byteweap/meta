package server

import (
	"context"
	"net/url"
)

type Kind string

const (
	KindGate Kind = "gate"
	KindMesh Kind = "mesh"
)

type Server interface {
	Kind() Kind
	// Ready 在服务完成初始化并可接收请求后关闭，启动失败时保持未关闭。
	// 通道从构造时起保持不变；每个服务实例仅用于一次启动生命周期。
	Ready() <-chan struct{}
	Start(ctx context.Context) error
	// Stop 必须能清理 Endpoint 提前创建的资源，并阻止尚未开始的 Start。
	Stop(ctx context.Context) error
	Endpoint(ctx context.Context) (*url.URL, error)
}

// Drainer 由支持排空的服务实现。App 在停止钩子和注销前调用 BeginDrain
// 应用就绪后，App 保留这类服务的 Start 上下文供已接纳的工作执行，并在清理结束后取消
type Drainer interface {
	// BeginDrain 停止接纳新请求，允许已接纳的工作在 ctx 的期限内完成
	// 方法立即返回。Stop 负责等待清理结果，重复调用不改变既有排空期限
	BeginDrain(ctx context.Context)
}
