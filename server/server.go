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
