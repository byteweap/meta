package rpc

import "github.com/byteweap/meta/examples/game/internal/service"

// Handler 处理游戏服务的 RPC 请求
type Handler struct {
	gs service.IRoomService
}

// New 返回使用 gs 的 RPC Handler
func New(gs service.IRoomService) *Handler {
	return &Handler{gs: gs}
}
