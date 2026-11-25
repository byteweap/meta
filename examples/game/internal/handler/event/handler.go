package event

import "github.com/byteweap/meta/examples/game/internal/service"

// Handler 处理游戏事件
type Handler struct {
	svc service.EventService
}

// New 返回使用 svc 处理游戏事件的 Handler
func New(svc service.EventService) *Handler {
	return &Handler{
		svc: svc,
	}
}
