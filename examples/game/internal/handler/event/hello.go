package event

import (
	"github.com/byteweap/meta/examples/game/internal/pb"
	"github.com/byteweap/meta/server/mesh"
)

// Hello 回复成功响应，用于演示事件处理
func (h *Handler) Hello(ctx *mesh.Context, _ *pb.ExitGameRequest) {
	ctx.OkResp()
}
