package rpc

import (
	"net/http"

	"github.com/byteweap/meta/examples/game/internal/pb"
	"github.com/byteweap/meta/server/mesh"
)

// Hello 返回示例 RPC 的固定响应
func (h *Handler) Hello(_ *mesh.RPCContext, _ *pb.FindRoomRequest) ([]byte, string, int) {
	return []byte("Hello RPC"), "", http.StatusOK
}
