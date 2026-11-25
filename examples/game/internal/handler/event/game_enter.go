package event

import (
	"context"

	"github.com/byteweap/meta/component/log"
	"github.com/byteweap/meta/examples/game/internal/pb"
	"github.com/byteweap/meta/server/mesh"
)

// EnterGame 记录进入游戏的玩家并返回空的成功响应
func (h *Handler) EnterGame(_ context.Context, caller mesh.Caller, req *pb.EnterGameRequest) (*pb.EnterGameResponse, error) {
	log.Infof("EnterGame, uid: %d, req: %v", caller.UID, req)
	return nil, nil
}
