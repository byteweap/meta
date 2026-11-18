package event

import (
	"context"

	"github.com/byteweap/meta/component/log"
	"github.com/byteweap/meta/examples/game/internal/pb"
	"github.com/byteweap/meta/server/mesh"
)

// ExitGame 记录退出游戏的玩家并返回空的成功响应
func (h *EventHandler) ExitGame(_ context.Context, caller mesh.Caller, req *pb.ExitGameRequest) (*pb.ExitGameResponse, error) {
	log.Infof("ExitGame, uid: %d, req: %v", caller.UID, req)
	return nil, nil
}
