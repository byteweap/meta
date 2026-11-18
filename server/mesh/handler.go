package mesh

import (
	"net/http"

	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/encoding/proto"
	"github.com/byteweap/meta/envelope"
)

type MessageHandler func(*Mesh, *broker.Message, *envelope.ClientEnvelope)

// Wrap 将业务函数包装成网关消息的路由处理器
// payload 非空时解码为 *T，解码失败时回复 400，省略或长度为 0 时向 handler 传入 nil
func Wrap[T any](handler func(*Context, *T)) MessageHandler {
	return func(m *Mesh, msg *broker.Message, e *envelope.ClientEnvelope) {
		ctx := newContext(m, msg, e)
		defer ctx.release()

		if len(e.GetPayload()) == 0 {
			handler(ctx, nil)
			return
		}
		var payload T
		if err := proto.Unmarshal(e.GetPayload(), &payload); err != nil {
			ctx.ErrResp(http.StatusBadRequest, unmarshalPayloadError)
			return
		}
		handler(ctx, &payload)
	}
}
