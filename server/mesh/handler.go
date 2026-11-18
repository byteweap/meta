package mesh

import (
	"fmt"
	"reflect"

	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/log"
	"github.com/byteweap/meta/encoding/proto"
	"github.com/byteweap/meta/envelope"
)

type MessageHandler func(*Mesh, *broker.Message, *envelope.IMessage)

// Wrap 将业务函数包装成网关消息的路由处理器
// payload 非空时解码为 *T，省略或长度为 0 时向 handler 传入 nil
func Wrap[T any](handler func(*Context, *T)) MessageHandler {
	return func(m *Mesh, msg *broker.Message, e *envelope.IMessage) {
		ctx := newContext(m, msg, e)
		defer ctx.release()

		if len(e.GetPayload()) == 0 {
			handler(ctx, nil)
			return
		}
		var payload T
		if err := proto.Unmarshal(e.GetPayload(), &payload); err != nil {
			log.Errorf("mesh pub-sub unmarshal payload error: %v", err)
			return
		}
		handler(ctx, &payload)
	}
}

// adaptMessageHandler 将不同签名的 handler 统一适配为 MessageHandler
// 原理:
// 1) 若本身就是 MessageHandler，直接返回
// 2) 若是 func(*Context, *T)，使用反射校验签名后包装
// 3) 包装函数内解码非空 payload，再调用业务 handler
func adaptMessageHandler(handler any) (MessageHandler, error) {
	if handler == nil {
		return nil, fmt.Errorf("mesh: handler is nil")
	}

	if mh, ok := handler.(MessageHandler); ok {
		return mh, nil
	}

	rv := reflect.ValueOf(handler)
	rt := rv.Type()
	if rt.Kind() != reflect.Func {
		return nil, fmt.Errorf("mesh: unsupported route handler type %T", handler)
	}
	if rt.NumIn() != 2 || rt.NumOut() != 0 {
		return nil, fmt.Errorf("mesh: handler must be func(*Context,*T) or MessageHandler, got %s", rt.String())
	}

	ctxType := reflect.TypeOf((*Context)(nil))
	if rt.In(0) != ctxType {
		return nil, fmt.Errorf("mesh: handler first arg must be *mesh.Context, got %s", rt.In(0).String())
	}
	argType := rt.In(1)
	if argType.Kind() != reflect.Pointer {
		return nil, fmt.Errorf("mesh: handler second arg must be pointer type, got %s", argType.String())
	}
	return func(m *Mesh, msg *broker.Message, e *envelope.IMessage) {
		ctx := newContext(m, msg, e)
		defer ctx.release()

		callArg := reflect.Zero(argType)

		if len(e.GetPayload()) > 0 {
			callArg = reflect.New(argType.Elem())
			if err := proto.Unmarshal(e.GetPayload(), callArg.Interface()); err != nil {
				log.Errorf("mesh unmarshal payload error: %v", err)
				return
			}
		}
		rv.Call([]reflect.Value{reflect.ValueOf(ctx), callArg})
	}, nil
}
