package mesh

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	"github.com/byteweap/meta/component/log"
)

// Caller 携带 Gate 已认证的用户身份，按值传递，可在处理器返回后保留
type Caller struct {
	UID int64
}

// ClientError 描述可公开给客户端的业务错误。Code 必须非零，Message 必须是有效 UTF-8
type ClientError struct {
	Code    int32
	Message string
}

// Error 返回可向客户端公开的提示
func (e *ClientError) Error() string { return e.Message }

// Handle 将类型化客户端请求处理器适配为 Route 所需的 MessageHandler。handler 为 nil 时 panic
// ctx 随 Mesh 停止而取消。空请求体传入 nil，返回 nil 响应且无错误时回复空的成功消息
// 返回的 *ClientError 支持错误包装，其余错误及处理器 panic 回复通用 500
func Handle[T any, Req interface {
	*T
	proto.Message
}, Resp proto.Message](handler func(context.Context, Caller, Req) (Resp, error)) MessageHandler {
	if handler == nil {
		panic("mesh: handler is nil")
	}
	// 只捕获业务调用，发布阶段的 panic 交给消息循环处理，避免重复发送
	invoke := func(c *Context, req *T) (resp Resp, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("handler panic: %v\n%s", r, debug.Stack())
			}
		}()
		return handler(c.mesh.ctx, Caller{UID: c.uid}, req)
	}
	return Wrap(func(c *Context, req *T) {
		resp, err := invoke(c, req)
		if err == nil {
			c.OkResp(resp)
			return
		}
		if public, ok := errors.AsType[*ClientError](err); ok && public != nil && public.Code != 0 && utf8.ValidString(public.Message) {
			c.ErrResp(int(public.Code), public.Message)
			return
		}
		log.Errorf("mesh handler cmd=%d version=%d: %v", c.cmd, c.version, err)
		c.ErrResp(http.StatusInternalServerError)
	})
}
