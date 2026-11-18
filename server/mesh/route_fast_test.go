package mesh

import (
	"bytes"
	"testing"

	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/encoding/proto"
	"github.com/byteweap/meta/envelope"
)

func TestRouteFastWithWrap(t *testing.T) {
	m := New()

	called := false
	m.Route(3001, 1, Wrap(func(_ *Context, req *envelope.Header) {
		if req != nil && req.GetCmd() == 3001 {
			called = true
		}
	}))

	raw := mustBusinessMessage(t, 3001, 1, "game", &envelope.Header{Cmd: 3001})

	h := mustLoadRouteHandler(t, m, 3001, 1)
	invokeRouteHandler(t, h, m, &broker.Message{Data: raw}, raw)
	if !called {
		t.Fatalf("route fast handler not called")
	}
}

func TestBusinessEnvelopeRequiresHeader(t *testing.T) {
	m := New()
	calls := 0
	m.Route(0, 0, func(*Mesh, *broker.Message, *envelope.IMessage) { calls++ })

	msg := &broker.Message{}
	m.handlerPubSubMessage(msg) // 空 protobuf 消息可以解码，但不包含 header
	if calls != 0 {
		t.Fatal("a missing header matched the zero route")
	}

	// 显式提供空 header 时，零值路由仍可正常使用
	var err error
	msg.Data, err = proto.Marshal(&envelope.IMessage{Header: &envelope.Header{}})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	m.handlerPubSubMessage(msg)
	if calls != 1 {
		t.Fatalf("zero route calls = %d, want 1", calls)
	}
}

func TestRequestRouteFastWithWrapRequest(t *testing.T) {
	mb := &mockBroker{}
	m := New(Broker(mb))

	called := false
	wantData := []byte("mesh-ok")
	m.RPCRoute("findUser", "v1", WrapRPC(func(_ *RPCContext, req *envelope.IMessage) ([]byte, string, int) {
		if req != nil && req.GetService() == "mesh" {
			called = true
		}
		return wantData, "ok", 200
	}))

	raw, err := proto.Marshal(&envelope.IMessage{
		Service: "mesh",
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	m.handlerRequestReplyMessage(&broker.Message{
		Reply: "svc.reply",
		Header: broker.Header{
			"cmd":     []string{"findUser"},
			"version": []string{"v1"},
		},
		Data: raw,
	})

	if !called {
		t.Fatalf("request route fast handler not called")
	}
	if mb.replyCalls != 1 {
		t.Fatalf("expected 1 reply call, got %d", mb.replyCalls)
	}
	if !bytes.Equal(mb.replyData, wantData) {
		t.Fatalf("unexpected reply data: %v", mb.replyData)
	}
	if mb.replyHdr.Get("code") != "200" || mb.replyHdr.Get("tip") != "ok" {
		t.Fatalf("unexpected reply header: %+v", mb.replyHdr)
	}
}
