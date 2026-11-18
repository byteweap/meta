package mesh

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/envelope"
	"github.com/byteweap/meta/internal/cluster"
)

func BenchmarkClientDispatch(b *testing.B) {
	b.Run("manual", func(b *testing.B) {
		benchmarkClientDispatch(b, Wrap(func(c *Context, req *wrapperspb.BytesValue) { c.OkResp(req) }))
	})
	b.Run("typed", func(b *testing.B) {
		benchmarkClientDispatch(b, Handle(func(_ context.Context, _ Caller, req *wrapperspb.BytesValue) (*wrapperspb.BytesValue, error) {
			return req, nil
		}))
	})
}

func benchmarkClientDispatch(b *testing.B, handler MessageHandler) {
	m := New(Broker(&mockBroker{}))
	m.ctx, m.appName = b.Context(), "game"
	m.Route(1, 1, handler)
	payload, err := proto.Marshal(wrapperspb.Bytes(make([]byte, 128)))
	if err != nil {
		b.Fatal(err)
	}
	data, err := proto.Marshal(&envelope.ClientEnvelope{Header: &envelope.Header{Cmd: 1, Version: 1, Seq: 7}, Service: "game", Payload: payload})
	if err != nil {
		b.Fatal(err)
	}
	header := cluster.BuildHeader(42, cluster.EventBusiness, "meta.game.gate.gate-1", "gate", "game")
	header.Set(cluster.FieldNameSessionID, "session-42")
	msg := &broker.Message{Header: header, Data: data}
	b.ReportAllocs()
	for b.Loop() {
		m.handlerMessage(msg)
	}
}
