package mesh

import (
	"strconv"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/byteweap/meta/component/broker"
)

func BenchmarkRPCDispatch(b *testing.B) {
	for _, withDeadline := range []bool{false, true} {
		name := "no-deadline"
		if withDeadline {
			name = "deadline"
		}
		b.Run(name, func(b *testing.B) {
			bro := &mockBroker{}
			m := New(Broker(bro))
			m.ctx = b.Context()
			m.RPCRoute("echo", "v1", WrapRPC(func(_ *RPCContext, req *wrapperspb.BytesValue) ([]byte, string, int) {
				return req.Value, "ok", 200
			}))
			payload, err := proto.Marshal(wrapperspb.Bytes(make([]byte, 128)))
			if err != nil {
				b.Fatal(err)
			}
			msg := &broker.Message{Reply: "inbox", Header: broker.Header{"cmd": {"echo"}, "version": {"v1"}}, Data: payload}
			if withDeadline {
				msg.Header.Set(rpcDeadlineHeader, strconv.FormatInt(time.Now().Add(time.Hour).UnixMilli(), 10))
			}
			b.ReportAllocs()
			for b.Loop() {
				m.handlerMessage(msg)
			}
			if bro.replyCalls != b.N || len(bro.replyData) != 128 {
				b.Fatal("unexpected RPC reply")
			}
		})
	}
}
