package event

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/envelope"
	"github.com/byteweap/meta/internal/cluster"
	"github.com/byteweap/meta/server/mesh"
)

type replyRecorder struct {
	broker.Broker
	replies [][]byte
}

func (r *replyRecorder) Pub(_ context.Context, _ string, data []byte, _ ...broker.PublishOption) error {
	r.replies = append(r.replies, data)
	return nil
}

func TestExitGameAcknowledgement(t *testing.T) {
	recorder := &replyRecorder{}
	request := &envelope.ClientEnvelope{Service: "game", Header: &envelope.Header{Cmd: 2, Version: 1, Seq: 17}}
	msg := &broker.Message{Header: cluster.BuildHeader(42, cluster.EventBusiness, "gate.reply", "gate", "game")}
	handle := mesh.Handle(New(nil).ExitGame)
	handle(mesh.New(mesh.Broker(recorder)), msg, request)

	if len(recorder.replies) != 1 {
		t.Fatalf("got %d replies, want one exit acknowledgement", len(recorder.replies))
	}
	var reply envelope.ServerEnvelope
	if err := proto.Unmarshal(recorder.replies[0], &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Kind != envelope.MsgType_RESPONSE || reply.Error != nil || len(reply.Payload) != 0 {
		t.Fatalf("want empty success response, got %v", &reply)
	}
	h := reply.GetHeader()
	if h.GetSeq() != request.Header.Seq || h.GetCmd() != request.Header.Cmd || h.GetVersion() != request.Header.Version {
		t.Fatalf("exit acknowledgement header = %v, request header = %v", h, request.Header)
	}
}
