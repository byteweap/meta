package gate

import (
	"context"
	"io"
	"testing"

	"github.com/olahol/melody"
	"google.golang.org/protobuf/proto"

	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/log"
	"github.com/byteweap/meta/envelope"
	"github.com/byteweap/meta/internal/cluster"
)

type dispatchBenchBroker struct {
	testBroker
	data []byte
}

func (b *dispatchBenchBroker) Pub(_ context.Context, _ string, data []byte, _ ...broker.PublishOption) error {
	b.data = data
	return nil
}

func BenchmarkGateForwardBoundSession(b *testing.B) {
	logger := log.GetLogger()
	log.SetLogger(log.NewStdLogger(io.Discard))
	b.Cleanup(func() { log.SetLogger(logger) })
	bro := &dispatchBenchBroker{}
	g := New(testGameDiscovery(), testGateSelector(), Locator(&envelopeTestLocator{}), Broker(bro))
	g.appName, g.appID = "gate", "gate-1"
	session := &melody.Session{Keys: map[string]any{"uid": int64(42), cluster.FieldNameSessionID: "session-42"}}
	frame, err := proto.Marshal(&envelope.ClientEnvelope{
		Header: &envelope.Header{Cmd: 1, Version: 1, Seq: 7}, Service: "game", Payload: make([]byte, 128),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = g.Stop(context.Background()) })
	if _, err := g.ensure("game"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		g.handleBinaryMessage(session, frame)
	}
	if len(bro.data) != len(frame) || &bro.data[0] != &frame[0] {
		b.Fatal("gate did not publish the original frame")
	}
}
