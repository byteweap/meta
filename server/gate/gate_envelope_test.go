package gate

import (
	"context"
	"testing"

	"github.com/olahol/melody"
	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta/component/broker"
)

type envelopeTestLocator struct{ testLocator }

func (*envelopeTestLocator) Node(context.Context, int64, string) (string, error) {
	return "game-1", nil
}

type envelopeTestBroker struct {
	testBroker
	data []byte
}

func (b *envelopeTestBroker) Pub(ctx context.Context, subject string, data []byte, opts ...broker.PublishOption) error {
	b.data = data
	return b.testBroker.Pub(ctx, subject, data, opts...)
}

func TestGateBinaryEnvelopeForwardsOriginalBytes(t *testing.T) {
	bro := &envelopeTestBroker{}
	g := New(Locator(&envelopeTestLocator{}), Broker(bro))
	g.appName = "gate"
	session := &melody.Session{Keys: map[string]any{"uid": int64(42)}}

	// service 在 header 前面。标准解码接受这个顺序，重新编码会改变它
	frame := []byte("\x12\x04game\x0a\x02\x10\x01")
	g.handleBinaryMessage(session, frame)

	require.Equal(t, []string{"meta.gate.game.game-1"}, bro.pubSubjects)
	require.Equal(t, frame, bro.data)
}

func TestGateBinaryEnvelopeRejectsMissingOrInvalidHeader(t *testing.T) {
	bro := &envelopeTestBroker{}
	g := New(Locator(&envelopeTestLocator{}), Broker(bro))
	g.appName = "gate"
	session := &melody.Session{Keys: map[string]any{"uid": int64(42)}}

	g.handleBinaryMessage(session, []byte("\x12\x04game"))
	g.handleBinaryMessage(session, []byte("\x12\x04game\x0a\x01\xff"))
	require.Empty(t, bro.pubSubjects)
}
