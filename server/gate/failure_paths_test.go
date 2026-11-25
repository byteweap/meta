package gate

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"

	"github.com/olahol/melody"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/byteweap/meta"
	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/selector"
	"github.com/byteweap/meta/envelope"
	es "github.com/byteweap/meta/errors"
	"github.com/byteweap/meta/internal/cluster"
	"github.com/byteweap/meta/server"
)

func TestGateValidatesRequiredDependencies(t *testing.T) {
	makeGate := func(opts ...Option) *Gate {
		return New(append([]Option{UserIDExtractor(func(*http.Request) int64 { return 1 })}, opts...)...)
	}
	loc, bus, discovery := Locator(&testLocator{}), Broker(&testBroker{}), Discovery(&testRegistry{})
	for _, tc := range []struct {
		options []Option
		want    error
	}{
		{nil, es.ErrLocatorRequired},
		{[]Option{loc}, es.ErrBrokerRequired},
		{[]Option{loc, bus}, es.ErrDiscoveryRequired},
		{[]Option{loc, bus, discovery}, es.ErrSelectorRequired},
	} {
		require.ErrorIs(t, makeGate(tc.options...).validate(), tc.want)
	}
	require.ErrorIs(t, (&Gate{}).validate(), es.ErrOptionsRequired)
	require.Equal(t, server.KindGate, makeGate().Kind())
	require.ErrorIs(t, makeGate().Start(t.Context()), es.ErrAppNotFound)
}

func TestGateEndpointAndStartPropagateListenErrors(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = occupied.Close() })
	g := New(Addr(occupied.Addr().String()), UserIDExtractor(func(*http.Request) int64 { return 1 }),
		Locator(&testLocator{}), Broker(&testBroker{}), Discovery(&testRegistry{}),
		SelectorFunc(func() selector.Selector { return &testSelector{} }))
	ctx := meta.NewContext(t.Context(), testAppInfo{id: "node", name: "gate"})

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	got, err := g.Endpoint(canceled)
	require.Nil(t, got)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, g.Start(canceled), context.Canceled)
	got, err = g.Endpoint(ctx)
	require.Nil(t, got)
	require.Error(t, err)
	require.ErrorContains(t, g.Start(ctx), "setup failed")
	require.ErrorContains(t, g.Start(ctx), "already started")
	require.NoError(t, g.Stop(ctx))
}

type failedBroadcastLocator struct {
	*testLocator
	err error
}

func (l failedBroadcastLocator) AllNodes(context.Context, int64) (map[string]string, error) {
	return nil, l.err
}

func TestGateRejectsUnroutableFrames(t *testing.T) {
	bus := &testBroker{}
	g := New(Broker(bus))
	g.handleMessage(&broker.Message{})
	g.handleMessage(&broker.Message{Header: cluster.BuildHeader(42, cluster.EventBusiness, "", "game", "gate")})
	g.handleRequestReplyMessage(nil)
	g.handleRequestReplyMessage(&broker.Message{})
	require.ErrorContains(t, g.replyError(nil, http.StatusBadRequest, ""), "request message is nil")

	empty := &melody.Session{}
	g.handleDisconnect(empty)
	require.Empty(t, sessionRemoteAddr(nil))
	require.Empty(t, sessionRemoteAddr(empty))
	require.NoError(t, g.handleClose(empty, 1000, "test complete"))

	frame, err := proto.Marshal(&envelope.ClientEnvelope{Header: &envelope.Header{Seq: 1}, Service: "game"})
	require.NoError(t, err)
	g.handleBinaryMessage(empty, frame)
	empty.Set("uid", int64(42))
	g.handleBinaryMessage(empty, frame)
	g.appName = string([]byte{0xff})
	g.writeDispatchError(empty, &envelope.Header{Seq: 1}, http.StatusBadRequest)
	require.Empty(t, bus.pubSubjects)
	require.Zero(t, bus.replyCalls)
}

func TestBroadcastPropagatesLocatorFailure(t *testing.T) {
	failure := errors.New("location lookup failed")
	bus := &testBroker{}
	g := New(Broker(bus), Locator(failedBroadcastLocator{testLocator: &testLocator{}, err: failure}))
	require.ErrorIs(t, g.broadcastEvent(t.Context(), 42, "sid", cluster.EventOnline), failure)
	require.Empty(t, bus.pubSubjects)
}
