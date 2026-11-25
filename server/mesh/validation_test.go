package mesh

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta"
	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/envelope"
	es "github.com/byteweap/meta/errors"
	"github.com/byteweap/meta/internal/cluster"
	"github.com/byteweap/meta/server"
)

func TestMeshStartupValidation(t *testing.T) {
	m := New()
	require.Equal(t, server.KindMesh, m.Kind())
	require.ErrorIs(t, m.Start(t.Context()), es.ErrAppNotFound)
	got, err := m.Endpoint(t.Context())
	require.Nil(t, got)
	require.ErrorIs(t, err, es.ErrAppNotFound)
	ctx := meta.NewContext(t.Context(), meta.New(meta.Name("game"), meta.ID("node")))
	require.ErrorIs(t, m.Start(ctx), es.ErrBrokerRequired)
	require.ErrorIs(t, New(Broker(&mockBroker{})).Start(ctx), es.ErrLocatorRequired)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, New(Broker(&mockBroker{}), Locator(&lifecycleLocator{})).Start(canceled), context.Canceled)
	m.appName = ""
	close(m.ready)
	require.ErrorContains(t, m.Push(t.Context(), 42, 1, 1, nil), "invalid push source service")
	_, tip, code, err := m.RequestService(canceled, "game", "cmd", "v1", nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, tip)
	require.Zero(t, code)
}

func TestMessageContextsExposeProtocolMetadata(t *testing.T) {
	msg := &broker.Message{Subject: "meta.gate.game.node", Header: cluster.BuildHeader(42, cluster.EventBusiness, "", "gate", "game")}
	frame := &envelope.ClientEnvelope{Header: &envelope.Header{Seq: 7, Cmd: 8, Version: 9, SentAtUnixMs: 10}}
	ctx := newContext(New(), msg, frame)
	copied := ctx.Copy()
	ctx.OkResp()
	ctx.release()
	require.EqualValues(t, 42, copied.UID())
	require.EqualValues(t, 7, copied.Seq())
	require.EqualValues(t, 8, copied.Cmd())
	require.EqualValues(t, 9, copied.Version())
	require.EqualValues(t, 10, copied.Timestamp())
	require.Equal(t, "gate", copied.FromService())
	require.Equal(t, "game", copied.ToService())
	require.Equal(t, msg.Subject, copied.Subject())
	var absent *Context
	require.Equal(t, &Context{}, absent.Copy())
	absent.release()

	msg.Header.Set("cmd", "hello")
	msg.Header.Set("version", "v2")
	rpc := newRPCContext(t.Context(), msg)
	defer rpc.release()
	require.Equal(t, msg.Subject, rpc.Subject())
	require.Equal(t, "hello", rpc.Cmd())
	require.Equal(t, "v2", rpc.Version())
}

func TestRPCAdapterRejectsUnsupportedSignaturesAndMalformedWire(t *testing.T) {
	for _, tt := range []struct {
		invalid any
		want    string
	}{
		{nil, "handler is nil"},
		{5, "unsupported route handler type"},
		{func(string, *envelope.Header) ([]byte, string, int) { return nil, "", 0 }, "first arg"},
		{func(*RPCContext, int) ([]byte, string, int) { return nil, "", 0 }, "second arg"},
		{func(*RPCContext, *envelope.Header) (int, string, int) { return 0, "", 0 }, "first return"},
		{func(*RPCContext, *envelope.Header) ([]byte, int, int) { return nil, 0, 0 }, "second return"},
		{func(*RPCContext, *envelope.Header) ([]byte, string, error) { return nil, "", nil }, "third return"},
	} {
		adapted, err := adaptRPCMessageHandler(tt.invalid)
		require.Nil(t, adapted)
		require.ErrorContains(t, err, tt.want)
	}
	called := false
	handler := func(*RPCContext, *envelope.Header) ([]byte, string, int) {
		called = true
		return nil, "", http.StatusOK
	}
	reflected, err := adaptRPCMessageHandler(handler)
	require.NoError(t, err)
	for _, adapted := range []RPCMessageHandler{reflected, WrapRPC(handler)} {
		data, tip, status := adapted(t.Context(), New(), &broker.Message{Data: []byte{0xff}})
		require.Empty(t, data)
		require.Equal(t, unmarshalPayloadError, tip)
		require.Equal(t, http.StatusInternalServerError, status)
	}
	require.False(t, called)
}

func TestDispatchRPCRejectsMissingHeadersUnknownRoutesAndCanceledMesh(t *testing.T) {
	bus := &mockBroker{}
	m := New(Broker(bus))
	var timer rpcReplyTimer
	defer timer.stop()

	_, _, status := m.dispatchRPC(&broker.Message{})
	require.Equal(t, rpcMissingHeaderCode, status)
	_, _, status = m.dispatchRPC(&broker.Message{Header: broker.Header{"cmd": {"unknown"}}})
	require.Equal(t, http.StatusNotFound, status)
	m.replyRequestResult(&timer, &broker.Message{}, nil, "", 0)
	require.Zero(t, bus.replyCalls)
	m.replyRequestResult(&timer, &broker.Message{Reply: "reply"}, []byte("not sent"), "", 0)
	require.Equal(t, "500", bus.replyHdr.Get("code"))
	require.Equal(t, "request failed", bus.replyHdr.Get("tip"))
	require.Empty(t, bus.replyData)

	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	m.ctx = ctx
	_, _, status = m.dispatchRPC(&broker.Message{Header: broker.Header{}})
	require.Equal(t, http.StatusGatewayTimeout, status)
	m.cancel()
	m.ctx = mTestCanceledContext(t)
	_, _, status = m.dispatchRPC(&broker.Message{Header: broker.Header{}})
	require.Equal(t, http.StatusServiceUnavailable, status)

	var empty func(context.Context, Caller, *envelope.Header) (*envelope.Header, error)
	require.Panics(t, func() { Handle(empty) })
	require.Panics(t, func() { m.Route(1, 1, nil) })
	require.Panics(t, func() { m.RPCRoute("test", "v1", nil) })
	m.OnlineHandler(nil)
	m.OfflineHandler(nil)
	m.ReconnectHandler(nil)
	m.handlerPubSubMessage(&broker.Message{Data: []byte{0xff}})
	require.Nil(t, m.onlineHandler)
	require.Nil(t, m.offlineHandler)
	require.Nil(t, m.reconnectHandler)
}

func mTestCanceledContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	return ctx
}
