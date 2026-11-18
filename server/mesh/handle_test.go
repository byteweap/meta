package mesh

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/envelope"
	"github.com/byteweap/meta/internal/cluster"
)

func TestClientHandlerResponse(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cmd     uint32
		code    int32
		message string
	}{
		{"echo", 1, 0, ""},
		{"empty", 1, 0, ""},
		{"unknown", 99, 404, "route not found"},
		{"malformed", 1, 400, unmarshalPayloadError},
		{"manual malformed", 2, 400, unmarshalPayloadError},
		{"public", 1, 409, "room full"},
		{"internal", 1, 500, "mesh internal error"},
		{"invalid code", 1, 500, "mesh internal error"},
		{"panic", 1, 500, "mesh internal error"},
		{"marshal", 1, 500, "mesh internal error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bro := &errorResponseBroker{}
			m := New(Broker(bro))
			m.ctx, m.appName = t.Context(), "game"
			calls := 0
			m.Route(1, 2, Handle(func(ctx context.Context, caller Caller, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				calls++
				require.Equal(t, m.ctx, ctx)
				require.EqualValues(t, 42, caller.UID)
				switch tc.name {
				case "empty":
					require.Nil(t, req)
					return nil, nil
				case "public":
					return nil, fmt.Errorf("private details: %w", &ClientError{Code: 409, Message: "room full"})
				case "internal":
					return nil, errors.New("private details")
				case "invalid code":
					return nil, &ClientError{Message: "private details"}
				case "panic":
					panic("private details")
				case "marshal":
					return wrapperspb.String("\xff"), nil
				}
				return req, nil
			}))
			manual := func(*Context, *wrapperspb.StringValue) { t.Fatal("malformed request reached handler") }
			m.Route(2, 2, Wrap(manual))
			h := &envelope.Header{Cmd: tc.cmd, Version: 2, Seq: 7}
			payload, err := proto.Marshal(wrapperspb.String("hello"))
			require.NoError(t, err)
			switch tc.name {
			case "empty":
				h.Seq = 0
				payload = nil
			case "malformed", "manual malformed":
				payload = []byte{0xff}
			}
			data, err := proto.Marshal(&envelope.ClientEnvelope{Header: h, Service: "game", Payload: payload})
			require.NoError(t, err)
			header := cluster.BuildHeader(42, cluster.EventBusiness, "reply-to-gate", "gate", "game")
			header.Set(cluster.FieldNameSessionID, "original")
			m.handlerMessage(&broker.Message{Header: header, Data: data})

			require.Len(t, bro.messages, 1)
			reply := bro.messages[0]
			require.Equal(t, "reply-to-gate", reply.Subject)
			require.EqualValues(t, 42, cluster.GetUIDBy(reply.Header))
			require.Equal(t, "original", cluster.GetSessionIDBy(reply.Header))
			require.Equal(t, cluster.DeliveryResponse, reply.Header.Get(cluster.FieldNameDelivery))
			var out envelope.ServerEnvelope
			require.NoError(t, proto.Unmarshal(reply.Data, &out))
			require.Equal(t, envelope.MsgType_RESPONSE, out.Kind)
			require.Equal(t, "game", out.Service)
			require.Equal(t, h.Seq, out.Header.Seq)
			require.Equal(t, h.Cmd, out.Header.Cmd)
			require.Equal(t, h.Version, out.Header.Version)
			require.Equal(t, tc.code, out.Error.GetCode())
			require.Equal(t, tc.message, out.Error.GetMessage())
			if tc.code == 0 {
				require.Nil(t, out.Error)
			}
			if tc.name == "echo" {
				require.Equal(t, payload, out.Payload)
			} else {
				require.Empty(t, out.Payload)
			}
			if tc.code == 400 || tc.code == 404 {
				require.Zero(t, calls)
			} else {
				require.Equal(t, 1, calls)
			}
		})
	}
}

func TestDuplicateClientRouteKeepsOriginal(t *testing.T) {
	m := New()
	calls := 0
	handler := func(*Context, *envelope.Header) { calls++ }
	m.Route(1, 1, Wrap(handler))
	replacement := func(*Context, *envelope.Header) { t.Fatal("duplicate replaced original handler") }
	require.Panics(t, func() { m.Route(1, 1, Wrap(replacement)) })
	m.Route(1, 2, Wrap(handler))
	m.handlerPubSubMessage(&broker.Message{Data: mustBusinessMessage(t, 1, 1, "game", nil)})
	require.Equal(t, 1, calls)
}

type panicPublishBroker struct {
	mockBroker
	attempts int
}

func (b *panicPublishBroker) Pub(context.Context, string, []byte, ...broker.PublishOption) error {
	b.attempts++
	panic("publish failed")
}

func TestTypedResponsePublishPanicAttemptsOnce(t *testing.T) {
	bro := &panicPublishBroker{}
	m := New(Broker(bro))
	m.ctx = t.Context()
	m.Route(1, 1, Handle(func(_ context.Context, _ Caller, req *envelope.Header) (*envelope.Header, error) { return req, nil }))
	header := cluster.BuildHeader(42, cluster.EventBusiness, "reply", "gate", "game")
	m.handlerMessage(&broker.Message{Header: header, Data: mustBusinessMessage(t, 1, 1, "game", nil)})
	require.Equal(t, 1, bro.attempts)
}
