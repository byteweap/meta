package mesh

import (
	"context"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/envelope"
	"github.com/byteweap/meta/internal/cluster"
)

type errorResponseBroker struct {
	mockBroker
	messages []*broker.Message
}

func (b *errorResponseBroker) Pub(_ context.Context, subject string, data []byte, opts ...broker.PublishOption) error {
	o := &broker.PublishOptions{}
	for _, opt := range opts {
		opt(o)
	}
	b.messages = append(b.messages, &broker.Message{Subject: subject, Data: data, Header: o.Header})
	return nil
}

func TestContextErrRespPublishesExpectedMessage(t *testing.T) {
	const code = 4321
	for _, tc := range []struct {
		name    string
		call    func(*Context)
		message string
	}{
		{"omitted", func(c *Context) { c.ErrResp(code) }, "mesh internal error"},
		{"nil slice", func(c *Context) { c.ErrResp(code, []string(nil)...) }, "mesh internal error"},
		{"empty slice", func(c *Context) { c.ErrResp(code, []string{}...) }, "mesh internal error"},
		{"custom", func(c *Context) { c.ErrResp(code, "room is full") }, "room is full"},
		{"explicit empty", func(c *Context) { c.ErrResp(code, "") }, ""},
		{"multiple", func(c *Context) { c.ErrResp(code, "first", "second") }, "first"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bro := &errorResponseBroker{}
			m := New(Broker(bro))
			m.ctx = context.Background()
			m.appName = "game"
			header := cluster.BuildHeader(42, cluster.EventBusiness, "meta.game.gate.gate-1", "gate", "game")
			header.Set(cluster.FieldNameSessionID, "session-42")
			c := newContext(m, &broker.Message{
				Header: header,
			}, &envelope.ClientEnvelope{Header: &envelope.Header{Seq: 7, Cmd: 1001, Version: 2}})
			copied := c.Copy()
			c.release()

			tc.call(copied)

			require.Len(t, bro.messages, 1)
			msg := bro.messages[0]
			require.Equal(t, "meta.game.gate.gate-1", msg.Subject)
			require.Equal(t, int64(42), cluster.GetUIDBy(msg.Header))
			require.Equal(t, "game", cluster.GetFromServiceBy(msg.Header))
			require.Equal(t, "gate", cluster.GetToServiceBy(msg.Header))
			require.Equal(t, "session-42", cluster.GetSessionIDBy(msg.Header))
			require.Equal(t, cluster.DeliveryResponse, msg.Header.Get(cluster.FieldNameDelivery))
			out := &envelope.ServerEnvelope{}
			require.NoError(t, proto.Unmarshal(msg.Data, out))
			require.Equal(t, int32(code), out.GetError().GetCode())
			require.Equal(t, tc.message, out.GetError().GetMessage())
			require.Equal(t, envelope.MsgType_RESPONSE, out.GetKind())
			require.Equal(t, "game", out.GetService())
			require.Equal(t, uint64(7), out.GetHeader().GetSeq())
			require.Equal(t, uint32(1001), out.GetHeader().GetCmd())
			require.Equal(t, uint32(2), out.GetHeader().GetVersion())
		})
	}
}

func TestContextResponseStatus(t *testing.T) {
	const requestSentAtUnixMs int64 = 1700000000000
	for _, tc := range []struct {
		name    string
		success bool
		code    int64
		session string
	}{
		{name: "success", success: true, session: "session-42"},
		{name: "success via old Gate", success: true},
		{name: "zero error code"},
		{name: "above int32", code: math.MaxInt32 + 1},
		{name: "below int32", code: math.MinInt32 - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code := int(tc.code)
			if int64(code) != tc.code {
				t.Skip("error code cannot be passed as int on this platform")
			}
			bro := &errorResponseBroker{}
			m := New(Broker(bro))
			m.ctx = context.Background()
			m.appName = "game"
			header := cluster.BuildHeader(42, cluster.EventBusiness, "meta.game.gate.gate-1", "gate", "game")
			if tc.session != "" {
				header.Set(cluster.FieldNameSessionID, tc.session)
			}
			c := newContext(m, &broker.Message{
				Header: header,
			}, &envelope.ClientEnvelope{Header: &envelope.Header{Seq: 7, Cmd: 1001, Version: 2, SentAtUnixMs: requestSentAtUnixMs}})
			defer c.release()
			require.Equal(t, requestSentAtUnixMs, c.Timestamp())

			if tc.success {
				c.OkResp()
			} else {
				c.ErrResp(code)
			}

			require.Len(t, bro.messages, 1)
			require.Equal(t, tc.session, cluster.GetSessionIDBy(bro.messages[0].Header))
			require.Equal(t, cluster.DeliveryResponse, bro.messages[0].Header.Get(cluster.FieldNameDelivery))
			var out envelope.ServerEnvelope
			require.NoError(t, proto.Unmarshal(bro.messages[0].Data, &out))
			require.NotNil(t, out.GetHeader())
			require.Equal(t, envelope.MsgType_RESPONSE, out.GetKind())
			require.WithinDuration(t, time.Now(), time.UnixMilli(out.GetHeader().GetSentAtUnixMs()), time.Minute)
			if tc.success {
				require.Nil(t, out.GetError())
				return
			}
			require.Equal(t, int32(http.StatusInternalServerError), out.GetError().GetCode())
		})
	}
}
