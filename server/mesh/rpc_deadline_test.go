package mesh

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/envelope"
	es "github.com/byteweap/meta/errors"
)

type rpcRequestBroker struct {
	mockBroker
	request func(context.Context, string, []byte, broker.Header) (*broker.Message, error)
}

func (b *rpcRequestBroker) Request(ctx context.Context, subject string, data []byte, opts ...broker.RequestOption) (*broker.Message, error) {
	var o broker.RequestOptions
	for _, opt := range opts {
		opt(&o)
	}
	return b.request(ctx, subject, data, o.Header)
}

func TestRPCRequestBeforeStartRequiresBroker(t *testing.T) {
	data, tip, code, err := New().Request(t.Context(), "svc", "work", "1", nil)
	require.ErrorIs(t, err, es.ErrBrokerRequired)
	require.Nil(t, data)
	require.Empty(t, tip)
	require.Zero(t, code)
}

type rpcReplyBroker struct {
	mockBroker
	beforeReply func(context.Context)
}

func (b *rpcReplyBroker) Reply(ctx context.Context, msg *broker.Message, data []byte, opts ...broker.ReplyOption) error {
	b.beforeReply(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	return b.mockBroker.Reply(ctx, msg, data, opts...)
}

func TestRPCReplySurvivesMeshShutdown(t *testing.T) {
	for _, tc := range []struct {
		name string
		want int
	}{
		{"stop during handler", http.StatusServiceUnavailable},
		{"deadline during handler", http.StatusGatewayTimeout},
		{"stop during reply", http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				bro := &rpcReplyBroker{}
				m := New(Broker(bro))
				runCtx, stop := context.WithTimeout(t.Context(), time.Minute)
				defer stop()
				m.ctx = runCtx
				m.RPCRoute("work", "1", func(ctx context.Context, _ *Mesh, _ *broker.Message) ([]byte, string, int) {
					switch tc.name {
					case "stop during handler":
						stop()
						return nil, "stopping", http.StatusServiceUnavailable
					case "deadline during handler":
						<-ctx.Done()
					}
					return []byte("done"), "ok", http.StatusOK
				})
				bro.beforeReply = func(ctx context.Context) {
					if tc.name == "stop during reply" {
						stop()
					}
					require.Error(t, runCtx.Err())
					require.NoError(t, ctx.Err())
					deadline, ok := ctx.Deadline()
					require.True(t, ok)
					require.Equal(t, rpcReplyTimeout, time.Until(deadline))
				}
				m.handlerMessage(&broker.Message{Reply: "inbox", Header: broker.Header{"cmd": {"work"}, "version": {"1"}}})
				require.Equal(t, 1, bro.replyCalls)
				require.Equal(t, strconv.Itoa(tc.want), bro.replyHdr.Get("code"))
			})
		})
	}
}

func TestRPCReplyDeadlineRenewal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := New()
		first := m.replyContext()
		time.Sleep(rpcReplyTimeout / 2)
		require.Same(t, first, m.replyContext())

		time.Sleep(time.Nanosecond)
		next := m.replyContext()
		require.ErrorIs(t, first.Err(), context.Canceled)
		deadline, ok := next.Deadline()
		require.True(t, ok)
		require.Equal(t, rpcReplyTimeout, time.Until(deadline))
		<-next.Done()
		require.ErrorIs(t, next.Err(), context.DeadlineExceeded)
	})
}

func TestRPCForwardsDeadlineToNestedRequest(t *testing.T) {
	for _, route := range []string{"raw", "typed", "reflective"} {
		t.Run(route, func(t *testing.T) {
			bro := &rpcRequestBroker{}
			m := New(Broker(bro))
			m.ctx = t.Context()
			deadline := time.Now().Add(time.Hour).Truncate(time.Millisecond)
			stamp := strconv.FormatInt(deadline.UnixMilli(), 10)
			request := &broker.Message{Reply: "inbox", Header: broker.Header{"cmd": {"first"}, "version": {"v1"}, rpcDeadlineHeader: {stamp}}}
			bro.request = func(ctx context.Context, subject string, data []byte, h broker.Header) (*broker.Message, error) {
				got, ok := ctx.Deadline()
				require.True(t, ok)
				require.Equal(t, deadline.UnixMilli(), got.UnixMilli())
				require.Equal(t, "next-service", subject)
				require.Equal(t, []byte("payload"), data)
				require.Equal(t, broker.Header{"cmd": {"second"}, "version": {"v2"}, rpcDeadlineHeader: {stamp}}, h)
				return &broker.Message{Data: []byte("result"), Header: broker.Header{"code": {"200"}, "tip": {"ok"}}}, nil
			}

			var executionCtx context.Context
			nested := func(ctx context.Context) ([]byte, string, int) {
				executionCtx = ctx
				data, tip, code, err := m.Request(ctx, "next-service", "second", "v2", []byte("payload"))
				require.NoError(t, err)
				return data, tip, code
			}
			typed := func(c *RPCContext, _ *envelope.Header) ([]byte, string, int) { return nested(c.Context()) }
			switch route {
			case "raw":
				m.RPCRoute("first", "v1", func(ctx context.Context, _ *Mesh, _ *broker.Message) ([]byte, string, int) { return nested(ctx) })
			case "typed":
				m.RPCRoute("first", "v1", WrapRPC(typed))
			case "reflective":
				m.RPCRouteX("first", "v1", typed)
			}
			m.handlerMessage(request)
			require.Equal(t, 1, bro.replyCalls)
			require.Equal(t, []byte("result"), bro.replyData)
			require.Equal(t, "200", bro.replyHdr.Get("code"))
			require.Equal(t, "ok", bro.replyHdr.Get("tip"))
			require.NotNil(t, executionCtx)
			require.ErrorIs(t, executionCtx.Err(), context.Canceled)
		})
	}
}

func TestRPCDeadlineValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []string
		code   string
	}{
		{"expired", []string{"1"}, "504"},
		{"invalid", []string{"later"}, "400"},
		{"empty", []string{""}, "400"},
		{"no values", nil, "400"},
		{"duplicate", []string{"1", "2"}, "400"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bro := &mockBroker{}
			m := New(Broker(bro))
			m.RPCRoute("work", "1", WrapRPC(func(*RPCContext, *envelope.Header) ([]byte, string, int) {
				t.Fatal("invalid or expired request reached handler")
				return nil, "", 0
			}))
			m.handlerMessage(&broker.Message{Reply: "inbox", Header: broker.Header{"cmd": {"work"}, "version": {"1"}, rpcDeadlineHeader: tc.values}})
			require.Equal(t, 1, bro.replyCalls)
			require.Equal(t, tc.code, bro.replyHdr.Get("code"))
			require.Empty(t, bro.replyData)
		})
	}
}

func TestRPCDeadlineIncludesQueueTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bro := &panicBroker{replies: make(chan panicReply, 2)}
		m := New(Broker(bro), Locator(lifecycleLocator{}))
		entered := make(chan struct{})
		m.RPCRoute("slow", "1", WrapRPC(func(c *RPCContext, _ *envelope.Header) ([]byte, string, int) {
			if c.Context() != m.ctx {
				t.Error("expected the Mesh context for a request without a deadline")
			}
			close(entered)
			time.Sleep(time.Second)
			return nil, "ok", http.StatusOK
		}))
		m.RPCRoute("queued", "1", WrapRPC(func(*RPCContext, *envelope.Header) ([]byte, string, int) {
			t.Error("queued request started after its deadline")
			return nil, "ok", http.StatusOK
		}))
		runPanicMesh(t, m)
		bro.handler(&broker.Message{Reply: "first", Header: broker.Header{"cmd": {"slow"}, "version": {"1"}}})
		<-entered
		stamp := strconv.FormatInt(time.Now().Add(100*time.Millisecond).UnixMilli(), 10)
		bro.handler(&broker.Message{Reply: "second", Header: broker.Header{"cmd": {"queued"}, "version": {"1"}, rpcDeadlineHeader: {stamp}}})
		require.Equal(t, "200", (<-bro.replies).code)
		require.Equal(t, panicReply{"second", "504", http.StatusText(http.StatusGatewayTimeout), nil}, <-bro.replies)
		require.Empty(t, bro.replies)
	})
}

func TestRPCDeadlineDuringHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bro := &mockBroker{}
		m := New(Broker(bro))
		deadline := time.Now().Add(time.Second)
		m.RPCRoute("work", "1", WrapRPC(func(c *RPCContext, _ *envelope.Header) ([]byte, string, int) {
			ctx := c.Context()
			<-ctx.Done()
			require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
			require.True(t, time.Now().Equal(deadline))
			return []byte("late"), "ok", http.StatusOK
		}))
		h := broker.Header{"cmd": {"work"}, "version": {"1"}, rpcDeadlineHeader: {strconv.FormatInt(deadline.UnixMilli(), 10)}}
		m.handlerMessage(&broker.Message{Reply: "inbox", Header: h})
		require.Equal(t, 1, bro.replyCalls)
		require.Equal(t, "504", bro.replyHdr.Get("code"))
		require.Empty(t, bro.replyData)
	})
}

func TestRPCRequestCancellation(t *testing.T) {
	for _, cause := range []string{"caller", "deadline", "mesh stop", "stop before start", "start then stop"} {
		t.Run(cause, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				bro := &rpcRequestBroker{}
				m := New(Broker(bro), Locator(lifecycleLocator{}))
				if cause != "stop before start" && cause != "start then stop" {
					runPanicMesh(t, m)
				}
				entered := make(chan struct{})
				bro.request = func(ctx context.Context, _ string, _ []byte, h broker.Header) (*broker.Message, error) {
					close(entered)
					_, hasDeadline := h[rpcDeadlineHeader]
					if hasDeadline != (cause == "deadline") {
						t.Errorf("unexpected deadline header %v", h)
					}
					<-ctx.Done()
					return nil, ctx.Err()
				}

				var ctx context.Context
				var cancel context.CancelFunc
				if cause == "deadline" {
					ctx, cancel = context.WithTimeout(t.Context(), time.Second)
				} else {
					ctx, cancel = context.WithCancel(t.Context())
				}
				defer cancel()
				result := make(chan error, 1)
				go func() {
					_, _, _, err := m.Request(ctx, "svc", "cmd", "1", nil)
					result <- err
				}()
				<-entered
				want := context.Canceled
				switch cause {
				case "caller":
					cancel()
				case "mesh stop", "stop before start":
					require.NoError(t, m.Stop(t.Context()))
				case "start then stop":
					runPanicMesh(t, m)
					require.NoError(t, m.Stop(t.Context()))
				case "deadline":
					want = context.DeadlineExceeded
				}
				require.ErrorIs(t, <-result, want)
				// 已取消的请求应在调用 Broker 前直接返回
				_, _, _, err := m.Request(ctx, "svc", "cmd", "1", nil)
				require.ErrorIs(t, err, want)
			})
		})
	}
}

func TestRPCRequestUsesEarlierMeshDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bro := &rpcRequestBroker{}
		m := New(Broker(bro))
		limit := time.Now().Add(time.Second)
		var cancel context.CancelFunc
		m.ctx, cancel = context.WithDeadline(t.Context(), limit)
		defer cancel()
		caller, cancelCaller := context.WithTimeout(t.Context(), time.Hour)
		defer cancelCaller()
		bro.request = func(ctx context.Context, _ string, _ []byte, h broker.Header) (*broker.Message, error) {
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.True(t, deadline.Equal(limit))
			require.Equal(t, strconv.FormatInt(limit.UnixMilli(), 10), h.Get(rpcDeadlineHeader))
			<-ctx.Done()
			return nil, ctx.Err()
		}
		_, _, _, err := m.Request(caller, "svc", "cmd", "1", nil)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.NoError(t, caller.Err())
	})
}
