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
	"github.com/byteweap/meta/internal/cluster"
)

type panicReply struct {
	subject string
	code    string
	tip     string
	data    []byte
}

type panicBroker struct {
	mockBroker
	handler       broker.Handler
	replies       chan panicReply
	panicReplyFor string
}

func (b *panicBroker) Sub(_ context.Context, _ string, handler broker.Handler, _ ...broker.SubscribeOption) (broker.Subscription, error) {
	b.handler = handler
	return &mockSubscription{}, nil
}

func (b *panicBroker) Reply(_ context.Context, msg *broker.Message, data []byte, opts ...broker.ReplyOption) error {
	o := &broker.ReplyOptions{}
	for _, opt := range opts {
		opt(o)
	}
	b.replies <- panicReply{msg.Reply, o.Header.Get("code"), o.Header.Get("tip"), append([]byte(nil), data...)}
	if msg.Reply == b.panicReplyFor {
		panic("reply transport failed")
	}
	return nil
}

func runPanicMesh(t *testing.T, m *Mesh) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- m.Start(meta.NewContext(ctx, lifecycleAppInfo{})) }()
	t.Cleanup(func() {
		cancel()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
		defer stopCancel()
		require.NoError(t, m.Stop(stopCtx))
		select {
		case err := <-finished:
			require.NoError(t, err)
		case <-stopCtx.Done():
			t.Error("mesh did not stop after panic test")
		}
	})
	select {
	case <-m.Ready():
	case <-time.After(time.Second):
		t.Fatal("mesh did not become ready")
	}
}

func TestMeshPanicBusinessAndEventsContinue(t *testing.T) {
	for _, event := range []cluster.Event{cluster.EventBusiness, cluster.EventOnline, cluster.EventOffline, cluster.EventReconnect} {
		t.Run(string(event), func(t *testing.T) {
			bro := &panicBroker{}
			m := New(Locator(lifecycleLocator{}), Broker(bro))
			called := make(chan int64, 2)
			handler := func(uid int64) {
				called <- uid
				if uid == 1 {
					panic("business handler failed")
				}
			}
			m.Route(1, 1, func(_ *Mesh, msg *broker.Message, _ *envelope.IMessage) {
				handler(cluster.GetUIDBy(msg.Header))
			})
			m.OnlineHandler(handler)
			m.OfflineHandler(handler)
			m.ReconnectHandler(handler)
			raw := mustBusinessMessage(t, 1, 1, "game", &envelope.Header{})
			runPanicMesh(t, m)
			for _, uid := range []int64{1, 2} {
				bro.handler(&broker.Message{Header: cluster.BuildHeader(uid, event, "", "gate", "game"), Data: raw})
			}
			for _, want := range []int64{1, 2} {
				select {
				case uid := <-called:
					require.Equal(t, want, uid)
				case <-time.After(time.Second):
					t.Fatal("message loop did not continue after handler panic")
				}
			}
		})
	}
}

func TestMeshPanicRPCContinuesWithoutDuplicateReply(t *testing.T) {
	for _, transportPanic := range []bool{false, true} {
		name := "handler panic"
		if transportPanic {
			name = "reply transport panic"
		}
		t.Run(name, func(t *testing.T) {
			bro := &panicBroker{replies: make(chan panicReply, 4)}
			if transportPanic {
				bro.panicReplyFor = "failed.reply"
			}
			m := New(Locator(lifecycleLocator{}), Broker(bro))
			m.RPCRoute("first", "v1", func(*Mesh, *broker.Message) ([]byte, string, int) {
				if !transportPanic {
					panic("private handler panic detail")
				}
				return []byte("first"), "ok", http.StatusOK
			})
			m.RPCRoute("next", "v1", func(*Mesh, *broker.Message) ([]byte, string, int) {
				return []byte("next"), "ok", http.StatusOK
			})
			processed := make(chan struct{})
			m.Route(99, 1, func(*Mesh, *broker.Message, *envelope.IMessage) { close(processed) })
			barrier := mustBusinessMessage(t, 99, 1, "game", &envelope.Header{})
			runPanicMesh(t, m)
			bro.handler(&broker.Message{Reply: "failed.reply", Header: broker.Header{"cmd": {"first"}, "version": {"v1"}}})
			bro.handler(&broker.Message{Reply: "next.reply", Header: broker.Header{"cmd": {"next"}, "version": {"v1"}}})
			bro.handler(&broker.Message{Data: barrier})
			select {
			case <-processed:
			case <-time.After(time.Second):
				t.Fatal("message loop did not process the request after panic")
			}

			// The following business message confirms both RPC dispatches have finished.
			require.Len(t, bro.replies, 2, "each request must attempt exactly one reply")
			first, next := <-bro.replies, <-bro.replies
			require.Equal(t, "failed.reply", first.subject)
			if transportPanic {
				require.Equal(t, "200", first.code)
				require.Equal(t, []byte("first"), first.data)
			} else {
				require.Equal(t, "500", first.code)
				require.Equal(t, http.StatusText(http.StatusInternalServerError), first.tip)
				require.Nil(t, first.data)
			}
			require.NotContains(t, first.tip, "private handler panic detail")
			require.Equal(t, panicReply{"next.reply", "200", "ok", []byte("next")}, next)
		})
	}
}
