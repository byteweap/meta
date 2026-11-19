package mesh

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/byteweap/meta"
	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/envelope"
	"github.com/byteweap/meta/internal/cluster"
)

type drainBroker struct {
	panicBroker
	intake      context.Context
	published   chan []byte
	requested   chan string
	beforeReply func(context.Context)
}

func (b *drainBroker) Sub(ctx context.Context, subject string, handler broker.Handler, opts ...broker.SubscribeOption) (broker.Subscription, error) {
	b.intake = ctx
	return b.panicBroker.Sub(ctx, subject, handler, opts...)
}

func (b *drainBroker) Pub(ctx context.Context, _ string, data []byte, _ ...broker.PublishOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.published <- data
	return nil
}

func (b *drainBroker) Request(ctx context.Context, subject string, _ []byte, _ ...broker.RequestOption) (*broker.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.requested <- subject
	return &broker.Message{}, nil
}

func (b *drainBroker) Reply(ctx context.Context, msg *broker.Message, data []byte, opts ...broker.ReplyOption) error {
	if b.beforeReply != nil {
		b.beforeReply(ctx)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return b.panicBroker.Reply(ctx, msg, data, opts...)
}

func TestAppDrainsAcceptedMeshRequests(t *testing.T) {
	for _, workers := range []int{0, 1} {
		t.Run("workers="+strconv.Itoa(workers), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				bro := &drainBroker{
					panicBroker: panicBroker{replies: make(chan panicReply, 5)},
					published:   make(chan []byte, 3),
					requested:   make(chan string, 1),
				}
				options := []Option{Broker(bro), Locator(lifecycleLocator{})}
				if workers > 0 {
					options = append(options, RPCConcurrency(workers, 2))
				}
				m := New(options...)
				entered, release := make(chan struct{}), make(chan struct{})
				finish := sync.OnceFunc(func() { close(release) })
				defer finish()
				m.RPCRoute("work", "1", func(ctx context.Context, m *Mesh, msg *broker.Message) ([]byte, string, int) {
					switch msg.Reply {
					case "running":
						close(entered)
						<-release
						if _, _, _, err := m.Request(ctx, "downstream", "work", "1", nil); err != nil {
							t.Errorf("outbound RPC during drain: %v", err)
						}
					case "expired", "late":
						t.Errorf("unexpected RPC execution: %s", msg.Reply)
					}
					return []byte(msg.Reply), "", http.StatusOK
				})
				m.Route(1, 1, func(m *Mesh, msg *broker.Message, frame *envelope.ClientEnvelope) {
					<-release
					ctx := newContext(m, msg, frame)
					defer ctx.release()
					ctx.respond(nil, nil)
				})

				started := make(chan struct{})
				stopping := make(chan error, 1)
				app := meta.New(meta.Name("game"), meta.Signal(), meta.Server(m),
					meta.PostStart(func(context.Context) error { close(started); return nil }),
					meta.PreStop(func(context.Context) error { stopping <- bro.intake.Err(); return nil }),
				)
				done := make(chan error, 1)
				go func() { done <- app.Run() }()
				defer func() { _ = app.Stop() }()
				<-started
				bro.handler(poolRequest("running"))
				<-entered
				expired := poolRequest("expired")
				expired.Header.Set(rpcDeadlineHeader, strconv.FormatInt(time.Now().Add(time.Second).UnixMilli(), 10))
				bro.handler(expired)
				bro.handler(poolRequest("queued"))
				deliverClient := func(seq uint64) {
					header := cluster.BuildHeader(42, cluster.EventBusiness, "client-reply", "gate", "game")
					data, err := proto.Marshal(&envelope.ClientEnvelope{
						Service: "game",
						Header:  &envelope.Header{Cmd: 1, Version: 1, Seq: seq},
					})
					require.NoError(t, err)
					bro.handler(&broker.Message{Header: header, Data: data})
				}
				deliverClient(1)
				deliverClient(2)
				time.Sleep(time.Second)
				require.NoError(t, app.Stop())
				require.ErrorIs(t, <-stopping, context.Canceled)
				require.NoError(t, m.ctx.Err())
				bro.handler(poolRequest("late"))
				deliverClient(3)
				finish()
				require.NoError(t, <-done)
				require.Equal(t, "downstream", <-bro.requested)

				codes := map[string]string{}
				for len(bro.replies) > 0 {
					reply := <-bro.replies
					require.NotContains(t, codes, reply.subject)
					codes[reply.subject] = reply.code
				}
				want := map[string]string{"running": "200", "queued": "200", "expired": "504"}
				if workers > 0 {
					want["late"] = "503"
				}
				require.Equal(t, want, codes)
				for seq := uint64(1); seq <= 2; seq++ {
					require.NotEmpty(t, bro.published)
					var reply envelope.ServerEnvelope
					require.NoError(t, proto.Unmarshal(<-bro.published, &reply))
					require.Equal(t, seq, reply.GetHeader().GetSeq())
				}
				require.Empty(t, bro.published)
			})
		})
	}
}

func TestDrainDeadlineCancelsStartedReply(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{})
		brokerDone := make(chan error, 1)
		bro := &drainBroker{}
		bro.beforeReply = func(ctx context.Context) {
			close(entered)
			<-ctx.Done()
			brokerDone <- ctx.Err()
		}
		m := New(Broker(bro), Locator(lifecycleLocator{}))
		m.RPCRoute("work", "1", func(context.Context, *Mesh, *broker.Message) ([]byte, string, int) {
			return nil, "", http.StatusOK
		})
		runTestMesh(t, m)
		bro.handler(poolRequest("running"))
		<-entered
		timeout, cancel := context.WithTimeout(t.Context(), time.Millisecond)
		defer cancel()
		_ = m.Stop(timeout)
		require.ErrorIs(t, <-brokerDone, context.Canceled)
		require.NoError(t, m.Stop(t.Context()))
	})
}

func TestDrainStopsBlockedSerialAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bro := &panicBroker{replies: make(chan panicReply, 3)}
		m := New(Broker(bro), Locator(lifecycleLocator{}), MessageBufferSize(1))
		entered, release := make(chan struct{}), make(chan struct{})
		finish := sync.OnceFunc(func() { close(release) })
		defer finish()
		m.RPCRoute("work", "1", func(_ context.Context, _ *Mesh, msg *broker.Message) ([]byte, string, int) {
			if msg.Reply == "running" {
				close(entered)
				<-release
			}
			return []byte(msg.Reply), "", http.StatusOK
		})
		runTestMesh(t, m)
		bro.handler(poolRequest("running"))
		<-entered
		bro.handler(poolRequest("queued"))
		returned := make(chan struct{})
		go func() {
			bro.handler(poolRequest("blocked"))
			close(returned)
		}()
		synctest.Wait()
		require.Empty(t, returned)
		m.BeginDrain(t.Context())
		<-returned
		finish()
		require.NoError(t, m.Stop(t.Context()))
		require.Len(t, bro.replies, 2)
		require.Equal(t, "running", (<-bro.replies).subject)
		require.Equal(t, "queued", (<-bro.replies).subject)
	})
}
