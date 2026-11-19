package mesh

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta"
	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/locator"
	"github.com/byteweap/meta/envelope"
	"github.com/byteweap/meta/internal/cluster"
)

func poolRequest(id string) *broker.Message {
	return &broker.Message{Reply: id, Header: broker.Header{"cmd": {"work"}, "version": {"1"}}}
}

func TestRPCConcurrencyConfig(t *testing.T) {
	for _, cfg := range [][2]int{{-1, 0}, {1, -1}, {0, 2}, {1, math.MaxInt}} {
		m := New(Broker(&mockBroker{}), Locator(lifecycleLocator{}), RPCConcurrency(cfg[0], cfg[1]))
		err := m.Start(meta.NewContext(t.Context(), lifecycleAppInfo{}))
		require.ErrorContains(t, err, "invalid mesh RPC concurrency")
	}
}

func TestRPCConcurrencyDispatchOrder(t *testing.T) {
	for _, mode := range []string{"serial", "two workers"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				bro := &panicBroker{replies: make(chan panicReply, 2)}
				loc := sessionEventLocator{lookup: func(context.Context, int64, string) (locator.Session, error) {
					return locator.Session{Node: "gate-1", ID: "current"}, nil
				}}
				opts := []Option{Broker(bro), Locator(loc)}
				if mode == "two workers" {
					opts = append(opts, RPCConcurrency(2, 2))
				}
				m := New(opts...)
				slow, release := make(chan struct{}), make(chan struct{})
				m.RPCRoute("work", "1", func(ctx context.Context, _ *Mesh, msg *broker.Message) ([]byte, string, int) {
					if msg.Reply == "slow" {
						close(slow)
						select {
						case <-release:
						case <-ctx.Done():
						}
					}
					return nil, "", http.StatusOK
				})
				client, finishClient, online := make(chan struct{}), make(chan struct{}), make(chan struct{})
				unblock := sync.OnceFunc(func() { close(finishClient) })
				defer unblock()
				m.Route(1, 1, func(*Mesh, *broker.Message, *envelope.ClientEnvelope) {
					close(client)
					<-finishClient
				})
				m.OnlineHandler(func(int64) { close(online) })
				runTestMesh(t, m)
				bro.handler(poolRequest("slow"))
				<-slow
				bro.handler(&broker.Message{Data: mustBusinessMessage(t, 1, 1, "game", nil)})
				bro.handler(&broker.Message{Header: sessionEventHeader(42, cluster.EventOnline, "current")})
				bro.handler(poolRequest("fast"))
				synctest.Wait()
				if mode == "serial" {
					select {
					case <-client:
						t.Fatal("client started while the serial RPC handler was blocked")
					default:
					}
					require.Empty(t, bro.replies)
					close(release)
					require.Equal(t, "slow", (<-bro.replies).subject)
				} else {
					require.Equal(t, "fast", (<-bro.replies).subject)
				}
				<-client
				synctest.Wait()
				select {
				case <-online:
					t.Fatal("session event overtook the client handler")
				default:
				}
				unblock()
				<-online
				if mode == "serial" {
					require.Equal(t, "fast", (<-bro.replies).subject)
				} else {
					close(release)
					require.Equal(t, "slow", (<-bro.replies).subject)
				}
				require.Empty(t, bro.replies)
			})
		})
	}
}

func TestRPCConcurrencyCapacityAndExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bro := &panicBroker{replies: make(chan panicReply, 5), panicReplyFor: "overflow"}
		m := New(Broker(bro), Locator(lifecycleLocator{}), RPCConcurrency(2, 1))
		entered, release := make(chan string, 5), make(chan struct{})
		m.RPCRoute("work", "1", func(ctx context.Context, _ *Mesh, msg *broker.Message) ([]byte, string, int) {
			entered <- msg.Reply
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil, "", http.StatusOK
		})
		runTestMesh(t, m)
		first, second := poolRequest("first"), poolRequest("second")
		second.Header = first.Header
		for _, msg := range []*broker.Message{first, second} {
			bro.handler(msg)
			require.Equal(t, msg.Reply, <-entered)
		}
		queued := poolRequest("queued")
		queued.Header.Set(rpcDeadlineHeader, strconv.FormatInt(time.Now().Add(time.Second).UnixMilli(), 10))
		bro.handler(queued)
		bro.handler(poolRequest("overflow"))
		require.Equal(t, panicReply{"overflow", "503", "Service Unavailable", nil}, <-bro.replies)
		time.Sleep(time.Second)
		close(release)
		synctest.Wait()
		require.Len(t, bro.replies, 3)
		require.ElementsMatch(t, []panicReply{
			{"first", "200", "ok", nil}, {"second", "200", "ok", nil}, {"queued", "504", "Gateway Timeout", nil},
		}, []panicReply{<-bro.replies, <-bro.replies, <-bro.replies})
		require.Empty(t, entered)
		require.NotContains(t, first.Header, "code")
		require.NotContains(t, first.Header, "tip")
	})
}

func TestRPCConcurrencyZeroQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bro := &panicBroker{replies: make(chan panicReply, 2)}
		m := New(Broker(bro), Locator(lifecycleLocator{}), RPCConcurrency(1, 0))
		entered := make(chan struct{})
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		m.RPCRoute("work", "1", func(context.Context, *Mesh, *broker.Message) ([]byte, string, int) {
			close(entered)
			<-release
			return []byte("done"), "", http.StatusOK
		})
		runTestMesh(t, m)
		bro.handler(poolRequest("running"))
		synctest.Wait()
		require.Empty(t, bro.replies, "Ready must expose the first worker slot")
		<-entered
		bro.handler(poolRequest("overflow"))
		require.Equal(t, "overflow", (<-bro.replies).subject)
		unblock()
		require.NoError(t, m.Stop(t.Context()))
		require.Equal(t, panicReply{"running", "200", "ok", []byte("done")}, <-bro.replies)
		require.Empty(t, bro.replies)
	})
}

func TestRPCConcurrencyStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bro := &panicBroker{replies: make(chan panicReply, 4), panicReplyFor: "running"}
		m := New(Broker(bro), Locator(lifecycleLocator{}), RPCConcurrency(1, 2))
		canceled, release := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		m.RPCRoute("work", "1", func(ctx context.Context, _ *Mesh, msg *broker.Message) ([]byte, string, int) {
			if msg.Reply != "running" {
				t.Error("queued request reached business during shutdown")
				return nil, "", http.StatusInternalServerError
			}
			<-ctx.Done()
			close(canceled)
			<-release
			return nil, "stopping", http.StatusServiceUnavailable
		})
		runTestMesh(t, m)
		bro.handler(poolRequest("running"))
		synctest.Wait()
		bro.handler(poolRequest("queued"))
		timeout, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		require.ErrorIs(t, m.Stop(timeout), context.DeadlineExceeded)
		<-canceled
		require.Empty(t, bro.replies, "reply budget has expired")
		unblock()
		require.NoError(t, m.Stop(t.Context()))
		bro.handler(poolRequest("late"))
		require.Empty(t, bro.replies)
	})
}

type slowRejectBroker struct {
	panicBroker
	started chan struct{}
	release chan struct{}
}

func (b *slowRejectBroker) Reply(ctx context.Context, msg *broker.Message, data []byte, opts ...broker.ReplyOption) error {
	if msg.Reply == "overflow" {
		close(b.started)
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.panicBroker.Reply(ctx, msg, data, opts...)
}

func TestRPCConcurrencyStopWaitsForOverloadReply(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bro := &slowRejectBroker{
			panicBroker: panicBroker{replies: make(chan panicReply, 3)},
			started:     make(chan struct{}), release: make(chan struct{}),
		}
		m := New(Broker(bro), Locator(lifecycleLocator{}), RPCConcurrency(1, 1))
		entered := make(chan struct{})
		finish := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(finish) })
		defer unblock()
		m.RPCRoute("work", "1", func(_ context.Context, _ *Mesh, msg *broker.Message) ([]byte, string, int) {
			if msg.Reply == "running" {
				close(entered)
				<-finish
			}
			return []byte("done"), "", http.StatusOK
		})
		runTestMesh(t, m)
		bro.handler(poolRequest("running"))
		<-entered
		bro.handler(poolRequest("queued"))
		go bro.handler(poolRequest("overflow"))
		<-bro.started

		stopped := make(chan error, 1)
		go func() { stopped <- m.Stop(t.Context()) }()
		synctest.Wait()
		unblock()
		synctest.Wait()
		require.Empty(t, stopped)
		require.Len(t, bro.replies, 2)
		require.ElementsMatch(t, []panicReply{
			{"running", "200", "ok", []byte("done")}, {"queued", "200", "ok", []byte("done")},
		}, []panicReply{<-bro.replies, <-bro.replies})
		close(bro.release)
		require.NoError(t, <-stopped)
		require.Equal(t, panicReply{"overflow", "503", "Service Unavailable", nil}, <-bro.replies)
		require.Empty(t, bro.replies)
	})
}

func TestRPCConcurrencyDeliveryDuringStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bro := &panicBroker{replies: make(chan panicReply, 1)}
		m := New(Broker(bro), Locator(lifecycleLocator{}), RPCConcurrency(1, 0))
		entered, release := make(chan struct{}), make(chan struct{})
		defer close(release)
		m.Route(1, 1, func(*Mesh, *broker.Message, *envelope.ClientEnvelope) {
			close(entered)
			<-release
		})
		runTestMesh(t, m)
		bro.handler(&broker.Message{Data: mustBusinessMessage(t, 1, 1, "game", nil)})
		<-entered
		timeout, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		m.BeginDrain(timeout)
		bro.handler(poolRequest("during-stop"))
		require.Len(t, bro.replies, 1, "a delivered RPC must receive a shutdown reply")
		require.Equal(t, panicReply{"during-stop", "503", "Service Unavailable", nil}, <-bro.replies)
		require.ErrorIs(t, m.Stop(timeout), context.DeadlineExceeded)
		bro.handler(poolRequest("after-timeout"))
		require.Empty(t, bro.replies, "no reply is started after the drain deadline")
	})
}

type failedRPCSubscription struct{ panicBroker }

func (*failedRPCSubscription) Sub(_ context.Context, _ string, deliver broker.Handler, _ ...broker.SubscribeOption) (broker.Subscription, error) {
	deliver(poolRequest("before-ready"))
	return nil, errors.New("subscribe failed")
}

func TestRPCConcurrencyFailedSubscription(t *testing.T) {
	bro := &failedRPCSubscription{panicBroker{replies: make(chan panicReply, 1)}}
	m := New(Broker(bro), Locator(lifecycleLocator{}), RPCConcurrency(1, 0))
	m.RPCRoute("work", "1", func(context.Context, *Mesh, *broker.Message) ([]byte, string, int) {
		t.Error("failed subscription started a business handler")
		return nil, "", http.StatusOK
	})
	err := m.Start(meta.NewContext(t.Context(), lifecycleAppInfo{}))
	require.ErrorContains(t, err, "subscribe failed")
	require.Len(t, bro.replies, 1)
	require.Equal(t, panicReply{"before-ready", "503", "Service Unavailable", nil}, <-bro.replies)
}
