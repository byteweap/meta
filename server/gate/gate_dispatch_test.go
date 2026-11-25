package gate

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/byteweap/meta/component/observe"
	"github.com/byteweap/meta/component/registry"
	"github.com/byteweap/meta/envelope"
)

type dispatchTestLocator struct {
	testLocator
	calls atomic.Int32
	node  func() (string, error)
}

type dispatchObserver struct {
	stages  chan observe.Stage
	reasons chan observe.Reason
}

func (o dispatchObserver) Observe(stage observe.Stage, _ time.Duration) { o.stages <- stage }
func (o dispatchObserver) Reject(reason observe.Reason)                 { o.reasons <- reason }

func (l *dispatchTestLocator) Node(context.Context, int64, string) (string, error) {
	l.calls.Add(1)
	return l.node()
}

func TestGateDispatchErrorResponse(t *testing.T) {
	failure := errors.New("private backend details")
	for _, tc := range []struct {
		name      string
		instances []*registry.ServiceInstance
		getErr    error
		service   string
		nodeErr   error
		watchErr  error
		node      string
		code      int
		lookups   int32
		watches   int32
		snapshots int32
		wantPub   bool
	}{
		{name: "unknown", service: "game", code: 404, snapshots: 2},
		{name: "missing instance ID", instances: []*registry.ServiceInstance{{Name: "game"}}, service: "game", code: 404, snapshots: 2},
		{name: "other service", instances: initialDiscoverySnapshot("game-1"), service: "Game", code: 404, snapshots: 2},
		{name: "missing service", code: 404},
		{name: "invalid name", service: "../internal", code: 404},
		{name: "lookup error", service: "game", getErr: failure, code: 503, snapshots: 2},
		{name: "watch error", service: "game", instances: initialDiscoverySnapshot("game-1"), watchErr: failure, code: 503, watches: 2, snapshots: 2},
		{name: "locator error", service: "game", instances: initialDiscoverySnapshot("game-1"), nodeErr: failure, code: 503, watches: 1, snapshots: 2, lookups: 2},
		{name: "stale binding", service: "game", instances: initialDiscoverySnapshot("game-2"), node: "game-1", code: 503, watches: 1, snapshots: 2, lookups: 2},
		{
			name: "publish error", service: "game", instances: initialDiscoverySnapshot("game-1"), node: "game-1",
			code: 503, lookups: 2, watches: 1, snapshots: 2, wantPub: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loc := &dispatchTestLocator{node: func() (string, error) { return tc.node, tc.nodeErr }}
			reg := &initialDiscoveryRegistry{
				watch: func(context.Context, string) (registry.Watcher, error) {
					if tc.watchErr != nil {
						return nil, tc.watchErr
					}
					return newInitialDiscoveryWatcher(), nil
				},
				get: func(context.Context, string) ([]*registry.ServiceInstance, error) { return tc.instances, tc.getErr },
			}
			bro := &testBroker{pubErrs: map[string]error{"meta.gate.game.game-1": failure}}
			obs := dispatchObserver{stages: make(chan observe.Stage, 3), reasons: make(chan observe.Reason, 3)}
			opts := []Option{Locator(loc), Discovery(reg), Broker(bro), testGateSelector(), Observer(obs)}
			s := newOwnershipServer(t, newOwnershipLocator(), "gate-1", opts...)
			conn := s.dial(t, 42, "current")
			// 同一连接连续发送，任何多余的回包都会被下一次读取捕获
			for _, seq := range []uint64{0, 17} {
				sendDispatchRequest(t, conn, tc.service, seq)
				requireDispatchError(t, conn, seq, tc.code)
			}
			require.Len(t, obs.stages, 2)
			require.Len(t, obs.reasons, 2)
			for range 2 {
				require.Equal(t, observe.Routing, <-obs.stages)
				require.Equal(t, observe.Unavailable, <-obs.reasons)
			}
			require.Equal(t, tc.lookups, loc.calls.Load())
			require.Equal(t, tc.watches, reg.watchCount.Load())
			require.Equal(t, tc.snapshots, reg.getCount.Load())
			bro.mu.Lock()
			subjects := append([]string(nil), bro.pubSubjects...)
			bro.mu.Unlock()
			if tc.wantPub {
				require.Equal(t, []string{"meta.gate.game.game-1", "meta.gate.game.game-1"}, subjects)
			} else {
				require.Empty(t, subjects)
			}
		})
	}
}

func sendDispatchRequest(t *testing.T, conn *websocket.Conn, service string, seq uint64) {
	t.Helper()
	data, err := proto.Marshal(&envelope.ClientEnvelope{
		Service: service, Header: &envelope.Header{Cmd: 7, Version: 2, Seq: seq, SentAtUnixMs: 1},
	})
	require.NoError(t, err)
	require.NoError(t, conn.WriteMessage(websocket.BinaryMessage, data))
}

func requireDispatchError(t *testing.T, conn *websocket.Conn, seq uint64, code int) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
	kind, data, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, websocket.BinaryMessage, kind)
	var reply envelope.ServerEnvelope
	require.NoError(t, proto.Unmarshal(data, &reply))
	require.Equal(t, envelope.MsgType_MSG_TYPE_RESPONSE, reply.Kind)
	require.Equal(t, "gate", reply.Service)
	require.Equal(t, seq, reply.GetHeader().GetSeq())
	require.EqualValues(t, 7, reply.GetHeader().GetCmd())
	require.EqualValues(t, 2, reply.GetHeader().GetVersion())
	require.WithinDuration(t, time.Now(), time.UnixMilli(reply.GetHeader().GetSentAtUnixMs()), time.Minute)
	require.EqualValues(t, code, reply.Error.GetCode())
	require.Equal(t, http.StatusText(code), reply.Error.GetMessage())
	require.Empty(t, reply.Payload)
}

func TestDispatchFailureAfterReconnect(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	loc := &dispatchTestLocator{node: func() (string, error) {
		close(entered)
		<-release
		return "", errors.New("lookup failed")
	}}
	s := newOwnershipServer(t, newOwnershipLocator(), "gate-1", testGameDiscovery(), testGateSelector(), Locator(loc))
	t.Cleanup(unblock)
	old := s.dial(t, 42, "old")
	sendDispatchRequest(t, old, "game", 1)
	awaitShutdownConnection(t, entered)
	current := s.dial(t, 42, "current")
	requireOwnershipClosed(t, old)
	unblock()
	require.Equal(t, "old", awaitShutdownConnection(t, s.finished))
	sendDispatchRequest(t, current, "", 1)
	requireDispatchError(t, current, 1, http.StatusNotFound)
}
