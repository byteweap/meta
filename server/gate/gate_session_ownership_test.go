package gate

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/byteweap/meta"
	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/locator"
	"github.com/byteweap/meta/envelope"
	"github.com/byteweap/meta/internal/cluster"
	"github.com/byteweap/meta/server/mesh"
)

type ownershipClaim struct {
	uid  int64
	id   string
	node string
}

type ownershipRelease struct {
	ownershipClaim
	released bool
}

// This locator models atomic ownership changes independently of the ordinary
// locator test double, including a stale release that succeeds without deleting.
type ownershipLocator struct {
	testLocator
	stateMu     sync.Mutex
	states      map[int64]locator.Session
	claimCount  int
	beforeClaim func(context.Context, int64, int) error
	claims      chan ownershipClaim
	releases    chan ownershipRelease
}

func newOwnershipLocator() *ownershipLocator {
	return &ownershipLocator{
		states: make(map[int64]locator.Session),
		claims: make(chan ownershipClaim, 16), releases: make(chan ownershipRelease, 16),
	}
}

func (l *ownershipLocator) Claim(ctx context.Context, uid int64, _, node, id string) (bool, error) {
	l.stateMu.Lock()
	l.claimCount++
	call := l.claimCount
	l.stateMu.Unlock()
	l.claims <- ownershipClaim{uid, id, node}
	if l.beforeClaim != nil {
		if err := l.beforeClaim(ctx, uid, call); err != nil {
			return false, err
		}
	}
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	replaced := l.states[uid].Node != ""
	l.states[uid] = locator.Session{ID: id, Node: node}
	return replaced, nil
}

func (l *ownershipLocator) Release(_ context.Context, uid int64, _, node, id string) (bool, error) {
	l.stateMu.Lock()
	current := l.states[uid]
	released := current.ID == id && current.Node == node && node != ""
	if released {
		current.Node = ""
		l.states[uid] = current
	}
	l.stateMu.Unlock()
	l.releases <- ownershipRelease{ownershipClaim{uid, id, node}, released}
	return released, nil
}

func (l *ownershipLocator) Session(_ context.Context, uid int64, _ string) (locator.Session, error) {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	return l.states[uid], nil
}

func (l *ownershipLocator) AllNodes(ctx context.Context, uid int64) (map[string]string, error) {
	state, _ := l.Session(ctx, uid, "gate")
	nodes := map[string]string{"game": "game-1"}
	if state.Node != "" {
		nodes["gate"] = state.Node
	}
	return nodes, nil
}

type ownershipEvent struct {
	uid   int64
	id    string
	event cluster.Event
}

type ownershipBroker struct {
	testBroker
	events chan ownershipEvent
}

func (b *ownershipBroker) Pub(_ context.Context, _ string, _ []byte, opts ...broker.PublishOption) error {
	o := &broker.PublishOptions{}
	for _, opt := range opts {
		opt(o)
	}
	b.events <- ownershipEvent{
		uid: cluster.GetUIDBy(o.Header), id: o.Header.Get(cluster.FieldNameSessionID),
		event: cluster.GetEventBy(o.Header),
	}
	return nil
}

type ownershipServer struct {
	gate     *Gate
	url      string
	finished chan string
	events   chan ownershipEvent
}

func newOwnershipServer(t *testing.T, loc *ownershipLocator, node string, opts ...Option) *ownershipServer {
	t.Helper()
	b := &ownershipBroker{events: make(chan ownershipEvent, 16)}
	options := make([]Option, 0, 4+len(opts))
	options = append(options, Addr("127.0.0.1:0"), Locator(loc), Broker(b), UserIDExtractor(func(r *http.Request) int64 {
		uid, _ := strconv.ParseInt(r.Header.Get("X-Test-UID"), 10, 64)
		return uid
	}))
	g := New(append(options, opts...)...)
	require.NoError(t, g.setup(g.ctx, "gate", node))
	finished := make(chan string, 16)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { finished <- r.Header.Get("X-Test-Connection") }()
		g.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, g.Stop(ctx))
		ts.Close()
	})
	return &ownershipServer{g, "ws" + strings.TrimPrefix(ts.URL, "http"), finished, b.events}
}

func (s *ownershipServer) dial(t *testing.T, uid int64, name string) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.DefaultDialer.Dial(s.url, http.Header{
		"X-Test-UID": {strconv.FormatInt(uid, 10)}, "X-Test-Connection": {name},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func requireOwnershipClosed(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
	_, _, err := conn.ReadMessage()
	require.Error(t, err, "replaced or rejected connection must be closed")
	if netErr, ok := err.(interface{ Timeout() bool }); ok {
		require.False(t, netErr.Timeout(), "connection remained open until the read deadline")
	}
}

func requireOwnershipDelivery(t *testing.T, s *ownershipServer, conn *websocket.Conn, uid int64) {
	t.Helper()
	want := []byte("current session receives messages")
	s.gate.handlePubSubMessage(&broker.Message{
		Header: cluster.BuildHeader(uid, cluster.EventBusiness, "", "game", "gate"), Data: want,
	})
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
	kind, data, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, websocket.BinaryMessage, kind)
	require.Equal(t, want, data)
}

func requireOwnershipCurrent(t *testing.T, s *ownershipServer, loc *ownershipLocator, uid int64, id string) {
	t.Helper()
	state, err := loc.Session(context.Background(), uid, "gate")
	require.NoError(t, err)
	require.Equal(t, locator.Session{ID: id, Node: s.gate.appID}, state)
	current, ok := s.gate.sessions.get(uid)
	require.True(t, ok)
	currentID, ok := current.Get(cluster.FieldNameSessionID)
	require.True(t, ok)
	require.Equal(t, id, currentID)
}

func TestGateOwnershipFailedClaimPreservesOldConnection(t *testing.T) {
	loc := newOwnershipLocator()
	loc.beforeClaim = func(_ context.Context, _ int64, call int) error {
		if call == 2 {
			return errors.New("claim rejected before writing")
		}
		return nil
	}
	s := newOwnershipServer(t, loc, "gate-1")
	old := s.dial(t, 42, "old")
	online := awaitShutdownConnection(t, s.events)
	require.Equal(t, cluster.EventOnline, online.event)
	require.NotEmpty(t, online.id)
	failed := s.dial(t, 42, "failed")
	requireOwnershipClosed(t, failed)
	require.Equal(t, "failed", awaitShutdownConnection(t, s.finished))
	requireOwnershipCurrent(t, s, loc, 42, online.id)
	requireOwnershipDelivery(t, s, old, 42)
	require.Empty(t, s.events, "failed claim must not publish a state change")
	require.Empty(t, loc.releases, "failed claim must not release the old ownership")
}

func TestGateOwnershipSameNodeReconnectKeepsNewSession(t *testing.T) {
	loc := newOwnershipLocator()
	s := newOwnershipServer(t, loc, "gate-1")
	old := s.dial(t, 42, "old")
	first := awaitShutdownConnection(t, s.events)
	current := s.dial(t, 42, "current")
	second := awaitShutdownConnection(t, s.events)
	require.Equal(t, cluster.EventReconnect, second.event)
	require.NotEqual(t, first.id, second.id)
	requireOwnershipClosed(t, old)
	require.Equal(t, "old", awaitShutdownConnection(t, s.finished))
	requireOwnershipCurrent(t, s, loc, 42, second.id)
	requireOwnershipDelivery(t, s, current, 42)
	require.Empty(t, loc.releases, "old local session must not release its replacement")
	require.Empty(t, s.events)

	require.NoError(t, current.Close())
	offline := awaitShutdownConnection(t, s.events)
	require.Equal(t, ownershipEvent{42, second.id, cluster.EventOffline}, offline)
	require.Equal(t, "current", awaitShutdownConnection(t, s.finished))
	state, _ := loc.Session(context.Background(), 42, "gate")
	require.Equal(t, locator.Session{ID: second.id}, state)
	require.True(t, awaitShutdownConnection(t, loc.releases).released)
	require.Empty(t, loc.releases)
}

type sessionReplyLocator struct{ *ownershipLocator }

func (*sessionReplyLocator) Node(context.Context, int64, string) (string, error) {
	return "game-1", nil
}

type sessionReplyBroker struct {
	ownershipBroker
	meshHandler broker.Handler
	downstream  chan *broker.Message
}

func (b *sessionReplyBroker) Sub(_ context.Context, _ string, handler broker.Handler, _ ...broker.SubscribeOption) (broker.Subscription, error) {
	b.meshHandler = handler
	return &testSubscription{}, nil
}

func (b *sessionReplyBroker) Pub(ctx context.Context, subject string, data []byte, opts ...broker.PublishOption) error {
	var options broker.PublishOptions
	for _, option := range opts {
		option(&options)
	}
	header := options.Header
	if cluster.GetEventBy(header) != cluster.EventBusiness {
		return b.ownershipBroker.Pub(ctx, subject, data, opts...)
	}
	message := &broker.Message{Subject: subject, Header: header, Data: data}
	if cluster.GetToServiceBy(header) == "game" {
		b.meshHandler(message)
	} else {
		b.downstream <- message
	}
	return nil
}

func TestGateSessionReplyRejectsOldConnectionWithSameSequence(t *testing.T) {
	loc := newOwnershipLocator()
	bro := &sessionReplyBroker{
		ownershipBroker: ownershipBroker{events: make(chan ownershipEvent, 16)},
		downstream:      make(chan *broker.Message, 8),
	}
	srv := newOwnershipServer(t, loc, "gate-1", Locator(&sessionReplyLocator{loc}), Broker(bro), StrictSessionDelivery(true))
	game := mesh.New(mesh.Broker(bro), mesh.Locator(loc))
	requests := make(chan *mesh.Context, 2)
	game.Route(1, 1, mesh.Wrap(func(ctx *mesh.Context, _ *envelope.Header) {
		requests <- ctx.Copy()
	}))
	finished := make(chan error, 1)
	go func() {
		finished <- game.Start(meta.NewContext(context.Background(), testAppInfo{id: "game-1", name: "game"}))
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, game.Stop(ctx))
		require.NoError(t, awaitShutdownConnection(t, finished))
	})
	awaitShutdownConnection(t, game.Ready())

	frame, err := proto.Marshal(&envelope.ClientEnvelope{Service: "game", Header: &envelope.Header{Seq: 7, Cmd: 1, Version: 1}})
	require.NoError(t, err)
	old := srv.dial(t, 42, "old")
	first := awaitShutdownConnection(t, bro.events)
	require.NoError(t, old.WriteMessage(websocket.BinaryMessage, frame))
	oldRequest := awaitShutdownConnection(t, requests)

	current := srv.dial(t, 42, "current")
	second := awaitShutdownConnection(t, bro.events)
	require.NotEqual(t, first.id, second.id)
	requireOwnershipClosed(t, old)
	require.NoError(t, current.WriteMessage(websocket.BinaryMessage, frame))
	currentRequest := awaitShutdownConnection(t, requests)

	oldRequest.OkResp(&envelope.Header{Cmd: 99})
	oldRequest.ErrResp(409)
	currentRequest.OkResp()
	for _, sessionID := range []string{first.id, first.id, second.id} {
		reply := awaitShutdownConnection(t, bro.downstream)
		require.Equal(t, sessionID, cluster.GetSessionIDBy(reply.Header))
		srv.gate.handlePubSubMessage(reply)
	}
	push := &envelope.ServerEnvelope{Kind: envelope.MsgType_PUSH, Header: &envelope.Header{Cmd: 2}, Service: "game"}
	pushData, err := proto.Marshal(push)
	require.NoError(t, err)
	pushHeader := cluster.BuildHeader(42, cluster.EventBusiness, "", "game", "gate")
	pushHeader.Set(cluster.FieldNameDelivery, cluster.DeliveryPush)
	srv.gate.handlePubSubMessage(&broker.Message{Header: pushHeader, Data: pushData})

	require.NoError(t, current.SetReadDeadline(time.Now().Add(time.Second)))
	for _, want := range []*envelope.ServerEnvelope{
		{Kind: envelope.MsgType_RESPONSE, Header: &envelope.Header{Seq: 7, Cmd: 1, Version: 1}, Service: "game"},
		push,
	} {
		kind, data, readErr := current.ReadMessage()
		require.NoError(t, readErr)
		require.Equal(t, websocket.BinaryMessage, kind)
		got := &envelope.ServerEnvelope{}
		require.NoError(t, proto.Unmarshal(data, got))
		if got.GetKind() == envelope.MsgType_RESPONSE {
			got.Header.SentAtUnixMs = 0
		}
		require.True(t, proto.Equal(want, got), "want %v, got %v", want, got)
	}
}

func TestGateSessionDeliveryCompatibility(t *testing.T) {
	for _, mode := range []struct {
		name   string
		strict bool
	}{{"compatible", false}, {"strict", true}} {
		t.Run(mode.name, func(t *testing.T) {
			srv := newOwnershipServer(t, newOwnershipLocator(), "gate-1", StrictSessionDelivery(mode.strict))
			conn := srv.dial(t, 42, "current")
			session := awaitShutdownConnection(t, srv.events)
			for _, delivery := range []struct {
				label     string
				kind      string
				sessionID string
			}{
				{"legacy", "", ""},
				{"missing session", cluster.DeliveryResponse, ""},
				{"unmarked stale", "", "old"},
				{"stale response", cluster.DeliveryResponse, "old"},
				{"wrong push", cluster.DeliveryPush, session.id},
				{"unknown mode", "unknown", ""},
				{"current response", cluster.DeliveryResponse, session.id},
				{"push", cluster.DeliveryPush, ""},
			} {
				header := cluster.BuildHeader(42, cluster.EventBusiness, "", "game", "gate")
				if delivery.kind != "" {
					header.Set(cluster.FieldNameDelivery, delivery.kind)
				}
				if delivery.sessionID != "" {
					header.Set(cluster.FieldNameSessionID, delivery.sessionID)
				}
				srv.gate.handlePubSubMessage(&broker.Message{Header: header, Data: []byte(delivery.label)})
			}
			want := []string{"current response", "push"}
			if !mode.strict {
				want = append([]string{"legacy", "missing session"}, want...)
			}
			require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
			for _, label := range want {
				kind, data, err := conn.ReadMessage()
				require.NoError(t, err)
				require.Equal(t, websocket.BinaryMessage, kind)
				require.Equal(t, label, string(data))
			}
		})
	}
}

func TestGateOwnershipCrossNodeDisconnectDoesNotPublishOffline(t *testing.T) {
	loc := newOwnershipLocator()
	first := newOwnershipServer(t, loc, "gate-1")
	second := newOwnershipServer(t, loc, "gate-2")
	old := first.dial(t, 42, "old")
	oldEvent := awaitShutdownConnection(t, first.events)
	current := second.dial(t, 42, "current")
	newEvent := awaitShutdownConnection(t, second.events)
	require.Equal(t, cluster.EventReconnect, newEvent.event)
	require.NotEqual(t, oldEvent.id, newEvent.id)
	require.NoError(t, old.Close())
	require.Equal(t, "old", awaitShutdownConnection(t, first.finished))
	released := awaitShutdownConnection(t, loc.releases)
	require.False(t, released.released)
	require.Equal(t, oldEvent.id, released.id)
	require.Empty(t, first.events, "stale cross-node release must not announce offline")
	requireOwnershipCurrent(t, second, loc, 42, newEvent.id)
	requireOwnershipDelivery(t, second, current, 42)
}

func TestGateOwnershipSerializesOneUserWithoutBlockingOthers(t *testing.T) {
	loc := newOwnershipLocator()
	blocked := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(blocked) })
	loc.beforeClaim = func(_ context.Context, _ int64, call int) error {
		if call == 1 {
			<-blocked
		}
		return nil
	}
	s := newOwnershipServer(t, loc, "gate-1")
	t.Cleanup(unblock)
	old := s.dial(t, 42, "old")
	require.Equal(t, int64(42), awaitShutdownConnection(t, loc.claims).uid)
	current := s.dial(t, 42, "current")
	other := s.dial(t, 43, "other")
	require.Equal(t, int64(43), awaitShutdownConnection(t, loc.claims).uid)
	otherEvent := awaitShutdownConnection(t, s.events)
	require.Equal(t, int64(43), otherEvent.uid)
	requireOwnershipDelivery(t, s, other, 43)
	select {
	case claim := <-loc.claims:
		t.Fatalf("same-user claim entered before prior claim completed: %+v", claim)
	case <-time.After(30 * time.Millisecond):
	}
	unblock()
	first := awaitShutdownConnection(t, s.events)
	second := awaitShutdownConnection(t, s.events)
	require.Equal(t, int64(42), first.uid)
	require.Equal(t, cluster.EventOnline, first.event)
	require.Equal(t, ownershipEvent{42, second.id, cluster.EventReconnect}, second)
	require.NotEqual(t, first.id, second.id)
	requireOwnershipClosed(t, old)
	require.Equal(t, "old", awaitShutdownConnection(t, s.finished))
	requireOwnershipCurrent(t, s, loc, 42, second.id)
	requireOwnershipDelivery(t, s, current, 42)
}

func TestGateOwnershipQueuedConnectionDoesNotClaimAfterStop(t *testing.T) {
	loc := newOwnershipLocator()
	blocked := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(blocked) })
	loc.beforeClaim = func(_ context.Context, _ int64, call int) error {
		if call == 1 {
			<-blocked
		}
		return nil
	}
	s := newOwnershipServer(t, loc, "gate-1")
	t.Cleanup(unblock)
	first := s.dial(t, 42, "first")
	awaitShutdownConnection(t, loc.claims)
	queued := s.dial(t, 42, "queued")
	require.Eventually(t, func() bool { return s.gate.ws.Len() == 2 }, time.Second, time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- s.gate.Stop(ctx) }()
	require.Eventually(t, func() bool {
		s.gate.lifecycleMu.Lock()
		defer s.gate.lifecycleMu.Unlock()
		return s.gate.stopped
	}, time.Second, time.Millisecond)
	unblock()
	require.NoError(t, awaitShutdownConnection(t, stopped))
	requireOwnershipClosed(t, first)
	requireOwnershipClosed(t, queued)
	require.Empty(t, loc.claims, "queued connection must check stop before claiming")
	_, exists := s.gate.sessions.get(42)
	require.False(t, exists)
	state, _ := loc.Session(context.Background(), 42, "gate")
	require.Empty(t, state.Node)
}
