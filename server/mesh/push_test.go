package mesh

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/byteweap/meta"
	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/locator"
	"github.com/byteweap/meta/envelope"
	"github.com/byteweap/meta/internal/cluster"
)

type pushBroker struct {
	mockBroker
	pub func(context.Context, string, []byte, ...broker.PublishOption) error
}

func (b *pushBroker) Pub(ctx context.Context, subject string, data []byte, opts ...broker.PublishOption) error {
	return b.pub(ctx, subject, data, opts...)
}

func TestPushFollowsCurrentGate(t *testing.T) {
	var session locator.Session
	var frame envelope.ServerEnvelope
	var lookups, publishes int
	loc := sessionEventLocator{lookup: func(_ context.Context, uid int64, service string) (locator.Session, error) {
		lookups++
		require.Equal(t, int64(42), uid)
		require.Equal(t, "edge", service)
		return session, nil
	}}
	bro := &pushBroker{pub: func(_ context.Context, subject string, data []byte, opts ...broker.PublishOption) error {
		publishes++
		require.Equal(t, "test.game.edge."+session.Node, subject)
		var o broker.PublishOptions
		for _, opt := range opts {
			opt(&o)
		}
		require.Empty(t, o.Reply)
		require.Equal(t, int64(42), cluster.GetUIDBy(o.Header))
		require.Equal(t, "game", cluster.GetFromServiceBy(o.Header))
		require.Equal(t, "edge", cluster.GetToServiceBy(o.Header))
		require.Equal(t, cluster.DeliveryPush, o.Header.Get(cluster.FieldNameDelivery))
		require.NotContains(t, o.Header, cluster.FieldNameSessionID)
		require.NoError(t, proto.Unmarshal(data, &frame))
		require.Equal(t, envelope.MsgType_PUSH, frame.GetKind())
		require.Equal(t, "game", frame.GetService())
		require.Nil(t, frame.GetError())
		require.NotNil(t, frame.GetHeader())
		require.Zero(t, frame.Header.GetSeq())
		require.Equal(t, uint32(7), frame.Header.GetCmd())
		require.Equal(t, uint32(2), frame.Header.GetVersion())
		require.Positive(t, frame.Header.GetSentAtUnixMs())
		return nil
	}}
	m := New(Broker(bro), Locator(loc), Prefix("test"), GateService("edge"))
	require.Error(t, m.Push(t.Context(), 42, 7, 2, nil))
	require.Zero(t, lookups)
	runTestMesh(t, m)
	require.ErrorIs(t, m.Push(t.Context(), 42, 7, 2, wrapperspb.String("\xff")), ErrUserOffline)
	session = locator.Session{ID: "first", Node: "edge-1"}
	require.NoError(t, m.Push(t.Context(), 42, 7, 2, wrapperspb.String("notice")))
	var notice wrapperspb.StringValue
	require.NoError(t, proto.Unmarshal(frame.GetPayload(), &notice))
	require.Equal(t, "notice", notice.GetValue())
	session = locator.Session{ID: "new", Node: "edge-2"}
	require.NoError(t, m.Push(t.Context(), 42, 7, 2, nil))
	require.Empty(t, frame.GetPayload())
	session.Node = ""
	require.ErrorIs(t, m.Push(t.Context(), 42, 7, 2, nil), ErrUserOffline)
	require.Equal(t, 4, lookups)
	require.Equal(t, 2, publishes)
}

func TestPushFailures(t *testing.T) {
	lookupErr, pubErr := errors.New("lookup failed"), errors.New("publish failed")
	var lookups, publishes int
	var lookupFailure error
	node := "gate-1"
	m := New(Locator(sessionEventLocator{lookup: func(_ context.Context, _ int64, service string) (locator.Session, error) {
		lookups++
		require.Equal(t, "gate", service)
		return locator.Session{Node: node, ID: "current"}, lookupFailure
	}}), Broker(&pushBroker{pub: func(context.Context, string, []byte, ...broker.PublishOption) error {
		publishes++
		return pubErr
	}}))
	runTestMesh(t, m)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, m.Push(canceled, 42, 0, 0, nil), context.Canceled)
	require.Error(t, m.Push(t.Context(), 0, 0, 0, nil))
	require.Zero(t, lookups)
	lookupFailure = lookupErr
	require.ErrorIs(t, m.Push(t.Context(), 42, 0, 0, nil), lookupErr)
	lookupFailure = nil
	node = "*"
	require.Error(t, m.Push(t.Context(), 42, 0, 0, nil))
	node = "gate-1"
	require.Error(t, m.Push(t.Context(), 42, 0, 0, wrapperspb.String("\xff")))
	require.Zero(t, publishes)
	require.ErrorIs(t, m.Push(t.Context(), 42, 0, 0, nil), pubErr)
	require.Equal(t, 1, publishes)
	require.NoError(t, m.Stop(t.Context()))
	require.ErrorIs(t, m.Push(t.Context(), 42, 0, 0, nil), context.Canceled)
	require.Equal(t, 4, lookups)
}

func TestPushCancellationDuringLookup(t *testing.T) {
	for _, cause := range []string{"caller", "mesh"} {
		t.Run(cause, func(t *testing.T) {
			entered := make(chan struct{})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			loc := sessionEventLocator{lookup: func(ctx context.Context, _ int64, _ string) (locator.Session, error) {
				close(entered)
				<-ctx.Done()
				if cause == "caller" {
					return locator.Session{}, nil
				}
				return locator.Session{ID: "late", Node: "gate-1"}, nil
			}}
			m := New(Locator(loc), Broker(&pushBroker{pub: func(context.Context, string, []byte, ...broker.PublishOption) error {
				t.Error("published after canceled lookup")
				return nil
			}}))
			runTestMesh(t, m)
			result := make(chan error, 1)
			go func() { result <- m.Push(ctx, 42, 7, 1, nil) }()
			<-entered
			if cause == "caller" {
				cancel()
			} else {
				require.NoError(t, m.Stop(t.Context()))
			}
			select {
			case err := <-result:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("lookup was not canceled")
			}
		})
	}
}

func TestPushGateNameValidation(t *testing.T) {
	for _, name := range []string{"", "edge.*", "edge west"} {
		m := New(Broker(&mockBroker{}), Locator(lifecycleLocator{}), GateService(name))
		require.ErrorContains(t, m.Start(meta.NewContext(t.Context(), lifecycleAppInfo{})), "invalid Gate service name")
	}
}

// delayedPushDeadline 模拟期限已到但取消通知仍在等待调度的上下文
type delayedPushDeadline struct {
	context.Context
	deadline time.Time
}

func (c delayedPushDeadline) Deadline() (time.Time, bool) { return c.deadline, true }

func TestPushDeadlineBeforeCancellationNotification(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancel := context.WithCancel(t.Context())
		defer cancel()
		ctx := delayedPushDeadline{Context: parent, deadline: time.Now().Add(time.Second)}
		lookups := 0
		loc := sessionEventLocator{lookup: func(context.Context, int64, string) (locator.Session, error) {
			lookups++
			time.Sleep(time.Second)
			return locator.Session{ID: "current", Node: "gate-1"}, nil
		}}
		publishes := 0
		m := New(Locator(loc), Broker(&pushBroker{pub: func(context.Context, string, []byte, ...broker.PublishOption) error {
			publishes++
			return nil
		}}))
		runTestMesh(t, m)
		// 第一次定位耗尽预算，第二次进入 Push 时预算已经耗尽
		for range 2 {
			require.ErrorIs(t, m.Push(ctx, 42, 1, 1, nil), context.DeadlineExceeded)
			require.NoError(t, ctx.Err())
		}
		require.Equal(t, 1, lookups)
		require.Zero(t, publishes)
	})
}

func TestPushDeadlineDuringPublish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		loc := sessionEventLocator{lookup: func(context.Context, int64, string) (locator.Session, error) {
			return locator.Session{Node: "gate-1", ID: "session"}, nil
		}}
		calls := 0
		bro := &pushBroker{pub: func(ctx context.Context, _ string, _ []byte, _ ...broker.PublishOption) error {
			calls++
			<-ctx.Done()
			return ctx.Err()
		}}
		m := New(Broker(bro), Locator(loc))
		runTestMesh(t, m)
		require.ErrorIs(t, m.Push(ctx, 42, 1, 1, nil), context.DeadlineExceeded)
		require.Equal(t, 1, calls)
	})
}

func BenchmarkPush(b *testing.B) {
	loc := sessionEventLocator{lookup: func(context.Context, int64, string) (locator.Session, error) {
		return locator.Session{Node: "gate-1", ID: "session"}, nil
	}}
	m := New(Broker(&mockBroker{}), Locator(loc))
	runTestMesh(b, m)
	payload := wrapperspb.Bytes(make([]byte, 128))
	b.ReportAllocs()
	for b.Loop() {
		if err := m.Push(m.ctx, 42, 7, 1, payload); err != nil {
			b.Fatal(err)
		}
	}
}
