package mesh

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta/component/broker"
	"github.com/byteweap/meta/component/locator"
	"github.com/byteweap/meta/envelope"
	"github.com/byteweap/meta/internal/cluster"
)

type sessionEventLocator struct {
	locator.SessionLocator
	lookup func(context.Context, int64, string) (locator.Session, error)
}

func (l sessionEventLocator) Session(ctx context.Context, uid int64, service string) (locator.Session, error) {
	return l.lookup(ctx, uid, service)
}

func sessionEventHeader(uid int64, event cluster.Event, sessionID string) broker.Header {
	header := cluster.BuildHeader(uid, event, "", "gate", "game")
	header.Set(cluster.FieldNameSessionID, sessionID)
	return header
}

func TestMeshSessionEventsRequireCurrentOwnership(t *testing.T) {
	for _, event := range []cluster.Event{cluster.EventOnline, cluster.EventReconnect, cluster.EventOffline} {
		t.Run(string(event), func(t *testing.T) {
			node := "gate-1"
			if event == cluster.EventOffline {
				node = ""
			}
			for _, tc := range []struct {
				name       string
				state      locator.Session
				lookupErr  error
				editHeader func(broker.Header)
				wantCall   bool
				wantLookup bool
			}{
				{name: "current", state: locator.Session{ID: "current", Node: node}, wantCall: true, wantLookup: true},
				{name: "replaced on same node", state: locator.Session{ID: "new", Node: "gate-1"}, wantLookup: true},
				{name: "replaced on another node", state: locator.Session{ID: "new", Node: "gate-2"}, wantLookup: true},
				{name: "newer session already offline", state: locator.Session{ID: "new"}, wantLookup: true},
				{name: "missing state", wantLookup: true},
				{name: "lookup failed", state: locator.Session{ID: "current", Node: node}, lookupErr: errors.New("lookup failed"), wantLookup: true},
				{name: "missing session ID", editHeader: func(h broker.Header) { delete(h, cluster.FieldNameSessionID) }},
				{name: "missing service", editHeader: func(h broker.Header) { delete(h, cluster.FieldNameFromService) }},
				{name: "missing UID", editHeader: func(h broker.Header) { delete(h, cluster.FieldNameUID) }},
				{name: "invalid UID", editHeader: func(h broker.Header) { h.Set(cluster.FieldNameUID, "invalid") }},
				{name: "negative UID", editHeader: func(h broker.Header) { h.Set(cluster.FieldNameUID, "-1") }},
			} {
				t.Run(tc.name, func(t *testing.T) {
					lookups := 0
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					loc := sessionEventLocator{lookup: func(gotCtx context.Context, uid int64, service string) (locator.Session, error) {
						lookups++
						require.Same(t, ctx, gotCtx)
						require.Equal(t, int64(42), uid)
						require.Equal(t, "gate", service)
						return tc.state, tc.lookupErr
					}}
					m := New(Locator(loc))
					m.ctx = ctx
					var calls []cluster.Event
					m.OnlineHandler(func(uid int64) {
						require.Equal(t, int64(42), uid)
						calls = append(calls, cluster.EventOnline)
					})
					m.ReconnectHandler(func(uid int64) {
						require.Equal(t, int64(42), uid)
						calls = append(calls, cluster.EventReconnect)
					})
					m.OfflineHandler(func(uid int64) {
						require.Equal(t, int64(42), uid)
						calls = append(calls, cluster.EventOffline)
					})
					businessCalls := 0
					m.Route(1, 1, func(*Mesh, *broker.Message, *envelope.IMessage) { businessCalls++ })
					header := sessionEventHeader(42, event, "current")
					if tc.editHeader != nil {
						tc.editHeader(header)
					}
					m.handlerMessage(&broker.Message{Header: header, Data: mustBusinessMessage(t, 1, 1, "game", &envelope.Header{})})
					if tc.wantCall {
						require.Equal(t, []cluster.Event{event}, calls)
					} else {
						require.Empty(t, calls)
					}
					if tc.wantLookup {
						require.Equal(t, 1, lookups)
					} else {
						require.Zero(t, lookups)
					}
					require.Zero(t, businessCalls, "state events must not fall through to business routes")
				})
			}
		})
	}
}

func TestMeshSessionEventsIgnoreDelayedStateTransitions(t *testing.T) {
	current := locator.Session{ID: "first", Node: "gate-1"}
	m := New(Locator(sessionEventLocator{lookup: func(context.Context, int64, string) (locator.Session, error) {
		return current, nil
	}}))
	m.ctx = context.Background()
	var calls []cluster.Event
	m.OnlineHandler(func(int64) { calls = append(calls, cluster.EventOnline) })
	m.ReconnectHandler(func(int64) { calls = append(calls, cluster.EventReconnect) })
	m.OfflineHandler(func(int64) { calls = append(calls, cluster.EventOffline) })
	deliver := func(event cluster.Event, sessionID string) {
		t.Helper()
		m.handlerMessage(&broker.Message{Header: sessionEventHeader(42, event, sessionID)})
	}

	deliver(cluster.EventOnline, "first")
	deliver(cluster.EventOffline, "first") // still online: premature offline is ignored
	current.Node = ""
	deliver(cluster.EventOnline, "first") // same ID, but already offline
	deliver(cluster.EventReconnect, "first")
	deliver(cluster.EventOffline, "first")
	current = locator.Session{ID: "second", Node: "gate-2"}
	deliver(cluster.EventReconnect, "second")
	deliver(cluster.EventOffline, "first") // delayed after the next connection
	deliver(cluster.EventOnline, "first")
	current.Node = ""
	deliver(cluster.EventReconnect, "second")
	deliver(cluster.EventOffline, "first") // even while the latest session is offline
	deliver(cluster.EventOffline, "second")
	current = locator.Session{ID: "third", Node: "gate-2"}
	deliver(cluster.EventOnline, "third")
	deliver(cluster.EventOffline, "second")
	require.Equal(t, []cluster.Event{
		cluster.EventOnline, cluster.EventOffline, cluster.EventReconnect, cluster.EventOffline, cluster.EventOnline,
	}, calls)
}

func TestMeshSessionEventsWithoutCallbackSkipLookup(t *testing.T) {
	m := New(Locator(sessionEventLocator{lookup: func(context.Context, int64, string) (locator.Session, error) {
		t.Fatal("event with no callback must not query session ownership")
		return locator.Session{}, nil
	}}))
	for _, event := range []cluster.Event{cluster.EventOnline, cluster.EventReconnect, cluster.EventOffline} {
		m.handlerMessage(&broker.Message{Header: sessionEventHeader(42, event, "current")})
	}
}
