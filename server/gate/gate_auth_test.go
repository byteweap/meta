package gate

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/olahol/melody"
	"github.com/stretchr/testify/require"

	"github.com/byteweap/meta"
	"github.com/byteweap/meta/component/selector"
	es "github.com/byteweap/meta/errors"
)

func TestGateAuthRequiresExtractor(t *testing.T) {
	for name, opts := range map[string][]Option{
		"missing": nil,
		"nil":     {UserIDExtractor(nil)},
		"cleared": {UserIDExtractor(func(*http.Request) int64 { return 42 }), UserIDExtractor(nil)},
	} {
		t.Run(name, func(t *testing.T) {
			g := New(append([]Option{
				Locator(&testLocator{}),
				Broker(&testBroker{}),
				Discovery(&testRegistry{}),
				SelectorFunc(func() selector.Selector { return &testSelector{} }),
			}, opts...)...)
			require.ErrorIs(t, g.validate(), es.ErrIDExtractorRequired)
			ctx := meta.NewContext(context.Background(), testAppInfo{id: "gate-1", name: "gate"})
			require.ErrorIs(t, g.Start(ctx), es.ErrIDExtractorRequired)
			require.Nil(t, g.ln)
		})
	}
}

func TestGateAuthRejectsInvalidIdentityBeforeUpgrade(t *testing.T) {
	for name, uid := range map[string]int64{"zero": 0, "negative": -1} {
		t.Run(name, func(t *testing.T) {
			loc := &testLocator{nodes: map[string]string{"game": "game-1"}}
			bro := &testBroker{}
			var calls atomic.Int32
			g := New(
				Addr("127.0.0.1:0"), Path("/ws"), Locator(loc), Broker(bro),
				UserIDExtractor(func(*http.Request) int64 {
					calls.Add(1)
					return uid
				}),
			)
			existing := &melody.Session{}
			g.sessions.replace(42, existing)
			ts, _ := newGateAuthTestServer(t, g)

			conn, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http")+"/ws?uid=42", nil)
			if conn != nil {
				t.Cleanup(func() { _ = conn.Close() })
			}
			require.Error(t, err)
			require.NotNil(t, resp)
			t.Cleanup(func() { _ = resp.Body.Close() })
			require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
			require.Nil(t, conn)
			require.Equal(t, int32(1), calls.Load())
			current, ok := g.sessions.get(42)
			require.True(t, ok)
			require.Same(t, existing, current)
			_, ok = g.sessions.get(uid)
			require.False(t, ok)
			loc.mu.Lock()
			bindCalls, unbindCalls := loc.bindCalls, loc.unbindCalls
			loc.mu.Unlock()
			require.Zero(t, bindCalls)
			require.Zero(t, unbindCalls)
			bro.mu.Lock()
			publications := len(bro.pubSubjects)
			bro.mu.Unlock()
			require.Zero(t, publications)
		})
	}
}

type gateAuthLocator struct {
	*testLocator
	bound chan int64
}

func (l *gateAuthLocator) Claim(ctx context.Context, uid int64, service, node, sessionID, runID string) (bool, error) {
	replaced, err := l.testLocator.Claim(ctx, uid, service, node, sessionID, runID)
	l.bound <- uid
	return replaced, err
}

func TestGateAuthUsesVerifiedIdentityOnce(t *testing.T) {
	loc := &gateAuthLocator{testLocator: &testLocator{}, bound: make(chan int64, 1)}
	var calls atomic.Int32
	g := New(
		Addr("127.0.0.1:0"), Path("/ws"), Locator(loc), Broker(&testBroker{}),
		UserIDExtractor(func(r *http.Request) int64 {
			calls.Add(1)
			if r.Header.Get("Authorization") == "Bearer valid-token" {
				return 42
			}
			return 0
		}),
	)
	ts, finished := newGateAuthTestServer(t, g)
	conn, resp, err := websocket.DefaultDialer.Dial(
		"ws"+strings.TrimPrefix(ts.URL, "http")+"/ws?uid=99",
		http.Header{"Authorization": {"Bearer valid-token"}},
	)
	if resp != nil {
		require.NoError(t, resp.Body.Close())
	}
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("WebSocket handler did not finish after closing the connection")
		}
	})
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	select {
	case uid := <-loc.bound:
		require.Equal(t, int64(42), uid)
	case <-time.After(time.Second):
		t.Fatal("authenticated session was not bound")
	}
	require.Equal(t, int32(1), calls.Load())
	require.Eventually(t, func() bool {
		_, ok := g.sessions.get(42)
		return ok
	}, time.Second, time.Millisecond)
	session, ok := g.sessions.get(42)
	require.True(t, ok)
	uid, ok := session.Get("uid")
	require.True(t, ok)
	require.Equal(t, int64(42), uid)
	_, ok = g.sessions.get(99)
	require.False(t, ok)
}

func TestGateAuthSkipsUnknownPath(t *testing.T) {
	var calls atomic.Int32
	g := New(
		Addr("127.0.0.1:0"), Path("/ws"),
		UserIDExtractor(func(*http.Request) int64 {
			calls.Add(1)
			return 42
		}),
	)
	newGateAuthTestServer(t, g)
	response := httptest.NewRecorder()
	g.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/other?uid=42", nil))
	require.Equal(t, http.StatusNotFound, response.Code)
	require.Zero(t, calls.Load())
}

func newGateAuthTestServer(t *testing.T, g *Gate) (*httptest.Server, <-chan struct{}) {
	t.Helper()
	require.NoError(t, g.setup(context.Background(), "gate", "gate-1"))
	finished := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		g.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(func() {
		_ = g.ws.Close()
		_ = g.ln.Close()
		ts.Close()
	})
	return ts, finished
}
